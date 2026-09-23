use std::marker::PhantomData;

use br_core_integration::{Aggregate as BcAggregate, Bc, CommandCoords, Verb};
use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::BlobRef;
use service_engine::error::EngineError;
use service_engine::inbound::{ReactionCoordinates, ReactionMessage};
use service_engine::persistence::{Aggregate, Persistence, PersistenceStyle};
use service_engine::pipeline::{OutboundCommand, Reaction};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::aggregate::{File, FileCause};
use super::store::{config_error, sha256};
use crate::fault::DriveReactionFault;
use crate::host::DriveHost;
use crate::image::ImageName;
use crate::media::MediaType;

pub const IMAGE_LANDED_AGGREGATE: &str = "drive_image";
pub const IMAGE_LANDED_VERB: &str = "landed";
pub const IMAGE_LANDED_DURABLE: &str = "image-landed";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImageKey {
    pub file_id: Uuid,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobFacts {
    pub blob_ref: Uuid,
    pub media_type: MediaType,
    pub size_bytes: i64,
    pub sha256: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Landing {
    First,
    Swapped,
    Ignored,
}

pub struct ImageRecord<H> {
    pub file_id: Uuid,
    pub name: ImageName,
    pub page: i32,
    pub current: BlobFacts,
    pub landed_at: Option<DateTime<Utc>>,
    pub pending: Option<BlobFacts>,
    pub requested_at: DateTime<Utc>,
    pub(crate) host: PhantomData<fn() -> H>,
}

impl<H> Clone for ImageRecord<H> {
    fn clone(&self) -> Self {
        Self {
            file_id: self.file_id,
            name: self.name.clone(),
            page: self.page,
            current: self.current.clone(),
            landed_at: self.landed_at,
            pending: self.pending.clone(),
            requested_at: self.requested_at,
            host: PhantomData,
        }
    }
}

impl<H> ImageRecord<H> {
    pub fn new(file_id: Uuid, name: ImageName, facts: BlobFacts, at: DateTime<Utc>) -> Self {
        Self {
            file_id,
            page: name.page(),
            name,
            current: facts,
            landed_at: None,
            pending: None,
            requested_at: at,
            host: PhantomData,
        }
    }

    pub fn key(&self) -> ImageKey {
        ImageKey {
            file_id: self.file_id,
            name: self.name.as_str().to_string(),
        }
    }

    pub fn awaits_upload(&self) -> bool {
        self.pending.is_some() || self.landed_at.is_none()
    }

    pub fn is_landing(&self, now: DateTime<Utc>, window: std::time::Duration) -> bool {
        let window =
            chrono::Duration::from_std(window).unwrap_or_else(|_| chrono::Duration::zero());
        self.awaits_upload() && self.requested_at + window > now
    }

    pub fn request_replacement(&mut self, facts: BlobFacts, at: DateTime<Utc>) -> Vec<Uuid> {
        let mut released = Vec::new();
        if let Some(stale) = self.pending.take() {
            released.push(stale.blob_ref);
        }
        if self.landed_at.is_none() {
            released.push(std::mem::replace(&mut self.current, facts).blob_ref);
        } else {
            self.pending = Some(facts);
        }
        self.requested_at = at;
        released
    }

    pub fn land(&mut self, reference: Uuid, at: DateTime<Utc>) -> Landing {
        if self
            .pending
            .as_ref()
            .is_some_and(|facts| facts.blob_ref == reference)
        {
            self.current = self.pending.take().expect("checked just above");
            self.landed_at = Some(at);
            return Landing::Swapped;
        }
        if self.current.blob_ref == reference && self.landed_at.is_none() {
            self.landed_at = Some(at);
            return Landing::First;
        }
        Landing::Ignored
    }
}

pub struct ImageStore<H>(PhantomData<fn() -> H>);

const COLUMNS: &str = "file_id, name, page, blob_ref, media_type, size_bytes, sha256, landed_at, \
                       pending_blob_ref, pending_media_type, pending_size_bytes, pending_sha256, \
                       requested_at";

fn row_to_image<H>(row: &sqlx::postgres::PgRow) -> Result<ImageRecord<H>, EngineError> {
    let name: String = row.get("name");
    let media_type: String = row.get("media_type");
    let pending = match row.get::<Option<Uuid>, _>("pending_blob_ref") {
        Some(blob_ref) => {
            let media_type: String = row.get("pending_media_type");
            Some(BlobFacts {
                blob_ref,
                media_type: MediaType::parse(&media_type).map_err(config_error)?,
                size_bytes: row.get("pending_size_bytes"),
                sha256: sha256(row.get("pending_sha256"))?,
            })
        }
        None => None,
    };
    Ok(ImageRecord {
        file_id: row.get("file_id"),
        name: ImageName::parse(&name).map_err(config_error)?,
        page: row.get("page"),
        current: BlobFacts {
            blob_ref: row.get("blob_ref"),
            media_type: MediaType::parse(&media_type).map_err(config_error)?,
            size_bytes: row.get("size_bytes"),
            sha256: sha256(row.get("sha256"))?,
        },
        landed_at: row.get("landed_at"),
        pending,
        requested_at: row.get("requested_at"),
        host: PhantomData,
    })
}

type PgQuery<'q> = sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>;

fn bind_facts<'q, H>(query: PgQuery<'q>, image: &'q ImageRecord<H>) -> PgQuery<'q> {
    query
        .bind(image.page)
        .bind(image.current.blob_ref)
        .bind(image.current.media_type.as_str())
        .bind(image.current.size_bytes)
        .bind(image.current.sha256.to_vec())
        .bind(image.landed_at)
        .bind(image.pending.as_ref().map(|facts| facts.blob_ref))
        .bind(
            image
                .pending
                .as_ref()
                .map(|facts| facts.media_type.as_str()),
        )
        .bind(image.pending.as_ref().map(|facts| facts.size_bytes))
        .bind(image.pending.as_ref().map(|facts| facts.sha256.to_vec()))
        .bind(image.requested_at)
}

impl<H: DriveHost> Persistence for ImageStore<H> {
    type Aggregate = ImageRecord<H>;
    type Key = ImageKey;
    type Event = ();

    const STYLE: PersistenceStyle = PersistenceStyle::Crud;

    fn load<'a>(
        conn: &'a mut PgConnection,
        key: &'a ImageKey,
    ) -> BoxFuture<'a, Result<Option<ImageRecord<H>>, EngineError>> {
        Box::pin(async move {
            let row = sqlx::query(&format!(
                "SELECT {COLUMNS} FROM drive.file_image WHERE file_id = $1 AND name = $2"
            ))
            .bind(key.file_id)
            .bind(&key.name)
            .fetch_optional(conn)
            .await?;
            row.as_ref().map(row_to_image).transpose()
        })
    }

    fn lock<'a>(
        conn: &'a mut PgConnection,
        key: &'a ImageKey,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(
                "SELECT 1 FROM drive.file_image WHERE file_id = $1 AND name = $2 FOR UPDATE",
            )
            .bind(key.file_id)
            .bind(&key.name)
            .execute(conn)
            .await?;
            Ok(())
        })
    }

    fn save<'a>(
        conn: &'a mut PgConnection,
        image: &'a ImageRecord<H>,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            let query = sqlx::query(
                "UPDATE drive.file_image SET page = $3, blob_ref = $4, media_type = $5, \
                   size_bytes = $6, sha256 = $7, landed_at = $8, pending_blob_ref = $9, \
                   pending_media_type = $10, pending_size_bytes = $11, pending_sha256 = $12, \
                   requested_at = $13 \
                 WHERE file_id = $1 AND name = $2",
            )
            .bind(image.file_id)
            .bind(image.name.as_str());
            bind_facts(query, image).execute(conn).await?;
            Ok(())
        })
    }

    fn create<'a>(
        conn: &'a mut PgConnection,
        image: &'a ImageRecord<H>,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            let sql = format!(
                "INSERT INTO drive.file_image ({COLUMNS}) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)"
            );
            let query = sqlx::query(&sql)
                .bind(image.file_id)
                .bind(image.name.as_str());
            bind_facts(query, image).execute(conn).await?;
            Ok(())
        })
    }

    fn delete<'a>(
        conn: &'a mut PgConnection,
        key: &'a ImageKey,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query("DELETE FROM drive.file_image WHERE file_id = $1 AND name = $2")
                .bind(key.file_id)
                .bind(&key.name)
                .execute(conn)
                .await?;
            Ok(())
        })
    }
}

impl<H: DriveHost> Aggregate for ImageRecord<H> {
    type Store = ImageStore<H>;

    fn key(&self) -> ImageKey {
        ImageRecord::key(self)
    }

    fn blob_refs(&self) -> Vec<BlobRef> {
        std::iter::once(BlobRef(self.current.blob_ref))
            .chain(self.pending.iter().map(|facts| BlobRef(facts.blob_ref)))
            .collect()
    }
}

pub async fn image_names_of(
    conn: &mut PgConnection,
    file_id: Uuid,
) -> Result<Vec<String>, EngineError> {
    let rows = sqlx::query("SELECT name FROM drive.file_image WHERE file_id = $1 ORDER BY name")
        .bind(file_id)
        .fetch_all(conn)
        .await?;
    Ok(rows
        .iter()
        .map(|row| row.get::<String, _>("name"))
        .collect())
}

pub async fn image_names_of_page(
    conn: &mut PgConnection,
    file_id: Uuid,
    page: i32,
) -> Result<Vec<String>, EngineError> {
    let rows = sqlx::query(
        "SELECT name FROM drive.file_image WHERE file_id = $1 AND page = $2 ORDER BY name",
    )
    .bind(file_id)
    .bind(page)
    .fetch_all(conn)
    .await?;
    Ok(rows
        .iter()
        .map(|row| row.get::<String, _>("name"))
        .collect())
}

pub async fn drop_images(
    conn: &mut PgConnection,
    file_id: Uuid,
    names: &[String],
) -> Result<Vec<Uuid>, EngineError> {
    if names.is_empty() {
        return Ok(Vec::new());
    }
    let rows = sqlx::query(
        "DELETE FROM drive.file_image WHERE file_id = $1 AND name = ANY($2) \
         RETURNING blob_ref, pending_blob_ref",
    )
    .bind(file_id)
    .bind(names)
    .fetch_all(conn)
    .await?;
    Ok(rows
        .iter()
        .flat_map(|row| {
            [
                Some(row.get::<Uuid, _>("blob_ref")),
                row.get::<Option<Uuid>, _>("pending_blob_ref"),
            ]
        })
        .flatten()
        .collect())
}

pub async fn image_of_blob(
    conn: &mut PgConnection,
    reference: Uuid,
) -> Result<Option<ImageKey>, EngineError> {
    let row = sqlx::query(
        "SELECT file_id, name FROM drive.file_image WHERE blob_ref = $1 OR pending_blob_ref = $1",
    )
    .bind(reference)
    .fetch_optional(conn)
    .await?;
    Ok(row.map(|row| ImageKey {
        file_id: row.get("file_id"),
        name: row.get("name"),
    }))
}

fn boundary(byte: Option<u8>) -> bool {
    !byte.is_some_and(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
}

pub fn references_image(markdown: &str, name: &str) -> bool {
    if name.is_empty() {
        return false;
    }
    let bytes = markdown.as_bytes();
    markdown.match_indices(name).any(|(start, _)| {
        let before = start.checked_sub(1).map(|index| bytes[index]);
        let after = bytes.get(start + name.len()).copied();
        boundary(before) && boundary(after)
    })
}

#[derive(Serialize, Deserialize)]
pub struct ImageLanded<H> {
    pub file_id: Uuid,
    pub name: String,
    pub reference: Uuid,
    #[serde(skip)]
    host: PhantomData<fn() -> H>,
}

impl<H> ImageLanded<H> {
    pub fn new(key: ImageKey, reference: Uuid) -> Self {
        Self {
            file_id: key.file_id,
            name: key.name,
            reference,
            host: PhantomData,
        }
    }
}

fn landed_coords<H: DriveHost>() -> CommandCoords {
    CommandCoords {
        receiver: Bc::new(H::SERVICE).expect("the host service name is a valid bc"),
        aggregate: BcAggregate::new(IMAGE_LANDED_AGGREGATE).expect("a static aggregate segment"),
        verb: Verb::new(IMAGE_LANDED_VERB).expect("a static verb segment"),
        version: 1,
    }
}

impl<H: DriveHost> ReactionMessage for ImageLanded<H> {
    fn coordinates() -> ReactionCoordinates {
        ReactionCoordinates::Command(landed_coords::<H>())
    }

    fn decode(payload: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(payload)
    }
}

impl<H: DriveHost> OutboundCommand for ImageLanded<H> {
    fn coords(&self) -> CommandCoords {
        landed_coords::<H>()
    }

    fn command_id(&self) -> Uuid {
        Uuid::now_v7()
    }
}

pub fn image_landed<'r, H: DriveHost>(
    cx: &'r mut Reaction<'r>,
    message: ImageLanded<H>,
) -> BoxFuture<'r, Result<(), DriveReactionFault>> {
    Box::pin(async move {
        let key = ImageKey {
            file_id: message.file_id,
            name: message.name,
        };
        let Some(mut image) = cx.load::<ImageRecord<H>>(&key).await? else {
            return Ok(());
        };
        let previous = image.current.blob_ref;
        let now = cx.now().as_datetime();
        match image.land(message.reference, now) {
            Landing::Ignored => return Ok(()),
            Landing::Swapped => cx.release_blob(BlobRef(previous))?,
            Landing::First => {}
        }
        cx.save(&image).await?;
        cx.impact_caused::<File, _>(
            &image.file_id,
            FileCause::ImageAvailable {
                name: image.name.as_str().to_string(),
            },
        )?;
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn facts(reference: Uuid) -> BlobFacts {
        BlobFacts {
            blob_ref: reference,
            media_type: MediaType::parse("image/png").unwrap(),
            size_bytes: 7,
            sha256: [0; 32],
        }
    }

    fn image(at: DateTime<Utc>) -> (ImageRecord<()>, Uuid) {
        let first = Uuid::now_v7();
        let image = ImageRecord::<()>::new(
            Uuid::now_v7(),
            ImageName::parse("p001-img01.png").unwrap(),
            facts(first),
            at,
        );
        (image, first)
    }

    #[test]
    fn a_replacement_keeps_the_landed_blob_current_until_the_new_one_lands() {
        let t0 = Utc::now();
        let (mut image, first) = image(t0);
        assert_eq!(image.land(first, t0), Landing::First);
        let second = Uuid::now_v7();
        assert!(image.request_replacement(facts(second), t0).is_empty());
        assert_eq!(image.current.blob_ref, first);
        assert!(image.is_landing(t0, std::time::Duration::from_secs(60)));
        assert_eq!(image.land(Uuid::now_v7(), t0), Landing::Ignored);
        assert_eq!(image.land(second, t0), Landing::Swapped);
        assert_eq!(image.current.blob_ref, second);
        assert!(image.pending.is_none());
        assert_eq!(
            image.land(second, t0),
            Landing::Ignored,
            "a replayed landing is absorbed"
        );
    }

    #[test]
    fn a_request_past_the_window_replaces_a_blob_that_never_landed_and_releases_it() {
        let t0 = Utc::now();
        let (mut image, first) = image(t0);
        let later = t0 + chrono::Duration::seconds(120);
        assert!(!image.is_landing(later, std::time::Duration::from_secs(60)));
        let second = Uuid::now_v7();
        assert_eq!(image.request_replacement(facts(second), later), vec![first]);
        assert_eq!(image.current.blob_ref, second);
        assert!(image.landed_at.is_none());
    }

    #[test]
    fn an_expired_pending_replacement_is_released_by_the_next_request() {
        let t0 = Utc::now();
        let (mut image, first) = image(t0);
        image.land(first, t0);
        let second = Uuid::now_v7();
        image.request_replacement(facts(second), t0);
        let third = Uuid::now_v7();
        let later = t0 + chrono::Duration::seconds(120);
        assert_eq!(image.request_replacement(facts(third), later), vec![second]);
        assert_eq!(
            image.current.blob_ref, first,
            "the landed blob is untouched"
        );
        assert_eq!(image.pending.as_ref().unwrap().blob_ref, third);
    }

    #[test]
    fn an_image_reference_is_matched_on_a_word_boundary_not_a_substring() {
        let name = "p001-img01.png";
        for markdown in [
            "![a](p001-img01.png)",
            "![a](images/p001-img01.png \"title\")",
            "<img src=\"p001-img01.png\">",
            "see p001-img01.png here",
            "p001-img01.png",
        ] {
            assert!(references_image(markdown, name), "{markdown}");
        }
        for markdown in [
            "![a](p001-img01.pngx)",
            "![a](xp001-img01.png)",
            "![a](p001-img01.png.bak)",
            "![a](p001-img011.png)",
            "![a](p001-img01.jpg)",
            "",
        ] {
            assert!(!references_image(markdown, name), "{markdown}");
        }
    }
}
