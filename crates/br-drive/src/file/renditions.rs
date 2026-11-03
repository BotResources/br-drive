//! A file's renditions (`drive.file_rendition`): binary files a runner
//! derived from the source, each an object of the host's storage, replaced
//! by name only when the new object lands, gone with the file.

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

use super::aggregate::{File, FileCause, FileEvent, FileRow};
use super::images::{BlobFacts, Landing};
use super::store::{config_error, sha256};
use crate::facts::{self, FactMeta};
use crate::fault::DriveReactionFault;
use crate::host::DriveHost;
use crate::media::MediaType;
use crate::rendition::{RenditionKind, RenditionName};

pub const RENDITION_LANDED_AGGREGATE: &str = "drive_rendition";
pub const RENDITION_LANDED_VERB: &str = "landed";
pub const RENDITION_LANDED_DURABLE: &str = "rendition-landed";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RenditionKey {
    pub file_id: Uuid,
    pub name: String,
}

/// One object of a rendition: what it is, and the blob holding it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenditionBlob {
    pub kind: RenditionKind,
    pub blob: BlobFacts,
}

pub struct RenditionRecord<H> {
    pub file_id: Uuid,
    pub name: RenditionName,
    /// The landed object (or, before the first landing, the one awaited).
    pub current: RenditionBlob,
    pub landed_at: Option<DateTime<Utc>>,
    /// A replacement in flight: it becomes current only when it lands.
    pub pending: Option<RenditionBlob>,
    pub requested_at: DateTime<Utc>,
    pub(crate) host: PhantomData<fn() -> H>,
}

impl<H> Clone for RenditionRecord<H> {
    fn clone(&self) -> Self {
        Self {
            file_id: self.file_id,
            name: self.name.clone(),
            current: self.current.clone(),
            landed_at: self.landed_at,
            pending: self.pending.clone(),
            requested_at: self.requested_at,
            host: PhantomData,
        }
    }
}

impl<H> RenditionRecord<H> {
    pub fn new(file_id: Uuid, name: RenditionName, blob: RenditionBlob, at: DateTime<Utc>) -> Self {
        Self {
            file_id,
            name,
            current: blob,
            landed_at: None,
            pending: None,
            requested_at: at,
            host: PhantomData,
        }
    }

    pub fn key(&self) -> RenditionKey {
        RenditionKey {
            file_id: self.file_id,
            name: self.name.as_str().to_string(),
        }
    }

    pub fn awaits_upload(&self) -> bool {
        self.pending.is_some() || self.landed_at.is_none()
    }

    /// Whether an upload of this name is still in flight inside `window`.
    pub fn is_landing(&self, now: DateTime<Utc>, window: std::time::Duration) -> bool {
        let window =
            chrono::Duration::from_std(window).unwrap_or_else(|_| chrono::Duration::zero());
        self.awaits_upload() && self.requested_at + window > now
    }

    /// Stages `blob` for this name; answers the blob references it drops: an
    /// abandoned pending upload, or a first object that never landed. A
    /// landed object stays current until `blob` lands.
    ///
    /// As for an image, a first upload that never lands keeps its row (never
    /// listed, never readable) until a re-request replaces it or its file
    /// goes; the engine reaper drops its blob past `RENDITION_ORPHAN_AFTER`.
    /// Releasing such a reference afterwards is harmless: the engine's
    /// release is an `UPDATE … WHERE id = $1`, which matches no row once the
    /// reaper deleted it.
    pub fn request_replacement(&mut self, blob: RenditionBlob, at: DateTime<Utc>) -> Vec<Uuid> {
        let mut released = Vec::new();
        if let Some(stale) = self.pending.take() {
            released.push(stale.blob.blob_ref);
        }
        if self.landed_at.is_none() {
            released.push(std::mem::replace(&mut self.current, blob).blob.blob_ref);
        } else {
            self.pending = Some(blob);
        }
        self.requested_at = at;
        released
    }

    pub fn land(&mut self, reference: Uuid, at: DateTime<Utc>) -> Landing {
        if let Some(pending) = self
            .pending
            .take_if(|pending| pending.blob.blob_ref == reference)
        {
            self.current = pending;
            self.landed_at = Some(at);
            return Landing::Swapped;
        }
        if self.current.blob.blob_ref == reference && self.landed_at.is_none() {
            self.landed_at = Some(at);
            return Landing::First;
        }
        Landing::Ignored
    }
}

pub struct RenditionStore<H>(PhantomData<fn() -> H>);

const COLUMNS: &str = "file_id, name, kind, blob_ref, media_type, size_bytes, sha256, landed_at, \
                       pending_kind, pending_blob_ref, pending_media_type, pending_size_bytes, \
                       pending_sha256, requested_at";

fn row_to_rendition<H>(row: &sqlx::postgres::PgRow) -> Result<RenditionRecord<H>, EngineError> {
    let name: String = row.get("name");
    let kind: String = row.get("kind");
    let media_type: String = row.get("media_type");
    let pending = match row.get::<Option<Uuid>, _>("pending_blob_ref") {
        Some(blob_ref) => {
            let kind: String = row.get("pending_kind");
            let media_type: String = row.get("pending_media_type");
            Some(RenditionBlob {
                kind: RenditionKind::parse(&kind).map_err(config_error)?,
                blob: BlobFacts {
                    blob_ref,
                    media_type: MediaType::parse(&media_type).map_err(config_error)?,
                    size_bytes: row.get("pending_size_bytes"),
                    sha256: sha256(row.get("pending_sha256"))?,
                },
            })
        }
        None => None,
    };
    Ok(RenditionRecord {
        file_id: row.get("file_id"),
        name: RenditionName::parse(&name).map_err(config_error)?,
        current: RenditionBlob {
            kind: RenditionKind::parse(&kind).map_err(config_error)?,
            blob: BlobFacts {
                blob_ref: row.get("blob_ref"),
                media_type: MediaType::parse(&media_type).map_err(config_error)?,
                size_bytes: row.get("size_bytes"),
                sha256: sha256(row.get("sha256"))?,
            },
        },
        landed_at: row.get("landed_at"),
        pending,
        requested_at: row.get("requested_at"),
        host: PhantomData,
    })
}

type PgQuery<'q> = sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>;

fn bind_facts<'q, H>(query: PgQuery<'q>, rendition: &'q RenditionRecord<H>) -> PgQuery<'q> {
    let current = &rendition.current;
    let pending = rendition.pending.as_ref();
    query
        .bind(current.kind.as_str())
        .bind(current.blob.blob_ref)
        .bind(current.blob.media_type.as_str())
        .bind(current.blob.size_bytes)
        .bind(current.blob.sha256.to_vec())
        .bind(rendition.landed_at)
        .bind(pending.map(|pending| pending.kind.as_str()))
        .bind(pending.map(|pending| pending.blob.blob_ref))
        .bind(pending.map(|pending| pending.blob.media_type.as_str()))
        .bind(pending.map(|pending| pending.blob.size_bytes))
        .bind(pending.map(|pending| pending.blob.sha256.to_vec()))
        .bind(rendition.requested_at)
}

impl<H: DriveHost> Persistence for RenditionStore<H> {
    type Aggregate = RenditionRecord<H>;
    type Key = RenditionKey;
    type Event = ();

    const STYLE: PersistenceStyle = PersistenceStyle::Crud;

    fn lock<'a>(
        conn: &'a mut PgConnection,
        key: &'a RenditionKey,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(
                "SELECT 1 FROM drive.file_rendition WHERE file_id = $1 AND name = $2 FOR UPDATE",
            )
            .bind(key.file_id)
            .bind(&key.name)
            .execute(conn)
            .await?;
            Ok(())
        })
    }

    fn read_many<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [RenditionKey],
    ) -> BoxFuture<'a, Result<Vec<(RenditionKey, RenditionRecord<H>)>, EngineError>> {
        Box::pin(async move {
            let file_ids: Vec<Uuid> = keys.iter().map(|key| key.file_id).collect();
            let names: Vec<&str> = keys.iter().map(|key| key.name.as_str()).collect();
            let rows = sqlx::query(&format!(
                "SELECT {COLUMNS} FROM drive.file_rendition \
                 WHERE (file_id, name) IN (SELECT * FROM UNNEST($1::uuid[], $2::text[]))"
            ))
            .bind(&file_ids)
            .bind(&names)
            .fetch_all(conn)
            .await?;
            rows.iter()
                .map(|row| row_to_rendition(row).map(|rendition| (rendition.key(), rendition)))
                .collect()
        })
    }

    fn save<'a>(
        conn: &'a mut PgConnection,
        rendition: &'a RenditionRecord<H>,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            let query = sqlx::query(
                "UPDATE drive.file_rendition SET kind = $3, blob_ref = $4, media_type = $5, \
                   size_bytes = $6, sha256 = $7, landed_at = $8, pending_kind = $9, \
                   pending_blob_ref = $10, pending_media_type = $11, pending_size_bytes = $12, \
                   pending_sha256 = $13, requested_at = $14 \
                 WHERE file_id = $1 AND name = $2",
            )
            .bind(rendition.file_id)
            .bind(rendition.name.as_str());
            bind_facts(query, rendition).execute(conn).await?;
            Ok(())
        })
    }

    fn create<'a>(
        conn: &'a mut PgConnection,
        rendition: &'a RenditionRecord<H>,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            let sql = format!(
                "INSERT INTO drive.file_rendition ({COLUMNS}) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)"
            );
            let query = sqlx::query(&sql)
                .bind(rendition.file_id)
                .bind(rendition.name.as_str());
            bind_facts(query, rendition).execute(conn).await?;
            Ok(())
        })
    }

    fn delete<'a>(
        conn: &'a mut PgConnection,
        key: &'a RenditionKey,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query("DELETE FROM drive.file_rendition WHERE file_id = $1 AND name = $2")
                .bind(key.file_id)
                .bind(&key.name)
                .execute(conn)
                .await?;
            Ok(())
        })
    }
}

impl<H: DriveHost> Aggregate for RenditionRecord<H> {
    type Store = RenditionStore<H>;

    fn key(&self) -> RenditionKey {
        RenditionRecord::key(self)
    }

    fn blob_refs(&self) -> Vec<BlobRef> {
        std::iter::once(BlobRef(self.current.blob.blob_ref))
            .chain(
                self.pending
                    .iter()
                    .map(|pending| BlobRef(pending.blob.blob_ref)),
            )
            .collect()
    }
}

/// A landed rendition, as the views and the runner read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LandedRendition {
    pub file_id: Uuid,
    pub name: String,
    pub kind: String,
    pub media_type: String,
    pub size_bytes: i64,
    pub landed_at: DateTime<Utc>,
    pub blob_ref: Uuid,
}

/// The landed renditions of `files`, by file then name: the current object
/// of each name that has one. A first upload still in flight is not listed.
pub async fn landed_renditions_of(
    conn: &mut PgConnection,
    files: &[Uuid],
) -> Result<Vec<LandedRendition>, EngineError> {
    let rows = sqlx::query(
        "SELECT file_id, name, kind, media_type, size_bytes, landed_at, blob_ref \
         FROM drive.file_rendition WHERE file_id = ANY($1) AND landed_at IS NOT NULL \
         ORDER BY file_id, name",
    )
    .bind(files)
    .fetch_all(conn)
    .await?;
    Ok(rows
        .iter()
        .map(|row| LandedRendition {
            file_id: row.get("file_id"),
            name: row.get("name"),
            kind: row.get("kind"),
            media_type: row.get("media_type"),
            size_bytes: row.get("size_bytes"),
            landed_at: row.get("landed_at"),
            blob_ref: row.get("blob_ref"),
        })
        .collect())
}

pub async fn rendition_of_blob(
    conn: &mut PgConnection,
    reference: Uuid,
) -> Result<Option<RenditionKey>, EngineError> {
    let row = sqlx::query(
        "SELECT file_id, name FROM drive.file_rendition \
         WHERE blob_ref = $1 OR pending_blob_ref = $1",
    )
    .bind(reference)
    .fetch_optional(conn)
    .await?;
    Ok(row.map(|row| RenditionKey {
        file_id: row.get("file_id"),
        name: row.get("name"),
    }))
}

#[derive(Serialize, Deserialize)]
pub struct RenditionLanded<H> {
    pub file_id: Uuid,
    pub name: String,
    pub reference: Uuid,
    #[serde(skip)]
    host: PhantomData<fn() -> H>,
}

impl<H> RenditionLanded<H> {
    pub fn new(key: RenditionKey, reference: Uuid) -> Self {
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
        aggregate: BcAggregate::new(RENDITION_LANDED_AGGREGATE)
            .expect("a static aggregate segment"),
        verb: Verb::new(RENDITION_LANDED_VERB).expect("a static verb segment"),
        version: 1,
    }
}

impl<H: DriveHost> ReactionMessage for RenditionLanded<H> {
    fn coordinates() -> ReactionCoordinates {
        ReactionCoordinates::Command(landed_coords::<H>())
    }

    fn decode(payload: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(payload)
    }
}

impl<H: DriveHost> OutboundCommand for RenditionLanded<H> {
    fn coords(&self) -> CommandCoords {
        landed_coords::<H>()
    }

    fn command_id(&self) -> Uuid {
        Uuid::now_v7()
    }
}

/// A rendition's object landed: a first object becomes readable, a
/// replacement swaps in and the object it replaces is released, in this
/// transaction. A landing the row no longer awaits (a replayed message, an
/// upload superseded or gone with its file) changes nothing.
pub fn rendition_landed<'r, H: DriveHost>(
    cx: &'r mut Reaction<'r>,
    message: RenditionLanded<H>,
) -> BoxFuture<'r, Result<(), DriveReactionFault>> {
    Box::pin(async move {
        let key = RenditionKey {
            file_id: message.file_id,
            name: message.name,
        };
        // The file first, as every writer of its renditions locks it: a
        // runner's request on the same name waits, or is waited for.
        let Some(mut file) = cx.load::<FileRow<H>>(&key.file_id).await? else {
            return Ok(());
        };
        let Some(mut rendition) = cx.load::<RenditionRecord<H>>(&key).await? else {
            return Ok(());
        };
        let previous = rendition.current.blob.blob_ref;
        let now = cx.now().as_datetime();
        let landing = rendition.land(message.reference, now);
        match landing {
            Landing::Ignored => return Ok(()),
            Landing::Swapped => cx.release_blob(BlobRef(previous))?,
            Landing::First => {}
        }
        cx.save(&rendition).await?;
        let name = rendition.name.as_str().to_string();
        let kind = rendition.current.kind.as_str().to_string();
        let event = if landing == Landing::Swapped {
            FileEvent::RenditionReplaced {
                name: name.clone(),
                rendition_kind: kind,
            }
        } else {
            FileEvent::RenditionAvailable {
                name: name.clone(),
                rendition_kind: kind,
            }
        };
        let meta = FactMeta::of_reaction(cx);
        file.record(event, &meta);
        facts::save(cx, &mut file).await?;
        crate::owner::touch::<H>(cx, file.drive_id)?;
        cx.impact_caused::<File, _>(&rendition.file_id, FileCause::RenditionAvailable { name })?;
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blob(reference: Uuid, kind: &str) -> RenditionBlob {
        RenditionBlob {
            kind: RenditionKind::parse(kind).unwrap(),
            blob: BlobFacts {
                blob_ref: reference,
                media_type: MediaType::parse("application/octet-stream").unwrap(),
                size_bytes: 7,
                sha256: [0; 32],
            },
        }
    }

    fn rendition(at: DateTime<Utc>) -> (RenditionRecord<()>, Uuid) {
        let first = Uuid::now_v7();
        let rendition = RenditionRecord::<()>::new(
            Uuid::now_v7(),
            RenditionName::parse("base.duckdb").unwrap(),
            blob(first, "duckdb"),
            at,
        );
        (rendition, first)
    }

    #[test]
    fn a_replacement_keeps_the_landed_object_and_its_kind_current_until_the_new_one_lands() {
        let t0 = Utc::now();
        let (mut rendition, first) = rendition(t0);
        assert_eq!(rendition.land(first, t0), Landing::First);
        let second = Uuid::now_v7();
        assert!(
            rendition
                .request_replacement(blob(second, "parquet"), t0)
                .is_empty()
        );
        assert_eq!(rendition.current.blob.blob_ref, first);
        assert_eq!(rendition.current.kind.as_str(), "duckdb");
        assert!(rendition.is_landing(t0, std::time::Duration::from_secs(60)));
        assert_eq!(rendition.land(Uuid::now_v7(), t0), Landing::Ignored);
        assert_eq!(rendition.land(second, t0), Landing::Swapped);
        assert_eq!(rendition.current.blob.blob_ref, second);
        assert_eq!(rendition.current.kind.as_str(), "parquet");
        assert!(rendition.pending.is_none());
        assert_eq!(
            rendition.land(second, t0),
            Landing::Ignored,
            "a replayed landing is absorbed"
        );
    }

    #[test]
    fn a_request_past_the_window_replaces_an_object_that_never_landed_and_releases_it() {
        let t0 = Utc::now();
        let (mut rendition, first) = rendition(t0);
        let later = t0 + chrono::Duration::seconds(120);
        assert!(!rendition.is_landing(later, std::time::Duration::from_secs(60)));
        let second = Uuid::now_v7();
        assert_eq!(
            rendition.request_replacement(blob(second, "duckdb"), later),
            vec![first]
        );
        assert_eq!(rendition.current.blob.blob_ref, second);
        assert!(rendition.landed_at.is_none());
    }

    #[test]
    fn an_expired_pending_replacement_is_released_by_the_next_request() {
        let t0 = Utc::now();
        let (mut rendition, first) = rendition(t0);
        rendition.land(first, t0);
        let second = Uuid::now_v7();
        rendition.request_replacement(blob(second, "duckdb"), t0);
        let third = Uuid::now_v7();
        let later = t0 + chrono::Duration::seconds(120);
        assert_eq!(
            rendition.request_replacement(blob(third, "duckdb"), later),
            vec![second]
        );
        assert_eq!(
            rendition.current.blob.blob_ref, first,
            "the landed object is untouched"
        );
        assert_eq!(rendition.pending.as_ref().unwrap().blob.blob_ref, third);
    }
}
