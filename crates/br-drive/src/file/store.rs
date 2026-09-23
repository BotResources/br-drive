use std::collections::HashMap;
use std::marker::PhantomData;

use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::persistence::{Aggregate, CohortIndex, Persistence, PersistenceStyle};
use service_engine::{BlobRef, Cohort};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::aggregate::{Changes, FileRow, ImageRow, PageOrigin, PageRow, ProcessingState};
use crate::host::{DRIVE_DIM, DriveHost};
use crate::image::ImageName;
use crate::media::MediaType;
use crate::path::{DrivePath, FileName};

const COLUMNS: &str = "id, drive_id, path, name, protected, media_type, size_bytes, sha256, \
                       blob_ref, processing_state, processing_error, metadata, summary, \
                       page_count, estimated_tokens, created_by, created_at, updated_at";

fn config_error(error: impl std::fmt::Display) -> EngineError {
    EngineError::Config(error.to_string())
}

fn sha256(bytes: Vec<u8>) -> Result<[u8; 32], EngineError> {
    bytes
        .try_into()
        .map_err(|_| EngineError::Blob("a recorded sha256 is not 32 bytes".into()))
}

fn row_to_file<H>(row: &sqlx::postgres::PgRow) -> Result<FileRow<H>, EngineError> {
    let state: String = row.get("processing_state");
    let path: String = row.get("path");
    let name: String = row.get("name");
    let media_type: String = row.get("media_type");
    Ok(FileRow {
        id: row.get("id"),
        drive_id: row.get("drive_id"),
        path: DrivePath::parse(&path).map_err(config_error)?,
        name: FileName::parse(&name).map_err(config_error)?,
        protected: row.get("protected"),
        media_type: MediaType::parse(&media_type).map_err(config_error)?,
        size_bytes: row.get("size_bytes"),
        sha256: sha256(row.get("sha256"))?,
        blob_ref: row.get("blob_ref"),
        processing_state: ProcessingState::from_db_str(&state).map_err(config_error)?,
        processing_error: row.get("processing_error"),
        metadata: row.get("metadata"),
        summary: row.get("summary"),
        page_count: row.get("page_count"),
        estimated_tokens: row.get("estimated_tokens"),
        created_by: row.get("created_by"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        pages: Vec::new(),
        images: Vec::new(),
        changes: Changes::default(),
        host: PhantomData,
    })
}

fn row_to_page(row: &sqlx::postgres::PgRow) -> Result<(Uuid, PageRow), EngineError> {
    let origin: String = row.get("origin");
    Ok((
        row.get("file_id"),
        PageRow {
            number: row.get("number"),
            markdown: row.get("markdown"),
            origin: PageOrigin::from_db_str(&origin).map_err(config_error)?,
            updated_by: row.get("updated_by"),
            updated_at: row.get("updated_at"),
        },
    ))
}

fn row_to_image(row: &sqlx::postgres::PgRow) -> Result<(Uuid, ImageRow), EngineError> {
    let name: String = row.get("name");
    let media_type: String = row.get("media_type");
    Ok((
        row.get("file_id"),
        ImageRow {
            name: ImageName::parse(&name).map_err(config_error)?,
            blob_ref: row.get("blob_ref"),
            media_type: MediaType::parse(&media_type).map_err(config_error)?,
            size_bytes: row.get("size_bytes"),
            sha256: sha256(row.get("sha256"))?,
            page: row.get("page"),
        },
    ))
}

async fn load_files<H>(
    conn: &mut PgConnection,
    keys: &[Uuid],
) -> Result<Vec<FileRow<H>>, EngineError> {
    let rows = sqlx::query(&format!(
        "SELECT {COLUMNS} FROM drive.file WHERE id = ANY($1)"
    ))
    .bind(keys)
    .fetch_all(&mut *conn)
    .await?;
    let mut files: Vec<FileRow<H>> = rows.iter().map(row_to_file).collect::<Result<_, _>>()?;
    if files.is_empty() {
        return Ok(files);
    }
    let pages = sqlx::query(
        "SELECT file_id, number, markdown, origin, updated_by, updated_at \
         FROM drive.file_page WHERE file_id = ANY($1) ORDER BY file_id, number",
    )
    .bind(keys)
    .fetch_all(&mut *conn)
    .await?;
    let images = sqlx::query(
        "SELECT file_id, name, blob_ref, media_type, size_bytes, sha256, page \
         FROM drive.file_image WHERE file_id = ANY($1) ORDER BY file_id, name",
    )
    .bind(keys)
    .fetch_all(&mut *conn)
    .await?;
    let mut by_id: HashMap<Uuid, usize> = files
        .iter()
        .enumerate()
        .map(|(index, file)| (file.id, index))
        .collect();
    for row in &pages {
        let (file_id, page) = row_to_page(row)?;
        if let Some(index) = by_id.get_mut(&file_id) {
            files[*index].pages.push(page);
        }
    }
    for row in &images {
        let (file_id, image) = row_to_image(row)?;
        if let Some(index) = by_id.get_mut(&file_id) {
            files[*index].images.push(image);
        }
    }
    Ok(files)
}

async fn apply_changes<H>(conn: &mut PgConnection, file: &FileRow<H>) -> Result<(), EngineError> {
    if !file.changes.pages.is_empty() {
        let pages: Vec<&PageRow> = file
            .pages
            .iter()
            .filter(|page| file.changes.pages.contains(&page.number))
            .collect();
        let numbers: Vec<i32> = pages.iter().map(|page| page.number).collect();
        let markdowns: Vec<&str> = pages.iter().map(|page| page.markdown.as_str()).collect();
        let origins: Vec<&str> = pages.iter().map(|page| page.origin.as_str()).collect();
        let by: Vec<Uuid> = pages.iter().map(|page| page.updated_by).collect();
        let at: Vec<DateTime<Utc>> = pages.iter().map(|page| page.updated_at).collect();
        sqlx::query(
            "INSERT INTO drive.file_page (file_id, number, markdown, origin, updated_by, updated_at) \
             SELECT $1, * FROM unnest($2::int[], $3::text[], $4::text[], $5::uuid[], $6::timestamptz[]) \
             ON CONFLICT (file_id, number) DO UPDATE SET markdown = EXCLUDED.markdown, \
               origin = EXCLUDED.origin, updated_by = EXCLUDED.updated_by, \
               updated_at = EXCLUDED.updated_at",
        )
        .bind(file.id)
        .bind(&numbers)
        .bind(&markdowns)
        .bind(&origins)
        .bind(&by)
        .bind(&at)
        .execute(&mut *conn)
        .await?;
    }
    if !file.changes.dropped_images.is_empty() {
        let names: Vec<&str> = file
            .changes
            .dropped_images
            .iter()
            .map(String::as_str)
            .collect();
        sqlx::query("DELETE FROM drive.file_image WHERE file_id = $1 AND name = ANY($2)")
            .bind(file.id)
            .bind(&names)
            .execute(&mut *conn)
            .await?;
    }
    if !file.changes.images.is_empty() {
        let images: Vec<&ImageRow> = file
            .images
            .iter()
            .filter(|image| file.changes.images.contains(image.name.as_str()))
            .collect();
        let names: Vec<&str> = images.iter().map(|image| image.name.as_str()).collect();
        let refs: Vec<Uuid> = images.iter().map(|image| image.blob_ref).collect();
        let types: Vec<&str> = images
            .iter()
            .map(|image| image.media_type.as_str())
            .collect();
        let sizes: Vec<i64> = images.iter().map(|image| image.size_bytes).collect();
        let shas: Vec<Vec<u8>> = images.iter().map(|image| image.sha256.to_vec()).collect();
        let pages: Vec<Option<i32>> = images.iter().map(|image| image.page).collect();
        sqlx::query(
            "INSERT INTO drive.file_image (file_id, name, blob_ref, media_type, size_bytes, sha256, page) \
             SELECT $1, * FROM unnest($2::text[], $3::uuid[], $4::text[], $5::bigint[], $6::bytea[], $7::int[]) \
             ON CONFLICT (file_id, name) DO UPDATE SET blob_ref = EXCLUDED.blob_ref, \
               media_type = EXCLUDED.media_type, size_bytes = EXCLUDED.size_bytes, \
               sha256 = EXCLUDED.sha256, page = EXCLUDED.page",
        )
        .bind(file.id)
        .bind(&names)
        .bind(&refs)
        .bind(&types)
        .bind(&sizes)
        .bind(&shas)
        .bind(&pages)
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

pub struct FileStore<H>(PhantomData<fn() -> H>);

impl<H: DriveHost> Persistence for FileStore<H> {
    type Aggregate = FileRow<H>;
    type Key = Uuid;
    type Event = ();

    const STYLE: PersistenceStyle = PersistenceStyle::Crud;

    fn load<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<Option<FileRow<H>>, EngineError>> {
        Box::pin(async move { Ok(load_files(conn, std::slice::from_ref(key)).await?.pop()) })
    }

    fn lock<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Self::row_lock(conn, "drive.file", key)
    }

    fn read_many<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [Uuid],
    ) -> BoxFuture<'a, Result<Vec<(Uuid, FileRow<H>)>, EngineError>> {
        Box::pin(async move {
            Ok(load_files(conn, keys)
                .await?
                .into_iter()
                .map(|file| (file.id, file))
                .collect())
        })
    }

    fn save<'a>(
        conn: &'a mut PgConnection,
        file: &'a FileRow<H>,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(
                "UPDATE drive.file SET drive_id = $2, path = $3, name = $4, protected = $5, \
                   processing_state = $6, processing_error = $7, metadata = $8, summary = $9, \
                   page_count = $10, estimated_tokens = $11, updated_at = $12 \
                 WHERE id = $1",
            )
            .bind(file.id)
            .bind(file.drive_id)
            .bind(file.path.as_str())
            .bind(file.name.as_str())
            .bind(file.protected)
            .bind(file.processing_state.as_str())
            .bind(&file.processing_error)
            .bind(&file.metadata)
            .bind(&file.summary)
            .bind(file.page_count)
            .bind(file.estimated_tokens)
            .bind(file.updated_at)
            .execute(&mut *conn)
            .await?;
            apply_changes(conn, file).await
        })
    }

    fn create<'a>(
        conn: &'a mut PgConnection,
        file: &'a FileRow<H>,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(&format!(
                "INSERT INTO drive.file ({COLUMNS}) VALUES \
                 ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)"
            ))
            .bind(file.id)
            .bind(file.drive_id)
            .bind(file.path.as_str())
            .bind(file.name.as_str())
            .bind(file.protected)
            .bind(file.media_type.as_str())
            .bind(file.size_bytes)
            .bind(file.sha256.to_vec())
            .bind(file.blob_ref)
            .bind(file.processing_state.as_str())
            .bind(&file.processing_error)
            .bind(&file.metadata)
            .bind(&file.summary)
            .bind(file.page_count)
            .bind(file.estimated_tokens)
            .bind(file.created_by)
            .bind(file.created_at)
            .bind(file.updated_at)
            .execute(&mut *conn)
            .await?;
            apply_changes(conn, file).await
        })
    }

    fn delete<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query("DELETE FROM drive.file WHERE id = $1")
                .bind(key)
                .execute(conn)
                .await?;
            Ok(())
        })
    }
}

impl<H: DriveHost> CohortIndex for FileStore<H> {
    fn keys_in_cohorts<'a>(
        conn: &'a mut PgConnection,
        cohorts: &'a [Cohort],
    ) -> BoxFuture<'a, Result<Vec<Uuid>, EngineError>> {
        Box::pin(async move {
            let drives = Cohort::uuids(cohorts, DRIVE_DIM);
            ids_in_drives(conn, &drives).await
        })
    }
}

impl<H: DriveHost> Aggregate for FileRow<H> {
    type Store = FileStore<H>;

    fn key(&self) -> Uuid {
        self.id
    }

    fn blob_refs(&self) -> Vec<BlobRef> {
        std::iter::once(BlobRef(self.blob_ref))
            .chain(self.images.iter().map(|image| BlobRef(image.blob_ref)))
            .collect()
    }
}

pub async fn delete_many(conn: &mut PgConnection, ids: &[Uuid]) -> Result<u64, EngineError> {
    let done = sqlx::query("DELETE FROM drive.file WHERE id = ANY($1)")
        .bind(ids)
        .execute(conn)
        .await?;
    Ok(done.rows_affected())
}

pub async fn all_ids(conn: &mut PgConnection) -> Result<Vec<Uuid>, EngineError> {
    let rows = sqlx::query("SELECT id FROM drive.file ORDER BY id")
        .fetch_all(conn)
        .await?;
    Ok(rows.iter().map(|row| row.get::<Uuid, _>("id")).collect())
}

pub async fn ids_in_drives(
    conn: &mut PgConnection,
    drives: &[Uuid],
) -> Result<Vec<Uuid>, EngineError> {
    let rows = sqlx::query("SELECT id FROM drive.file WHERE drive_id = ANY($1) ORDER BY id")
        .bind(drives)
        .fetch_all(conn)
        .await?;
    Ok(rows.iter().map(|row| row.get::<Uuid, _>("id")).collect())
}

pub async fn ids_in_drive(conn: &mut PgConnection, drive: Uuid) -> Result<Vec<Uuid>, EngineError> {
    ids_in_drives(conn, &[drive]).await
}

pub async fn drive_of(conn: &mut PgConnection, file: Uuid) -> Result<Option<Uuid>, EngineError> {
    let row = sqlx::query("SELECT drive_id FROM drive.file WHERE id = $1")
        .bind(file)
        .fetch_optional(conn)
        .await?;
    Ok(row.map(|row| row.get::<Uuid, _>("drive_id")))
}

pub async fn file_of_image(
    conn: &mut PgConnection,
    blob_ref: Uuid,
) -> Result<Option<(Uuid, String)>, EngineError> {
    let row = sqlx::query("SELECT file_id, name FROM drive.file_image WHERE blob_ref = $1")
        .bind(blob_ref)
        .fetch_optional(conn)
        .await?;
    Ok(row.map(|row| (row.get("file_id"), row.get("name"))))
}

pub async fn sibling_names(
    conn: &mut PgConnection,
    drive: Uuid,
    path: &DrivePath,
) -> Result<Vec<String>, EngineError> {
    let rows = sqlx::query("SELECT name FROM drive.file WHERE drive_id = $1 AND path = $2")
        .bind(drive)
        .bind(path.as_str())
        .fetch_all(conn)
        .await?;
    Ok(rows
        .iter()
        .map(|row| row.get::<String, _>("name"))
        .collect())
}

pub async fn ids_under_prefix(
    conn: &mut PgConnection,
    drive: Uuid,
    prefix: &DrivePath,
) -> Result<Vec<Uuid>, EngineError> {
    let rows = sqlx::query(
        "SELECT id FROM drive.file \
         WHERE drive_id = $1 AND ($2 = '' OR path = $2 OR left(path, length($2) + 1) = $2 || '/') \
         ORDER BY id",
    )
    .bind(drive)
    .bind(prefix.as_str())
    .fetch_all(conn)
    .await?;
    Ok(rows.iter().map(|row| row.get::<Uuid, _>("id")).collect())
}

pub async fn entries_under_prefix(
    conn: &mut PgConnection,
    drive: Uuid,
    prefix: &DrivePath,
) -> Result<Vec<(Uuid, String, String)>, EngineError> {
    let rows = sqlx::query(
        "SELECT id, path, name FROM drive.file \
         WHERE drive_id = $1 AND ($2 = '' OR path = $2 OR left(path, length($2) + 1) = $2 || '/')",
    )
    .bind(drive)
    .bind(prefix.as_str())
    .fetch_all(conn)
    .await?;
    Ok(rows
        .iter()
        .map(|row| (row.get("id"), row.get("path"), row.get("name")))
        .collect())
}

pub async fn rebase_paths(
    conn: &mut PgConnection,
    ids: &[Uuid],
    from: &DrivePath,
    to: &DrivePath,
    now: DateTime<Utc>,
) -> Result<u64, EngineError> {
    let done = sqlx::query(
        "UPDATE drive.file \
         SET path = trim(both '/' from $3 || '/' || substr(path, length($2) + 2)), updated_at = $4 \
         WHERE id = ANY($1)",
    )
    .bind(ids)
    .bind(from.as_str())
    .bind(to.as_str())
    .bind(now)
    .execute(conn)
    .await?;
    Ok(done.rows_affected())
}
