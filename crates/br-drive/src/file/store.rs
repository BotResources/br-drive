use std::marker::PhantomData;

use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::persistence::{Aggregate, CohortIndex, Persistence, PersistenceStyle};
use service_engine::{BlobRef, Cohort};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::aggregate::{FileRow, ProcessingState};
use crate::host::{DRIVE_DIM, DriveHost};
use crate::media::MediaType;
use crate::path::{DrivePath, FileName};

const COLUMNS: &str = "id, drive_id, path, name, protected, media_type, size_bytes, sha256, \
                       blob_ref, processing_state, processing_error, metadata, created_by, \
                       created_at, updated_at";

fn row_to_file<H>(row: &sqlx::postgres::PgRow) -> Result<FileRow<H>, EngineError> {
    let sha: Vec<u8> = row.get("sha256");
    let sha256: [u8; 32] = sha
        .try_into()
        .map_err(|_| EngineError::Blob("a recorded file sha256 is not 32 bytes".into()))?;
    let state: String = row.get("processing_state");
    let processing_state = ProcessingState::from_db_str(&state)
        .map_err(|error| EngineError::Config(error.to_string()))?;
    let path: String = row.get("path");
    let name: String = row.get("name");
    let media_type: String = row.get("media_type");
    Ok(FileRow {
        id: row.get("id"),
        drive_id: row.get("drive_id"),
        path: DrivePath::parse(&path).map_err(|error| EngineError::Config(error.to_string()))?,
        name: FileName::parse(&name).map_err(|error| EngineError::Config(error.to_string()))?,
        protected: row.get("protected"),
        media_type: MediaType::parse(&media_type)
            .map_err(|error| EngineError::Config(error.to_string()))?,
        size_bytes: row.get("size_bytes"),
        sha256,
        blob_ref: row.get("blob_ref"),
        processing_state,
        processing_error: row.get("processing_error"),
        metadata: row.get("metadata"),
        created_by: row.get("created_by"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        host: PhantomData,
    })
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
        Box::pin(async move {
            let row = sqlx::query(&format!("SELECT {COLUMNS} FROM drive.file WHERE id = $1"))
                .bind(key)
                .fetch_optional(conn)
                .await?;
            row.as_ref().map(row_to_file).transpose()
        })
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
            let rows = sqlx::query(&format!(
                "SELECT {COLUMNS} FROM drive.file WHERE id = ANY($1)"
            ))
            .bind(keys)
            .fetch_all(conn)
            .await?;
            rows.iter()
                .map(|row| row_to_file(row).map(|file| (file.id, file)))
                .collect()
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
                   processing_state = $6, processing_error = $7, metadata = $8, updated_at = $9 \
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
            .bind(file.updated_at)
            .execute(conn)
            .await?;
            Ok(())
        })
    }

    fn create<'a>(
        conn: &'a mut PgConnection,
        file: &'a FileRow<H>,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(&format!(
                "INSERT INTO drive.file ({COLUMNS}) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)"
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
            .bind(file.created_by)
            .bind(file.created_at)
            .bind(file.updated_at)
            .execute(conn)
            .await?;
            Ok(())
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
        vec![BlobRef(self.blob_ref)]
    }
}

pub async fn delete_many(conn: &mut PgConnection, ids: &[Uuid]) -> Result<u64, EngineError> {
    let done = sqlx::query("DELETE FROM drive.file WHERE id = ANY($1)")
        .bind(ids)
        .execute(conn)
        .await?;
    Ok(done.rows_affected())
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
    now: chrono::DateTime<chrono::Utc>,
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
