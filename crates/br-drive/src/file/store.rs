use std::marker::PhantomData;

use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::persistence::{Aggregate, CohortIndex, Persistence, PersistenceStyle};
use service_engine::{BlobRef, Cohort};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::aggregate::{FileRow, PageOrigin, ProcessingState};
use crate::host::{DRIVE_DIM, DriveHost};
use crate::media::MediaType;
use crate::path::{DrivePath, FileName};

pub(crate) const FILE_COLUMNS: &str = "id, drive_id, path, name, protected, media_type, size_bytes, sha256, blob_ref, \
     processing_state, processing_error, metadata, summary, page_count, estimated_tokens, \
     ruleset_id, steps, step_index, step_count, step_runner_type, job_id, plan, \
     progress_index, progress_label, progress_at, triggered_by, done_at, completed_at, \
     created_by, created_at, updated_at";

/// The library is the only writer of these columns; a value that does not
/// decode is reported and read as absent rather than breaking the file's view.
fn decode_json<T: serde::de::DeserializeOwned>(
    what: &'static str,
    value: Option<serde_json::Value>,
) -> Option<T> {
    value.and_then(|value| match serde_json::from_value(value) {
        Ok(decoded) => Some(decoded),
        Err(error) => {
            tracing::warn!(%error, "{what} does not decode; read as absent");
            None
        }
    })
}

fn encode_json<T: serde::Serialize>(
    what: &'static str,
    value: Option<&T>,
) -> Result<Option<serde_json::Value>, EngineError> {
    value
        .map(serde_json::to_value)
        .transpose()
        .map_err(|source| EngineError::Encode { what, source })
}

pub(crate) fn config_error(error: impl std::fmt::Display) -> EngineError {
    EngineError::Config(error.to_string())
}

pub(crate) fn sha256(bytes: Vec<u8>) -> Result<[u8; 32], EngineError> {
    bytes
        .try_into()
        .map_err(|_| EngineError::Blob("a recorded sha256 is not 32 bytes".into()))
}

pub(crate) fn row_to_file<H>(row: &sqlx::postgres::PgRow) -> Result<FileRow<H>, EngineError> {
    row_to_file_prefixed(row, "")
}

pub(crate) fn row_to_file_prefixed<H>(
    row: &sqlx::postgres::PgRow,
    prefix: &str,
) -> Result<FileRow<H>, EngineError> {
    let column = |name: &str| format!("{prefix}{name}");
    let state: String = row.get(column("processing_state").as_str());
    let path: String = row.get(column("path").as_str());
    let name: String = row.get(column("name").as_str());
    let media_type: String = row.get(column("media_type").as_str());
    Ok(FileRow {
        id: row.get(column("id").as_str()),
        drive_id: row.get(column("drive_id").as_str()),
        path: DrivePath::parse(&path).map_err(config_error)?,
        name: FileName::parse(&name).map_err(config_error)?,
        protected: row.get(column("protected").as_str()),
        media_type: MediaType::parse(&media_type).map_err(config_error)?,
        size_bytes: row.get(column("size_bytes").as_str()),
        sha256: sha256(row.get(column("sha256").as_str()))?,
        blob_ref: row.get(column("blob_ref").as_str()),
        processing_state: ProcessingState::from_db_str(&state).map_err(config_error)?,
        processing_error: row.get(column("processing_error").as_str()),
        metadata: row.get(column("metadata").as_str()),
        summary: row.get(column("summary").as_str()),
        page_count: row.get(column("page_count").as_str()),
        estimated_tokens: row.get(column("estimated_tokens").as_str()),
        ruleset_id: row.get(column("ruleset_id").as_str()),
        steps: decode_json("a file's steps snapshot", row.get(column("steps").as_str())),
        step_index: row.get(column("step_index").as_str()),
        step_count: row.get(column("step_count").as_str()),
        step_runner_type: row.get(column("step_runner_type").as_str()),
        job_id: row.get(column("job_id").as_str()),
        plan: row.get(column("plan").as_str()),
        progress_index: row.get(column("progress_index").as_str()),
        progress_label: row.get(column("progress_label").as_str()),
        progress_at: row.get(column("progress_at").as_str()),
        triggered_by: decode_json(
            "a file's initiator",
            row.get(column("triggered_by").as_str()),
        ),
        done_at: row.get(column("done_at").as_str()),
        completed_at: row.get(column("completed_at").as_str()),
        created_by: row.get(column("created_by").as_str()),
        created_at: row.get(column("created_at").as_str()),
        updated_at: row.get(column("updated_at").as_str()),
        host: PhantomData,
    })
}

pub(crate) fn file_columns_as(alias: &str, prefix: &str) -> String {
    FILE_COLUMNS
        .split(", ")
        .map(|column| format!("{alias}.{column} AS {prefix}{column}"))
        .collect::<Vec<_>>()
        .join(", ")
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
            let row = sqlx::query(&format!(
                "SELECT {FILE_COLUMNS} FROM drive.file WHERE id = $1"
            ))
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
                "SELECT {FILE_COLUMNS} FROM drive.file WHERE id = ANY($1)"
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
                   processing_state = $6, processing_error = $7, metadata = $8, summary = $9, \
                   page_count = $10, estimated_tokens = $11, updated_at = $12, \
                   ruleset_id = $13, steps = $14, step_index = $15, step_count = $16, \
                   step_runner_type = $17, job_id = $18, plan = $19, progress_index = $20, \
                   progress_label = $21, progress_at = $22, triggered_by = $23, \
                   done_at = $24, completed_at = $25 \
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
            .bind(file.ruleset_id)
            .bind(encode_json("a file's steps snapshot", file.steps.as_ref())?)
            .bind(file.step_index)
            .bind(file.step_count)
            .bind(&file.step_runner_type)
            .bind(file.job_id)
            .bind(&file.plan)
            .bind(file.progress_index)
            .bind(&file.progress_label)
            .bind(file.progress_at)
            .bind(encode_json(
                "a file's initiator",
                file.triggered_by.as_ref(),
            )?)
            .bind(file.done_at)
            .bind(file.completed_at)
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
                "INSERT INTO drive.file ({FILE_COLUMNS}) VALUES \
                 ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, \
                  $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, \
                  $30, $31)"
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
            .bind(file.ruleset_id)
            .bind(encode_json("a file's steps snapshot", file.steps.as_ref())?)
            .bind(file.step_index)
            .bind(file.step_count)
            .bind(&file.step_runner_type)
            .bind(file.job_id)
            .bind(&file.plan)
            .bind(file.progress_index)
            .bind(&file.progress_label)
            .bind(file.progress_at)
            .bind(encode_json(
                "a file's initiator",
                file.triggered_by.as_ref(),
            )?)
            .bind(file.done_at)
            .bind(file.completed_at)
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

pub async fn image_refs_of_files(
    conn: &mut PgConnection,
    ids: &[Uuid],
) -> Result<Vec<Uuid>, EngineError> {
    let rows = sqlx::query(
        "SELECT blob_ref, pending_blob_ref FROM drive.file_image WHERE file_id = ANY($1)",
    )
    .bind(ids)
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

pub async fn ids_with_live_job(conn: &mut PgConnection) -> Result<Vec<Uuid>, EngineError> {
    let rows = sqlx::query("SELECT id FROM drive.file WHERE job_id IS NOT NULL")
        .fetch_all(conn)
        .await?;
    Ok(rows.iter().map(|row| row.get::<Uuid, _>("id")).collect())
}

pub async fn delete_pages(conn: &mut PgConnection, file_id: Uuid) -> Result<u64, EngineError> {
    let done = sqlx::query("DELETE FROM drive.file_page WHERE file_id = $1")
        .bind(file_id)
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

pub struct PageWrite<'a> {
    pub number: i32,
    pub markdown: &'a str,
    pub origin: PageOrigin,
}

pub async fn upsert_pages(
    conn: &mut PgConnection,
    file_id: Uuid,
    pages: &[PageWrite<'_>],
    by: Uuid,
    at: DateTime<Utc>,
) -> Result<(), EngineError> {
    if pages.is_empty() {
        return Ok(());
    }
    let numbers: Vec<i32> = pages.iter().map(|page| page.number).collect();
    let markdowns: Vec<&str> = pages.iter().map(|page| page.markdown).collect();
    let origins: Vec<&str> = pages.iter().map(|page| page.origin.as_str()).collect();
    sqlx::query(
        "INSERT INTO drive.file_page (file_id, number, markdown, origin, updated_by, updated_at) \
         SELECT $1, number, markdown, origin, $5, $6 \
         FROM unnest($2::int[], $3::text[], $4::text[]) AS batch(number, markdown, origin) \
         ON CONFLICT (file_id, number) DO UPDATE SET markdown = EXCLUDED.markdown, \
           origin = EXCLUDED.origin, updated_by = EXCLUDED.updated_by, \
           updated_at = EXCLUDED.updated_at",
    )
    .bind(file_id)
    .bind(&numbers)
    .bind(&markdowns)
    .bind(&origins)
    .bind(by)
    .bind(at)
    .execute(conn)
    .await?;
    Ok(())
}

pub async fn page_exists(
    conn: &mut PgConnection,
    file_id: Uuid,
    number: i32,
) -> Result<bool, EngineError> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM drive.file_page WHERE file_id = $1 AND number = $2)",
    )
    .bind(file_id)
    .bind(number)
    .fetch_one(conn)
    .await?;
    Ok(exists)
}

pub async fn page_numbers(conn: &mut PgConnection, file_id: Uuid) -> Result<Vec<i32>, EngineError> {
    let rows = sqlx::query("SELECT number FROM drive.file_page WHERE file_id = $1 ORDER BY number")
        .bind(file_id)
        .fetch_all(conn)
        .await?;
    Ok(rows.iter().map(|row| row.get::<i32, _>("number")).collect())
}
