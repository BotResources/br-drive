use std::marker::PhantomData;

use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::persistence::{Aggregate, CohortIndex, Persistence, PersistenceStyle};
use service_engine::{BlobRef, Cohort};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::aggregate::{FileEvent, FileRow, FileStatus, PageOrigin, ProcessingState};
use super::pages::PageEvent;
use crate::facts::{self, FactMeta, Pending, SoftEda, Stamped};
use crate::host::{DRIVE_DIM, DriveHost};
use crate::media::MediaType;
use crate::path::{DrivePath, FileName};
use crate::processing::{FileJob, Initiator, RunningFacts};
use crate::ruleset::Trigger;
use crate::title::FileTitle;

/// The columns of `drive.file` itself, in insert order.
pub(crate) const FILE_COLUMNS: &str = "id, drive_id, path, name, title, media_type, size_bytes, sha256, blob_ref, \
     committed_at, metadata, ruleset_id, steps, created_by, created_at, version, updated_at, upload_ruleset_id";

/// The columns of `drive.file_processed` a file row reads beside its own: what
/// the workers produced (absent until the first result).
const RESULT_COLUMNS: &str = "summary, page_count, estimated_tokens";

/// The file table joined to its processing and to its results, aliased `f`,
/// `s` and `r`.
pub(crate) const FILE_FROM: &str = "drive.file f \
     LEFT JOIN drive.file_processing s ON s.file_id = f.id \
     LEFT JOIN drive.file_processed r ON r.file_id = f.id";

/// The select list of a file row read through `FILE_FROM`, every column
/// named `{prefix}{column}`.
pub(crate) fn file_select(prefix: &str) -> String {
    let aliased = |alias: &'static str, columns: &'static str| {
        columns.split(", ").map(move |column| {
            format!(
                "{alias}.{column} AS {prefix}{column}",
                column = column.trim()
            )
        })
    };
    let processing = [
        ("state", "processing_state"),
        ("error_code", "processing_error"),
        ("job_id", "last_job_id"),
        ("step_index", "last_job_step"),
        ("trigger", "last_job_trigger"),
        ("triggered_by_id", "last_job_triggered_by_id"),
        ("triggered_by_name", "last_job_triggered_by_name"),
        ("job_created_at", "last_job_created_at"),
        ("plan", "last_job_plan"),
        ("plan_index", "last_job_plan_index"),
        ("plan_label", "last_job_plan_label"),
        ("plan_at", "last_job_plan_at"),
        ("cancel_requested_at", "last_job_cancel_requested_at"),
    ]
    .into_iter()
    .map(|(column, name)| format!("s.{column} AS {prefix}{name}"));
    aliased("f", FILE_COLUMNS)
        .chain(aliased("r", RESULT_COLUMNS))
        .chain(processing)
        // The file's last change is the latest of its own row's and of its
        // processing's.
        .chain(std::iter::once(format!(
            "GREATEST(f.updated_at, s.updated_at) AS {prefix}last_change"
        )))
        .collect::<Vec<_>>()
        .join(", ")
}

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
    let state: Option<String> = row.get(column("processing_state").as_str());
    let committed_at: Option<DateTime<Utc>> = row.get(column("committed_at").as_str());
    let path: String = row.get(column("path").as_str());
    let name: String = row.get(column("name").as_str());
    let title: String = row.get(column("title").as_str());
    let media_type: String = row.get(column("media_type").as_str());
    let state = match state.as_deref() {
        Some(state) => ProcessingState::from_db_str(state).map_err(config_error)?,
        None if committed_at.is_some() => ProcessingState::Ready,
        None => ProcessingState::Pending,
    };
    let last_job_id: Option<Uuid> = row.get(column("last_job_id").as_str());
    let last_job = match last_job_id {
        Some(job_id) => {
            let facts = if state == ProcessingState::Processing {
                let plan_at: Option<DateTime<Utc>> = row.get(column("last_job_plan_at").as_str());
                RunningFacts {
                    plan: row.get(column("last_job_plan").as_str()),
                    step: plan_at.map(|at| {
                        (
                            row.get::<Option<i32>, _>(column("last_job_plan_index").as_str())
                                .unwrap_or_default(),
                            row.get::<Option<String>, _>(column("last_job_plan_label").as_str())
                                .unwrap_or_default(),
                            at,
                        )
                    }),
                    cancel_requested_at: row.get(column("last_job_cancel_requested_at").as_str()),
                }
            } else {
                RunningFacts::default()
            };
            let triggered_by_id: Option<Uuid> =
                row.get(column("last_job_triggered_by_id").as_str());
            let trigger: Option<String> = row.get(column("last_job_trigger").as_str());
            Some(FileJob {
                job_id,
                step_index: row
                    .get::<Option<i32>, _>(column("last_job_step").as_str())
                    .unwrap_or_default(),
                trigger: trigger
                    .as_deref()
                    .map(Trigger::from_db_str)
                    .transpose()
                    .map_err(config_error)?,
                triggered_by: triggered_by_id.map(|id| Initiator {
                    id,
                    display_name: row.get(column("last_job_triggered_by_name").as_str()),
                }),
                events: facts.entries(),
                created_at: row
                    .get::<Option<DateTime<Utc>>, _>(column("last_job_created_at").as_str())
                    .unwrap_or_default(),
            })
        }
        None => None,
    };
    Ok(FileRow {
        id: row.get(column("id").as_str()),
        drive_id: row.get(column("drive_id").as_str()),
        path: DrivePath::parse(&path).map_err(config_error)?,
        name: FileName::parse(&name).map_err(config_error)?,
        title: FileTitle::parse(&title).map_err(config_error)?,
        media_type: MediaType::parse(&media_type).map_err(config_error)?,
        size_bytes: row.get(column("size_bytes").as_str()),
        sha256: sha256(row.get(column("sha256").as_str()))?,
        blob_ref: row.get(column("blob_ref").as_str()),
        committed_at,
        metadata: row.get(column("metadata").as_str()),
        summary: row.get(column("summary").as_str()),
        page_count: row.get(column("page_count").as_str()),
        estimated_tokens: row.get(column("estimated_tokens").as_str()),
        ruleset_id: row.get(column("ruleset_id").as_str()),
        steps: decode_json("a file's steps snapshot", row.get(column("steps").as_str())),
        upload_ruleset_id: row.get(column("upload_ruleset_id").as_str()),
        created_by: row.get(column("created_by").as_str()),
        created_at: row.get(column("created_at").as_str()),
        updated_at: row.get(column("last_change").as_str()),
        file_updated_at: row.get(column("updated_at").as_str()),
        version: row.get(column("version").as_str()),
        pending: Pending::default(),
        status: FileStatus {
            state,
            error: row.get(column("processing_error").as_str()),
            last_job,
        },
        host: PhantomData,
    })
}

pub struct FileStore<H>(PhantomData<fn() -> H>);

impl<H: DriveHost> Persistence for FileStore<H> {
    type Aggregate = FileRow<H>;
    type Key = Uuid;
    type Event = Stamped<FileEvent>;

    const STYLE: PersistenceStyle = PersistenceStyle::SoftEda;

    fn load<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<Option<FileRow<H>>, EngineError>> {
        Box::pin(async move {
            let row = sqlx::query(&format!(
                "SELECT {} FROM {FILE_FROM} WHERE f.id = $1",
                file_select("")
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
                "SELECT {} FROM {FILE_FROM} WHERE f.id = ANY($1)",
                file_select("")
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
        events: &'a [Stamped<FileEvent>],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(
                "UPDATE drive.file SET drive_id = $2, path = $3, name = $4, committed_at = $5, \
                   metadata = $6, ruleset_id = $7, steps = $8, title = $9, version = $10, \
                   updated_at = $11 \
                 WHERE id = $1",
            )
            .bind(file.id)
            .bind(file.drive_id)
            .bind(file.path.as_str())
            .bind(file.name.as_str())
            .bind(file.committed_at)
            .bind(&file.metadata)
            .bind(file.ruleset_id)
            .bind(encode_json("a file's steps snapshot", file.steps.as_ref())?)
            .bind(file.title.as_str())
            .bind(file.version)
            .bind(file.file_updated_at)
            .execute(&mut *conn)
            .await?;
            hand_file_facts::<H>(conn, file, events).await
        })
    }

    fn create<'a>(
        conn: &'a mut PgConnection,
        file: &'a FileRow<H>,
        events: &'a [Stamped<FileEvent>],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(&format!(
                "INSERT INTO drive.file ({FILE_COLUMNS}) VALUES \
                 ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)"
            ))
            .bind(file.id)
            .bind(file.drive_id)
            .bind(file.path.as_str())
            .bind(file.name.as_str())
            .bind(file.title.as_str())
            .bind(file.media_type.as_str())
            .bind(file.size_bytes)
            .bind(file.sha256.to_vec())
            .bind(file.blob_ref)
            .bind(file.committed_at)
            .bind(&file.metadata)
            .bind(file.ruleset_id)
            .bind(encode_json("a file's steps snapshot", file.steps.as_ref())?)
            .bind(file.created_by)
            .bind(file.created_at)
            .bind(file.version)
            .bind(file.file_updated_at)
            .bind(file.upload_ruleset_id)
            .execute(&mut *conn)
            .await?;
            hand_file_facts::<H>(conn, file, events).await
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

async fn hand_file_facts<H: DriveHost>(
    conn: &mut PgConnection,
    file: &FileRow<H>,
    events: &[Stamped<FileEvent>],
) -> Result<(), EngineError> {
    let facts = facts::facts_of(&facts::uuid_key(file.id), file.base_version(), events)?;
    facts::hand::<H>(conn, &facts).await
}

impl<H: DriveHost> Aggregate for FileRow<H> {
    type Store = FileStore<H>;

    fn key(&self) -> Uuid {
        self.id
    }

    fn pending_events(&self) -> &[Stamped<FileEvent>] {
        self.pending.as_slice()
    }

    fn blob_refs(&self) -> Vec<BlobRef> {
        vec![BlobRef(self.blob_ref)]
    }
}

impl<H: DriveHost> SoftEda for FileRow<H> {
    fn clear_pending(&mut self) {
        self.pending.clear();
    }
}

/// Hands the last fact of `file`, about to be deleted by the caller: `event`,
/// the file's next version. The file is loaded, so locked, with no pending
/// event.
pub(crate) async fn hand_gone<H: DriveHost>(
    conn: &mut PgConnection,
    meta: &FactMeta,
    file: &FileRow<H>,
    event: FileEvent,
) -> Result<(), EngineError> {
    let fact = facts::fact_at(facts::uuid_key(file.id), file.version + 1, &event, meta)?;
    facts::hand::<H>(conn, &[fact]).await
}

/// Deletes the files `ids`, each one's last fact `event` handed to the host at
/// its next version. `meta` is `None` only for the erase pipeline, whose
/// deletions are no gesture of the library's (erasure is out of the facts).
pub(crate) async fn delete_many<H: DriveHost>(
    conn: &mut PgConnection,
    ids: &[Uuid],
    gone: Option<(&FactMeta, FileEvent)>,
) -> Result<u64, EngineError> {
    let rows = sqlx::query("DELETE FROM drive.file WHERE id = ANY($1) RETURNING id, version")
        .bind(ids)
        .fetch_all(&mut *conn)
        .await?;
    if let Some((meta, event)) = gone {
        let facts = rows
            .iter()
            .map(|row| {
                let id: Uuid = row.get("id");
                let version: i64 = row.get("version");
                facts::fact_at(facts::uuid_key(id), version + 1, &event, meta)
            })
            .collect::<Result<Vec<_>, _>>()?;
        facts::hand::<H>(conn, &facts).await?;
    }
    Ok(u64::try_from(rows.len()).unwrap_or(u64::MAX))
}

/// Records `event` on each of the files `ids` without loading them: each
/// row's version and last change move in SQL, and one fact per row is handed
/// to the host.
pub(crate) async fn record_on_files<H: DriveHost>(
    conn: &mut PgConnection,
    meta: &FactMeta,
    ids: &[Uuid],
    event: &FileEvent,
) -> Result<(), EngineError> {
    if ids.is_empty() {
        return Ok(());
    }
    let rows = sqlx::query(
        "UPDATE drive.file SET version = version + 1, updated_at = $2 \
         WHERE id = ANY($1) RETURNING id, version",
    )
    .bind(ids)
    .bind(meta.occurred_at)
    .fetch_all(&mut *conn)
    .await?;
    let facts = rows
        .iter()
        .map(|row| {
            let id: Uuid = row.get("id");
            facts::fact_at(facts::uuid_key(id), row.get("version"), event, meta)
        })
        .collect::<Result<Vec<_>, _>>()?;
    facts::hand::<H>(conn, &facts).await
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

/// The files of `drive` still in flight: uploads not confirmed, and files
/// whose chain runs — what a drive freeze ends.
pub async fn ids_in_flight(conn: &mut PgConnection, drive: Uuid) -> Result<Vec<Uuid>, EngineError> {
    Ok(sqlx::query_scalar(
        "SELECT f.id FROM drive.file f \
         LEFT JOIN drive.file_processing s ON s.file_id = f.id \
         WHERE f.drive_id = $1 AND (f.committed_at IS NULL OR s.state = $2) \
         ORDER BY f.id",
    )
    .bind(drive)
    .bind(ProcessingState::Processing.as_str())
    .fetch_all(conn)
    .await?)
}

pub async fn drives_of(conn: &mut PgConnection, files: &[Uuid]) -> Result<Vec<Uuid>, EngineError> {
    let rows = sqlx::query("SELECT DISTINCT drive_id FROM drive.file WHERE id = ANY($1)")
        .bind(files)
        .fetch_all(conn)
        .await?;
    Ok(rows
        .iter()
        .map(|row| row.get::<Uuid, _>("drive_id"))
        .collect())
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

/// Moves the files `ids` from under `from` to under `to`, each one's
/// `FolderMoved` handed to the host at its next version.
pub(crate) async fn rebase_paths<H: DriveHost>(
    conn: &mut PgConnection,
    meta: &FactMeta,
    ids: &[Uuid],
    from: &DrivePath,
    to: &DrivePath,
) -> Result<u64, EngineError> {
    let rows = sqlx::query(
        "WITH moved AS ( \
           SELECT id, path AS from_path FROM drive.file WHERE id = ANY($1) \
         ) \
         UPDATE drive.file f \
         SET path = trim(both '/' from $3 || '/' || substr(f.path, length($2) + 2)), \
             version = f.version + 1, updated_at = $4 \
         FROM moved WHERE moved.id = f.id \
         RETURNING f.id, moved.from_path, f.path AS to_path, f.version",
    )
    .bind(ids)
    .bind(from.as_str())
    .bind(to.as_str())
    .bind(meta.occurred_at)
    .fetch_all(&mut *conn)
    .await?;
    let facts = rows
        .iter()
        .map(|row| {
            let id: Uuid = row.get("id");
            let event = super::FileEvent::FolderMoved {
                from_path: row.get("from_path"),
                to_path: row.get("to_path"),
            };
            facts::fact_at(facts::uuid_key(id), row.get("version"), &event, meta)
        })
        .collect::<Result<Vec<_>, _>>()?;
    facts::hand::<H>(conn, &facts).await?;
    Ok(u64::try_from(rows.len()).unwrap_or(u64::MAX))
}

pub struct PageWrite<'a> {
    pub number: i32,
    pub markdown: &'a str,
    pub origin: PageOrigin,
}

/// The key of a page's facts.
pub(crate) fn page_key(file_id: Uuid, number: i32) -> serde_json::Value {
    serde_json::json!({ "file_id": file_id, "number": number })
}

/// Writes `pages` of `file_id` (by number) — each one's version and last
/// change move — and hands each write to the host as `event` of its page.
pub(crate) async fn upsert_pages<H: DriveHost>(
    conn: &mut PgConnection,
    meta: &FactMeta,
    file_id: Uuid,
    pages: &[PageWrite<'_>],
    event: &PageEvent,
) -> Result<(), EngineError> {
    if pages.is_empty() {
        return Ok(());
    }
    super::processed::ensure(conn, file_id).await?;
    let numbers: Vec<i32> = pages.iter().map(|page| page.number).collect();
    let markdowns: Vec<&str> = pages.iter().map(|page| page.markdown).collect();
    let origins: Vec<&str> = pages.iter().map(|page| page.origin.as_str()).collect();
    let rows = sqlx::query(
        "INSERT INTO drive.file_page \
           (file_id, number, markdown, origin, version, updated_at, updated_by) \
         SELECT $1, number, markdown, origin, 1, $5, $6 \
         FROM unnest($2::int[], $3::text[], $4::text[]) AS batch(number, markdown, origin) \
         ON CONFLICT (file_id, number) DO UPDATE SET markdown = EXCLUDED.markdown, \
           origin = EXCLUDED.origin, version = drive.file_page.version + 1, \
           updated_at = EXCLUDED.updated_at, updated_by = EXCLUDED.updated_by \
         RETURNING number, version",
    )
    .bind(file_id)
    .bind(&numbers)
    .bind(&markdowns)
    .bind(&origins)
    .bind(meta.occurred_at)
    .bind(meta.actor_id)
    .fetch_all(&mut *conn)
    .await?;
    let mut written: Vec<(i32, i64)> = rows
        .iter()
        .map(|row| (row.get("number"), row.get("version")))
        .collect();
    written.sort_unstable();
    let facts = written
        .into_iter()
        .map(|(number, version)| facts::fact_at(page_key(file_id, number), version, event, meta))
        .collect::<Result<Vec<_>, _>>()?;
    facts::hand::<H>(conn, &facts).await
}

/// Deletes the pages numbered above `page_count` — edited ones included —
/// each one's `Trimmed` handed to the host, and answers their numbers.
pub(crate) async fn trim_pages<H: DriveHost>(
    conn: &mut PgConnection,
    meta: &FactMeta,
    file_id: Uuid,
    page_count: i32,
) -> Result<Vec<i32>, EngineError> {
    let rows = sqlx::query(
        "DELETE FROM drive.file_page WHERE file_id = $1 AND number > $2 \
         RETURNING number, version",
    )
    .bind(file_id)
    .bind(page_count)
    .fetch_all(&mut *conn)
    .await?;
    let mut trimmed: Vec<(i32, i64)> = rows
        .iter()
        .map(|row| (row.get("number"), row.get("version")))
        .collect();
    trimmed.sort_unstable();
    let facts = trimmed
        .iter()
        .map(|(number, version)| {
            facts::fact_at(
                page_key(file_id, *number),
                version + 1,
                &PageEvent::Trimmed,
                meta,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    facts::hand::<H>(conn, &facts).await?;
    Ok(trimmed.into_iter().map(|(number, _)| number).collect())
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
