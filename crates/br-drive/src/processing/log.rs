//! The per-file job log (`drive.file_job`): one row per Jobs job the library
//! created for a file, one job per chain step, each with the append-only list
//! of what was learned about it. A file's processing state is never stored:
//! the `drive.file_status` view computes it from the file's last job.

use chrono::{DateTime, Utc};
use serde::Serialize;
use service_engine::error::EngineError;
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::commands::Initiator;

/// The kinds of the log's entries: the eight Jobs facts, plus the two the
/// library writes itself where Jobs says nothing.
pub mod kind {
    pub const QUEUED: &str = "queued";
    pub const CREATION_REJECTED: &str = "creation_rejected";
    pub const STARTED: &str = "started";
    pub const PLAN_DECLARED: &str = "plan_declared";
    pub const STEP_STARTED: &str = "step_started";
    pub const COMPLETED: &str = "completed";
    pub const FAILED: &str = "failed";
    pub const CANCELLED: &str = "cancelled";
    /// A user asked to cancel the job; Jobs confirms with `cancelled`.
    pub const CANCEL_REQUESTED: &str = "cancel_requested";
}

/// One job of a file, as the file's reads see it: the last one carries the
/// file's processing state.
#[derive(Debug, Clone, PartialEq)]
pub struct FileJob {
    pub job_id: Uuid,
    /// The index of the chain step the job runs, in the file's `steps`.
    pub step_index: i32,
    /// The principal whose gesture started the chain, as sent to Jobs.
    pub triggered_by: Option<Initiator>,
    /// The log, in arrival order: `{kind, at, ...payload}`.
    pub events: Vec<serde_json::Value>,
    pub created_at: DateTime<Utc>,
}

/// Where the runner of a job says it is, read from the job's log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunProgress {
    pub plan: Vec<String>,
    pub current_index: Option<i32>,
    pub current_label: Option<String>,
    pub at: Option<DateTime<Utc>>,
}

fn entries_of<'a>(
    events: &'a [serde_json::Value],
    wanted: &'a str,
) -> impl Iterator<Item = &'a serde_json::Value> + 'a {
    events
        .iter()
        .filter(move |entry| entry.get("kind").and_then(serde_json::Value::as_str) == Some(wanted))
}

impl FileJob {
    /// The plan of the last `plan_declared`, and the latest `step_started` —
    /// by its start instant, then by index — whatever order they arrived in.
    pub fn progress(&self) -> RunProgress {
        let plan = entries_of(&self.events, kind::PLAN_DECLARED)
            .last()
            .and_then(|entry| entry.get("steps"))
            .and_then(|steps| serde_json::from_value::<Vec<String>>(steps.clone()).ok())
            .unwrap_or_default();
        let step = entries_of(&self.events, kind::STEP_STARTED)
            .filter_map(|entry| {
                let at = entry
                    .get("started_at")
                    .and_then(|at| serde_json::from_value::<DateTime<Utc>>(at.clone()).ok())?;
                let index = entry.get("index").and_then(serde_json::Value::as_i64)?;
                let label = entry
                    .get("label")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                Some((at, index, label))
            })
            .max_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
        RunProgress {
            plan,
            current_index: step
                .as_ref()
                .map(|(_, index, _)| i32::try_from(*index).unwrap_or(i32::MAX)),
            current_label: step.as_ref().map(|(_, _, label)| label.clone()),
            at: step.map(|(at, _, _)| at),
        }
    }

    /// The kind of the job's first terminal entry — a job settles once — as
    /// `drive.file_job.outcome` computes it.
    pub fn outcome(&self) -> Option<&str> {
        self.events
            .iter()
            .filter_map(|entry| entry.get("kind").and_then(serde_json::Value::as_str))
            .find(|kind| {
                matches!(
                    *kind,
                    kind::COMPLETED | kind::FAILED | kind::CANCELLED | kind::CREATION_REJECTED
                )
            })
    }
}

/// A log entry: `kind` and `at` first, then the fact's own fields as received.
pub(crate) fn entry(
    kind: &str,
    at: DateTime<Utc>,
    payload: impl Serialize,
) -> Result<serde_json::Value, EngineError> {
    let payload = serde_json::to_value(payload).map_err(|source| EngineError::Encode {
        what: "a job log entry",
        source,
    })?;
    let mut object = serde_json::Map::new();
    object.insert("kind".into(), serde_json::Value::String(kind.to_string()));
    object.insert(
        "at".into(),
        serde_json::to_value(at).map_err(|source| EngineError::Encode {
            what: "a job log instant",
            source,
        })?,
    );
    if let serde_json::Value::Object(fields) = payload {
        for (key, value) in fields {
            object.entry(key).or_insert(value);
        }
    }
    Ok(serde_json::Value::Object(object))
}

/// Records a new job of `file_id` for step `step_index`, with an empty log.
pub(crate) async fn insert_job(
    conn: &mut PgConnection,
    file_id: Uuid,
    job: &FileJob,
) -> Result<(), EngineError> {
    let triggered_by = job
        .triggered_by
        .as_ref()
        .map(serde_json::to_value)
        .transpose()
        .map_err(|source| EngineError::Encode {
            what: "a job's initiator",
            source,
        })?;
    sqlx::query(
        "INSERT INTO drive.file_job (job_id, file_id, step_index, triggered_by, events, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(job.job_id)
    .bind(file_id)
    .bind(job.step_index)
    .bind(triggered_by)
    .bind(serde_json::Value::Array(job.events.clone()))
    .bind(job.created_at)
    .execute(conn)
    .await?;
    Ok(())
}

/// Appends `entry` to the log of `job_id`; nothing when no file holds it.
pub(crate) async fn append(
    conn: &mut PgConnection,
    job_id: Uuid,
    entry: serde_json::Value,
) -> Result<(), EngineError> {
    sqlx::query(
        "UPDATE drive.file_job SET events = events || jsonb_build_array($2::jsonb) \
         WHERE job_id = $1",
    )
    .bind(job_id)
    .bind(entry)
    .execute(conn)
    .await?;
    Ok(())
}

/// The file a job belongs to, and the job's outcome kind before any new entry.
pub(crate) async fn owner_of(
    conn: &mut PgConnection,
    job_id: Uuid,
) -> Result<Option<(Uuid, Option<String>)>, EngineError> {
    let row = sqlx::query(
        "SELECT file_id, outcome ->> 'kind' AS outcome FROM drive.file_job WHERE job_id = $1",
    )
    .bind(job_id)
    .fetch_optional(conn)
    .await?;
    Ok(row.map(|row| (row.get("file_id"), row.get("outcome"))))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(events: Vec<serde_json::Value>) -> FileJob {
        FileJob {
            job_id: Uuid::now_v7(),
            step_index: 0,
            triggered_by: None,
            events,
            created_at: Utc::now(),
        }
    }

    #[test]
    fn an_entry_carries_its_kind_and_instant_then_the_payload_as_received() {
        let at = Utc::now();
        let job_id = Uuid::now_v7();
        let made = entry(
            kind::FAILED,
            at,
            serde_json::json!({ "job_id": job_id, "failure_cause": "x", "kind": "spoofed" }),
        )
        .unwrap();
        assert_eq!(
            made["kind"], "failed",
            "the payload never overrides the kind"
        );
        assert_eq!(made["failure_cause"], "x");
        assert_eq!(made["job_id"], serde_json::json!(job_id));
        assert_eq!(
            serde_json::from_value::<DateTime<Utc>>(made["at"].clone()).unwrap(),
            at
        );
    }

    #[test]
    fn progress_reads_the_last_plan_and_the_latest_step_whatever_their_arrival_order() {
        let early = Utc::now();
        let late = early + chrono::TimeDelta::seconds(5);
        let log = job(vec![
            serde_json::json!({ "kind": "plan_declared", "steps": ["a", "b"] }),
            serde_json::json!({ "kind": "step_started", "index": 1, "label": "b", "started_at": late }),
            serde_json::json!({ "kind": "step_started", "index": 0, "label": "a", "started_at": early }),
        ]);
        let progress = log.progress();
        assert_eq!(progress.plan, vec!["a", "b"]);
        assert_eq!(progress.current_index, Some(1));
        assert_eq!(progress.current_label.as_deref(), Some("b"));
        assert_eq!(progress.at, Some(late));
        assert_eq!(job(vec![]).progress().current_index, None);
    }

    #[test]
    fn the_outcome_is_the_first_terminal_entry_whatever_comes_after() {
        assert_eq!(job(vec![]).outcome(), None);
        let log = job(vec![
            serde_json::json!({ "kind": "queued" }),
            serde_json::json!({ "kind": "cancel_requested" }),
        ]);
        assert_eq!(log.outcome(), None, "a cancel request is not an outcome");
        let log = job(vec![
            serde_json::json!({ "kind": "started" }),
            serde_json::json!({ "kind": "completed" }),
            serde_json::json!({ "kind": "step_started" }),
            serde_json::json!({ "kind": "failed" }),
        ]);
        assert_eq!(log.outcome(), Some("completed"));
    }
}
