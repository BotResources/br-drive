//! The facts of a file's jobs, one insert-only table per fact the library
//! reads — the shape Jobs gives its own ledger:
//!
//! - `drive.file_job`: the job was created (one per chain step), numbered
//!   1, 2, 3… per file under the file's row lock;
//! - `drive.file_job_end`: the job ended (the runner's final report or its
//!   declared failure; Jobs' failed, cancelled or creation_rejected) — one row
//!   per job, the first end wins by the primary key;
//! - `drive.file_job_cancel`: a user asked to cancel it;
//! - `drive.file_job_plan` / `drive.file_job_step`: where its runner says it is.
//!
//! A file's processing state is never stored: the `drive.file_status` view
//! computes it from which of these rows exist for the file's last job.

use chrono::{DateTime, Utc};
use service_engine::error::EngineError;
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::commands::Initiator;
use crate::ruleset::Trigger;

/// The names of what is known of a job: the eight Jobs facts, plus what only
/// the library records — `reported_done` and `reported_failed` (the runner's
/// own end of the job, told through the host), `cancel_requested` (a user's
/// cancel). The end kinds (`reported_done`, `reported_failed`, `failed`,
/// `cancelled`, `creation_rejected`) are the `kind` of `drive.file_job_end`;
/// the library also records `cancelled` itself for a step it never started
/// because a user's cancel crossed the previous step's end. They also name the
/// entries of [`FileJob::events`].
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
    /// The runner's final report: the job's end (terminal).
    pub const REPORTED_DONE: &str = "reported_done";
    /// The runner declared the job failed, with its reason (terminal).
    pub const REPORTED_FAILED: &str = "reported_failed";
}

/// One job of a file, as the file's reads see it: the last one carries the
/// file's processing state.
#[derive(Debug, Clone, PartialEq)]
pub struct FileJob {
    pub job_id: Uuid,
    /// The index of the chain step the job runs, in the file's `steps`.
    pub step_index: i32,
    /// The gesture that started the job's chain; `None` for a job carried
    /// over from 0.1.
    pub trigger: Option<Trigger>,
    /// The principal whose gesture started the chain, as sent to Jobs.
    pub triggered_by: Option<Initiator>,
    /// What the job's reads need of its facts while it runs, as entries
    /// `{kind, ...}`: the current plan (`plan_declared`, `steps`), the latest
    /// step started (`step_started`, `index`, `label`, `started_at`) and a
    /// cancel request (`cancel_requested`). Built from the fact tables when
    /// the job is read; empty once the job ended.
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
    /// Whether a user asked to cancel this job.
    pub fn cancel_requested(&self) -> bool {
        entries_of(&self.events, kind::CANCEL_REQUESTED)
            .next()
            .is_some()
    }

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
}

/// What the file's reads know of its running job, as the status view gives
/// it: the current plan, the latest step started, the first cancel request.
#[derive(Debug, Default)]
pub(crate) struct RunningFacts {
    pub plan: Option<Vec<String>>,
    pub step: Option<(i32, String, DateTime<Utc>)>,
    pub cancel_requested_at: Option<DateTime<Utc>>,
}

impl RunningFacts {
    /// The facts as [`FileJob::events`] entries.
    pub(crate) fn entries(self) -> Vec<serde_json::Value> {
        let mut entries = Vec::new();
        if let Some(plan) = self.plan {
            entries.push(serde_json::json!({ "kind": kind::PLAN_DECLARED, "steps": plan }));
        }
        if let Some((index, label, started_at)) = self.step {
            entries.push(serde_json::json!({
                "kind": kind::STEP_STARTED,
                "index": index,
                "label": label,
                "started_at": started_at,
            }));
        }
        if let Some(at) = self.cancel_requested_at {
            entries.push(serde_json::json!({ "kind": kind::CANCEL_REQUESTED, "at": at }));
        }
        entries
    }
}

/// Records a new job of `file_id`: the file's next job number, under the
/// file's row lock the caller holds (a concurrent writer that skipped it
/// would meet the `(file_id, number)` primary key).
pub(crate) async fn insert_job(
    conn: &mut PgConnection,
    file_id: Uuid,
    job: &FileJob,
) -> Result<(), EngineError> {
    let (by_id, by_name) = match &job.triggered_by {
        Some(initiator) => (Some(initiator.id), initiator.display_name.clone()),
        None => (None, None),
    };
    sqlx::query(
        "INSERT INTO drive.file_job \
           (file_id, number, job_id, step_index, trigger, triggered_by_id, triggered_by_name, \
            created_at) \
         SELECT $1, COALESCE(max(number), 0) + 1, $2, $3, $4, $5, $6, $7 \
         FROM drive.file_job WHERE file_id = $1",
    )
    .bind(file_id)
    .bind(job.job_id)
    .bind(job.step_index)
    .bind(job.trigger.map(Trigger::as_str))
    .bind(by_id)
    .bind(by_name)
    .bind(job.created_at)
    .execute(conn)
    .await?;
    Ok(())
}

/// How a job ended: the `kind` of its `drive.file_job_end` row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EndKind {
    ReportedDone,
    ReportedFailed,
    Failed,
    Cancelled,
    CreationRejected,
}

impl EndKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::ReportedDone => kind::REPORTED_DONE,
            Self::ReportedFailed => kind::REPORTED_FAILED,
            Self::Failed => kind::FAILED,
            Self::Cancelled => kind::CANCELLED,
            Self::CreationRejected => kind::CREATION_REJECTED,
        }
    }
}

/// The end of a job, as recorded.
#[derive(Debug, Clone, Copy)]
pub(crate) struct End<'a> {
    pub kind: EndKind,
    /// Required for `ReportedFailed`, `Failed` and `CreationRejected`, absent
    /// otherwise.
    pub reason_code: Option<&'a str>,
    pub message: Option<&'a str>,
    pub at: DateTime<Utc>,
}

/// Records the end of `job_id`; `false` when the job had already ended — the
/// first end wins, a later one is not stored.
pub(crate) async fn end(
    conn: &mut PgConnection,
    job_id: Uuid,
    end: End<'_>,
) -> Result<bool, EngineError> {
    let done = sqlx::query(
        "INSERT INTO drive.file_job_end (job_id, kind, reason_code, message, at) \
         VALUES ($1, $2, $3, $4, $5) ON CONFLICT (job_id) DO NOTHING",
    )
    .bind(job_id)
    .bind(end.kind.as_str())
    .bind(end.reason_code)
    .bind(end.message)
    .bind(end.at)
    .execute(conn)
    .await?;
    let first = done.rows_affected() == 1;
    if !first {
        tracing::debug!(%job_id, kind = end.kind.as_str(), "the job had already ended; this end is not recorded");
    }
    Ok(first)
}

/// Records a user's request to cancel `job_id`, the job's next number.
pub(crate) async fn request_cancel(
    conn: &mut PgConnection,
    job_id: Uuid,
    requested_by: Uuid,
    at: DateTime<Utc>,
) -> Result<(), EngineError> {
    sqlx::query(
        "INSERT INTO drive.file_job_cancel (job_id, number, requested_by, at) \
         SELECT $1, COALESCE(max(number), 0) + 1, $2, $3 \
         FROM drive.file_job_cancel WHERE job_id = $1",
    )
    .bind(job_id)
    .bind(requested_by)
    .bind(at)
    .execute(conn)
    .await?;
    Ok(())
}

/// Records a plan the runner declared in `run_id`, as the job's next
/// declaration — unless it is the job's current plan already (a redelivered
/// declaration adds nothing).
pub(crate) async fn declare_plan(
    conn: &mut PgConnection,
    job_id: Uuid,
    run_id: Uuid,
    labels: &[String],
    at: DateTime<Utc>,
) -> Result<(), EngineError> {
    sqlx::query(
        "INSERT INTO drive.file_job_plan (job_id, number, run_id, labels, declared_at) \
         SELECT $1, COALESCE(max(number), 0) + 1, $2, $3, $4 \
         FROM drive.file_job_plan WHERE job_id = $1 \
         HAVING NOT EXISTS ( \
           SELECT 1 FROM ( \
             SELECT run_id, labels FROM drive.file_job_plan WHERE job_id = $1 \
             ORDER BY number DESC LIMIT 1 \
           ) current WHERE current.run_id = $2 AND current.labels = $3)",
    )
    .bind(job_id)
    .bind(run_id)
    .bind(labels)
    .bind(at)
    .execute(conn)
    .await?;
    Ok(())
}

/// Records that the runner started step `plan_index` of its plan in `run_id`;
/// a run starts each step once, so a redelivery adds nothing.
pub(crate) async fn start_step(
    conn: &mut PgConnection,
    job_id: Uuid,
    run_id: Uuid,
    plan_index: i32,
    label: &str,
    started_at: DateTime<Utc>,
) -> Result<(), EngineError> {
    sqlx::query(
        "INSERT INTO drive.file_job_step (job_id, run_id, plan_index, label, started_at) \
         VALUES ($1, $2, $3, $4, $5) ON CONFLICT (job_id, run_id, plan_index) DO NOTHING",
    )
    .bind(job_id)
    .bind(run_id)
    .bind(plan_index)
    .bind(label)
    .bind(started_at)
    .execute(conn)
    .await?;
    Ok(())
}

/// The file a job belongs to, if any.
pub(crate) async fn file_of(
    conn: &mut PgConnection,
    job_id: Uuid,
) -> Result<Option<Uuid>, EngineError> {
    let row = sqlx::query("SELECT file_id FROM drive.file_job WHERE job_id = $1")
        .bind(job_id)
        .fetch_optional(conn)
        .await?;
    Ok(row.map(|row| row.get("file_id")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(events: Vec<serde_json::Value>) -> FileJob {
        FileJob {
            job_id: Uuid::now_v7(),
            step_index: 0,
            trigger: None,
            triggered_by: None,
            events,
            created_at: Utc::now(),
        }
    }

    #[test]
    fn the_running_facts_read_back_as_the_progress_and_the_cancel_request() {
        let at = Utc::now();
        let facts = RunningFacts {
            plan: Some(vec!["read".into(), "write".into()]),
            step: Some((1, "write".into(), at)),
            cancel_requested_at: Some(at),
        };
        let read = job(facts.entries());
        let progress = read.progress();
        assert_eq!(progress.plan, vec!["read", "write"]);
        assert_eq!(progress.current_index, Some(1));
        assert_eq!(progress.current_label.as_deref(), Some("write"));
        assert_eq!(progress.at, Some(at));
        assert!(read.cancel_requested());
        let none = job(RunningFacts::default().entries());
        assert!(!none.cancel_requested());
        assert_eq!(none.progress().plan, Vec::<String>::new());
    }

    #[test]
    fn an_end_kind_is_named_as_the_table_checks_it() {
        assert_eq!(EndKind::ReportedDone.as_str(), "reported_done");
        assert_eq!(EndKind::ReportedFailed.as_str(), "reported_failed");
        assert_eq!(EndKind::Failed.as_str(), "failed");
        assert_eq!(EndKind::Cancelled.as_str(), "cancelled");
        assert_eq!(EndKind::CreationRejected.as_str(), "creation_rejected");
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
}
