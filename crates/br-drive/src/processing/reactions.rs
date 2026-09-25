use contract_jobs::command::CancelJob;
use contract_jobs::event::{
    JobCancelled, JobCompleted, JobCreationRejected, JobFailed, JobPlanDeclared, JobQueued,
    JobStarted, JobStepStarted, REASON_DUPLICATE_ACTIVE_ENTITY,
};
use futures_util::future::BoxFuture;
use serde::Serialize;
use service_engine::inbound::{ReactionCoordinates, ReactionMessage};
use service_engine::pipeline::Reaction;
use uuid::Uuid;

use super::chain::{advance, refresh_status};
use super::commands::JobCancel;
use super::log::{self, FileJob, kind};
use crate::fault::DriveReactionFault;
use crate::file::{FileCause, FileRow};
use crate::host::DriveHost;

/// A fact logged on its job: the file (loaded, so locked, and its status read
/// again after the write), the job as it stood before the entry, and whether
/// the job is the file's last one — the only job whose facts move the file.
struct Logged<H> {
    file: FileRow<H>,
    job: Option<FileJob>,
    prior_outcome: Option<String>,
}

impl<H> Logged<H> {
    /// The fact is the first outcome of the file's current job.
    fn settles_the_file(&self) -> bool {
        self.job.is_some() && self.prior_outcome.is_none()
    }
}

/// Appends the fact to the log of its own job — never another job's — after
/// locking the job's file. A job no file holds (a deleted file, a job the
/// library never created) is acknowledged and ignored.
async fn log_fact<H: DriveHost>(
    cx: &mut Reaction<'_>,
    job_id: Uuid,
    entry_kind: &str,
    payload: impl Serialize,
) -> Result<Option<Logged<H>>, DriveReactionFault> {
    let Some((file_id, _)) = log::owner_of(cx.connection(), job_id).await? else {
        return Ok(None);
    };
    let Some(mut file) = cx.load::<FileRow<H>>(&file_id).await? else {
        return Ok(None);
    };
    // Read under the file's lock: a concurrent fact of the same job waits.
    let prior_outcome = log::owner_of(cx.connection(), job_id)
        .await?
        .and_then(|(_, outcome)| outcome);
    let job = file.last_job().filter(|job| job.job_id == job_id).cloned();
    let entry = log::entry(entry_kind, cx.now().as_datetime(), payload)?;
    log::append(cx.connection(), job_id, entry).await?;
    refresh_status(cx, &mut file).await?;
    Ok(Some(Logged {
        file,
        job,
        prior_outcome,
    }))
}

/// A fact that changes no state: the file's views recompute, and a live
/// session hears of it only if what it shows changed — a plan or a step of
/// the running job that moves its progress does (`ProgressChanged`); a queued
/// job, a started run, a redelivery, or any fact of a job that is no longer
/// running does not.
fn progressed<H: DriveHost>(
    cx: &mut Reaction<'_>,
    logged: &Logged<H>,
    shows: bool,
) -> Result<(), DriveReactionFault> {
    let moved = || {
        let before = logged.job.as_ref().map(FileJob::progress);
        let after = logged.file.active_job().map(FileJob::progress);
        before != after
    };
    if shows && logged.settles_the_file() && moved() {
        crate::file::file_changed::<H>(cx, &logged.file, FileCause::ProgressChanged)?;
    } else {
        crate::file::file_touched::<H>(cx, &logged.file)?;
    }
    Ok(())
}

macro_rules! job_fact {
    ($name:ident, $payload:ty, $coords:path) => {
        pub struct $name(pub $payload);

        impl ReactionMessage for $name {
            fn coordinates() -> ReactionCoordinates {
                ReactionCoordinates::Event(
                    $coords().expect("the published jobs coordinates are valid"),
                )
            }

            fn decode(payload: &[u8]) -> Result<Self, serde_json::Error> {
                serde_json::from_slice(payload).map(Self)
            }
        }
    };
}

job_fact!(
    QueuedFact,
    JobQueued,
    contract_jobs::evt_job_queued_v1_coords
);
job_fact!(
    CreationRejectedFact,
    JobCreationRejected,
    contract_jobs::evt_job_creation_rejected_v1_coords
);
job_fact!(
    StartedFact,
    JobStarted,
    contract_jobs::evt_job_started_v1_coords
);
job_fact!(
    PlanDeclaredFact,
    JobPlanDeclared,
    contract_jobs::evt_job_plan_declared_v1_coords
);
job_fact!(
    StepStartedFact,
    JobStepStarted,
    contract_jobs::evt_job_step_started_v1_coords
);
job_fact!(
    CompletedFact,
    JobCompleted,
    contract_jobs::evt_job_completed_v1_coords
);
job_fact!(
    FailedFact,
    JobFailed,
    contract_jobs::evt_job_failed_v1_coords
);
job_fact!(
    CancelledFact,
    JobCancelled,
    contract_jobs::evt_job_cancelled_v1_coords
);

/// The rejection parameters Jobs sets on `duplicate_active_entity`: the job
/// holding the source entity, and the source it names.
const ACTIVE_JOB_ID_PARAM: &str = "activeJobId";
const SOURCE_ENTITY_PARAM: &str = "sourceEntityId";
const SOURCE_BC_PARAM: &str = "sourceBc";

pub const DURABLE_QUEUED: &str = "job-queued";
pub const DURABLE_CREATION_REJECTED: &str = "job-creation-rejected";
pub const DURABLE_STARTED: &str = "job-started";
pub const DURABLE_PLAN_DECLARED: &str = "job-plan-declared";
pub const DURABLE_STEP_STARTED: &str = "job-step-started";
pub const DURABLE_COMPLETED: &str = "job-completed";
pub const DURABLE_FAILED: &str = "job-failed";
pub const DURABLE_CANCELLED: &str = "job-cancelled";

/// A terminal fact of the file's current job lands it FAILED with `reason`;
/// of any other job it is only logged.
fn settled_failed<H: DriveHost>(
    cx: &mut Reaction<'_>,
    logged: &Logged<H>,
    reason: String,
) -> Result<(), DriveReactionFault> {
    if !logged.settles_the_file() {
        return progressed(cx, logged, false);
    }
    crate::file::file_changed::<H>(cx, &logged.file, FileCause::ProcessingFailed { reason })?;
    Ok(())
}

macro_rules! progress_reaction {
    ($(#[$doc:meta])* $name:ident, $fact:ident, $kind:expr, $shows:expr) => {
        $(#[$doc])*
        pub fn $name<'r, H: DriveHost>(
            cx: &'r mut Reaction<'r>,
            fact: $fact,
        ) -> BoxFuture<'r, Result<(), DriveReactionFault>> {
            Box::pin(async move {
                let job_id = fact.0.job_id;
                let Some(logged) = log_fact::<H>(cx, job_id, $kind, &fact.0).await? else {
                    return Ok(());
                };
                progressed(cx, &logged, $shows)
            })
        }
    };
}

progress_reaction!(
    /// Jobs queued the job.
    on_queued, QueuedFact, kind::QUEUED, false
);
progress_reaction!(
    /// A run of the job started.
    on_started, StartedFact, kind::STARTED, false
);
progress_reaction!(
    /// The runner declared its plan: `progress.plan`.
    on_plan_declared, PlanDeclaredFact, kind::PLAN_DECLARED, true
);
progress_reaction!(
    /// The runner started a step of its plan: `progress.currentIndex`,
    /// `currentLabel`, `at` (the latest start wins, whatever the arrival order).
    on_step_started, StepStartedFact, kind::STEP_STARTED, true
);

/// Jobs refused to create the job: the file lands FAILED with the code. On
/// `duplicate_active_entity` Jobs still holds a live job on the file that the
/// library does not know as live (a lost `job.finish`, a restored database, a
/// cancel and a create crossed on Jobs' separate consumers): that job is
/// cancelled, so the user's reprocess gets through; a cancel is idempotent.
pub fn on_creation_rejected<'r, H: DriveHost>(
    cx: &'r mut Reaction<'r>,
    fact: CreationRejectedFact,
) -> BoxFuture<'r, Result<(), DriveReactionFault>> {
    Box::pin(async move {
        let job_id = fact.0.job_id;
        let Some(logged) = log_fact::<H>(cx, job_id, kind::CREATION_REJECTED, &fact.0).await?
        else {
            return Ok(());
        };
        if logged.settles_the_file() && fact.0.reason_code == REASON_DUPLICATE_ACTIVE_ENTITY {
            match active_job_of::<H>(&fact.0.params, logged.file.id) {
                Some(active) => cx.command(JobCancel {
                    payload: CancelJob { job_id: active },
                })?,
                None => tracing::warn!(
                    file = %logged.file.id,
                    params = %fact.0.params,
                    "a duplicate_active_entity rejection names no active job of this file; \
                     nothing to cancel"
                ),
            }
        }
        settled_failed(cx, &logged, fact.0.reason_code.clone())
    })
}

/// The live job Jobs names on a `duplicate_active_entity` rejection — only when
/// the rejection is about this very file of this host, as Jobs states it.
fn active_job_of<H: DriveHost>(params: &serde_json::Value, file: Uuid) -> Option<Uuid> {
    active_job_of_service(params, file, H::SERVICE)
}

fn active_job_of_service(params: &serde_json::Value, file: Uuid, service: &str) -> Option<Uuid> {
    let text = |key: &str| params.get(key).and_then(serde_json::Value::as_str);
    let names_elsewhere = text(SOURCE_ENTITY_PARAM).is_some_and(|id| id != file.to_string())
        || text(SOURCE_BC_PARAM).is_some_and(|bc| bc != service);
    if names_elsewhere {
        return None;
    }
    text(ACTIVE_JOB_ID_PARAM).and_then(|id| Uuid::parse_str(id).ok())
}

/// Jobs completed the job — only ever after the library's own `job.finish`,
/// sent with the runner's final report. The file's current job completing
/// advances the chain (the next step's job, or READY).
pub fn on_completed<'r, H: DriveHost>(
    cx: &'r mut Reaction<'r>,
    fact: CompletedFact,
) -> BoxFuture<'r, Result<(), DriveReactionFault>> {
    Box::pin(async move {
        let job_id = fact.0.job_id;
        let Some(mut logged) = log_fact::<H>(cx, job_id, kind::COMPLETED, &fact.0).await? else {
            return Ok(());
        };
        match logged.job.clone() {
            Some(job) if logged.settles_the_file() => {
                advance(cx, &mut logged.file, &job).await?;
                Ok(())
            }
            _ => progressed(cx, &logged, false),
        }
    })
}

pub fn on_failed<'r, H: DriveHost>(
    cx: &'r mut Reaction<'r>,
    fact: FailedFact,
) -> BoxFuture<'r, Result<(), DriveReactionFault>> {
    Box::pin(async move {
        let job_id = fact.0.job_id;
        let Some(logged) = log_fact::<H>(cx, job_id, kind::FAILED, &fact.0).await? else {
            return Ok(());
        };
        let reason = logged
            .file
            .processing_error()
            .map(str::to_string)
            .unwrap_or_else(|| fact.0.failure_cause.clone());
        settled_failed(cx, &logged, reason)
    })
}

pub fn on_cancelled<'r, H: DriveHost>(
    cx: &'r mut Reaction<'r>,
    fact: CancelledFact,
) -> BoxFuture<'r, Result<(), DriveReactionFault>> {
    Box::pin(async move {
        let job_id = fact.0.job_id;
        let Some(logged) = log_fact::<H>(cx, job_id, kind::CANCELLED, &fact.0).await? else {
            return Ok(());
        };
        settled_failed(cx, &logged, super::CANCELLED.to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_duplicate_rejection_names_a_job_only_about_this_file_of_this_host() {
        let file = Uuid::now_v7();
        let active = Uuid::now_v7();
        let params = |entity: Uuid, bc: &str, job: serde_json::Value| {
            serde_json::json!({
                "activeJobId": job, "sourceEntityId": entity, "sourceBc": bc,
            })
        };
        let named = |value: serde_json::Value| active_job_of_service(&value, file, "host");
        assert_eq!(
            named(params(file, "host", serde_json::json!(active))),
            Some(active)
        );
        assert_eq!(
            named(serde_json::json!({ "activeJobId": active })),
            Some(active),
            "without the source keys the job is taken as named"
        );
        assert_eq!(
            named(params(Uuid::now_v7(), "host", serde_json::json!(active))),
            None
        );
        assert_eq!(
            named(params(file, "another", serde_json::json!(active))),
            None
        );
        assert_eq!(
            named(params(file, "host", serde_json::json!("not-a-uuid"))),
            None
        );
        assert_eq!(named(params(file, "host", serde_json::json!(42))), None);
        assert_eq!(named(serde_json::json!({})), None);
    }
}
