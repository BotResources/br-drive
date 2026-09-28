use contract_jobs::command::CancelJob;
use contract_jobs::event::{
    JobCancelled, JobCompleted, JobCreationRejected, JobFailed, JobPlanDeclared, JobQueued,
    JobStarted, JobStepStarted, REASON_DUPLICATE_ACTIVE_ENTITY,
};
use futures_util::future::BoxFuture;
use service_engine::inbound::{ReactionCoordinates, ReactionMessage};
use service_engine::pipeline::Reaction;
use uuid::Uuid;

use super::chain::refresh_status;
use super::commands::JobCancel;
use super::log::{self, End, EndKind, FileJob};
use crate::fault::DriveReactionFault;
use crate::file::{FileCause, FileRow};
use crate::host::DriveHost;

/// The job a Jobs fact is about, with its file (loaded, so locked: a
/// concurrent fact of the same file waits) and whether the job is the file's
/// running one — its last job, not ended — the only job whose facts may move
/// the file. `None` for a job no file holds (a deleted file, a job the library
/// never created): the fact is acknowledged and ignored.
struct Target<H> {
    file: FileRow<H>,
    job_id: Uuid,
    running: bool,
}

async fn target<H: DriveHost>(
    cx: &mut Reaction<'_>,
    job_id: Uuid,
) -> Result<Option<Target<H>>, DriveReactionFault> {
    let Some(file_id) = log::file_of(cx.connection(), job_id).await? else {
        return Ok(None);
    };
    let Some(file) = cx.load::<FileRow<H>>(&file_id).await? else {
        return Ok(None);
    };
    let running = file.active_job().is_some_and(|job| job.job_id == job_id);
    Ok(Some(Target {
        file,
        job_id,
        running,
    }))
}

/// A progress fact of the running job is recorded, and a live session hears
/// of it only when what it shows moved (`ProgressChanged`, never on the host
/// object: progress changes no count). A progress fact of any other job — an
/// older one, or one that already ended — is read by nothing and not stored.
async fn record_progress<H: DriveHost>(
    cx: &mut Reaction<'_>,
    job_id: Uuid,
    record: impl for<'c> FnOnce(
        &'c mut sqlx::PgConnection,
    ) -> BoxFuture<'c, Result<(), service_engine::error::EngineError>>,
) -> Result<(), DriveReactionFault> {
    let Some(mut target) = target::<H>(cx, job_id).await? else {
        return Ok(());
    };
    if !target.running {
        tracing::debug!(%job_id, "a progress fact of a job that no longer runs; not recorded");
        return Ok(());
    }
    let before = target.file.active_job().map(FileJob::progress);
    record(cx.connection()).await?;
    refresh_status(cx, &mut target.file).await?;
    let after = target.file.active_job().map(FileJob::progress);
    if before != after {
        crate::file::file_progressed::<H>(cx, &target.file)?;
    }
    Ok(())
}

/// A Jobs end of the job is recorded — unless the job had already ended: the
/// first end wins. The end of the running job lands the file FAILED with
/// `reason` (`updatedAt` moves); nothing else moves.
async fn record_end<H: DriveHost>(
    cx: &mut Reaction<'_>,
    target: &mut Target<H>,
    kind: EndKind,
    reason: Option<&str>,
    message: Option<&str>,
) -> Result<bool, DriveReactionFault> {
    let now = cx.now().as_datetime();
    let first = log::end(
        cx.connection(),
        target.job_id,
        End {
            kind,
            reason_code: reason,
            message,
            at: now,
        },
    )
    .await?;
    if !(first && target.running) {
        return Ok(false);
    }
    refresh_status(cx, &mut target.file).await?;
    target.file.updated_at = now;
    cx.save(&target.file).await?;
    let reason = target
        .file
        .processing_error()
        .unwrap_or(super::CANCELLED)
        .to_string();
    crate::file::file_changed::<H>(cx, &target.file, FileCause::ProcessingFailed { reason })?;
    Ok(true)
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

/// Jobs queued the job, started a run of it, or completed it: nothing reads
/// these facts — Jobs' `completed` only confirms the library's own
/// `job.finish`, sent when the runner's final report ended the job. They are
/// acknowledged, so the host's durables never hold them.
macro_rules! unread_reaction {
    ($(#[$doc:meta])* $name:ident, $fact:ident) => {
        $(#[$doc])*
        pub fn $name<'r>(
            _cx: &'r mut Reaction<'r>,
            fact: $fact,
        ) -> BoxFuture<'r, Result<(), DriveReactionFault>> {
            Box::pin(async move {
                tracing::trace!(job_id = %fact.0.job_id, "a Jobs fact nothing reads");
                Ok(())
            })
        }
    };
}

unread_reaction!(
    /// Jobs queued the job.
    on_queued, QueuedFact
);
unread_reaction!(
    /// A run of the job started.
    on_started, StartedFact
);
unread_reaction!(
    /// Jobs completed the job — only ever after the library's own
    /// `job.finish`, sent in the transaction of the runner's final report,
    /// which already ended the job and moved the chain on.
    on_completed, CompletedFact
);

/// The runner declared its plan: `progress.plan` (the latest declaration).
pub fn on_plan_declared<'r, H: DriveHost>(
    cx: &'r mut Reaction<'r>,
    fact: PlanDeclaredFact,
) -> BoxFuture<'r, Result<(), DriveReactionFault>> {
    Box::pin(async move {
        let JobPlanDeclared {
            job_id,
            run_id,
            steps,
        } = fact.0;
        let at = cx.now().as_datetime();
        record_progress::<H>(cx, job_id, move |conn| {
            Box::pin(async move { log::declare_plan(conn, job_id, run_id, &steps, at).await })
        })
        .await
    })
}

/// The runner started a step of its plan: `progress.currentIndex`,
/// `currentLabel`, `at` (the latest start wins, whatever the arrival order).
pub fn on_step_started<'r, H: DriveHost>(
    cx: &'r mut Reaction<'r>,
    fact: StepStartedFact,
) -> BoxFuture<'r, Result<(), DriveReactionFault>> {
    Box::pin(async move {
        let JobStepStarted {
            job_id,
            run_id,
            index,
            label,
            started_at,
        } = fact.0;
        let index = i32::try_from(index).unwrap_or(i32::MAX);
        record_progress::<H>(cx, job_id, move |conn| {
            Box::pin(async move {
                log::start_step(conn, job_id, run_id, index, &label, started_at).await
            })
        })
        .await
    })
}

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
        let Some(mut target) = target::<H>(cx, fact.0.job_id).await? else {
            return Ok(());
        };
        let reason = non_blank(&fact.0.reason_code, super::log::kind::CREATION_REJECTED);
        let settled = record_end(
            cx,
            &mut target,
            EndKind::CreationRejected,
            Some(reason),
            None,
        )
        .await?;
        if settled && fact.0.reason_code == REASON_DUPLICATE_ACTIVE_ENTITY {
            match active_job_of::<H>(&fact.0.params, target.file.id) {
                Some(active) => cx.command(JobCancel {
                    payload: CancelJob { job_id: active },
                })?,
                None => tracing::warn!(
                    file = %target.file.id,
                    params = %fact.0.params,
                    "a duplicate_active_entity rejection names no active job of this file; \
                     nothing to cancel"
                ),
            }
        }
        Ok(())
    })
}

/// A reason as the end records it: never blank (the table requires one).
fn non_blank<'a>(reason: &'a str, fallback: &'a str) -> &'a str {
    if reason.trim().is_empty() {
        fallback
    } else {
        reason
    }
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

/// Jobs failed the job: the runner's reported code, else Jobs' cause.
pub fn on_failed<'r, H: DriveHost>(
    cx: &'r mut Reaction<'r>,
    fact: FailedFact,
) -> BoxFuture<'r, Result<(), DriveReactionFault>> {
    Box::pin(async move {
        let Some(mut target) = target::<H>(cx, fact.0.job_id).await? else {
            return Ok(());
        };
        let cause = non_blank(&fact.0.failure_cause, super::log::kind::FAILED);
        let reason = fact
            .0
            .failure_report
            .as_ref()
            .map(|report| non_blank(&report.reason_code, cause))
            .unwrap_or(cause);
        record_end(
            cx,
            &mut target,
            EndKind::Failed,
            Some(reason),
            fact.0.note.as_deref(),
        )
        .await?;
        Ok(())
    })
}

/// Jobs cancelled the job: the file is FAILED `cancelled`.
pub fn on_cancelled<'r, H: DriveHost>(
    cx: &'r mut Reaction<'r>,
    fact: CancelledFact,
) -> BoxFuture<'r, Result<(), DriveReactionFault>> {
    Box::pin(async move {
        let Some(mut target) = target::<H>(cx, fact.0.job_id).await? else {
            return Ok(());
        };
        record_end(cx, &mut target, EndKind::Cancelled, None, None).await?;
        Ok(())
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
