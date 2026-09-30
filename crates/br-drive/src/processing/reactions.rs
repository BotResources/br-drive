use contract_jobs::command::CancelJob;
use contract_jobs::event::{
    JobCancelled, JobCompleted, JobCreationRejected, JobFailed, JobPlanDeclared, JobQueued,
    JobStarted, JobStepStarted, REASON_DUPLICATE_ACTIVE_ENTITY,
};
use futures_util::future::BoxFuture;
use service_engine::inbound::{ReactionCoordinates, ReactionMessage};
use service_engine::pipeline::Reaction;
use uuid::Uuid;

use super::commands::JobCancel;
use super::state::{self, Applied, CANCELLED, FileProcessing, Received};
use crate::facts::{self, FactMeta};
use crate::fault::DriveReactionFault;
use crate::file::{FileCause, FileRow};
use crate::host::DriveHost;

/// Records a Jobs fact about `job_id` on the processing of the file that job
/// belongs to — the file loaded first, so locked: a concurrent fact of the
/// same file waits — and stages what it moved: the file's status when it
/// ended the running job (`updatedAt` moves), its progress when it moved what
/// the runner says (`ProgressChanged`, never on the host object: progress
/// changes no count), nothing otherwise. A fact that changes nothing any more
/// is still recorded, as `JobFactIgnored`. A job no file holds (a deleted
/// file, a job the library never created) is acknowledged and not recorded:
/// there is no processing to record it on. Answers the file when the fact
/// ended its running job.
async fn receive<H: DriveHost>(
    cx: &mut Reaction<'_>,
    job_id: Uuid,
    fact: Received,
) -> Result<Option<FileRow<H>>, DriveReactionFault> {
    let Some(file_id) = state::file_of_job(cx.connection(), job_id).await? else {
        tracing::debug!(%job_id, "a Jobs fact about a job no file holds; not recorded");
        return Ok(None);
    };
    let Some(mut file) = cx.load::<FileRow<H>>(&file_id).await? else {
        return Ok(None);
    };
    let Some(mut processing) = cx.load::<FileProcessing<H>>(&file_id).await? else {
        return Ok(None);
    };
    let meta = FactMeta::of_reaction(cx);
    let applied = processing.receive(job_id, fact, &meta);
    facts::save(cx, &mut processing).await?;
    file.status = processing.status();
    file.updated_at = file.updated_at.max(processing.updated_at);
    match applied {
        Applied::Ended => {
            let reason = file.processing_error().unwrap_or(CANCELLED).to_string();
            crate::file::file_changed::<H>(cx, &file, FileCause::ProcessingFailed { reason })?;
            Ok(Some(file))
        }
        Applied::Progressed => {
            crate::file::file_progressed::<H>(cx, &file)?;
            Ok(None)
        }
        Applied::Noted | Applied::Ignored => Ok(None),
    }
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

/// Jobs queued the job.
pub fn on_queued<'r, H: DriveHost>(
    cx: &'r mut Reaction<'r>,
    fact: QueuedFact,
) -> BoxFuture<'r, Result<(), DriveReactionFault>> {
    Box::pin(async move {
        let JobQueued {
            job_id,
            runner_type,
        } = fact.0;
        receive::<H>(cx, job_id, Received::Queued { runner_type }).await?;
        Ok(())
    })
}

/// A run of the job started.
pub fn on_started<'r, H: DriveHost>(
    cx: &'r mut Reaction<'r>,
    fact: StartedFact,
) -> BoxFuture<'r, Result<(), DriveReactionFault>> {
    Box::pin(async move {
        let JobStarted { job_id, run_id } = fact.0;
        receive::<H>(cx, job_id, Received::Started { run_id }).await?;
        Ok(())
    })
}

/// Jobs completed the job — only ever after the library's own `job.finish`,
/// sent in the transaction of the runner's final report, which already ended
/// the job and moved the chain on: recorded, it moves nothing.
pub fn on_completed<'r, H: DriveHost>(
    cx: &'r mut Reaction<'r>,
    fact: CompletedFact,
) -> BoxFuture<'r, Result<(), DriveReactionFault>> {
    Box::pin(async move {
        receive::<H>(cx, fact.0.job_id, Received::Completed).await?;
        Ok(())
    })
}

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
        receive::<H>(
            cx,
            job_id,
            Received::PlanDeclared {
                run_id,
                labels: steps,
            },
        )
        .await?;
        Ok(())
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
        receive::<H>(
            cx,
            job_id,
            Received::StepStarted {
                run_id,
                index,
                label,
                started_at,
            },
        )
        .await?;
        Ok(())
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
        let JobCreationRejected {
            job_id,
            reason_code,
            params,
        } = fact.0;
        let reason = non_blank(&reason_code, super::state::kind::CREATION_REJECTED).to_string();
        let ended = receive::<H>(
            cx,
            job_id,
            Received::CreationRejected {
                reason_code: reason,
                params: params.clone(),
            },
        )
        .await?;
        if let Some(file) = ended
            && reason_code == REASON_DUPLICATE_ACTIVE_ENTITY
        {
            match active_job_of::<H>(&params, file.id) {
                Some(active) => cx.command(JobCancel {
                    payload: CancelJob { job_id: active },
                })?,
                None => tracing::warn!(
                    file = %file.id,
                    params = %params,
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
        let JobFailed {
            job_id,
            failure_cause,
            failure_report,
            note,
        } = fact.0;
        let cause = non_blank(&failure_cause, super::state::kind::FAILED).to_string();
        let reason = failure_report
            .as_ref()
            .map(|report| non_blank(&report.reason_code, &cause).to_string())
            .unwrap_or_else(|| cause.clone());
        receive::<H>(
            cx,
            job_id,
            Received::Failed {
                failure_cause: cause,
                reason_code: reason,
                note,
            },
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
        receive::<H>(cx, fact.0.job_id, Received::Cancelled).await?;
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
