use chrono::SubsecRound;
use contract_jobs::event::{
    JobCancelled, JobCompleted, JobCreationRejected, JobFailed, JobPlanDeclared, JobQueued,
    JobStarted, JobStepStarted,
};
use futures_util::future::BoxFuture;
use service_engine::inbound::{ReactionCoordinates, ReactionMessage};
use service_engine::pipeline::Reaction;
use uuid::Uuid;

use super::CANCELLED;
use super::chain::{advance, file_of_job, mark_failed};
use crate::fault::DriveReactionFault;
use crate::file::{File, FileCause, FileRow, ProcessingState};
use crate::host::DriveHost;

async fn active_file<H: DriveHost>(
    cx: &mut Reaction<'_>,
    job_id: Uuid,
) -> Result<Option<FileRow<H>>, DriveReactionFault> {
    let Some(file_id) = file_of_job(cx.connection(), job_id).await? else {
        return Ok(None);
    };
    let file = cx.load::<FileRow<H>>(&file_id).await?;
    Ok(file.filter(|file| {
        file.job_id == Some(job_id) && file.processing_state == ProcessingState::Processing
    }))
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

pub const DURABLE_QUEUED: &str = "job-queued";
pub const DURABLE_CREATION_REJECTED: &str = "job-creation-rejected";
pub const DURABLE_STARTED: &str = "job-started";
pub const DURABLE_PLAN_DECLARED: &str = "job-plan-declared";
pub const DURABLE_STEP_STARTED: &str = "job-step-started";
pub const DURABLE_COMPLETED: &str = "job-completed";
pub const DURABLE_FAILED: &str = "job-failed";
pub const DURABLE_CANCELLED: &str = "job-cancelled";

async fn fail_file<H: DriveHost>(
    cx: &mut Reaction<'_>,
    mut file: FileRow<H>,
    reason: &str,
) -> Result<(), DriveReactionFault> {
    mark_failed(&mut file, reason);
    file.updated_at = cx.now().as_datetime();
    cx.save(&file).await?;
    cx.impact_caused::<File, _>(
        &file.id,
        FileCause::ProcessingFailed {
            reason: reason.to_string(),
        },
    )?;
    Ok(())
}

pub fn on_queued<'r>(
    cx: &'r mut Reaction<'r>,
    fact: QueuedFact,
) -> BoxFuture<'r, Result<(), DriveReactionFault>> {
    Box::pin(async move {
        if let Some(file) = file_of_job(cx.connection(), fact.0.job_id).await? {
            tracing::debug!(%file, job = %fact.0.job_id, runner_type = %fact.0.runner_type, "job queued");
        }
        Ok(())
    })
}

pub fn on_started<'r>(
    cx: &'r mut Reaction<'r>,
    fact: StartedFact,
) -> BoxFuture<'r, Result<(), DriveReactionFault>> {
    Box::pin(async move {
        if let Some(file) = file_of_job(cx.connection(), fact.0.job_id).await? {
            tracing::debug!(%file, job = %fact.0.job_id, run = %fact.0.run_id, "job started");
        }
        Ok(())
    })
}

pub fn on_creation_rejected<'r, H: DriveHost>(
    cx: &'r mut Reaction<'r>,
    fact: CreationRejectedFact,
) -> BoxFuture<'r, Result<(), DriveReactionFault>> {
    Box::pin(async move {
        let Some(file) = active_file::<H>(cx, fact.0.job_id).await? else {
            return Ok(());
        };
        fail_file(cx, file, &fact.0.reason_code).await
    })
}

pub fn on_plan_declared<'r, H: DriveHost>(
    cx: &'r mut Reaction<'r>,
    fact: PlanDeclaredFact,
) -> BoxFuture<'r, Result<(), DriveReactionFault>> {
    Box::pin(async move {
        let Some(mut file) = active_file::<H>(cx, fact.0.job_id).await? else {
            return Ok(());
        };
        if file.plan.as_deref() == Some(fact.0.steps.as_slice()) {
            return Ok(());
        }
        file.plan = Some(fact.0.steps);
        file.updated_at = cx.now().as_datetime();
        cx.save(&file).await?;
        cx.impact_caused::<File, _>(&file.id, FileCause::ProgressChanged)?;
        Ok(())
    })
}

pub fn on_step_started<'r, H: DriveHost>(
    cx: &'r mut Reaction<'r>,
    fact: StepStartedFact,
) -> BoxFuture<'r, Result<(), DriveReactionFault>> {
    Box::pin(async move {
        let Some(mut file) = active_file::<H>(cx, fact.0.job_id).await? else {
            return Ok(());
        };
        let index = i32::try_from(fact.0.index).unwrap_or(i32::MAX);
        // The row stores microseconds; the wire may carry nanoseconds. Compared
        // at the row's precision, a redelivered fact is never "newer".
        let started_at = fact.0.started_at.trunc_subsecs(6);
        // A later index on the same run moves the cursor forward; a newer start
        // instant (a retry attempt restarting the plan) moves it too. Anything
        // else is a redelivery or a stale fact.
        let forward = match (file.progress_index, file.progress_at) {
            (Some(current), Some(at)) => index > current || started_at > at,
            _ => true,
        };
        if !forward {
            return Ok(());
        }
        file.progress_index = Some(index);
        file.progress_label = Some(fact.0.label);
        file.progress_at = Some(started_at);
        file.updated_at = cx.now().as_datetime();
        cx.save(&file).await?;
        cx.impact_caused::<File, _>(&file.id, FileCause::ProgressChanged)?;
        Ok(())
    })
}

pub fn on_completed<'r, H: DriveHost>(
    cx: &'r mut Reaction<'r>,
    fact: CompletedFact,
) -> BoxFuture<'r, Result<(), DriveReactionFault>> {
    Box::pin(async move {
        let Some(mut file) = active_file::<H>(cx, fact.0.job_id).await? else {
            return Ok(());
        };
        if file.done_at.is_none() {
            // Jobs says the job is over but the runner's final report has not
            // landed: the fact is kept on the row and the report advances the
            // chain when it comes.
            if file.completed_at.is_none() {
                file.completed_at = Some(cx.now().as_datetime());
                cx.save(&file).await?;
            }
            return Ok(());
        }
        advance(cx, &mut file, fact.0.job_id).await?;
        Ok(())
    })
}

pub fn on_failed<'r, H: DriveHost>(
    cx: &'r mut Reaction<'r>,
    fact: FailedFact,
) -> BoxFuture<'r, Result<(), DriveReactionFault>> {
    Box::pin(async move {
        let Some(file) = active_file::<H>(cx, fact.0.job_id).await? else {
            return Ok(());
        };
        let reason = fact
            .0
            .failure_report
            .as_ref()
            .map(|report| report.reason_code.clone())
            .unwrap_or(fact.0.failure_cause);
        fail_file(cx, file, &reason).await
    })
}

pub fn on_cancelled<'r, H: DriveHost>(
    cx: &'r mut Reaction<'r>,
    fact: CancelledFact,
) -> BoxFuture<'r, Result<(), DriveReactionFault>> {
    Box::pin(async move {
        let Some(file) = active_file::<H>(cx, fact.0.job_id).await? else {
            return Ok(());
        };
        fail_file(cx, file, CANCELLED).await
    })
}
