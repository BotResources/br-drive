use chrono::SubsecRound;
use contract_jobs::command::{CancelJob, CreateJob, FinishJob};
use service_engine::BlobRef;
use service_engine::error::EngineError;
use service_engine::pipeline::Ops;
use sqlx::PgConnection;
use uuid::Uuid;

use super::RUNNER_TYPE_UNAVAILABLE;
use super::backstop::{schedule_deadline, schedule_retry};
use super::commands::{Initiator, JobCancel, JobCreate, JobFinish};
use super::roots::roots;
use crate::catalogue;
use crate::fault::DriveFault;
use crate::file::images::drop_images;
use crate::file::store;
use crate::file::{FileCause, FileRow, ProcessingState};
use crate::host::DriveHost;
use crate::ruleset::{RulesetRow, RulesetStep};

fn merge_options(base: &serde_json::Value, extra: &serde_json::Value) -> serde_json::Value {
    let mut merged = match base {
        serde_json::Value::Object(map) => map.clone(),
        _ => serde_json::Map::new(),
    };
    if let serde_json::Value::Object(extra) = extra {
        for (key, value) in extra {
            merged.insert(key.clone(), value.clone());
        }
    }
    serde_json::Value::Object(merged)
}

/// What a chain runs: the rule it came from (none when a file replays its own
/// snapshot) and the steps, with the gesture's options merged into the first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainPlan {
    pub ruleset_id: Option<Uuid>,
    pub steps: Vec<RulesetStep>,
}

impl ChainPlan {
    pub fn from_ruleset(ruleset: &RulesetRow, first_options: Option<&serde_json::Value>) -> Self {
        let mut steps = ruleset.steps.clone();
        if let (Some(first), Some(extra)) = (steps.first_mut(), first_options) {
            first.options = merge_options(&first.options, extra);
        }
        Self {
            ruleset_id: Some(ruleset.id),
            steps,
        }
    }

    pub fn replay<H>(file: &FileRow<H>) -> Option<Self> {
        let steps = file.steps.clone().filter(|steps| !steps.is_empty())?;
        Some(Self {
            ruleset_id: file.ruleset_id,
            steps,
        })
    }
}

fn clear_run<H>(file: &mut FileRow<H>) {
    file.job_id = None;
    file.step_index = None;
    file.step_count = None;
    file.step_runner_type = None;
    file.step_entered_at = None;
    file.step_alive_at = None;
    file.run_started_at = None;
    file.plan = None;
    file.progress_index = None;
    file.progress_label = None;
    file.progress_at = None;
    file.done_at = None;
    file.completed_at = None;
}

pub(super) fn mark_failed<H>(file: &mut FileRow<H>, reason: &str) {
    clear_run(file);
    file.processing_state = ProcessingState::Failed;
    file.processing_error = Some(reason.to_string());
}

fn mark_ready<H>(file: &mut FileRow<H>) {
    clear_run(file);
    file.processing_state = ProcessingState::Ready;
    file.processing_error = None;
}

pub async fn wipe_rendition<H: DriveHost>(
    cx: &mut Ops<'_>,
    file: &mut FileRow<H>,
) -> Result<(), DriveFault> {
    store::delete_pages(cx.connection(), file.id).await?;
    let names = crate::file::images::image_names_of(cx.connection(), file.id).await?;
    for reference in drop_images(cx.connection(), file.id, &names).await? {
        cx.release_blob(BlobRef(reference))?;
    }
    file.summary = None;
    file.page_count = None;
    file.estimated_tokens = None;
    Ok(())
}

/// Where a launch left the chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Launched {
    /// A `job.create` was staged for the step.
    Job(Uuid),
    /// The step waits for the host's first catalogue scan; a retry is scheduled.
    Deferred,
    /// The chain is over: READY past the last step, FAILED when the step cannot run.
    Ended,
}

/// Enters step `index` of the file's snapshot: the step's clock starts, its
/// deadline is scheduled, and its job is staged (or its launch deferred).
async fn launch_step<H: DriveHost>(
    cx: &mut Ops<'_>,
    file: &mut FileRow<H>,
    index: usize,
) -> Result<Launched, DriveFault> {
    let steps = file.steps.clone().unwrap_or_default();
    let Some(step) = steps.get(index) else {
        mark_ready(file);
        return Ok(Launched::Ended);
    };
    clear_run(file);
    file.processing_state = ProcessingState::Processing;
    file.processing_error = None;
    file.step_index = Some(index as i32);
    file.step_count = Some(steps.len() as i32);
    file.step_runner_type = Some(step.runner_type.clone());
    // The step's identity is its index and this instant, compared at the
    // row's microsecond precision (the engine clock already is).
    let entered = cx.now().as_datetime().trunc_subsecs(6);
    file.step_entered_at = Some(entered);
    file.step_alive_at = Some(entered);
    let launched = stage_job(cx, file, 0).await?;
    if launched != Launched::Ended {
        schedule_deadline(cx, file)?;
    }
    Ok(launched)
}

/// Stages the job of the step the file is in. A step whose runner type the
/// host cannot vouch for yet — no catalogue scan ever completed — is deferred,
/// not failed: a fresh host is not an unavailable runner.
pub(super) async fn stage_job<H: DriveHost>(
    cx: &mut Ops<'_>,
    file: &mut FileRow<H>,
    attempt: u32,
) -> Result<Launched, DriveFault> {
    let (Some(index), Some(_)) = (file.step_index, file.step_entered_at) else {
        return Err(DriveFault::Engine(EngineError::Config(
            "a job is staged only for a file inside a step".into(),
        )));
    };
    let steps = file.steps.clone().unwrap_or_default();
    let Some(step) = usize::try_from(index).ok().and_then(|i| steps.get(i)) else {
        mark_ready(file);
        return Ok(Launched::Ended);
    };
    if !catalogue::scanned(cx.connection()).await? {
        if attempt == 0 {
            tracing::warn!(
                file = %file.id,
                runner_type = %step.runner_type,
                "no runner-type catalogue scan has completed on this host yet; the step's \
                 launch is deferred (start `br_drive::watch_runner_types` next to the engine)"
            );
        } else {
            tracing::debug!(file = %file.id, attempt, "the step's launch is deferred again");
        }
        schedule_retry(cx, file, attempt)?;
        return Ok(Launched::Deferred);
    }
    if !catalogue::is_active(cx.connection(), &step.runner_type).await? {
        mark_failed(file, RUNNER_TYPE_UNAVAILABLE);
        return Ok(Launched::Ended);
    }
    // A job Jobs may still hold on this file blocks every create on it
    // (`duplicate_active_entity`): it is cancelled before the new one is asked
    // for, and stays known until Jobs queues the new one — Jobs consumes the
    // cancel and the create on separate durables, so they may cross.
    if let Some(stray) = file.stray_job_id {
        cx.command(JobCancel {
            payload: CancelJob { job_id: stray },
        })?;
    }
    let roots = roots::<H>()?;
    let initiator = file.triggered_by.clone().unwrap_or(Initiator {
        id: file.created_by,
        display_name: None,
    });
    let job_id = Uuid::now_v7();
    let config = serde_json::json!({
        "host": H::SERVICE,
        "file_id": file.id,
        "job_id": job_id,
        "context_root": roots.context_root,
        "image_upload_root": roots.image_upload_root,
        "report_root": roots.report_root,
        "step": index,
        "options": step.options,
    });
    file.job_id = Some(job_id);
    // The pickup deadline runs from this job's creation until Jobs reports its
    // run started: a deferral or a relaunch before it does not eat into it.
    file.step_alive_at = Some(cx.now().as_datetime());
    file.run_started_at = None;
    // The chain is a host-side sequence correlated by the file id: no step names
    // a parent. Jobs refuses a terminal parent, and a live one would make the
    // step the parent runner's work instead of the host's.
    cx.command(JobCreate {
        payload: CreateJob {
            job_id,
            runner_type: step.runner_type.clone(),
            producer: H::SERVICE.to_string(),
            config: Some(config),
            parent_job_id: None,
            triggered_by: initiator.triggered_by(),
            source_bc: Some(H::SERVICE.to_string()),
            source_entity_id: Some(file.id),
            max_attempts: None,
        },
    })?;
    Ok(Launched::Job(job_id))
}

pub(super) fn outcome_cause<H>(file: &FileRow<H>, launched: Launched, step: usize) -> FileCause {
    match launched {
        Launched::Job(job_id) => FileCause::ProcessingStarted {
            job_id,
            step: step as i32,
        },
        Launched::Deferred => FileCause::LaunchDeferred { step: step as i32 },
        Launched::Ended if file.processing_state == ProcessingState::Ready => {
            FileCause::ProcessingFinished
        }
        Launched::Ended => FileCause::ProcessingFailed {
            reason: file
                .processing_error
                .clone()
                .unwrap_or_else(|| RUNNER_TYPE_UNAVAILABLE.to_string()),
        },
    }
}

pub async fn start_chain<H: DriveHost>(
    cx: &mut Ops<'_>,
    file: &mut FileRow<H>,
    plan: ChainPlan,
    initiator: Initiator,
) -> Result<(), DriveFault> {
    file.ruleset_id = plan.ruleset_id;
    file.steps = Some(plan.steps);
    file.triggered_by = Some(initiator);
    file.updated_at = cx.now().as_datetime();
    let launched = launch_step(cx, file, 0).await?;
    cx.save(file).await?;
    crate::file::file_changed::<H>(cx, file, outcome_cause(file, launched, 0))?;
    Ok(())
}

/// The step whose job just completed is over and the runner reported `done`:
/// the next step is launched, or the chain lands READY.
pub async fn advance<H: DriveHost>(
    cx: &mut Ops<'_>,
    file: &mut FileRow<H>,
) -> Result<(), DriveFault> {
    // Jobs accepted and completed the step's job, so no stray job stood in
    // its way any more.
    file.stray_job_id = None;
    let next = file.step_index.unwrap_or(0) as usize + 1;
    let launched = launch_step(cx, file, next).await?;
    file.updated_at = cx.now().as_datetime();
    cx.save(file).await?;
    crate::file::file_changed::<H>(cx, file, outcome_cause(file, launched, next))?;
    Ok(())
}

/// Cancels every job Jobs may hold on the file: its live step's, and a stray
/// one the library had lost track of.
pub fn cancel_active_job<H: DriveHost>(
    cx: &mut Ops<'_>,
    file: &FileRow<H>,
) -> Result<(), DriveFault> {
    for job_id in [file.job_id, file.stray_job_id].into_iter().flatten() {
        cx.command(JobCancel {
            payload: CancelJob { job_id },
        })?;
    }
    Ok(())
}

pub fn finish_active_job<H: DriveHost>(
    cx: &mut Ops<'_>,
    file: &FileRow<H>,
) -> Result<(), DriveFault> {
    if let Some(job_id) = file.job_id {
        cx.command(JobFinish {
            payload: FinishJob { job_id },
        })?;
    }
    Ok(())
}

pub async fn file_of_job(
    conn: &mut PgConnection,
    job_id: Uuid,
) -> Result<Option<Uuid>, EngineError> {
    let id: Option<Uuid> = sqlx::query_scalar("SELECT id FROM drive.file WHERE job_id = $1")
        .bind(job_id)
        .fetch_optional(conn)
        .await?;
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ruleset(steps: Vec<RulesetStep>) -> RulesetRow {
        RulesetRow {
            id: Uuid::now_v7(),
            name: "regen".into(),
            trigger: crate::ruleset::Trigger::RegeneratePage,
            media_types: vec!["*".into()],
            steps,
            is_default: true,
            created_by: Uuid::now_v7(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn the_first_steps_options_are_merged_with_the_gestures_own() {
        let ruleset = ruleset(vec![
            RulesetStep {
                runner_type: "render".into(),
                options: serde_json::json!({ "dpi": 300 }),
            },
            RulesetStep {
                runner_type: "index".into(),
                options: serde_json::json!({}),
            },
        ]);
        let plan =
            ChainPlan::from_ruleset(&ruleset, Some(&serde_json::json!({ "page": 3, "dpi": 72 })));
        assert_eq!(plan.ruleset_id, Some(ruleset.id));
        assert_eq!(
            plan.steps[0].options,
            serde_json::json!({ "dpi": 72, "page": 3 }),
            "the gesture's keys win over the rule's"
        );
        assert_eq!(plan.steps[1].options, serde_json::json!({}));
    }
}
