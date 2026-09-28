use contract_jobs::command::{CancelJob, CreateJob, FailJob, FinishJob};
use service_engine::BlobRef;
use service_engine::error::EngineError;
use service_engine::impact::Dims;
use service_engine::pipeline::Ops;
use uuid::Uuid;

use super::commands::{Initiator, JobCancel, JobCreate, JobFail, JobFinish};
use super::log::{FileJob, append, entry, insert_job, kind};
use super::roots::roots;
use crate::fault::{DriveFault, codes};
use crate::file::images::drop_images;
use crate::file::{FileCause, FileRow, Page, PageKey, processed, store};
use crate::host::DriveHost;
use crate::ruleset::{RulesetRow, RulesetStep, Trigger};

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

/// Reads the file's status again after the library wrote its job log in this
/// transaction, so the rest of the gesture sees what the view now computes.
pub(crate) async fn refresh_status<H>(
    cx: &mut Ops<'_>,
    file: &mut FileRow<H>,
) -> Result<(), DriveFault> {
    if let Some(status) = store::status_of(cx.connection(), file.id).await? {
        file.status = status;
    }
    Ok(())
}

/// Starts step `index` of the file's snapshot: a new job in the file's log and
/// its `job.create`, staged through the outbox. `None` past the last step.
async fn launch_step<H: DriveHost>(
    cx: &mut Ops<'_>,
    file: &mut FileRow<H>,
    index: usize,
    trigger: Option<Trigger>,
    initiator: Option<Initiator>,
) -> Result<Option<Uuid>, DriveFault> {
    let steps = file.steps.clone().unwrap_or_default();
    let Some(step) = steps.get(index) else {
        return Ok(None);
    };
    let step_index = i32::try_from(index).map_err(|_| {
        DriveFault::Engine(EngineError::Config("a chain step index overflows".into()))
    })?;
    let roots = roots::<H>()?;
    let job = FileJob {
        job_id: Uuid::now_v7(),
        step_index,
        trigger,
        triggered_by: initiator,
        events: Vec::new(),
        created_at: cx.now().as_datetime(),
    };
    let config = serde_json::json!({
        "host": H::SERVICE,
        "file_id": file.id,
        "job_id": job.job_id,
        "context_root": roots.context_root,
        "image_upload_root": roots.image_upload_root,
        "report_root": roots.report_root,
        "step": index,
        "options": step.options,
    });
    let initiator = job.triggered_by.clone().unwrap_or(Initiator {
        id: file.created_by,
        display_name: None,
    });
    insert_job(cx.connection(), file.id, &job)
        .await
        .map_err(live_job_taken)?;
    // The chain is a host-side sequence correlated by the file id: no step names
    // a parent. Jobs refuses a terminal parent, and a live one would make the
    // step the parent runner's work instead of the host's.
    cx.command(JobCreate {
        payload: CreateJob {
            job_id: job.job_id,
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
    let job_id = job.job_id;
    refresh_status(cx, file).await?;
    Ok(Some(job_id))
}

/// A second unsettled job for one file violates `file_job_one_live_idx`: the
/// file is already processing. The gestures' row lock prevents it; this keeps
/// the answer a code should it ever happen.
fn live_job_taken(error: EngineError) -> DriveFault {
    match &error {
        EngineError::Db(sqlx::Error::Database(db)) if db.is_unique_violation() => {
            DriveFault::Refused(codes::FILE_PROCESSING)
        }
        _ => DriveFault::Engine(error),
    }
}

/// Starts a chain on a READY or FAILED file: the plan becomes the file's
/// snapshot and its first step a job. Nothing of the previous results is
/// touched — they stay readable while the new run reports over them.
pub async fn start_chain<H: DriveHost>(
    cx: &mut Ops<'_>,
    file: &mut FileRow<H>,
    plan: ChainPlan,
    trigger: Trigger,
    initiator: Initiator,
) -> Result<(), DriveFault> {
    file.ruleset_id = plan.ruleset_id;
    file.steps = Some(plan.steps);
    file.updated_at = cx.now().as_datetime();
    cx.save(file).await?;
    let Some(job_id) = launch_step(cx, file, 0, Some(trigger), Some(initiator)).await? else {
        return Err(DriveFault::Refused(codes::INVALID_RULESET));
    };
    crate::file::file_changed::<H>(cx, file, FileCause::ProcessingStarted { job_id, step: 0 })?;
    Ok(())
}

/// The runner's final report ended `job`, the file's running job, in the
/// report's transaction: `reported_done` is logged (the job settles), Jobs is
/// told (`job.finish`), and the chain moves on — the next step's job, or the
/// end of the chain (READY). Jobs' own `completed` then only confirms it. A
/// user's cancel that crossed the report stops the chain there: the next step
/// is recorded as never started, `cancelled`; on the last step there is
/// nothing left to stop and the file is READY.
pub(crate) async fn report_done<H: DriveHost>(
    cx: &mut Ops<'_>,
    file: &mut FileRow<H>,
    job: &FileJob,
) -> Result<(), DriveFault> {
    let now = cx.now().as_datetime();
    append(
        cx.connection(),
        job.job_id,
        entry(kind::REPORTED_DONE, now, serde_json::json!({}))?,
    )
    .await?;
    finish_job(cx, job.job_id)?;
    let next = usize::try_from(job.step_index).unwrap_or(0) + 1;
    let has_next = file.steps.as_ref().is_some_and(|steps| next < steps.len());
    if job.cancel_requested() && has_next {
        return stop_before(cx, file, next, job).await;
    }
    let cause = match launch_step(cx, file, next, job.trigger, job.triggered_by.clone()).await? {
        Some(job_id) => FileCause::ProcessingStarted {
            job_id,
            step: i32::try_from(next).unwrap_or(i32::MAX),
        },
        None => {
            refresh_status(cx, file).await?;
            conclude::<H>(cx, file).await?;
            FileCause::ProcessingFinished
        }
    };
    file.updated_at = now;
    cx.save(file).await?;
    crate::file::file_changed::<H>(cx, file, cause)?;
    Ok(())
}

/// The chain is over: the pages numbered above the file's page count go —
/// edited ones included — and so do the images no page references any more.
/// What the run did not report again stays as it was.
async fn conclude<H: DriveHost>(cx: &mut Ops<'_>, file: &FileRow<H>) -> Result<(), DriveFault> {
    if let Some(page_count) = file.page_count {
        // A trimmed page leaves its live windows: the engine delivers the
        // `DriveRemove` by repopulation, without a cause.
        for number in processed::trim_pages(cx.connection(), file.id, page_count).await? {
            cx.impact::<Page>(
                &PageKey {
                    file_id: file.id,
                    number,
                },
                Dims::ALL,
            )?;
        }
    }
    let unreferenced = processed::unreferenced_images(cx.connection(), file.id).await?;
    if unreferenced.is_empty() {
        return Ok(());
    }
    for reference in drop_images(cx.connection(), file.id, &unreferenced).await? {
        cx.release_blob(BlobRef(reference))?;
    }
    crate::file::file_changed::<H>(
        cx,
        file,
        FileCause::ImagesDropped {
            names: unreferenced,
        },
    )?;
    Ok(())
}

/// The runner declared `job`, the file's running job, failed: the declaration
/// is logged with its reason (the job settles, the file is FAILED with that
/// reason) and Jobs is told (`job.fail`). The results reported so far stay.
pub(crate) async fn report_failed<H: DriveHost>(
    cx: &mut Ops<'_>,
    file: &mut FileRow<H>,
    job: &FileJob,
    reason_code: &str,
    message: Option<&str>,
) -> Result<(), DriveFault> {
    let now = cx.now().as_datetime();
    append(
        cx.connection(),
        job.job_id,
        entry(
            kind::REPORTED_FAILED,
            now,
            serde_json::json!({ "reason_code": reason_code, "message": message }),
        )?,
    )
    .await?;
    let note = match message {
        Some(message) => format!("{reason_code}: {message}"),
        None => reason_code.to_string(),
    };
    cx.command(JobFail {
        payload: FailJob {
            job_id: job.job_id,
            note: Some(note),
        },
    })?;
    refresh_status(cx, file).await?;
    file.updated_at = now;
    cx.save(file).await?;
    crate::file::file_changed::<H>(
        cx,
        file,
        FileCause::ProcessingFailed {
            reason: reason_code.to_string(),
        },
    )?;
    Ok(())
}

/// Records step `next` as cancelled before it started: a row of its own, never
/// sent to Jobs, whose only entry is the library's `cancelled`.
async fn stop_before<H: DriveHost>(
    cx: &mut Ops<'_>,
    file: &mut FileRow<H>,
    next: usize,
    ended: &FileJob,
) -> Result<(), DriveFault> {
    let now = cx.now().as_datetime();
    let stopped = FileJob {
        job_id: Uuid::now_v7(),
        step_index: i32::try_from(next).unwrap_or(i32::MAX),
        trigger: ended.trigger,
        triggered_by: ended.triggered_by.clone(),
        events: vec![entry(
            kind::CANCELLED,
            now,
            serde_json::json!({ "before_start": true }),
        )?],
        created_at: now,
    };
    insert_job(cx.connection(), file.id, &stopped)
        .await
        .map_err(live_job_taken)?;
    refresh_status(cx, file).await?;
    file.updated_at = now;
    cx.save(file).await?;
    crate::file::file_changed::<H>(
        cx,
        file,
        FileCause::ProcessingFailed {
            reason: super::CANCELLED.to_string(),
        },
    )?;
    Ok(())
}

/// Asks Jobs to cancel the job running on the file, if any.
pub fn cancel_active_job<H: DriveHost>(
    cx: &mut Ops<'_>,
    file: &FileRow<H>,
) -> Result<(), DriveFault> {
    if let Some(job) = file.active_job() {
        cx.command(JobCancel {
            payload: CancelJob { job_id: job.job_id },
        })?;
    }
    Ok(())
}

fn finish_job(cx: &mut Ops<'_>, job_id: Uuid) -> Result<(), DriveFault> {
    cx.command(JobFinish {
        payload: FinishJob { job_id },
    })?;
    Ok(())
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
