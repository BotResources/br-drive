use contract_jobs::command::{CancelJob, CreateJob, FailJob, FinishJob};
use service_engine::BlobRef;
use service_engine::error::EngineError;
use service_engine::impact::Dims;
use service_engine::pipeline::Ops;
use uuid::Uuid;

use super::commands::{Initiator, JobCancel, JobCreate, JobFail, JobFinish};
use super::roots::roots;
use super::state::{CANCELLED, FileProcessing};
use crate::facts::{self, FactMeta};
use crate::fault::{DriveFault, codes};
use crate::file::images::drop_images;
use crate::file::{FileCause, FileEvent, FileRow, Page, PageKey, processed, store};
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

/// The file's processing, locked: the stored one, or a blank for a file never
/// processed (saved only once a chain gave it a job).
pub(crate) async fn load_processing<H: DriveHost>(
    cx: &mut Ops<'_>,
    file: &FileRow<H>,
) -> Result<FileProcessing<H>, DriveFault> {
    Ok(match cx.load::<FileProcessing<H>>(&file.id).await? {
        Some(processing) => processing,
        None => FileProcessing::blank(file.id, cx.now().as_datetime()),
    })
}

/// Saves the processing and lets the rest of the gesture read the file as it
/// now is: its status, and its last change.
async fn save_processing<H: DriveHost>(
    cx: &mut Ops<'_>,
    file: &mut FileRow<H>,
    processing: &mut FileProcessing<H>,
) -> Result<(), DriveFault> {
    facts::save(cx, processing).await?;
    file.status = processing.status();
    file.updated_at = file.updated_at.max(processing.updated_at);
    Ok(())
}

/// Starts step `index` of the file's snapshot: the file's running job and its
/// `job.create`, staged through the outbox. `None` past the last step.
async fn launch_step<H: DriveHost>(
    cx: &mut Ops<'_>,
    meta: &FactMeta,
    file: &FileRow<H>,
    processing: &mut FileProcessing<H>,
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
    let job_id = Uuid::now_v7();
    let config = serde_json::json!({
        "host": H::SERVICE,
        "file_id": file.id,
        "job_id": job_id,
        "context_root": roots.context_root,
        "image_upload_root": roots.image_upload_root,
        "report_root": roots.report_root,
        "failure_root": roots.failure_root,
        "step": index,
        "options": step.options,
    });
    let named = initiator.clone().unwrap_or(Initiator {
        id: file.created_by,
        display_name: None,
    });
    processing.create_job(
        job_id,
        step_index,
        step.runner_type.clone(),
        trigger,
        initiator,
        meta,
    );
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
            triggered_by: named.triggered_by(),
            source_bc: Some(H::SERVICE.to_string()),
            source_entity_id: Some(file.id),
            max_attempts: None,
        },
    })?;
    Ok(Some(job_id))
}

/// Starts a chain on a READY or FAILED file: the plan becomes the file's
/// snapshot and its first step a job. Nothing of the previous results is
/// touched — they stay readable while the new run reports over them.
pub(crate) async fn start_chain<H: DriveHost>(
    cx: &mut Ops<'_>,
    meta: &FactMeta,
    file: &mut FileRow<H>,
    plan: ChainPlan,
    trigger: Trigger,
    initiator: Initiator,
) -> Result<(), DriveFault> {
    file.ruleset_id = plan.ruleset_id;
    file.steps = Some(plan.steps.clone());
    facts::save(cx, file).await?;
    let mut processing = load_processing(cx, file).await?;
    processing.start_chain(trigger, plan.ruleset_id, plan.steps, meta);
    let Some(job_id) = launch_step(
        cx,
        meta,
        file,
        &mut processing,
        0,
        Some(trigger),
        Some(initiator),
    )
    .await?
    else {
        return Err(DriveFault::Refused(codes::INVALID_RULESET));
    };
    save_processing(cx, file, &mut processing).await?;
    crate::file::file_changed::<H>(cx, file, FileCause::ProcessingStarted { job_id, step: 0 })?;
    Ok(())
}

/// The runner's final report ended the file's running job, in the report's
/// transaction: the job settles, Jobs is told (`job.finish`), and the chain
/// moves on — the next step's job, or the end of the chain (READY). Jobs' own
/// `completed` then only confirms it. A user's cancel that crossed the report
/// stops the chain there: the next step is skipped (`StepSkipped`), never
/// launched, and the file is FAILED `cancelled`; on the last step there is nothing left to stop and the file
/// is READY.
pub(crate) async fn report_done<H: DriveHost>(
    cx: &mut Ops<'_>,
    meta: &FactMeta,
    file: &mut FileRow<H>,
    processing: &mut FileProcessing<H>,
) -> Result<(), DriveFault> {
    let job_id = processing.job_id;
    let step_index = processing.step_index;
    let trigger = processing.trigger;
    let initiator = processing.triggered_by.clone();
    let cancel_requested = processing.cancel_requested();
    processing.report_done(meta);
    finish_job(cx, job_id)?;
    let next = usize::try_from(step_index).unwrap_or(0) + 1;
    let has_next = file.steps.as_ref().is_some_and(|steps| next < steps.len());
    if cancel_requested && has_next {
        let runner_type = file
            .steps
            .as_ref()
            .and_then(|steps| steps.get(next))
            .map(|step| step.runner_type.clone())
            .unwrap_or_default();
        processing.skip_step(i32::try_from(next).unwrap_or(i32::MAX), runner_type, meta);
        save_processing(cx, file, processing).await?;
        crate::file::file_changed::<H>(
            cx,
            file,
            FileCause::ProcessingFailed {
                reason: CANCELLED.to_string(),
            },
        )?;
        return Ok(());
    }
    let cause = match launch_step(cx, meta, file, processing, next, trigger, initiator).await? {
        Some(job_id) => FileCause::ProcessingStarted {
            job_id,
            step: i32::try_from(next).unwrap_or(i32::MAX),
        },
        None => {
            conclude::<H>(cx, meta, file).await?;
            FileCause::ProcessingFinished
        }
    };
    save_processing(cx, file, processing).await?;
    crate::file::file_changed::<H>(cx, file, cause)?;
    Ok(())
}

/// The chain is over: the pages numbered above the file's page count go —
/// edited ones included — and so do the images no page references any more.
/// What the run did not report again stays as it was.
async fn conclude<H: DriveHost>(
    cx: &mut Ops<'_>,
    meta: &FactMeta,
    file: &mut FileRow<H>,
) -> Result<(), DriveFault> {
    if let Some(page_count) = file.page_count {
        // A trimmed page leaves its live windows: the engine delivers the
        // `DriveRemove` by repopulation, without a cause.
        for number in store::trim_pages::<H>(cx.connection(), meta, file.id, page_count).await? {
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
    file.record(
        FileEvent::ImagesDropped {
            names: unreferenced.clone(),
        },
        meta,
    );
    facts::save(cx, file).await?;
    crate::file::file_changed::<H>(
        cx,
        file,
        FileCause::ImagesDropped {
            names: unreferenced,
        },
    )?;
    Ok(())
}

/// The runner declared the file's running job failed: the job settles, the
/// file is FAILED with that reason, and Jobs is told (`job.fail`). The results
/// reported so far stay.
pub(crate) async fn report_failed<H: DriveHost>(
    cx: &mut Ops<'_>,
    meta: &FactMeta,
    file: &mut FileRow<H>,
    processing: &mut FileProcessing<H>,
    reason_code: &str,
    message: Option<&str>,
) -> Result<(), DriveFault> {
    let job_id = processing.job_id;
    processing.report_failed(reason_code, message, meta);
    let note = match message {
        Some(message) => format!("{reason_code}: {message}"),
        None => reason_code.to_string(),
    };
    cx.command(JobFail {
        payload: FailJob {
            job_id,
            note: Some(note),
        },
    })?;
    save_processing(cx, file, processing).await?;
    crate::file::file_changed::<H>(
        cx,
        file,
        FileCause::ProcessingFailed {
            reason: reason_code.to_string(),
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
