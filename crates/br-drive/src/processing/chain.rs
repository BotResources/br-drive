use contract_jobs::command::{CancelJob, CreateJob, FinishJob};
use service_engine::BlobRef;
use service_engine::error::EngineError;
use service_engine::pipeline::Ops;
use uuid::Uuid;

use super::commands::{Initiator, JobCancel, JobCreate, JobFinish};
use super::log::{FileJob, insert_job};
use super::roots::roots;
use crate::fault::{DriveFault, codes};
use crate::file::images::drop_images;
use crate::file::store;
use crate::file::{FileCause, FileRow};
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
    insert_job(cx.connection(), file.id, &job).await?;
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

pub async fn start_chain<H: DriveHost>(
    cx: &mut Ops<'_>,
    file: &mut FileRow<H>,
    plan: ChainPlan,
    initiator: Initiator,
) -> Result<(), DriveFault> {
    file.ruleset_id = plan.ruleset_id;
    file.steps = Some(plan.steps);
    file.updated_at = cx.now().as_datetime();
    cx.save(file).await?;
    let Some(job_id) = launch_step(cx, file, 0, Some(initiator)).await? else {
        return Err(DriveFault::Refused(codes::INVALID_RULESET));
    };
    crate::file::file_changed::<H>(cx, file, FileCause::ProcessingStarted { job_id, step: 0 })?;
    Ok(())
}

/// The file's last job completed: the next step is launched, or the chain is
/// over and the file is READY (its last job's outcome is `completed`).
pub(crate) async fn advance<H: DriveHost>(
    cx: &mut Ops<'_>,
    file: &mut FileRow<H>,
    completed: &FileJob,
) -> Result<(), DriveFault> {
    let next = usize::try_from(completed.step_index).unwrap_or(0) + 1;
    let cause = match launch_step(cx, file, next, completed.triggered_by.clone()).await? {
        Some(job_id) => FileCause::ProcessingStarted {
            job_id,
            step: i32::try_from(next).unwrap_or(i32::MAX),
        },
        None => FileCause::ProcessingFinished,
    };
    file.updated_at = cx.now().as_datetime();
    cx.save(file).await?;
    crate::file::file_changed::<H>(cx, file, cause)?;
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

pub fn finish_job(cx: &mut Ops<'_>, job_id: Uuid) -> Result<(), DriveFault> {
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
