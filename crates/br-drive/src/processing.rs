use std::any::TypeId;
use std::collections::HashMap;
use std::sync::Mutex;

use br_core_integration::CommandCoords;
use contract_jobs::command::{CancelJob, CreateJob, FinishJob, TriggeredBy};
use contract_jobs::event::{
    JobCancelled, JobCompleted, JobCreationRejected, JobFailed, JobPlanDeclared, JobQueued,
    JobStarted, JobStepStarted,
};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::BlobRef;
use service_engine::error::EngineError;
use service_engine::graphql::RootPrefix;
use service_engine::inbound::{ReactionCoordinates, ReactionMessage};
use service_engine::pipeline::{Ops, OutboundCommand, Reaction};
use sqlx::PgConnection;
use uuid::Uuid;

use crate::catalogue;
use crate::fault::{DriveFault, DriveReactionFault};
use crate::file::images::drop_images;
use crate::file::store;
use crate::file::{File, FileCause, FileRow, ProcessingState};
use crate::host::DriveHost;
use crate::ruleset::{RulesetRow, RulesetStep};

pub const RUNNER_TYPE_UNAVAILABLE: &str = "runner_type_unavailable";
pub const CATALOGUE_NOT_WATCHED: &str = "catalogue_not_watched";
pub const CANCELLED: &str = "cancelled";

/// The name of one of the library's durables on the host's NATS consumers: every
/// host service gets its own consumer on the shared integration streams, so two
/// hosts on one cluster never split the facts between them.
pub fn durable(service: &str, suffix: &str) -> String {
    format!("{service}-drive-{suffix}")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootNames {
    pub context_root: String,
    pub image_upload_root: String,
    pub report_root: String,
}

impl RootNames {
    pub fn for_prefix(prefix: &'static str) -> Result<Self, EngineError> {
        let prefix = RootPrefix::from_snake(prefix)?;
        let camel = prefix.as_str();
        Ok(Self {
            context_root: format!("{camel}RunnerContext"),
            image_upload_root: format!("{camel}RunnerRequestImageUpload"),
            report_root: format!("{camel}RunnerReport"),
        })
    }
}

static ROOTS: Mutex<Option<HashMap<TypeId, RootNames>>> = Mutex::new(None);

pub fn declare_roots<H: DriveHost>(prefix: &'static str) -> Result<(), EngineError> {
    let roots = RootNames::for_prefix(prefix)?;
    let mut declared = ROOTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let declared = declared.get_or_insert_with(HashMap::new);
    match declared.get(&TypeId::of::<H>()) {
        Some(existing) if *existing == roots => Ok(()),
        Some(existing) => Err(EngineError::Config(format!(
            "the drive slice is already registered for this principal under the root names \
             {existing:?}; one drive slice per host principal"
        ))),
        None => {
            declared.insert(TypeId::of::<H>(), roots);
            Ok(())
        }
    }
}

fn roots<H: DriveHost>() -> Result<RootNames, EngineError> {
    ROOTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .as_ref()
        .and_then(|declared| declared.get(&TypeId::of::<H>()).cloned())
        .ok_or_else(|| {
            EngineError::Config(
                "the drive slice's root names are not declared for this principal; register the \
                 slice through `drive_slice!` before starting a processing chain"
                    .into(),
            )
        })
}

macro_rules! outgoing {
    ($name:ident, $payload:ty, $coords:path) => {
        #[derive(Serialize)]
        pub struct $name {
            #[serde(flatten)]
            pub payload: $payload,
        }

        impl OutboundCommand for $name {
            fn coords(&self) -> CommandCoords {
                $coords().expect("the published jobs coordinates are valid")
            }

            fn command_id(&self) -> Uuid {
                Uuid::now_v7()
            }
        }
    };
}

outgoing!(
    JobCreate,
    CreateJob,
    contract_jobs::cmd_job_create_v1_coords
);
outgoing!(
    JobFinish,
    FinishJob,
    contract_jobs::cmd_job_finish_v2_coords
);
outgoing!(
    JobCancel,
    CancelJob,
    contract_jobs::cmd_job_cancel_v2_coords
);

/// The principal whose gesture started a chain; every step of the chain names
/// it as the job's `triggered_by`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Initiator {
    pub id: Uuid,
    pub display_name: Option<String>,
}

impl Initiator {
    pub fn of<H: DriveHost>(principal: &H) -> Self {
        Self {
            id: principal.id().as_uuid(),
            display_name: principal.display_name(),
        }
    }

    fn triggered_by(&self) -> TriggeredBy {
        match &self.display_name {
            Some(display_name) => TriggeredBy::Identified {
                id: self.id,
                display_name: display_name.clone(),
            },
            None => TriggeredBy::Anonymous(self.id),
        }
    }
}

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
    file.plan = None;
    file.progress_index = None;
    file.progress_label = None;
    file.progress_at = None;
    file.done_at = None;
    file.completed_at = None;
}

pub(crate) fn mark_failed<H>(file: &mut FileRow<H>, reason: &str) {
    clear_run(file);
    file.processing_state = ProcessingState::Failed;
    file.processing_error = Some(reason.to_string());
}

fn mark_ready<H>(file: &mut FileRow<H>) {
    clear_run(file);
    file.processing_state = ProcessingState::Ready;
    file.processing_error = None;
}

pub(crate) async fn wipe_rendition<H: DriveHost>(
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

/// Launches step `index` of the file's snapshot: `Ok(true)` when a job was
/// staged, `Ok(false)` when the chain ended instead (READY past the last step,
/// FAILED when the step cannot run).
async fn launch_step<H: DriveHost>(
    cx: &mut Ops<'_>,
    file: &mut FileRow<H>,
    index: usize,
    parent_job_id: Option<Uuid>,
) -> Result<bool, DriveFault> {
    let steps = file.steps.clone().unwrap_or_default();
    let Some(step) = steps.get(index) else {
        mark_ready(file);
        return Ok(false);
    };
    if !catalogue::scanned(cx.connection()).await? {
        tracing::error!(
            file = %file.id,
            runner_type = %step.runner_type,
            "no runner-type catalogue watch has ever scanned on this host; start \
             `br_drive::watch_runner_types` next to the engine"
        );
        mark_failed(file, CATALOGUE_NOT_WATCHED);
        return Ok(false);
    }
    if !catalogue::is_active(cx.connection(), &step.runner_type).await? {
        mark_failed(file, RUNNER_TYPE_UNAVAILABLE);
        return Ok(false);
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
    clear_run(file);
    file.processing_state = ProcessingState::Processing;
    file.processing_error = None;
    file.job_id = Some(job_id);
    file.step_index = Some(index as i32);
    file.step_count = Some(steps.len() as i32);
    file.step_runner_type = Some(step.runner_type.clone());
    cx.command(JobCreate {
        payload: CreateJob {
            job_id,
            runner_type: step.runner_type.clone(),
            producer: H::SERVICE.to_string(),
            config: Some(config),
            parent_job_id,
            triggered_by: Some(initiator.triggered_by()),
            source_bc: Some(H::SERVICE.to_string()),
            source_entity_id: Some(file.id),
            max_attempts: None,
        },
    })?;
    Ok(true)
}

fn outcome_cause<H>(file: &FileRow<H>, launched: bool, step: usize) -> FileCause {
    match (launched, file.processing_state) {
        (true, _) => FileCause::ProcessingStarted {
            job_id: file.job_id.expect("a launched step carries its job"),
            step: step as i32,
        },
        (false, ProcessingState::Ready) => FileCause::ProcessingFinished,
        (false, _) => FileCause::ProcessingFailed {
            reason: file
                .processing_error
                .clone()
                .unwrap_or_else(|| RUNNER_TYPE_UNAVAILABLE.to_string()),
        },
    }
}

pub(crate) async fn start_chain<H: DriveHost>(
    cx: &mut Ops<'_>,
    file: &mut FileRow<H>,
    plan: ChainPlan,
    initiator: Initiator,
) -> Result<(), DriveFault> {
    file.ruleset_id = plan.ruleset_id;
    file.steps = Some(plan.steps);
    file.triggered_by = Some(initiator);
    file.updated_at = cx.now().as_datetime();
    let launched = launch_step(cx, file, 0, None).await?;
    cx.save(file).await?;
    cx.impact_caused::<File, _>(&file.id, outcome_cause(file, launched, 0))?;
    Ok(())
}

/// The step whose job `finished` is over and the runner reported `done`: the
/// next step is launched, or the chain lands READY.
pub(crate) async fn advance<H: DriveHost>(
    cx: &mut Ops<'_>,
    file: &mut FileRow<H>,
    finished: Uuid,
) -> Result<(), DriveFault> {
    let next = file.step_index.unwrap_or(0) as usize + 1;
    let launched = launch_step(cx, file, next, Some(finished)).await?;
    file.updated_at = cx.now().as_datetime();
    cx.save(file).await?;
    cx.impact_caused::<File, _>(&file.id, outcome_cause(file, launched, next))?;
    Ok(())
}

pub(crate) fn cancel_active_job<H: DriveHost>(
    cx: &mut Ops<'_>,
    file: &FileRow<H>,
) -> Result<(), DriveFault> {
    if let Some(job_id) = file.job_id {
        cx.command(JobCancel {
            payload: CancelJob { job_id },
        })?;
    }
    Ok(())
}

pub(crate) fn finish_active_job<H: DriveHost>(
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
        // A later index on the same run moves the cursor forward; a newer start
        // instant (a retry attempt restarting the plan) moves it too. Anything
        // else is a redelivery or a stale fact.
        let forward = match (file.progress_index, file.progress_at) {
            (Some(current), Some(at)) => index > current || fact.0.started_at > at,
            _ => true,
        };
        if !forward {
            return Ok(());
        }
        file.progress_index = Some(index);
        file.progress_label = Some(fact.0.label);
        file.progress_at = Some(fact.0.started_at);
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

    #[test]
    fn the_root_names_are_camel_cased_like_the_graphql_fields() {
        let roots = RootNames::for_prefix("workspace").unwrap();
        assert_eq!(roots.context_root, "workspaceRunnerContext");
        let roots = RootNames::for_prefix("my_host").unwrap();
        assert_eq!(roots.context_root, "myHostRunnerContext");
        assert_eq!(roots.image_upload_root, "myHostRunnerRequestImageUpload");
        assert_eq!(roots.report_root, "myHostRunnerReport");
        assert!(RootNames::for_prefix("Bad_Prefix").is_err());
    }

    #[test]
    fn a_durable_is_namespaced_by_the_host_service() {
        assert_eq!(
            durable("workspace", DURABLE_COMPLETED),
            "workspace-drive-job-completed"
        );
        assert_ne!(
            durable("workspace", DURABLE_COMPLETED),
            durable("archive", DURABLE_COMPLETED)
        );
    }
}
