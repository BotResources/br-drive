use service_engine::Engine;
use service_engine::error::EngineError;

use crate::erase::DriveErasure;
use crate::file::images::{IMAGE_LANDED_DURABLE, ImageLanded, image_landed};
use crate::file::{
    DeleteFile, DriveFiles, DrivePages, EditPage, Process, RegeneratePage, RetitleFile, UpdateFile,
    delete_file, edit_page, process, regenerate_page, retitle_file, update_file,
};
use crate::folders::{self, DeleteFolder, MoveFolder};
use crate::host::DriveHost;
use crate::import::{self, ImportCommit, ImportImage, ImportPages};
use crate::label::{
    CreateLabel, DeleteLabel, DriveLabels, SetFileLabels, UpdateLabel, create_label, delete_label,
    set_file_labels, update_label,
};
use crate::processing::{
    self, CancelledFact, CompletedFact, CreationRejectedFact, FailedFact, PlanDeclaredFact,
    QueuedFact, StartedFact, StepStartedFact,
};
use crate::ruleset::{
    CreateRuleset, DeleteRuleset, DriveRulesets, UpdateRuleset, create_ruleset, delete_ruleset,
    update_ruleset,
};
use crate::runner::{
    RunnerReport, RunnerRequestImageUpload, RunnerSources, runner_report,
    runner_request_image_upload,
};
use crate::upload::{self, CommitUpload, RequestUpload, UPLOAD_DEADLINE_DURABLE, UploadDeadline};

pub fn register<H: DriveHost>(
    engine: &mut Engine<H>,
    prefix: &'static str,
) -> Result<(), EngineError> {
    processing::declare_roots::<H>(prefix)?;
    processing::step_timeout::<H>()?;
    processing::pickup_timeout::<H>()?;
    if !engine.blobs_configured() {
        return Err(EngineError::Config(
            "br-drive needs object storage: configure EngineConfig::with_blob_storage before \
             registering the drive slice"
                .into(),
        ));
    }
    engine.register_view(DriveFiles::<H>::default())?;
    engine.register_view(DrivePages::<H>::default())?;
    engine.register_view(RunnerSources::<H>::default())?;
    engine.register_view(DriveRulesets::<H>::default())?;
    engine.register_view(DriveLabels::<H>::default())?;
    engine.register_erasable(DriveErasure::<H>::default())?;
    engine.register_mutation::<RequestUpload, _>(upload::request_upload::<H>)?;
    let reader = engine.blob_reader();
    engine.register_mutation::<CommitUpload, _>(move |cx, input| {
        upload::commit_upload::<H>(cx, input, reader.clone())
    })?;
    engine.register_mutation::<UpdateFile, _>(update_file::<H>)?;
    engine.register_mutation::<RetitleFile, _>(retitle_file::<H>)?;
    engine.register_mutation::<DeleteFile, _>(delete_file::<H>)?;
    engine.register_mutation::<EditPage, _>(edit_page::<H>)?;
    engine.register_mutation::<Process, _>(process::<H>)?;
    engine.register_mutation::<RegeneratePage, _>(regenerate_page::<H>)?;
    engine.register_mutation::<CreateRuleset, _>(create_ruleset::<H>)?;
    engine.register_mutation::<UpdateRuleset, _>(update_ruleset::<H>)?;
    engine.register_mutation::<DeleteRuleset, _>(delete_ruleset::<H>)?;
    engine.register_mutation::<CreateLabel, _>(create_label::<H>)?;
    engine.register_mutation::<UpdateLabel, _>(update_label::<H>)?;
    engine.register_mutation::<SetFileLabels, _>(set_file_labels::<H>)?;
    engine.register_mutation::<RunnerRequestImageUpload, _>(runner_request_image_upload::<H>)?;
    engine.register_mutation::<RunnerReport, _>(runner_report::<H>)?;
    engine.register_mutation::<ImportPages, _>(import::import_pages::<H>)?;
    engine.register_mutation::<ImportImage, _>(import::import_image::<H>)?;
    let reader = engine.blob_reader();
    engine.register_mutation::<ImportCommit, _>(move |cx, input| {
        import::import_commit::<H>(cx, input, reader.clone())
    })?;
    engine.register_bulk::<MoveFolder, _>(folders::move_folder::<H>)?;
    engine.register_bulk::<DeleteFolder, _>(folders::delete_folder::<H>)?;
    engine.register_bulk::<DeleteLabel, _>(delete_label::<H>)?;
    let durable = |suffix: &str| processing::durable(H::SERVICE, suffix);
    engine.register_reaction::<UploadDeadline<H>, _, _>(
        &durable(UPLOAD_DEADLINE_DURABLE),
        upload::upload_deadline::<H>,
    )?;
    engine.register_reaction::<ImageLanded<H>, _, _>(
        &durable(IMAGE_LANDED_DURABLE),
        image_landed::<H>,
    )?;
    engine.register_reaction::<processing::StepDeadline<H>, _, _>(
        &durable(processing::STEP_DEADLINE_DURABLE),
        processing::step_deadline::<H>,
    )?;
    engine.register_reaction::<processing::LaunchRetry<H>, _, _>(
        &durable(processing::LAUNCH_RETRY_DURABLE),
        processing::launch_retry::<H>,
    )?;
    engine.register_reaction::<QueuedFact, _, _>(
        &durable(processing::DURABLE_QUEUED),
        processing::on_queued::<H>,
    )?;
    engine.register_reaction::<CreationRejectedFact, _, _>(
        &durable(processing::DURABLE_CREATION_REJECTED),
        processing::on_creation_rejected::<H>,
    )?;
    engine.register_reaction::<StartedFact, _, _>(
        &durable(processing::DURABLE_STARTED),
        processing::on_started::<H>,
    )?;
    engine.register_reaction::<PlanDeclaredFact, _, _>(
        &durable(processing::DURABLE_PLAN_DECLARED),
        processing::on_plan_declared::<H>,
    )?;
    engine.register_reaction::<StepStartedFact, _, _>(
        &durable(processing::DURABLE_STEP_STARTED),
        processing::on_step_started::<H>,
    )?;
    engine.register_reaction::<CompletedFact, _, _>(
        &durable(processing::DURABLE_COMPLETED),
        processing::on_completed::<H>,
    )?;
    engine.register_reaction::<FailedFact, _, _>(
        &durable(processing::DURABLE_FAILED),
        processing::on_failed::<H>,
    )?;
    engine.register_reaction::<CancelledFact, _, _>(
        &durable(processing::DURABLE_CANCELLED),
        processing::on_cancelled::<H>,
    )?;
    crate::blob::register::<H>(engine)?;
    Ok(())
}
