use service_engine::Engine;
use service_engine::error::EngineError;

use crate::file::{
    DeleteFile, DriveFiles, EditPage, UpdateFile, delete_file, edit_page, update_file,
};
use crate::folders::{self, DeleteFolder, MoveFolder};
use crate::host::DriveHost;
use crate::runner::{
    RunnerFiles, RunnerReport, RunnerRequestImageUpload, runner_report, runner_request_image_upload,
};
use crate::upload::{self, CommitUpload, RequestUpload, UPLOAD_DEADLINE_DURABLE, UploadDeadline};

pub fn register<H: DriveHost>(engine: &mut Engine<H>) -> Result<(), EngineError> {
    if !engine.blobs_configured() {
        return Err(EngineError::Config(
            "br-drive needs object storage: configure EngineConfig::with_blob_storage before \
             registering the drive slice"
                .into(),
        ));
    }
    engine.register_view(DriveFiles::<H>::default())?;
    engine.register_view(RunnerFiles::<H>::default())?;
    engine.register_mutation::<RequestUpload, _>(upload::request_upload::<H>)?;
    let reader = engine.blob_reader();
    engine.register_mutation::<CommitUpload, _>(move |cx, input| {
        upload::commit_upload::<H>(cx, input, reader.clone())
    })?;
    engine.register_mutation::<UpdateFile, _>(update_file::<H>)?;
    engine.register_mutation::<DeleteFile, _>(delete_file::<H>)?;
    engine.register_mutation::<EditPage, _>(edit_page::<H>)?;
    engine.register_mutation::<RunnerRequestImageUpload, _>(runner_request_image_upload::<H>)?;
    engine.register_mutation::<RunnerReport, _>(runner_report::<H>)?;
    engine.register_bulk::<MoveFolder, _>(folders::move_folder::<H>)?;
    engine.register_bulk::<DeleteFolder, _>(folders::delete_folder::<H>)?;
    engine.register_reaction::<UploadDeadline<H>, _, _>(
        UPLOAD_DEADLINE_DURABLE,
        upload::upload_deadline::<H>,
    )?;
    crate::blob::register::<H>(engine)?;
    Ok(())
}
