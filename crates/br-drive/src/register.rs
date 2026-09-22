use service_engine::Engine;
use service_engine::error::EngineError;

use crate::file::DriveFiles;
use crate::folders::{self, DeleteFile, DeleteFolder, MoveFolder, UpdateFile};
use crate::host::DriveHost;
use crate::upload::{self, CommitUpload, RequestUpload, UPLOAD_DEADLINE_DURABLE, UploadDeadline};

pub fn register<H: DriveHost>(engine: &mut Engine<H>) -> Result<(), EngineError> {
    engine.register_view(DriveFiles::<H>::default())?;
    engine.register_mutation::<RequestUpload, _>(upload::request_upload::<H>)?;
    let reader = engine.blob_reader();
    engine.register_mutation::<CommitUpload, _>(move |cx, input| {
        upload::commit_upload::<H>(cx, input, reader.clone())
    })?;
    engine.register_mutation::<UpdateFile, _>(folders::update_file::<H>)?;
    engine.register_mutation::<DeleteFile, _>(folders::delete_file::<H>)?;
    engine.register_mutation::<MoveFolder, _>(folders::move_folder::<H>)?;
    engine.register_mutation::<DeleteFolder, _>(folders::delete_folder::<H>)?;
    engine.register_reaction::<UploadDeadline<H>, _, _>(
        UPLOAD_DEADLINE_DURABLE,
        upload::upload_deadline::<H>,
    )?;
    crate::blob::register::<H>(engine)?;
    Ok(())
}
