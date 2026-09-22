use std::marker::PhantomData;

use br_core_integration::{Aggregate as BcAggregate, Bc, CommandCoords, Verb};
use chrono::TimeDelta;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::blobs::{Sha256Digest, UploadExpectation};
use service_engine::inbound::{ReactionCoordinates, ReactionMessage};
use service_engine::pipeline::{Mutation, MutationInput, OneShot, Reaction};
use service_engine::{BlobReader, BlobRef, UploadUrl};
use uuid::Uuid;

use crate::blob::DriveSource;
use crate::drive::DriveRow;
use crate::fault::{DriveFault, DriveReactionFault, codes};
use crate::file::store;
use crate::file::{File, FileCause, FileRow, ProcessingState};
use crate::host::{DriveHost, DriveRequest};
use crate::path::{DrivePath, FileName};

pub const UPLOAD_DEADLINE_AGGREGATE: &str = "drive_file";
pub const UPLOAD_DEADLINE_VERB: &str = "upload-deadline";
pub const UPLOAD_DEADLINE_DURABLE: &str = "drive-upload-deadline";

#[derive(Debug, Deserialize)]
pub struct RequestUpload {
    pub file_id: Uuid,
    pub drive_id: Uuid,
    pub path: String,
    pub name: String,
    pub media_type: String,
    pub size: u64,
    pub sha256_hex: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadTicket {
    pub file_id: Uuid,
    pub upload: UploadUrl,
}

impl MutationInput for RequestUpload {
    type Output = OneShot<UploadTicket>;
    type Error = DriveFault;
    const NAME: &'static str = "drive_request_upload";
}

pub fn request_upload<'m, H: DriveHost>(
    cx: &'m mut Mutation<'m, H>,
    input: RequestUpload,
) -> BoxFuture<'m, Result<OneShot<UploadTicket>, DriveFault>> {
    Box::pin(async move {
        let path = DrivePath::parse(&input.path).map_err(service_engine::gate::Reason::from)?;
        let name = FileName::parse(&input.name).map_err(service_engine::gate::Reason::from)?;
        let digest = Sha256Digest::from_hex(&input.sha256_hex)
            .map_err(|_| DriveFault::Refused(codes::INVALID_SHA256))?;
        cx.load::<DriveRow>(&input.drive_id)
            .await?
            .ok_or(DriveFault::Refused(codes::DRIVE_NOT_FOUND))?;
        cx.principal()
            .drive_gate(&DriveRequest::CreateFile {
                drive: input.drive_id,
                path: &path,
                name: &name,
                media_type: &input.media_type,
                size: input.size,
            })
            .require()?;
        let taken = store::sibling_names(cx.connection(), input.drive_id, &path).await?;
        let name = name.first_free(&taken);
        let blob = cx.blob_verified::<DriveSource>(
            name.as_str().to_string(),
            input.media_type.clone(),
            UploadExpectation::new(input.size, digest),
        )?;
        let now = cx.now().as_datetime();
        let file = FileRow::<H> {
            id: input.file_id,
            drive_id: input.drive_id,
            path,
            name,
            protected: false,
            media_type: input.media_type,
            size_bytes: i64::try_from(input.size)
                .map_err(|_| DriveFault::Refused(codes::FILE_TOO_LARGE))?,
            sha256: *digest.as_bytes(),
            blob_ref: blob.reference().as_uuid(),
            processing_state: ProcessingState::Pending,
            processing_error: None,
            metadata: serde_json::Value::Object(serde_json::Map::new()),
            created_by: cx.principal().id().as_uuid(),
            created_at: now,
            updated_at: now,
            host: PhantomData,
        };
        cx.create(&file).await?;
        cx.impact_caused::<File, _>(&file.id, FileCause::UploadRequested)?;
        let window = TimeDelta::from_std(cx.principal().upload_window()).map_err(|_| {
            DriveFault::Engine(service_engine::error::EngineError::Config(
                "the host's upload window does not fit a scheduled deadline".into(),
            ))
        })?;
        let deadline = cx.now() + window;
        cx.schedule_at(deadline, UploadDeadline::<H>::new(file.id))?;
        Ok(OneShot(UploadTicket {
            file_id: file.id,
            upload: blob.upload_url(),
        }))
    })
}

#[derive(Debug, Deserialize)]
pub struct CommitUpload {
    pub file_id: Uuid,
}

impl MutationInput for CommitUpload {
    type Output = ();
    type Error = DriveFault;
    const NAME: &'static str = "drive_commit_upload";
}

pub fn commit_upload<'m, H: DriveHost>(
    cx: &'m mut Mutation<'m, H>,
    input: CommitUpload,
    reader: BlobReader,
) -> BoxFuture<'m, Result<(), DriveFault>> {
    Box::pin(async move {
        let mut file = cx
            .load::<FileRow<H>>(&input.file_id)
            .await?
            .ok_or(DriveFault::Refused(codes::FILE_NOT_FOUND))?;
        cx.principal()
            .drive_gate(&file.as_create_request())
            .require()?;
        file.require_pending()?;
        let head = reader
            .head(BlobRef(file.blob_ref))
            .await?
            .ok_or(DriveFault::Refused(codes::UPLOAD_NOT_LANDED))?;
        if head.verified() != Some(true) {
            return Err(DriveFault::Refused(codes::UPLOAD_MISMATCH));
        }
        file.processing_state = ProcessingState::Ready;
        file.updated_at = cx.now().as_datetime();
        cx.save(&file).await?;
        cx.impact_caused::<File, _>(&file.id, FileCause::UploadCommitted)?;
        Ok(())
    })
}

#[derive(Serialize, Deserialize)]
pub struct UploadDeadline<H> {
    pub file_id: Uuid,
    #[serde(skip)]
    host: PhantomData<fn() -> H>,
}

impl<H> UploadDeadline<H> {
    pub fn new(file_id: Uuid) -> Self {
        Self {
            file_id,
            host: PhantomData,
        }
    }
}

impl<H: DriveHost> ReactionMessage for UploadDeadline<H> {
    fn coordinates() -> ReactionCoordinates {
        ReactionCoordinates::Command(CommandCoords {
            receiver: Bc::new(H::SERVICE).expect("the host service name is a valid bc"),
            aggregate: BcAggregate::new(UPLOAD_DEADLINE_AGGREGATE)
                .expect("a static aggregate segment"),
            verb: Verb::new(UPLOAD_DEADLINE_VERB).expect("a static verb segment"),
            version: 1,
        })
    }

    fn decode(payload: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(payload)
    }
}

pub fn upload_deadline<'r, H: DriveHost>(
    cx: &'r mut Reaction<'r>,
    message: UploadDeadline<H>,
) -> BoxFuture<'r, Result<(), DriveReactionFault>> {
    Box::pin(async move {
        let Some(file) = cx.load::<FileRow<H>>(&message.file_id).await? else {
            return Ok(());
        };
        if file.processing_state != ProcessingState::Pending {
            return Ok(());
        }
        cx.delete(&file).await?;
        cx.impact_caused::<File, _>(&file.id, FileCause::UploadAbandoned)?;
        Ok(())
    })
}
