use std::marker::PhantomData;

use br_core_integration::{Aggregate as BcAggregate, Bc, CommandCoords, Verb};
use chrono::TimeDelta;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::blobs::{Sha256Digest, UploadExpectation};
use service_engine::gate::Reason;
use service_engine::inbound::{ReactionCoordinates, ReactionMessage};
use service_engine::pipeline::{Mutation, MutationInput, OneShot, Reaction};
use service_engine::{BlobReader, BlobRef, JsonScalar, UploadUrl};
use uuid::Uuid;

use crate::blob::DriveSource;
use crate::drive::DriveRow;
use crate::fault::{DriveFault, DriveReactionFault, codes};
use crate::file::store;
use crate::file::{File, FileCause, FileRow, ProcessingState};
use crate::host::{DriveHost, DriveRequest};
use crate::media::MediaType;
use crate::path::{DrivePath, FileName};
use crate::processing;
use crate::ruleset::{Trigger, select_ruleset};

pub const UPLOAD_DEADLINE_AGGREGATE: &str = "drive_file";
pub const UPLOAD_DEADLINE_VERB: &str = "upload-deadline";
pub const UPLOAD_DEADLINE_DURABLE: &str = "upload-deadline";

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

#[derive(Debug, Clone, PartialEq, Eq, async_graphql::SimpleObject)]
pub struct UploadTicket {
    pub file_id: Uuid,
    pub url: String,
    pub fields: JsonScalar,
}

impl UploadTicket {
    pub(crate) fn new(file_id: Uuid, upload: UploadUrl) -> Self {
        let (url, fields) = upload.into_parts();
        let fields: serde_json::Map<String, serde_json::Value> = fields
            .into_iter()
            .map(|(name, value)| (name, serde_json::Value::String(value)))
            .collect();
        Self {
            file_id,
            url,
            fields: async_graphql::Json(serde_json::Value::Object(fields)),
        }
    }
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
        // The file id is the source entity every job of the file names, and
        // Jobs accepts only a UUIDv7 there: anything else would fail every
        // chain of the file for good.
        if input.file_id.get_version_num() != 7 {
            return Err(DriveFault::Refused(codes::INVALID_FILE_ID));
        }
        let path = DrivePath::parse(&input.path).map_err(Reason::from)?;
        let name = FileName::parse(&input.name).map_err(Reason::from)?;
        let media_type = MediaType::parse(&input.media_type)
            .map_err(|_| DriveFault::Refused(codes::INVALID_MEDIA_TYPE))?;
        let digest = Sha256Digest::from_hex(&input.sha256_hex)
            .map_err(|_| DriveFault::Refused(codes::INVALID_SHA256))?;
        cx.principal()
            .drive_gate(&DriveRequest::CreateFile {
                drive: input.drive_id,
                path: &path,
                name: &name,
                media_type: &media_type,
                size: input.size,
            })
            .require()?;
        cx.load::<DriveRow>(&input.drive_id)
            .await?
            .ok_or(DriveFault::Refused(codes::DRIVE_NOT_FOUND))?;
        let taken = store::sibling_names(cx.connection(), input.drive_id, &path).await?;
        let name = name.first_free(&taken);
        let blob = cx.blob_verified::<DriveSource>(
            name.as_str().to_string(),
            media_type.as_str().to_string(),
            UploadExpectation::new(input.size, digest),
        )?;
        let now = cx.now().as_datetime();
        let file = FileRow::<H> {
            id: input.file_id,
            drive_id: input.drive_id,
            path,
            name,
            protected: false,
            media_type,
            size_bytes: i64::try_from(input.size)
                .map_err(|_| DriveFault::Refused(codes::FILE_TOO_LARGE))?,
            sha256: *digest.as_bytes(),
            blob_ref: blob.reference().as_uuid(),
            processing_state: ProcessingState::Pending,
            processing_error: None,
            metadata: serde_json::Value::Object(serde_json::Map::new()),
            summary: None,
            page_count: None,
            estimated_tokens: None,
            ruleset_id: None,
            steps: None,
            step_index: None,
            step_count: None,
            step_runner_type: None,
            job_id: None,
            plan: None,
            progress_index: None,
            progress_label: None,
            progress_at: None,
            triggered_by: None,
            done_at: None,
            completed_at: None,
            step_entered_at: None,
            step_alive_at: None,
            stray_job_id: None,
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
        Ok(OneShot(UploadTicket::new(file.id, blob.upload_url())))
    })
}

#[derive(Debug, Deserialize)]
pub struct CommitUpload {
    pub file_id: Uuid,
    pub ruleset_id: Option<Uuid>,
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
        file.commit_gate(cx.principal()).require()?;
        let landed = reader
            .head(BlobRef(file.blob_ref))
            .await?
            .is_some_and(|head| head.verified() == Some(true));
        if !landed {
            return Err(DriveFault::Refused(codes::UPLOAD_NOT_LANDED));
        }
        file.processing_state = ProcessingState::Ready;
        file.updated_at = cx.now().as_datetime();
        let ruleset = select_ruleset(
            cx.connection(),
            Trigger::Upload,
            &file.media_type,
            input.ruleset_id,
        )
        .await?;
        cx.impact_caused::<File, _>(&file.id, FileCause::UploadCommitted)?;
        match ruleset {
            Some(ruleset) => {
                let initiator = processing::Initiator::of(cx.principal());
                let plan = processing::ChainPlan::from_ruleset(&ruleset, None);
                processing::start_chain(cx, &mut file, plan, initiator).await?;
            }
            None => cx.save(&file).await?,
        }
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
