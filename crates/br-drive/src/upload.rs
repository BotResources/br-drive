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
use crate::facts::{self, FactMeta};
use crate::fault::{DriveFault, DriveReactionFault, codes};
use crate::file::store;
use crate::file::{FileCause, FileEvent, FileRow, FileStatus};
use crate::host::{DriveHost, DriveRequest};
use crate::media::MediaType;
use crate::path::{DrivePath, FileName};
use crate::processing;
use crate::ruleset::{Trigger, select_ruleset};
use crate::title::FileTitle;

pub const UPLOAD_DEADLINE_AGGREGATE: &str = "drive_file";
pub const UPLOAD_DEADLINE_VERB: &str = "upload-deadline";
pub const UPLOAD_DEADLINE_DURABLE: &str = "upload-deadline";

#[derive(Debug, Deserialize)]
pub struct RequestUpload {
    pub file_id: Uuid,
    pub drive_id: Uuid,
    pub path: String,
    pub name: String,
    /// The file's title; absent, it defaults to `name` without its extension.
    pub title: Option<String>,
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

/// `RequestUpload` naming the `upload` rule the commit runs (the GraphQL
/// root's `rulesetId`): an `upload` rule matching the file's media type
/// (`RULESET_NOT_FOUND`, `RULESET_MISMATCH`), pinned on the pending file.
/// Without one, `RequestUpload` pins nothing and the commit runs the default
/// `upload` rule matching the file, as in 0.5.0. Since 0.5.1.
#[derive(Debug, Deserialize)]
pub struct RequestUploadWithRuleset {
    #[serde(flatten)]
    pub upload: RequestUpload,
    pub ruleset_id: Uuid,
}

impl MutationInput for RequestUploadWithRuleset {
    type Output = OneShot<UploadTicket>;
    type Error = DriveFault;
    const NAME: &'static str = "drive_request_upload_with_ruleset";
}

pub fn request_upload<'m, H: DriveHost>(
    cx: &'m mut Mutation<'m, H>,
    input: RequestUpload,
) -> BoxFuture<'m, Result<OneShot<UploadTicket>, DriveFault>> {
    Box::pin(request_upload_in(cx, input, None))
}

pub fn request_upload_with_ruleset<'m, H: DriveHost>(
    cx: &'m mut Mutation<'m, H>,
    input: RequestUploadWithRuleset,
) -> BoxFuture<'m, Result<OneShot<UploadTicket>, DriveFault>> {
    Box::pin(request_upload_in(cx, input.upload, Some(input.ruleset_id)))
}

async fn request_upload_in<H: DriveHost>(
    cx: &mut Mutation<'_, H>,
    input: RequestUpload,
    upload_ruleset: Option<Uuid>,
) -> Result<OneShot<UploadTicket>, DriveFault> {
    {
        // The file id is the source entity every job of the file names, and
        // Jobs accepts only a UUIDv7 there: anything else would fail every
        // chain of the file for good.
        if input.file_id.get_version_num() != 7 {
            return Err(DriveFault::Refused(codes::INVALID_FILE_ID));
        }
        let path = DrivePath::parse(&input.path).map_err(Reason::from)?;
        let name = FileName::parse(&input.name).map_err(Reason::from)?;
        let title = match input.title.as_deref() {
            Some(title) => {
                FileTitle::parse(title).map_err(|_| DriveFault::Refused(codes::INVALID_TITLE))?
            }
            None => FileTitle::from_name(&name),
        };
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
        if let Some(id) = upload_ruleset {
            // Validated as `ProcessFile` validates a named rule: it exists,
            // carries the `upload` trigger and matches the media type.
            select_ruleset(cx.connection(), Trigger::Upload, &media_type, Some(id)).await?;
        }
        let taken = store::sibling_names(cx.connection(), input.drive_id, &path).await?;
        let name = name.first_free(&taken);
        let blob = cx.blob_verified::<DriveSource>(
            name.as_str().to_string(),
            media_type.as_str().to_string(),
            UploadExpectation::new(input.size, digest),
        )?;
        let now = cx.now().as_datetime();
        let size_bytes =
            i64::try_from(input.size).map_err(|_| DriveFault::Refused(codes::FILE_TOO_LARGE))?;
        let mut file = FileRow::<H> {
            id: input.file_id,
            drive_id: input.drive_id,
            path,
            name,
            title,
            media_type,
            size_bytes,
            sha256: *digest.as_bytes(),
            blob_ref: blob.reference().as_uuid(),
            committed_at: None,
            metadata: serde_json::Value::Object(serde_json::Map::new()),
            summary: None,
            page_count: None,
            estimated_tokens: None,
            ruleset_id: None,
            steps: None,
            upload_ruleset_id: upload_ruleset,
            created_by: cx.principal().id().as_uuid(),
            created_at: now,
            updated_at: now,
            file_updated_at: now,
            version: 0,
            pending: Default::default(),
            status: FileStatus::pending(),
            host: PhantomData,
        };
        let meta = FactMeta::of(cx.principal(), now);
        file.record(
            FileEvent::UploadTicketIssued {
                drive_id: file.drive_id,
                path: file.path.as_str().to_string(),
                name: file.name.as_str().to_string(),
                title: file.title.as_str().to_string(),
                media_type: file.media_type.as_str().to_string(),
                size_bytes,
            },
            &meta,
        );
        if let Some(ruleset_id) = upload_ruleset {
            file.record(FileEvent::UploadRulesetChosen { ruleset_id }, &meta);
        }
        facts::create(cx, &mut file).await?;
        crate::file::file_changed::<H>(cx, &file, FileCause::UploadRequested)?;
        let window = TimeDelta::from_std(cx.principal().upload_window()).map_err(|_| {
            DriveFault::Engine(service_engine::error::EngineError::Config(
                "the host's upload window does not fit a scheduled deadline".into(),
            ))
        })?;
        let deadline = cx.now() + window;
        cx.schedule_at(deadline, UploadDeadline::<H>::new(file.id))?;
        Ok(OneShot(UploadTicket::new(file.id, blob.upload_url())))
    }
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

/// Confirms a pending upload: the object is present and is the pinned bytes
/// (a live storage HEAD). On a host that processes on commit
/// (`DriveHost::process_on_commit`, the default), the default `upload` rule
/// matching the file's media type starts its chain in this very transaction —
/// the file goes PROCESSING, never READY first — when the host's `Process`
/// gate allows it; without such a rule, or with the gate refusing, the file is
/// READY, stored only, and the commit is not refused. Otherwise the file is
/// READY and processing is a gesture of its own (`ProcessFile`).
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
        require_landed(&reader, &file).await?;
        let now = cx.now().as_datetime();
        file.committed_at = Some(now);
        file.status = FileStatus::stored();
        let meta = FactMeta::of(cx.principal(), now);
        file.record(FileEvent::UploadCommitted, &meta);
        facts::save(cx, &mut file).await?;
        crate::file::file_changed::<H>(cx, &file, FileCause::UploadCommitted)?;
        if cx.principal().process_on_commit() && file.process_gate(cx.principal()).is_allowed() {
            let rule = commit_rule(cx.connection(), &file).await?;
            if let Some(rule) = rule {
                let initiator = processing::Initiator::of(cx.principal());
                processing::start_chain(
                    cx,
                    &meta,
                    &mut file,
                    processing::ChainPlan::from_ruleset(&rule, None),
                    Trigger::Upload,
                    initiator,
                )
                .await?;
            }
        }
        Ok(())
    })
}

/// The rule the commit's chain runs: the one the uploader chose, while it
/// still carries the `upload` trigger and matches the file — a chosen rule
/// deleted or changed since leaves the file stored only, never processed by
/// the default in its place — else the default `upload` rule matching the
/// file.
async fn commit_rule<H>(
    conn: &mut sqlx::PgConnection,
    file: &FileRow<H>,
) -> Result<Option<crate::ruleset::RulesetRow>, DriveFault> {
    let Some(chosen) = file.upload_ruleset_id else {
        return select_ruleset(conn, Trigger::Upload, &file.media_type, None).await;
    };
    match select_ruleset(conn, Trigger::Upload, &file.media_type, Some(chosen)).await {
        Err(DriveFault::Refused(reason))
            if reason == codes::RULESET_NOT_FOUND || reason == codes::RULESET_MISMATCH =>
        {
            Ok(None)
        }
        selected => selected,
    }
}

/// A live storage HEAD: the pending file's object is present and is the
/// pinned bytes (`UPLOAD_NOT_LANDED` otherwise).
pub(crate) async fn require_landed<H>(
    reader: &BlobReader,
    file: &FileRow<H>,
) -> Result<(), DriveFault> {
    let landed = reader
        .head(BlobRef(file.blob_ref))
        .await?
        .is_some_and(|head| head.verified() == Some(true));
    if landed {
        Ok(())
    } else {
        Err(DriveFault::Refused(codes::UPLOAD_NOT_LANDED))
    }
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
        // An upload never confirmed: its object never arrived, or arrived and
        // was never committed.
        if file.committed_at.is_some() {
            return Ok(());
        }
        let meta = FactMeta::of_reaction(cx);
        crate::file::store::hand_gone::<H>(
            cx.connection(),
            &meta,
            &file,
            FileEvent::UploadAbandoned,
        )
        .await?;
        cx.delete(&file).await?;
        crate::file::file_changed::<H>(cx, &file, FileCause::UploadAbandoned)?;
        Ok(())
    })
}
