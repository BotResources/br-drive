use std::collections::BTreeSet;
use std::marker::PhantomData;

use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::blobs::{Disposition, Sha256Digest, UploadExpectation};
use service_engine::error::EngineError;
use service_engine::name::ProjectorName;
use service_engine::persistence::{Aggregate, Persistence, PersistenceExt, PersistenceStyle};
use service_engine::pipeline::{Mutation, MutationInput, OneShot, Ops};
use service_engine::population::Population;
use service_engine::view::{Populate, Projector};
use service_engine::visibility::Unrestricted;
use service_engine::{BlobRef, Query};
use sqlx::PgConnection;
use uuid::Uuid;

use crate::blob::{DriveImage, DriveRendition};
use crate::facts::{self, FactMeta};
use crate::fault::{DriveFault, codes};
use crate::file::images::{
    BlobFacts, ImageKey, ImageRecord, drop_images, image_names_of, image_names_of_page,
    references_image,
};
use crate::file::pages::{Page, PageCause, PageKey, RunnerPage, read_pages};
use crate::file::processed;
use crate::file::renditions::{
    LandedRendition, RenditionBlob, RenditionKey, RenditionRecord, landed_renditions_of,
};
use crate::file::report::{apply_indexing, validate_report};
use crate::file::store::{self, FileStore, PageWrite};
use crate::file::{ByteCount, File, FileCause, FileEvent, FileRow, PageEvent, PageOrigin};
use crate::host::DriveHost;
use crate::image::ImageName;
use crate::media::MediaType;
use crate::rendition::{RenditionKind, RenditionName};
use crate::ruleset::Trigger;
use crate::upload::UploadTicket;

pub const MAX_REPORT_PAGES: usize = 512;

service_engine::open_access!(
    pub RunnerAccess = "the runner source presign is gated on the service passport's runner scope and the file's active job, never on a drive cohort"
);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct RunnerContext {
    pub file_id: Uuid,
    pub media_type: String,
    pub name: String,
    pub page_count: Option<i32>,
    pub summary: Option<String>,
    pub pages: Vec<RunnerPage>,
    pub images: Vec<String>,
    /// The file's landed renditions, each with a fresh presigned GET: a later
    /// step of the chain reads what an earlier one produced.
    pub renditions: Vec<RunnerRendition>,
    pub source_url: Option<String>,
    #[graphql(skip)]
    pub source: Uuid,
}

/// A landed rendition as a runner reads it: what it is, and a presigned GET
/// minted at this call.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct RunnerRendition {
    pub name: String,
    pub kind: String,
    pub media_type: String,
    pub size_bytes: ByteCount,
    /// A presigned GET, minted at this call.
    pub url: String,
}

/// How many times a presign is retried on a rendition replaced while it is
/// signed: each retry follows a landing that swapped the name's object.
const RENDITION_PRESIGN_ATTEMPTS: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunnerSource {
    pub file_id: Uuid,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunnerWindow {
    pub file_id: Option<Uuid>,
}

pub struct RunnerSources<H>(PhantomData<fn() -> H>);

impl<H> Default for RunnerSources<H> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<H> RunnerSources<H> {
    pub const NAME: ProjectorName = ProjectorName::from_static("drive_runner_sources");
}

impl<H: DriveHost> Projector for RunnerSources<H> {
    type Principal = H;
    type Noun = File;
    type Store = FileStore<H>;
    type Query = RunnerWindow;
    type Out = RunnerSource;
    type Visibility = Unrestricted<FileRow<H>, H, RunnerAccess>;

    const NAME: ProjectorName = Self::NAME;

    async fn populate(
        cx: &Populate<'_, H>,
        query: &RunnerWindow,
    ) -> Result<Population<Uuid>, EngineError> {
        if !cx.principal().is_runner() {
            return Ok(Population::Keys(BTreeSet::new()));
        }
        // The population is the one file the runner's job names, and only
        // while that job is the file's active one: the presign re-checks what
        // `runner_context` checked, it never widens it.
        let Ok(RunnerScope {
            file_id, job_id, ..
        }) = RUNNER_JOB.try_with(|scope| *scope)
        else {
            return Ok(Population::Keys(BTreeSet::new()));
        };
        if query.file_id.is_some_and(|wanted| wanted != file_id) {
            return Ok(Population::Keys(BTreeSet::new()));
        }
        let mut conn = cx.pool().acquire().await.map_err(EngineError::from)?;
        let active = FileStore::<H>::load(&mut conn, &file_id)
            .await?
            .is_some_and(|file| file.require_active_job(job_id).is_ok());
        let keys = if active {
            BTreeSet::from([file_id])
        } else {
            BTreeSet::new()
        };
        Ok(Population::Keys(keys))
    }

    fn project(row: &FileRow<H>, _principal: &H) -> Result<RunnerSource, EngineError> {
        Ok(RunnerSource { file_id: row.id })
    }

    /// The gate of every read of a runner source, the presign
    /// (`Query::download` gates on the row, not on the population) and the
    /// runner context's read: a runner, reading inside a runner scope, the
    /// job's own file — and, for the presign (`scoped_to_job`), only while
    /// that job is the file's active one. The context read checks the job
    /// itself, so it answers `JOB_NOT_ACTIVE` rather than hide the file. No
    /// scope, no row: the gate fails closed.
    fn visible(row: &FileRow<H>, principal: &H) -> bool {
        principal.is_runner()
            && RUNNER_JOB
                .try_with(|scope| {
                    row.id == scope.file_id
                        && (!scope.require_active || row.require_active_job(scope.job_id).is_ok())
                })
                .unwrap_or(false)
    }
}

/// The runner read in flight: the file its job names, the job, and whether
/// the gate itself requires the job to be the file's active one.
#[derive(Debug, Clone, Copy)]
struct RunnerScope {
    file_id: Uuid,
    job_id: Uuid,
    require_active: bool,
}

tokio::task_local! {
    static RUNNER_JOB: RunnerScope;
}

/// Runs `presign` with the runner source gate scoped to `file_id` and
/// `job_id`: the presign's visibility check (`RunnerSources::visible`) and
/// population admit only the job's own file while that job is its active
/// one. Engine 0.4.0 runs the check inside the `download` future itself (no
/// spawned task); were it ever to move to another task, the check would read
/// no scope and admit no row — the runner would get `SOURCE_NOT_AVAILABLE`,
/// never a wider presign. Not a host API: the `drive_slice!` expansion calls
/// it.
#[doc(hidden)]
pub async fn scoped_to_job<F: std::future::Future>(
    file_id: Uuid,
    job_id: Uuid,
    presign: F,
) -> F::Output {
    let scope = RunnerScope {
        file_id,
        job_id,
        require_active: true,
    };
    RUNNER_JOB.scope(scope, presign).await
}

/// The file a runner's job names, with the objects of its landed
/// renditions: the row the rendition presigns of `RunnerContext` are gated
/// on (`Query::download` admits only a reference the row holds).
pub struct RunnerFile<H> {
    pub file: FileRow<H>,
    pub renditions: Vec<LandedRendition>,
}

impl<H> Clone for RunnerFile<H> {
    fn clone(&self) -> Self {
        Self {
            file: self.file.clone(),
            renditions: self.renditions.clone(),
        }
    }
}

pub struct RunnerFileStore<H>(PhantomData<fn() -> H>);

fn read_only() -> EngineError {
    EngineError::Config("the runner file store is read-only; writes go through FileStore".into())
}

impl<H: DriveHost> Persistence for RunnerFileStore<H> {
    type Aggregate = RunnerFile<H>;
    type Key = Uuid;
    type Event = ();

    const STYLE: PersistenceStyle = PersistenceStyle::Crud;

    fn read_many<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [Uuid],
    ) -> BoxFuture<'a, Result<Vec<(Uuid, RunnerFile<H>)>, EngineError>> {
        Box::pin(async move {
            let files = FileStore::<H>::read_many(&mut *conn, keys).await?;
            let mut renditions = landed_renditions_of(&mut *conn, keys).await?;
            Ok(files
                .into_iter()
                .map(|(id, file)| {
                    let (own, others) = std::mem::take(&mut renditions)
                        .into_iter()
                        .partition(|rendition| rendition.file_id == id);
                    renditions = others;
                    (
                        id,
                        RunnerFile {
                            file,
                            renditions: own,
                        },
                    )
                })
                .collect())
        })
    }

    fn save<'a>(
        _conn: &'a mut PgConnection,
        _row: &'a RunnerFile<H>,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async { Err(read_only()) })
    }

    fn create<'a>(
        _conn: &'a mut PgConnection,
        _row: &'a RunnerFile<H>,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async { Err(read_only()) })
    }
}

impl<H: DriveHost> Aggregate for RunnerFile<H> {
    type Store = RunnerFileStore<H>;

    fn key(&self) -> Uuid {
        self.file.id
    }

    fn blob_refs(&self) -> Vec<BlobRef> {
        self.renditions
            .iter()
            .map(|rendition| BlobRef(rendition.blob_ref))
            .collect()
    }
}

/// The rendition presigns of a runner: the same gate as the source's
/// ([`RunnerSources`]) — a runner, inside a runner scope, the job's own file
/// while that job is its active one — over the file's landed renditions.
pub struct RunnerRenditions<H>(PhantomData<fn() -> H>);

impl<H> Default for RunnerRenditions<H> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<H> RunnerRenditions<H> {
    pub const NAME: ProjectorName = ProjectorName::from_static("drive_runner_renditions");
}

impl<H: DriveHost> Projector for RunnerRenditions<H> {
    type Principal = H;
    type Noun = File;
    type Store = RunnerFileStore<H>;
    type Query = RunnerWindow;
    type Out = RunnerSource;
    type Visibility = Unrestricted<RunnerFile<H>, H, RunnerAccess>;

    const NAME: ProjectorName = Self::NAME;

    async fn populate(
        cx: &Populate<'_, H>,
        query: &RunnerWindow,
    ) -> Result<Population<Uuid>, EngineError> {
        RunnerSources::<H>::populate(cx, query).await
    }

    fn project(row: &RunnerFile<H>, _principal: &H) -> Result<RunnerSource, EngineError> {
        Ok(RunnerSource {
            file_id: row.file.id,
        })
    }

    fn visible(row: &RunnerFile<H>, principal: &H) -> bool {
        RunnerSources::<H>::visible(&row.file, principal)
    }
}

/// The landed renditions of the runner's file, read behind the runner
/// rendition gate; `None` once the job is no longer the file's active one or
/// the file is gone.
async fn gated_renditions<H: DriveHost>(
    query: &Query<'_, H>,
    file_id: Uuid,
    job_id: Uuid,
) -> async_graphql::Result<Option<Vec<LandedRendition>>> {
    let row = scoped_to_job(
        file_id,
        job_id,
        query.load_visible::<RunnerRenditions<H>>(&file_id),
    )
    .await?;
    Ok(row.map(|row| row.renditions))
}

/// Fills `context.renditions` with every landed rendition of the file, each
/// with a fresh presigned GET, behind the runner rendition gate scoped to the
/// file and `job_id`. The list is read once from the gated row; a rendition
/// replaced between that read and its presign (the gate then holds only the
/// new object) is re-read and its current object signed, so a landed
/// rendition is never left out. Not a host API: the `drive_slice!` expansion
/// calls it.
#[doc(hidden)]
pub async fn presign_renditions<H: DriveHost>(
    query: &Query<'_, H>,
    job_id: Uuid,
    context: &mut RunnerContext,
) -> async_graphql::Result<()> {
    let file_id = context.file_id;
    let not_active = || DriveFault::Refused(codes::JOB_NOT_ACTIVE).into_graphql();
    let landed = gated_renditions(query, file_id, job_id)
        .await?
        .ok_or_else(not_active)?;
    let mut presigned = Vec::with_capacity(landed.len());
    for mut rendition in landed {
        let mut attempts = 0;
        loop {
            let url = scoped_to_job(
                file_id,
                job_id,
                query.download::<RunnerRenditions<H>>(
                    &file_id,
                    BlobRef(rendition.blob_ref),
                    Disposition::Inline,
                ),
            )
            .await?;
            if let Some(url) = url {
                presigned.push(RunnerRendition {
                    name: rendition.name.clone(),
                    kind: rendition.kind.clone(),
                    media_type: rendition.media_type.clone(),
                    size_bytes: ByteCount(u64::try_from(rendition.size_bytes).unwrap_or(0)),
                    url: url.into_string(),
                });
                break;
            }
            attempts += 1;
            if attempts >= RENDITION_PRESIGN_ATTEMPTS {
                return Err(service_engine::coded_error(
                    codes::SOURCE_NOT_AVAILABLE.code(),
                    "a rendition kept changing while it was presigned; retry",
                ));
            }
            // Replaced since the read: sign the name's current object.
            let current = gated_renditions(query, file_id, job_id)
                .await?
                .ok_or_else(not_active)?;
            match current
                .into_iter()
                .find(|fresh| fresh.name == rendition.name)
            {
                Some(fresh) => rendition = fresh,
                // A rendition is only ever gone with its file.
                None => break,
            }
        }
    }
    context.renditions = presigned;
    Ok(())
}

fn runner_only<H: DriveHost>(principal: &H) -> Result<(), DriveFault> {
    if principal.is_runner() {
        Ok(())
    } else {
        Err(DriveFault::Refused(codes::RUNNER_SCOPE_REQUIRED))
    }
}

/// What a runner reads of the file its job processes: the file, its pages
/// and its image names, read in one snapshot behind the runner source gate
/// (`Query::behind`). The runner scope is required, then the file, then the
/// job as the file's active one (`JOB_NOT_ACTIVE`).
pub async fn runner_context<H: DriveHost>(
    query: &Query<'_, H>,
    principal: &H,
    file_id: Uuid,
    job_id: Uuid,
) -> async_graphql::Result<RunnerContext> {
    runner_only(principal).map_err(DriveFault::into_graphql)?;
    let scope = RunnerScope {
        file_id,
        job_id,
        require_active: false,
    };
    let behind = query.behind::<RunnerSources<H>>(&file_id);
    let read = RUNNER_JOB
        .scope(
            scope,
            behind.read(|file, conn| {
                Box::pin(async move {
                    if let Err(reason) = file.require_active_job(job_id) {
                        return Ok(Err(DriveFault::Refused(reason)));
                    }
                    let pages = read_pages(&mut *conn, file.id).await?;
                    let images = image_names_of(&mut *conn, file.id).await?;
                    Ok(Ok(RunnerContext {
                        file_id: file.id,
                        media_type: file.media_type.as_str().to_string(),
                        name: file.name.as_str().to_string(),
                        page_count: file.page_count,
                        summary: file.summary.clone(),
                        pages,
                        images,
                        // Read and presigned behind their own gate by
                        // `presign_renditions`.
                        renditions: Vec::new(),
                        source_url: None,
                        source: file.blob_ref,
                    }))
                })
            }),
        )
        .await?;
    match read {
        Some(Ok(context)) => Ok(context),
        Some(Err(fault)) => Err(fault.into_graphql()),
        None => Err(DriveFault::Refused(codes::FILE_NOT_FOUND).into_graphql()),
    }
}

#[derive(Debug, Deserialize)]
pub struct RunnerRequestImageUpload {
    pub file_id: Uuid,
    pub job_id: Uuid,
    pub name: String,
    pub media_type: String,
    pub size: u64,
    pub sha256_hex: String,
}

impl MutationInput for RunnerRequestImageUpload {
    type Output = OneShot<UploadTicket>;
    type Error = DriveFault;
    const NAME: &'static str = "drive_runner_request_image_upload";
}

pub fn runner_request_image_upload<'m, H: DriveHost>(
    cx: &'m mut Mutation<'m, H>,
    input: RunnerRequestImageUpload,
) -> BoxFuture<'m, Result<OneShot<UploadTicket>, DriveFault>> {
    Box::pin(async move {
        runner_only(cx.principal())?;
        let name = ImageName::parse(&input.name)
            .map_err(|_| DriveFault::Refused(codes::INVALID_IMAGE_NAME))?;
        let media_type = MediaType::parse(&input.media_type)
            .map_err(|_| DriveFault::Refused(codes::INVALID_MEDIA_TYPE))?;
        let digest = Sha256Digest::from_hex(&input.sha256_hex)
            .map_err(|_| DriveFault::Refused(codes::INVALID_SHA256))?;
        let size_bytes =
            i64::try_from(input.size).map_err(|_| DriveFault::Refused(codes::FILE_TOO_LARGE))?;
        let mut file = cx
            .load::<FileRow<H>>(&input.file_id)
            .await?
            .ok_or(DriveFault::Refused(codes::FILE_NOT_FOUND))?;
        file.require_active_job(input.job_id)?;
        let window = cx.principal().upload_window();
        let meta = FactMeta::of(cx.principal(), cx.now().as_datetime());
        let upload = ImageUpload {
            name,
            media_type,
            size_bytes,
            digest,
            window,
        };
        let ticket = stage_image(cx, &meta, &mut file, upload).await?;
        Ok(OneShot(ticket))
    })
}

/// An image a runner asks to upload.
pub(crate) struct ImageUpload {
    pub name: ImageName,
    pub media_type: MediaType,
    pub size_bytes: i64,
    pub digest: Sha256Digest,
    pub window: std::time::Duration,
}

/// Stages a verified image upload on `file`: a new row, or the replacement of
/// an existing name that swaps only when the new object lands.
pub(crate) async fn stage_image<H: DriveHost>(
    cx: &mut Ops<'_>,
    meta: &FactMeta,
    file: &mut FileRow<H>,
    upload: ImageUpload,
) -> Result<UploadTicket, DriveFault> {
    let ImageUpload {
        name,
        media_type,
        size_bytes,
        digest,
        window,
    } = upload;
    let key = ImageKey {
        file_id: file.id,
        name: name.as_str().to_string(),
    };
    let now = cx.now().as_datetime();
    let existing = cx.load::<ImageRecord<H>>(&key).await?;
    if existing
        .as_ref()
        .is_some_and(|image| image.is_landing(now, window))
    {
        return Err(DriveFault::Refused(codes::IMAGE_UPLOAD_PENDING));
    }
    let size = u64::try_from(size_bytes).map_err(|_| DriveFault::Refused(codes::FILE_TOO_LARGE))?;
    let blob = cx.blob_verified::<DriveImage>(
        name.as_str().to_string(),
        media_type.as_str().to_string(),
        UploadExpectation::new(size, digest),
    )?;
    let facts = BlobFacts {
        blob_ref: blob.reference().as_uuid(),
        media_type,
        size_bytes,
        sha256: *digest.as_bytes(),
    };
    match existing {
        Some(mut image) => {
            for reference in image.request_replacement(facts, now) {
                cx.release_blob(BlobRef(reference))?;
            }
            cx.save(&image).await?;
        }
        None => {
            processed::ensure(cx.connection(), file.id).await?;
            let image = ImageRecord::<H>::new(file.id, name.clone(), facts, now);
            cx.create(&image).await?;
        }
    }
    file.record(
        FileEvent::ImageUploadTicketIssued {
            name: name.as_str().to_string(),
        },
        meta,
    );
    facts::save(cx, file).await?;
    crate::file::file_changed::<H>(
        cx,
        file,
        FileCause::ImageRequested {
            name: name.as_str().to_string(),
        },
    )?;
    Ok(UploadTicket::new(file.id, blob.upload_url()))
}

#[derive(Debug, Deserialize)]
pub struct RunnerRequestRenditionUpload {
    pub file_id: Uuid,
    pub job_id: Uuid,
    pub name: String,
    pub kind: String,
    pub media_type: String,
    pub size: u64,
    pub sha256_hex: String,
}

impl MutationInput for RunnerRequestRenditionUpload {
    type Output = OneShot<UploadTicket>;
    type Error = DriveFault;
    const NAME: &'static str = "drive_runner_request_rendition_upload";
}

/// A runner hands back a rendition: a verified presigned POST for a
/// `drive_rendition` blob, and the file's rendition row of that name — a new
/// one, or the replacement of a landed one, swapped in only when the new
/// object lands. Same checks as an image: the runner scope, then the file's
/// running job.
pub fn runner_request_rendition_upload<'m, H: DriveHost>(
    cx: &'m mut Mutation<'m, H>,
    input: RunnerRequestRenditionUpload,
) -> BoxFuture<'m, Result<OneShot<UploadTicket>, DriveFault>> {
    Box::pin(async move {
        runner_only(cx.principal())?;
        let name = RenditionName::parse(&input.name)
            .map_err(|_| DriveFault::Refused(codes::INVALID_RENDITION_NAME))?;
        let kind = RenditionKind::parse(&input.kind)
            .map_err(|_| DriveFault::Refused(codes::INVALID_RENDITION_KIND))?;
        let media_type = MediaType::parse(&input.media_type)
            .map_err(|_| DriveFault::Refused(codes::INVALID_MEDIA_TYPE))?;
        let digest = Sha256Digest::from_hex(&input.sha256_hex)
            .map_err(|_| DriveFault::Refused(codes::INVALID_SHA256))?;
        let size_bytes =
            i64::try_from(input.size).map_err(|_| DriveFault::Refused(codes::FILE_TOO_LARGE))?;
        let mut file = cx
            .load::<FileRow<H>>(&input.file_id)
            .await?
            .ok_or(DriveFault::Refused(codes::FILE_NOT_FOUND))?;
        file.require_active_job(input.job_id)?;
        let window = cx.principal().upload_window();
        let meta = FactMeta::of(cx.principal(), cx.now().as_datetime());
        let key = RenditionKey {
            file_id: file.id,
            name: name.as_str().to_string(),
        };
        let now = cx.now().as_datetime();
        let existing = cx.load::<RenditionRecord<H>>(&key).await?;
        if existing
            .as_ref()
            .is_some_and(|rendition| rendition.is_landing(now, window))
        {
            return Err(DriveFault::Refused(codes::RENDITION_UPLOAD_PENDING));
        }
        let blob = cx.blob_verified::<DriveRendition>(
            name.as_str().to_string(),
            media_type.as_str().to_string(),
            UploadExpectation::new(input.size, digest),
        )?;
        let requested = RenditionBlob {
            kind: kind.clone(),
            blob: BlobFacts {
                blob_ref: blob.reference().as_uuid(),
                media_type,
                size_bytes,
                sha256: *digest.as_bytes(),
            },
        };
        match existing {
            Some(mut rendition) => {
                for reference in rendition.request_replacement(requested, now) {
                    cx.release_blob(BlobRef(reference))?;
                }
                cx.save(&rendition).await?;
            }
            None => {
                let rendition = RenditionRecord::<H>::new(file.id, name.clone(), requested, now);
                cx.create(&rendition).await?;
            }
        }
        file.record(
            FileEvent::RenditionUploadTicketIssued {
                name: name.as_str().to_string(),
                rendition_kind: kind.as_str().to_string(),
            },
            &meta,
        );
        facts::save(cx, &mut file).await?;
        crate::file::file_changed::<H>(
            cx,
            &file,
            FileCause::RenditionRequested {
                name: name.as_str().to_string(),
            },
        )?;
        Ok(OneShot(UploadTicket::new(file.id, blob.upload_url())))
    })
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReportedPage {
    pub number: i32,
    pub markdown: String,
}

#[derive(Debug, Clone, async_graphql::InputObject)]
pub struct ReportedPageInput {
    pub number: i32,
    pub markdown: String,
}

impl From<ReportedPageInput> for ReportedPage {
    fn from(input: ReportedPageInput) -> Self {
        Self {
            number: input.number,
            markdown: input.markdown,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct RunnerReport {
    pub file_id: Uuid,
    pub job_id: Uuid,
    pub pages: Vec<ReportedPage>,
    pub origin: PageOrigin,
    pub summary: Option<String>,
    pub page_count: Option<i32>,
    pub estimated_tokens: Option<i64>,
    pub done: bool,
}

impl MutationInput for RunnerReport {
    type Output = ();
    type Error = DriveFault;
    const NAME: &'static str = "drive_runner_report";
}

fn validate(input: &RunnerReport) -> Result<(), DriveFault> {
    if input.origin == PageOrigin::Edited {
        return Err(DriveFault::Refused(codes::INVALID_PAGE_ORIGIN));
    }
    let numbers: Vec<i32> = input.pages.iter().map(|page| page.number).collect();
    let indexed = validate_report(
        &numbers,
        input.summary.as_deref(),
        input.page_count,
        input.estimated_tokens,
    )?;
    if input.pages.is_empty() && !indexed && !input.done {
        return Err(DriveFault::Refused(codes::NOTHING_TO_CHANGE));
    }
    Ok(())
}

pub fn runner_report<'m, H: DriveHost>(
    cx: &'m mut Mutation<'m, H>,
    input: RunnerReport,
) -> BoxFuture<'m, Result<(), DriveFault>> {
    Box::pin(async move {
        runner_only(cx.principal())?;
        validate(&input)?;
        let mut file = cx
            .load::<FileRow<H>>(&input.file_id)
            .await?
            .ok_or(DriveFault::Refused(codes::FILE_NOT_FOUND))?;
        file.require_active_job(input.job_id)?;
        let job = file
            .active_job()
            .cloned()
            .ok_or(DriveFault::Refused(codes::JOB_NOT_ACTIVE))?;
        let meta = FactMeta::of(cx.principal(), cx.now().as_datetime());

        // A page a person edited is kept by an upload or a reprocess run; a
        // page regeneration is asked for that very page, and overwrites it.
        let kept = if job.trigger == Some(Trigger::RegeneratePage) {
            Vec::new()
        } else {
            let numbers: Vec<i32> = input.pages.iter().map(|page| page.number).collect();
            processed::edited_among(cx.connection(), file.id, &numbers).await?
        };
        let pages: Vec<&ReportedPage> = input
            .pages
            .iter()
            .filter(|page| !kept.contains(&page.number))
            .collect();

        let mut dropped = Vec::new();
        if input.origin == PageOrigin::Regenerated {
            for page in &pages {
                let unreferenced: Vec<String> =
                    image_names_of_page(cx.connection(), file.id, page.number)
                        .await?
                        .into_iter()
                        .filter(|name| !references_image(&page.markdown, name))
                        .collect();
                for reference in drop_images(cx.connection(), file.id, &unreferenced).await? {
                    cx.release_blob(BlobRef(reference))?;
                }
                dropped.extend(unreferenced);
            }
        }
        let writes: Vec<PageWrite<'_>> = pages
            .iter()
            .map(|page| PageWrite {
                number: page.number,
                markdown: &page.markdown,
                origin: input.origin,
            })
            .collect();
        let reported = PageCause::Reported {
            job_id: input.job_id,
            origin: input.origin,
        };
        let event = PageEvent::Reported {
            job_id: input.job_id,
            origin: input.origin,
        };
        store::upsert_pages::<H>(cx.connection(), &meta, file.id, &writes, &event).await?;
        for page in &pages {
            cx.impact_caused::<Page, _>(
                &PageKey {
                    file_id: file.id,
                    number: page.number,
                },
                reported.clone(),
            )?;
        }
        let mut causes = Vec::new();
        if !dropped.is_empty() {
            file.record(
                FileEvent::ImagesDropped {
                    names: dropped.clone(),
                },
                &meta,
            );
            causes.push(FileCause::ImagesDropped { names: dropped });
        }
        if let (Some(summary), Some(page_count)) = (input.summary, input.page_count)
            && apply_indexing(&mut file, summary, page_count, input.estimated_tokens)
        {
            processed::store_indexing(
                cx.connection(),
                file.id,
                file.summary.as_deref(),
                file.page_count,
                file.estimated_tokens,
            )
            .await?;
            file.record(
                FileEvent::ReportStored {
                    job_id: input.job_id,
                    done: input.done,
                },
                &meta,
            );
            causes.push(FileCause::ReportStored {
                job_id: input.job_id,
                done: input.done,
            });
        }
        facts::save(cx, &mut file).await?;
        for cause in causes {
            crate::file::file_changed::<H>(cx, &file, cause)?;
        }
        // The final report ends the job, here: the chain moves on in this
        // transaction and Jobs is told (`job.finish`); its `completed` then
        // only confirms it.
        if input.done {
            let mut processing = crate::processing::load_processing(cx, &file).await?;
            crate::processing::report_done(cx, &meta, &mut file, &mut processing).await?;
        }
        Ok(())
    })
}

/// The longest reason code a runner may declare.
pub const MAX_FAILURE_REASON_BYTES: usize = 128;
/// The longest message a runner may attach to a declared failure.
pub const MAX_FAILURE_MESSAGE_BYTES: usize = 4096;

#[derive(Debug, Deserialize)]
pub struct RunnerReportFailure {
    pub file_id: Uuid,
    pub job_id: Uuid,
    pub reason_code: String,
    pub message: Option<String>,
}

impl MutationInput for RunnerReportFailure {
    type Output = ();
    type Error = DriveFault;
    const NAME: &'static str = "drive_runner_report_failure";
}

/// A reason code as a runner declares it: `[a-z0-9_]`, starting with a
/// letter, at most `MAX_FAILURE_REASON_BYTES` — a code a front can translate,
/// never a sentence.
fn valid_reason_code(code: &str) -> bool {
    !code.is_empty()
        && code.len() <= MAX_FAILURE_REASON_BYTES
        && code.starts_with(|c: char| c.is_ascii_lowercase())
        && code
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

/// The runner declares its job failed: the file lands FAILED with the given
/// reason code (`processingError`), Jobs is told (`job.fail`), and what the
/// run reported so far stays readable. Same checks as a report: the runner
/// scope, then the file's running job.
pub fn runner_report_failure<'m, H: DriveHost>(
    cx: &'m mut Mutation<'m, H>,
    input: RunnerReportFailure,
) -> BoxFuture<'m, Result<(), DriveFault>> {
    Box::pin(async move {
        runner_only(cx.principal())?;
        if !valid_reason_code(&input.reason_code)
            || input
                .message
                .as_ref()
                .is_some_and(|message| message.len() > MAX_FAILURE_MESSAGE_BYTES)
        {
            return Err(DriveFault::Refused(codes::INVALID_FAILURE_REASON));
        }
        let mut file = cx
            .load::<FileRow<H>>(&input.file_id)
            .await?
            .ok_or(DriveFault::Refused(codes::FILE_NOT_FOUND))?;
        file.require_active_job(input.job_id)?;
        let meta = FactMeta::of(cx.principal(), cx.now().as_datetime());
        let mut processing = crate::processing::load_processing(cx, &file).await?;
        crate::processing::report_failed(
            cx,
            &meta,
            &mut file,
            &mut processing,
            &input.reason_code,
            input.message.as_deref(),
        )
        .await?;
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_declared_reason_is_a_code_never_a_sentence() {
        for good in ["ocr_timeout", "a", "unreadable_scan_2"] {
            assert!(valid_reason_code(good), "{good}");
        }
        for bad in [
            "",
            "Timeout",
            "the scan is unreadable",
            "2fast",
            "_x",
            "ocr-timeout",
            &"a".repeat(MAX_FAILURE_REASON_BYTES + 1),
        ] {
            assert!(!valid_reason_code(bad), "{bad}");
        }
        assert!(valid_reason_code(&"a".repeat(MAX_FAILURE_REASON_BYTES)));
    }
}
