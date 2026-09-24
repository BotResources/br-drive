use std::collections::BTreeSet;
use std::marker::PhantomData;

use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::BlobRef;
use service_engine::blobs::{Sha256Digest, UploadExpectation};
use service_engine::error::EngineError;
use service_engine::name::ProjectorName;
use service_engine::persistence::Persistence;
use service_engine::pipeline::{Mutation, MutationInput, OneShot, Ops};
use service_engine::population::Population;
use service_engine::view::{Populate, Projector};
use service_engine::visibility::Unrestricted;
use sqlx::PgPool;
use uuid::Uuid;

use crate::blob::DriveImage;
use crate::fault::{DriveFault, codes};
use crate::file::images::{
    BlobFacts, ImageKey, ImageRecord, drop_images, image_names_of, image_names_of_page,
    references_image,
};
use crate::file::pages::{Page, PageCause, PageKey, RunnerPage, read_pages};
use crate::file::rendition::{apply_indexing, validate_rendition};
use crate::file::store::{self, FileStore, PageWrite};
use crate::file::{File, FileCause, FileRow, PageOrigin};
use crate::host::DriveHost;
use crate::image::ImageName;
use crate::media::MediaType;
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
    pub source_url: Option<String>,
    #[graphql(skip)]
    pub source: Uuid,
}

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
        let Ok((file_id, job_id)) = RUNNER_JOB.try_with(|scope| *scope) else {
            return Ok(Population::Keys(BTreeSet::new()));
        };
        if query.file_id.is_some_and(|wanted| wanted != file_id) {
            return Ok(Population::Keys(BTreeSet::new()));
        }
        let mut conn = cx.pool().acquire().await.map_err(EngineError::from)?;
        let active = <FileStore<H> as Persistence>::load(&mut conn, &file_id)
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
}

tokio::task_local! {
    static RUNNER_JOB: (Uuid, Uuid);
}

/// Runs `presign` with the runner source population scoped to `file_id` and
/// `job_id`. The engine asks a view's population without the key it is about
/// to serve, so the runner context resolver names the job's own file here
/// rather than letting the population cover every in-flight file. Engine 0.3.0
/// polls the population inside the `download` future itself (no spawned
/// task); were it ever to move to another task, the population would read no
/// scope and be empty — the runner would get `SOURCE_NOT_AVAILABLE`, never a
/// wider presign. Not a host API: the `drive_slice!` expansion calls it.
#[doc(hidden)]
pub async fn scoped_to_job<F: std::future::Future>(
    file_id: Uuid,
    job_id: Uuid,
    presign: F,
) -> F::Output {
    RUNNER_JOB.scope((file_id, job_id), presign).await
}

fn runner_only<H: DriveHost>(principal: &H) -> Result<(), DriveFault> {
    if principal.is_runner() {
        Ok(())
    } else {
        Err(DriveFault::Refused(codes::RUNNER_SCOPE_REQUIRED))
    }
}

pub async fn runner_context<H: DriveHost>(
    pool: &PgPool,
    principal: &H,
    file_id: Uuid,
    job_id: Uuid,
) -> Result<RunnerContext, DriveFault> {
    runner_only(principal)?;
    let mut conn = pool.acquire().await.map_err(EngineError::from)?;
    let file = <FileStore<H> as Persistence>::load(&mut conn, &file_id)
        .await?
        .ok_or(DriveFault::Refused(codes::FILE_NOT_FOUND))?;
    file.require_active_job(job_id)?;
    let pages = read_pages(&mut conn, file.id).await?;
    let images = image_names_of(&mut conn, file.id).await?;
    Ok(RunnerContext {
        file_id: file.id,
        media_type: file.media_type.as_str().to_string(),
        name: file.name.as_str().to_string(),
        page_count: file.page_count,
        summary: file.summary.clone(),
        pages,
        images,
        source_url: None,
        source: file.blob_ref,
    })
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
        let file = cx
            .load::<FileRow<H>>(&input.file_id)
            .await?
            .ok_or(DriveFault::Refused(codes::FILE_NOT_FOUND))?;
        file.require_active_job(input.job_id)?;
        let window = cx.principal().upload_window();
        let ticket = stage_image(cx, &file, name, media_type, size_bytes, digest, window).await?;
        Ok(OneShot(ticket))
    })
}

/// Stages a verified image upload on `file`: a new row, or the replacement of
/// an existing name that swaps only when the new object lands. Shared by the
/// runner's image ticket and the host's import.
pub(crate) async fn stage_image<H: DriveHost>(
    cx: &mut Ops<'_>,
    file: &FileRow<H>,
    name: ImageName,
    media_type: MediaType,
    size_bytes: i64,
    digest: Sha256Digest,
    window: std::time::Duration,
) -> Result<UploadTicket, DriveFault> {
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
            let image = ImageRecord::<H>::new(file.id, name.clone(), facts, now);
            cx.create(&image).await?;
        }
    }
    crate::file::file_changed::<H>(
        cx,
        file,
        FileCause::ImageRequested {
            name: name.as_str().to_string(),
        },
    )?;
    Ok(UploadTicket::new(file.id, blob.upload_url()))
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
    let indexed = validate_rendition(
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
        let by = cx.principal().id().as_uuid();
        let now = cx.now().as_datetime();
        // A report is a sign of life of a started run: the step's deadline
        // moves back (and the pickup stage is over, should the started fact
        // come late).
        crate::processing::run_alive(&mut file, now);

        let mut dropped = Vec::new();
        if input.origin == PageOrigin::Regenerated {
            for page in &input.pages {
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
        let writes: Vec<PageWrite<'_>> = input
            .pages
            .iter()
            .map(|page| PageWrite {
                number: page.number,
                markdown: &page.markdown,
                origin: input.origin,
            })
            .collect();
        store::upsert_pages(cx.connection(), file.id, &writes, by, now).await?;
        for page in &input.pages {
            cx.impact_caused::<Page, _>(
                &PageKey {
                    file_id: file.id,
                    number: page.number,
                },
                PageCause::Reported {
                    job_id: input.job_id,
                    origin: input.origin,
                },
            )?;
        }
        if !dropped.is_empty() {
            crate::file::file_changed::<H>(cx, &file, FileCause::ImagesDropped { names: dropped })?;
        }
        let mut dirty = false;
        let mut cause = None;
        if let (Some(summary), Some(page_count)) = (input.summary, input.page_count)
            && apply_indexing(&mut file, summary, page_count, input.estimated_tokens)
        {
            dirty = true;
            cause = Some(FileCause::ReportStored {
                job_id: input.job_id,
                done: input.done,
            });
        }
        if input.done && file.done_at.is_none() {
            file.done_at = Some(now);
            dirty = true;
            if file.completed_at.is_some() {
                // Jobs already said the job is over: the report is what the
                // chain was waiting for.
                if dirty {
                    file.updated_at = now;
                }
                if let Some(cause) = cause {
                    crate::file::file_changed::<H>(cx, &file, cause)?;
                }
                crate::processing::advance(cx, &mut file).await?;
                return Ok(());
            }
            crate::processing::finish_active_job(cx, &file)?;
        }
        // Saved every time for the step's sign of life; `updated_at` moves only
        // with what the file shows.
        if dirty {
            file.updated_at = now;
        }
        cx.save(&file).await?;
        if let Some(cause) = cause {
            crate::file::file_changed::<H>(cx, &file, cause)?;
        }
        Ok(())
    })
}
