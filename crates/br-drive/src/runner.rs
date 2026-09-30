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
use crate::facts::{self, FactMeta};
use crate::fault::{DriveFault, codes};
use crate::file::images::{
    BlobFacts, ImageKey, ImageRecord, drop_images, image_names_of, image_names_of_page,
    references_image,
};
use crate::file::pages::{Page, PageCause, PageKey, RunnerPage, read_pages};
use crate::file::processed;
use crate::file::rendition::{apply_indexing, validate_rendition};
use crate::file::store::{self, FileStore, PageWrite};
use crate::file::{File, FileCause, FileEvent, FileRow, PageEvent, PageOrigin};
use crate::host::DriveHost;
use crate::image::ImageName;
use crate::media::MediaType;
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
        FileEvent::ImageRequested {
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
