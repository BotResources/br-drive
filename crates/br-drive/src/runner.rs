use std::collections::{BTreeSet, HashSet};
use std::marker::PhantomData;

use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::BlobRef;
use service_engine::blobs::{Sha256Digest, UploadExpectation};
use service_engine::error::EngineError;
use service_engine::name::ProjectorName;
use service_engine::persistence::Persistence;
use service_engine::pipeline::{Mutation, MutationInput, OneShot};
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
        let keys: BTreeSet<Uuid> = match query.file_id {
            Some(file_id) => BTreeSet::from([file_id]),
            None => {
                let mut conn = cx.pool().acquire().await.map_err(EngineError::from)?;
                store::ids_with_live_job(&mut conn)
                    .await?
                    .into_iter()
                    .collect()
            }
        };
        Ok(Population::Keys(keys))
    }

    fn project(row: &FileRow<H>, _principal: &H) -> Result<RunnerSource, EngineError> {
        Ok(RunnerSource { file_id: row.id })
    }
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
        let key = ImageKey {
            file_id: file.id,
            name: name.as_str().to_string(),
        };
        let now = cx.now().as_datetime();
        let existing = cx.load::<ImageRecord<H>>(&key).await?;
        if existing
            .as_ref()
            .is_some_and(|image| image.is_landing(now, cx.principal().upload_window()))
        {
            return Err(DriveFault::Refused(codes::IMAGE_UPLOAD_PENDING));
        }
        let blob = cx.blob_verified::<DriveImage>(
            name.as_str().to_string(),
            media_type.as_str().to_string(),
            UploadExpectation::new(input.size, digest),
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
        cx.impact_caused::<File, _>(
            &file.id,
            FileCause::ImageRequested {
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
    if input.pages.len() > MAX_REPORT_PAGES {
        return Err(DriveFault::Refused(codes::BATCH_TOO_LARGE));
    }
    let mut numbers = HashSet::with_capacity(input.pages.len());
    if input
        .pages
        .iter()
        .any(|page| page.number < 1 || !numbers.insert(page.number))
    {
        return Err(DriveFault::Refused(codes::INVALID_PAGE));
    }
    let indexer = [
        input.summary.is_some(),
        input.page_count.is_some(),
        input.estimated_tokens.is_some(),
    ];
    if indexer.iter().any(|given| *given) && !indexer.iter().all(|given| *given) {
        return Err(DriveFault::Refused(codes::INDEXER_FIELDS_TOGETHER));
    }
    if input.page_count.is_some_and(|count| count < 0)
        || input.estimated_tokens.is_some_and(|tokens| tokens < 0)
    {
        return Err(DriveFault::Refused(codes::INVALID_INDEXER_VALUE));
    }
    if input.pages.is_empty() && !indexer[0] && !input.done {
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
            cx.impact_caused::<File, _>(&file.id, FileCause::ImagesDropped { names: dropped })?;
        }
        let mut dirty = false;
        let mut cause = None;
        if let (Some(summary), Some(page_count), Some(estimated_tokens)) =
            (input.summary, input.page_count, input.estimated_tokens)
        {
            let changed = file.summary.as_deref() != Some(summary.as_str())
                || file.page_count != Some(page_count)
                || file.estimated_tokens != Some(estimated_tokens);
            if changed {
                file.summary = Some(summary);
                file.page_count = Some(page_count);
                file.estimated_tokens = Some(estimated_tokens);
                dirty = true;
                cause = Some(FileCause::ReportStored {
                    job_id: input.job_id,
                    done: input.done,
                });
            }
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
                    cx.impact_caused::<File, _>(&file.id, cause)?;
                }
                crate::processing::advance(cx, &mut file, input.job_id).await?;
                return Ok(());
            }
            crate::processing::finish_active_job(cx, &file)?;
        }
        if dirty {
            file.updated_at = now;
            cx.save(&file).await?;
        }
        if let Some(cause) = cause {
            cx.impact_caused::<File, _>(&file.id, cause)?;
        }
        Ok(())
    })
}
