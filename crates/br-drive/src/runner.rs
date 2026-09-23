use std::collections::{BTreeSet, HashSet};
use std::marker::PhantomData;

use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::blobs::{Sha256Digest, UploadExpectation};
use service_engine::error::EngineError;
use service_engine::name::ProjectorName;
use service_engine::pipeline::{Mutation, MutationInput, OneShot};
use service_engine::population::Population;
use service_engine::view::{Populate, Projector};
use service_engine::visibility::Unrestricted;
use uuid::Uuid;

use crate::blob::DriveImage;
use crate::fault::{DriveFault, codes};
use crate::file::store::{self, FileStore};
use crate::file::{DrivePage, File, FileCause, FileRow, ImageRow, PageOrigin};
use crate::host::DriveHost;
use crate::image::ImageName;
use crate::media::MediaType;
use crate::upload::UploadTicket;

service_engine::open_access!(
    pub RunnerAccess = "the runner roots are gated on the service passport's runner scope, never on a drive cohort"
);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct RunnerContext {
    pub file_id: Uuid,
    pub media_type: String,
    pub name: String,
    pub page_count: Option<i32>,
    pub pages: Vec<DrivePage>,
    pub images: Vec<String>,
    pub source_url: Option<String>,
    #[graphql(skip)]
    pub source: Uuid,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunnerWindow {
    pub file_id: Option<Uuid>,
}

pub struct RunnerFiles<H>(PhantomData<fn() -> H>);

impl<H> Default for RunnerFiles<H> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<H> RunnerFiles<H> {
    pub const NAME: ProjectorName = ProjectorName::from_static("drive_runner_files");
}

impl<H: DriveHost> Projector for RunnerFiles<H> {
    type Principal = H;
    type Noun = File;
    type Store = FileStore<H>;
    type Query = RunnerWindow;
    type Out = RunnerContext;
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
                store::all_ids(&mut conn).await?.into_iter().collect()
            }
        };
        Ok(Population::Keys(keys))
    }

    fn project(row: &FileRow<H>, _principal: &H) -> Result<RunnerContext, EngineError> {
        Ok(RunnerContext {
            file_id: row.id,
            media_type: row.media_type.as_str().to_string(),
            name: row.name.as_str().to_string(),
            page_count: row.page_count,
            pages: row.pages.iter().map(DrivePage::from).collect(),
            images: row
                .images
                .iter()
                .map(|image| image.name.as_str().to_string())
                .collect(),
            source_url: None,
            source: row.blob_ref,
        })
    }
}

fn runner_only<H: DriveHost>(principal: &H) -> Result<(), DriveFault> {
    if principal.is_runner() {
        Ok(())
    } else {
        Err(DriveFault::Refused(codes::RUNNER_SCOPE_REQUIRED))
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
        let mut file = cx
            .load::<FileRow<H>>(&input.file_id)
            .await?
            .ok_or(DriveFault::Refused(codes::FILE_NOT_FOUND))?;
        file.require_active_job(input.job_id)?;
        let blob = cx.blob_verified::<DriveImage>(
            name.as_str().to_string(),
            media_type.as_str().to_string(),
            UploadExpectation::new(input.size, digest),
        )?;
        let page = name.page();
        file.put_image(ImageRow {
            name: name.clone(),
            blob_ref: blob.reference().as_uuid(),
            media_type,
            size_bytes: i64::try_from(input.size)
                .map_err(|_| DriveFault::Refused(codes::FILE_TOO_LARGE))?,
            sha256: *digest.as_bytes(),
            page: Some(page),
        });
        file.updated_at = cx.now().as_datetime();
        cx.save(&file).await?;
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

pub fn report_done<H: DriveHost>(_file: &FileRow<H>, _job_id: Uuid) {}

pub fn runner_report<'m, H: DriveHost>(
    cx: &'m mut Mutation<'m, H>,
    input: RunnerReport,
) -> BoxFuture<'m, Result<(), DriveFault>> {
    Box::pin(async move {
        runner_only(cx.principal())?;
        if input.origin == PageOrigin::Edited {
            return Err(DriveFault::Refused(codes::INVALID_PAGE_ORIGIN));
        }
        if input.pages.iter().any(|page| page.number < 1) {
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
            return Err(DriveFault::Refused(codes::INVALID_PAGE));
        }
        let mut file = cx
            .load::<FileRow<H>>(&input.file_id)
            .await?
            .ok_or(DriveFault::Refused(codes::FILE_NOT_FOUND))?;
        file.require_active_job(input.job_id)?;
        let by = cx.principal().id().as_uuid();
        let now = cx.now().as_datetime();
        let reported = input.pages.len();
        for page in input.pages {
            if input.origin == PageOrigin::Regenerated {
                let referenced: Vec<String> = file
                    .images
                    .iter()
                    .map(|image| image.name.as_str().to_string())
                    .filter(|name| page.markdown.contains(name.as_str()))
                    .collect();
                let keep: HashSet<&str> = referenced.iter().map(String::as_str).collect();
                file.drop_page_images_except(page.number, &keep);
            }
            file.upsert_page(page.number, page.markdown, input.origin, by, now);
        }
        if let (Some(summary), Some(page_count), Some(estimated_tokens)) =
            (input.summary, input.page_count, input.estimated_tokens)
        {
            file.summary = Some(summary);
            file.page_count = Some(page_count);
            file.estimated_tokens = Some(estimated_tokens);
        }
        file.updated_at = now;
        cx.save(&file).await?;
        if input.done {
            report_done::<H>(&file, input.job_id);
        }
        cx.impact_caused::<File, _>(
            &file.id,
            FileCause::ReportStored {
                job_id: input.job_id,
                pages: reported,
                done: input.done,
            },
        )?;
        Ok(())
    })
}
