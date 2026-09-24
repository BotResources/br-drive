//! The host-privileged import: a rendition and its images written into a READY
//! file without a runner and without a job — for a downstream project moving
//! an existing corpus in, whose pages (human edits included) it already holds.
//! Every import gesture asks the host (`DriveRequest::Import`, or
//! `DriveRequest::ImportCommit` for the commit), so each host decides who may
//! call them, and they write through the same paths a
//! commit and a runner report do, so the views learn of every change. A
//! migrated source is committed without processing (`ImportCommit`), so a host
//! that declared its upload rules first still gets a READY file to import into.

use futures_util::future::BoxFuture;
use serde::Deserialize;
use service_engine::BlobReader;
use service_engine::blobs::Sha256Digest;
use service_engine::pipeline::{Mutation, MutationInput, OneShot};
use uuid::Uuid;

use crate::fault::{DriveFault, codes};
use crate::file::rendition::{apply_indexing, validate_rendition};
use crate::file::store::{self, PageWrite};
use crate::file::{FileCause, FileRow, Page, PageCause, PageKey, PageOrigin, ProcessingState};
use crate::host::DriveHost;
use crate::image::ImageName;
use crate::media::MediaType;
use crate::runner::stage_image;
use crate::upload::{UploadTicket, require_landed};

/// The library's own opt-in, before the host's per-file decision: only a
/// service account holding the host's `IMPORT_SCOPE` imports, and a host that
/// declares no scope has no import at all.
fn importer_only<H: DriveHost>(principal: &H) -> Result<(), DriveFault> {
    if principal.is_importer() {
        Ok(())
    } else {
        Err(DriveFault::Refused(codes::IMPORT_SCOPE_REQUIRED))
    }
}

#[derive(Debug, Deserialize)]
pub struct ImportCommit {
    pub file_id: Uuid,
}

impl MutationInput for ImportCommit {
    type Output = ();
    type Error = DriveFault;
    const NAME: &'static str = "drive_import_commit";
}

/// Confirms a pending upload the way `CommitUpload` does — the object is
/// present and is the pinned bytes — and lands the file READY without running
/// any rule, even when an `upload` rule matches: the file's rendition comes
/// from `ImportPages` / `ImportImage`, not from a runner. The same right as an
/// import: the host's `IMPORT_SCOPE`, then its own `ImportCommit` gate on the
/// pending row. A normal commit is unchanged.
pub fn import_commit<'m, H: DriveHost>(
    cx: &'m mut Mutation<'m, H>,
    input: ImportCommit,
    reader: BlobReader,
) -> BoxFuture<'m, Result<(), DriveFault>> {
    Box::pin(async move {
        importer_only(cx.principal())?;
        let mut file = cx
            .load::<FileRow<H>>(&input.file_id)
            .await?
            .ok_or(DriveFault::Refused(codes::FILE_NOT_FOUND))?;
        file.import_commit_gate(cx.principal()).require()?;
        require_landed(&reader, &file).await?;
        file.processing_state = ProcessingState::Ready;
        file.updated_at = cx.now().as_datetime();
        cx.save(&file).await?;
        crate::file::file_changed::<H>(cx, &file, FileCause::UploadCommitted)?;
        Ok(())
    })
}

#[derive(Debug, Clone, Deserialize)]
pub struct ImportedPage {
    pub number: i32,
    pub markdown: String,
    pub origin: PageOrigin,
}

#[derive(Debug, Clone, async_graphql::InputObject)]
pub struct ImportedPageInput {
    pub number: i32,
    pub markdown: String,
    /// `RUNNER` when absent; `EDITED` keeps a page a person had corrected;
    /// `REGENERATED` is refused (`INVALID_PAGE_ORIGIN`).
    pub origin: Option<PageOrigin>,
}

impl From<ImportedPageInput> for ImportedPage {
    fn from(input: ImportedPageInput) -> Self {
        Self {
            number: input.number,
            markdown: input.markdown,
            origin: input.origin.unwrap_or_default(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ImportPages {
    pub file_id: Uuid,
    pub pages: Vec<ImportedPage>,
    pub summary: Option<String>,
    pub page_count: Option<i32>,
    pub estimated_tokens: Option<i64>,
}

impl MutationInput for ImportPages {
    type Output = ();
    type Error = DriveFault;
    const NAME: &'static str = "drive_import_pages";
}

/// Upserts pages (by number, in batches of at most `MAX_REPORT_PAGES`) and,
/// optionally, the file's indexing — on a READY file, with no job and no
/// runner scope. The file stays READY.
pub fn import_pages<'m, H: DriveHost>(
    cx: &'m mut Mutation<'m, H>,
    input: ImportPages,
) -> BoxFuture<'m, Result<(), DriveFault>> {
    Box::pin(async move {
        importer_only(cx.principal())?;
        // An import writes what a runner or a person wrote before; a page
        // regenerated from its runner never comes from elsewhere.
        if input
            .pages
            .iter()
            .any(|page| page.origin == PageOrigin::Regenerated)
        {
            return Err(DriveFault::Refused(codes::INVALID_PAGE_ORIGIN));
        }
        let numbers: Vec<i32> = input.pages.iter().map(|page| page.number).collect();
        let indexed = validate_rendition(
            &numbers,
            input.summary.as_deref(),
            input.page_count,
            input.estimated_tokens,
        )?;
        let mut file = cx
            .load::<FileRow<H>>(&input.file_id)
            .await?
            .ok_or(DriveFault::Refused(codes::FILE_NOT_FOUND))?;
        file.import_gate(cx.principal()).require()?;
        if input.pages.is_empty() && !indexed {
            return Err(DriveFault::Refused(codes::NOTHING_TO_CHANGE));
        }
        let by = cx.principal().id().as_uuid();
        let now = cx.now().as_datetime();
        let writes: Vec<PageWrite<'_>> = input
            .pages
            .iter()
            .map(|page| PageWrite {
                number: page.number,
                markdown: &page.markdown,
                origin: page.origin,
            })
            .collect();
        store::upsert_pages(cx.connection(), file.id, &writes, by, now).await?;
        for page in &input.pages {
            cx.impact_caused::<Page, _>(
                &PageKey {
                    file_id: file.id,
                    number: page.number,
                },
                PageCause::Imported {
                    origin: page.origin,
                },
            )?;
        }
        if let (Some(summary), Some(page_count)) = (input.summary, input.page_count)
            && apply_indexing(&mut file, summary, page_count, input.estimated_tokens)
        {
            file.updated_at = now;
            cx.save(&file).await?;
            crate::file::file_changed::<H>(cx, &file, FileCause::RenditionImported)?;
        }
        Ok(())
    })
}

#[derive(Debug, Deserialize)]
pub struct ImportImage {
    pub file_id: Uuid,
    pub name: String,
    pub media_type: String,
    pub size: u64,
    pub sha256_hex: String,
}

impl MutationInput for ImportImage {
    type Output = OneShot<UploadTicket>;
    type Error = DriveFault;
    const NAME: &'static str = "drive_import_image";
}

/// A verified upload ticket for one image of a READY file, named by the page
/// convention — the runner's image path, without its job.
pub fn import_image<'m, H: DriveHost>(
    cx: &'m mut Mutation<'m, H>,
    input: ImportImage,
) -> BoxFuture<'m, Result<OneShot<UploadTicket>, DriveFault>> {
    Box::pin(async move {
        importer_only(cx.principal())?;
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
        file.import_gate(cx.principal()).require()?;
        let window = cx.principal().upload_window();
        let ticket = stage_image(cx, &file, name, media_type, size_bytes, digest, window).await?;
        Ok(OneShot(ticket))
    })
}
