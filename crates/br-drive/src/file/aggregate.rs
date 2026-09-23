use std::collections::{BTreeSet, HashSet};
use std::marker::PhantomData;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use service_engine::Cohort;
use service_engine::gate::{Gate, Reason};
use service_engine::impact::Deps;
use service_engine::name::NounName;
use service_engine::visibility::{Cohorts, Visibility};
use service_engine::wire::Noun;
use uuid::Uuid;

use crate::fault::codes;
use crate::host::{DRIVE_DIM, DriveHost, DriveRequest};
use crate::image::ImageName;
use crate::media::MediaType;
use crate::path::{DrivePath, FileName};

pub struct File;

impl Noun for File {
    type Key = Uuid;
    const NAME: NounName = NounName::from_static("drive_file");
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, async_graphql::Enum)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProcessingState {
    Pending,
    Processing,
    Ready,
    Failed,
}

impl ProcessingState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Processing => "processing",
            Self::Ready => "ready",
            Self::Failed => "failed",
        }
    }

    pub fn from_db_str(text: &str) -> Result<Self, UnknownDbValue> {
        match text {
            "pending" => Ok(Self::Pending),
            "processing" => Ok(Self::Processing),
            "ready" => Ok(Self::Ready),
            "failed" => Ok(Self::Failed),
            other => Err(UnknownDbValue("processing_state", other.to_string())),
        }
    }
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, async_graphql::Enum,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PageOrigin {
    #[default]
    Runner,
    Regenerated,
    Edited,
}

impl PageOrigin {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Runner => "runner",
            Self::Regenerated => "regenerated",
            Self::Edited => "edited",
        }
    }

    pub fn from_db_str(text: &str) -> Result<Self, UnknownDbValue> {
        match text {
            "runner" => Ok(Self::Runner),
            "regenerated" => Ok(Self::Regenerated),
            "edited" => Ok(Self::Edited),
            other => Err(UnknownDbValue("origin", other.to_string())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown_db_value: {0} = {1}")]
pub struct UnknownDbValue(pub &'static str, pub String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum FileCause {
    UploadRequested,
    UploadCommitted,
    UploadAbandoned,
    SourceAvailable,
    Renamed,
    Moved {
        from_drive: Uuid,
    },
    FolderMoved,
    ProtectionChanged {
        protected: bool,
    },
    MetadataChanged,
    ImageRequested {
        name: String,
    },
    ImageAvailable {
        name: String,
    },
    ReportStored {
        job_id: Uuid,
        pages: usize,
        done: bool,
    },
    PageEdited {
        number: i32,
    },
    Deleted,
    FolderDeleted,
    DriveDeleted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageRow {
    pub number: i32,
    pub markdown: String,
    pub origin: PageOrigin,
    pub updated_by: Uuid,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageRow {
    pub name: ImageName,
    pub blob_ref: Uuid,
    pub media_type: MediaType,
    pub size_bytes: i64,
    pub sha256: [u8; 32],
    pub page: Option<i32>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Changes {
    pub pages: BTreeSet<i32>,
    pub images: BTreeSet<String>,
    pub dropped_images: BTreeSet<String>,
}

pub struct FileRow<H> {
    pub id: Uuid,
    pub drive_id: Uuid,
    pub path: DrivePath,
    pub name: FileName,
    pub protected: bool,
    pub media_type: MediaType,
    pub size_bytes: i64,
    pub sha256: [u8; 32],
    pub blob_ref: Uuid,
    pub processing_state: ProcessingState,
    pub processing_error: Option<String>,
    pub metadata: serde_json::Value,
    pub summary: Option<String>,
    pub page_count: Option<i32>,
    pub estimated_tokens: Option<i64>,
    pub created_by: Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub pages: Vec<PageRow>,
    pub images: Vec<ImageRow>,
    pub(crate) changes: Changes,
    pub(crate) host: PhantomData<fn() -> H>,
}

impl<H> Clone for FileRow<H> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            drive_id: self.drive_id,
            path: self.path.clone(),
            name: self.name.clone(),
            protected: self.protected,
            media_type: self.media_type.clone(),
            size_bytes: self.size_bytes,
            sha256: self.sha256,
            blob_ref: self.blob_ref,
            processing_state: self.processing_state,
            processing_error: self.processing_error.clone(),
            metadata: self.metadata.clone(),
            summary: self.summary.clone(),
            page_count: self.page_count,
            estimated_tokens: self.estimated_tokens,
            created_by: self.created_by,
            created_at: self.created_at,
            updated_at: self.updated_at,
            pages: self.pages.clone(),
            images: self.images.clone(),
            changes: self.changes.clone(),
            host: PhantomData,
        }
    }
}

impl<H> std::fmt::Debug for FileRow<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileRow")
            .field("id", &self.id)
            .field("drive_id", &self.drive_id)
            .field("path", &self.path)
            .field("name", &self.name)
            .field("processing_state", &self.processing_state)
            .field("pages", &self.pages.len())
            .field("images", &self.images.len())
            .finish_non_exhaustive()
    }
}

impl<H> FileRow<H> {
    pub fn page(&self, number: i32) -> Option<&PageRow> {
        self.pages.iter().find(|page| page.number == number)
    }

    pub fn image(&self, name: &str) -> Option<&ImageRow> {
        self.images.iter().find(|image| image.name.as_str() == name)
    }

    pub fn upsert_page(
        &mut self,
        number: i32,
        markdown: String,
        origin: PageOrigin,
        by: Uuid,
        at: DateTime<Utc>,
    ) {
        let page = PageRow {
            number,
            markdown,
            origin,
            updated_by: by,
            updated_at: at,
        };
        match self.pages.iter_mut().find(|page| page.number == number) {
            Some(existing) => *existing = page,
            None => {
                self.pages.push(page);
                self.pages.sort_by_key(|page| page.number);
            }
        }
        self.changes.pages.insert(number);
    }

    pub fn put_image(&mut self, image: ImageRow) {
        let name = image.name.as_str().to_string();
        match self
            .images
            .iter_mut()
            .find(|existing| existing.name == image.name)
        {
            Some(existing) => *existing = image,
            None => self.images.push(image),
        }
        self.changes.dropped_images.remove(&name);
        self.changes.images.insert(name);
    }

    pub fn drop_page_images_except(&mut self, page: i32, keep: &HashSet<&str>) {
        let dropped: Vec<String> = self
            .images
            .iter()
            .filter(|image| image.page == Some(page) && !keep.contains(image.name.as_str()))
            .map(|image| image.name.as_str().to_string())
            .collect();
        for name in dropped {
            self.images.retain(|image| image.name.as_str() != name);
            self.changes.images.remove(&name);
            self.changes.dropped_images.insert(name);
        }
    }
}

fn unprotected<H: DriveHost>(
    file: &FileRow<H>,
    principal: &H,
    request: DriveRequest<'_, H>,
) -> Gate {
    if file.protected {
        return Gate::blocked(codes::FILE_PROTECTED);
    }
    principal.drive_gate(&request)
}

fn ready<H: DriveHost>(file: &FileRow<H>, principal: &H, request: DriveRequest<'_, H>) -> Gate {
    if file.processing_state != ProcessingState::Ready {
        return Gate::blocked(codes::FILE_NOT_READY);
    }
    principal.drive_gate(&request)
}

service_engine::gated! {
    generics [H: DriveHost];
    FileRow<H>, H;
    "delete" => fn delete_gate(this, principal) {
        unprotected(this, principal, DriveRequest::DeleteFile { file: this })
    }
    "rename" => fn rename_gate(this, principal) {
        unprotected(
            this,
            principal,
            DriveRequest::UpdateFile {
                file: this,
                target_drive: this.drive_id,
            },
        )
    }
    "move" => fn move_gate(this, principal) {
        unprotected(
            this,
            principal,
            DriveRequest::UpdateFile {
                file: this,
                target_drive: this.drive_id,
            },
        )
    }
    "download" => fn download_gate(this, principal) {
        ready(this, principal, DriveRequest::ReadFile { file: this })
    }
    "editPage" => fn edit_page_gate(this, principal) {
        ready(this, principal, DriveRequest::EditPage { file: this })
    }
}

impl<H: DriveHost> FileRow<H> {
    pub fn require_pending(&self) -> Result<(), Reason> {
        if self.processing_state == ProcessingState::Pending {
            Ok(())
        } else {
            Err(codes::FILE_NOT_PENDING)
        }
    }

    pub fn move_to_gate(&self, principal: &H, target_drive: Uuid) -> Gate {
        unprotected(
            self,
            principal,
            DriveRequest::UpdateFile {
                file: self,
                target_drive,
            },
        )
    }

    pub fn as_create_request(&self) -> DriveRequest<'_, H> {
        DriveRequest::CreateFile {
            drive: self.drive_id,
            path: &self.path,
            name: &self.name,
            media_type: &self.media_type,
            size: u64::try_from(self.size_bytes).unwrap_or(0),
        }
    }

    pub fn require_active_job(&self, job_id: Uuid) -> Result<(), Reason> {
        if H::active_job(self) == Some(job_id) {
            Ok(())
        } else {
            Err(codes::JOB_NOT_ACTIVE)
        }
    }
}

pub struct FileVisibility<H>(PhantomData<fn() -> H>);

impl<H: DriveHost> Visibility for FileVisibility<H> {
    type Row = FileRow<H>;
    type Principal = H;

    const DEPS: Deps = H::VISIBILITY_DEPS;

    fn cohorts(row: &FileRow<H>) -> Cohorts {
        vec![Cohort::uuid(DRIVE_DIM, row.drive_id)]
    }

    fn memberships(principal: &H) -> Cohorts {
        principal
            .visible_drives()
            .into_iter()
            .map(|drive| Cohort::uuid(DRIVE_DIM, drive))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_processing_state_maps_totally_and_an_unknown_text_is_a_typed_error() {
        for state in [
            ProcessingState::Pending,
            ProcessingState::Processing,
            ProcessingState::Ready,
            ProcessingState::Failed,
        ] {
            assert_eq!(ProcessingState::from_db_str(state.as_str()), Ok(state));
        }
        assert_eq!(
            ProcessingState::from_db_str("weird"),
            Err(UnknownDbValue("processing_state", "weird".into()))
        );
    }

    #[test]
    fn the_page_origin_maps_totally() {
        for origin in [
            PageOrigin::Runner,
            PageOrigin::Regenerated,
            PageOrigin::Edited,
        ] {
            assert_eq!(PageOrigin::from_db_str(origin.as_str()), Ok(origin));
        }
        assert!(PageOrigin::from_db_str("guessed").is_err());
    }

    #[test]
    fn the_state_serializes_as_screaming_snake_on_the_wire() {
        assert_eq!(
            serde_json::to_string(&ProcessingState::Ready).unwrap(),
            "\"READY\""
        );
        assert_eq!(
            serde_json::to_string(&PageOrigin::Regenerated).unwrap(),
            "\"REGENERATED\""
        );
    }
}
