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
use crate::media::MediaType;
use crate::path::{DrivePath, FileName};
use crate::processing::FileJob;
use crate::ruleset::RulesetStep;
use crate::title::FileTitle;

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
#[non_exhaustive]
pub enum FileCause {
    UploadRequested,
    UploadCommitted,
    UploadAbandoned,
    SourceAvailable,
    Renamed,
    Retitled,
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
    ImagesDropped {
        names: Vec<String>,
    },
    ReportStored {
        job_id: Uuid,
        done: bool,
    },
    RenditionImported,
    ProcessingStarted {
        job_id: Uuid,
        step: i32,
    },
    ProgressChanged,
    ProcessingFinished,
    ProcessingFailed {
        reason: String,
    },
    /// A user asked to cancel the running job; Jobs' `cancelled` follows.
    CancelRequested {
        job_id: Uuid,
    },
    LabelsChanged {
        detached: Option<Uuid>,
    },
    Erased,
    Deleted,
    FolderDeleted,
    DriveDeleted,
}

pub struct FileRow<H> {
    pub id: Uuid,
    pub drive_id: Uuid,
    pub path: DrivePath,
    pub name: FileName,
    pub title: FileTitle,
    pub protected: bool,
    pub media_type: MediaType,
    pub size_bytes: i64,
    pub sha256: [u8; 32],
    pub blob_ref: Uuid,
    /// When the upload was confirmed; `None` while the file is PENDING.
    pub committed_at: Option<DateTime<Utc>>,
    pub metadata: serde_json::Value,
    pub summary: Option<String>,
    pub page_count: Option<i32>,
    pub estimated_tokens: Option<i64>,
    pub ruleset_id: Option<Uuid>,
    pub steps: Option<Vec<RulesetStep>>,
    pub created_by: Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// The file's processing status, read from `drive.file_status` when the
    /// row is loaded — never written: the library changes it by writing the
    /// file's job log.
    pub(crate) status: FileStatus,
    pub(crate) host: PhantomData<fn() -> H>,
}

/// A file's processing status as `drive.file_status` computes it from the
/// file's last job, with that job.
#[derive(Debug, Clone, PartialEq)]
pub struct FileStatus {
    pub state: ProcessingState,
    pub error: Option<String>,
    pub last_job: Option<FileJob>,
}

impl FileStatus {
    /// A file just requested: no job, upload not confirmed.
    pub(crate) fn pending() -> Self {
        Self {
            state: ProcessingState::Pending,
            error: None,
            last_job: None,
        }
    }
}

impl<H> Clone for FileRow<H> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            drive_id: self.drive_id,
            path: self.path.clone(),
            name: self.name.clone(),
            title: self.title.clone(),
            protected: self.protected,
            media_type: self.media_type.clone(),
            size_bytes: self.size_bytes,
            sha256: self.sha256,
            blob_ref: self.blob_ref,
            committed_at: self.committed_at,
            metadata: self.metadata.clone(),
            summary: self.summary.clone(),
            page_count: self.page_count,
            estimated_tokens: self.estimated_tokens,
            ruleset_id: self.ruleset_id,
            steps: self.steps.clone(),
            created_by: self.created_by,
            created_at: self.created_at,
            updated_at: self.updated_at,
            status: self.status.clone(),
            host: PhantomData,
        }
    }
}

impl<H> FileRow<H> {
    /// `PENDING | PROCESSING | READY | FAILED`, computed from the job log.
    pub fn processing_state(&self) -> ProcessingState {
        self.status.state
    }

    /// The reason a FAILED file failed, read from its last job's log.
    pub fn processing_error(&self) -> Option<&str> {
        self.status.error.as_deref()
    }

    /// The file's last job (the one running while PROCESSING), if any.
    pub fn last_job(&self) -> Option<&FileJob> {
        self.status.last_job.as_ref()
    }

    /// The job running now: the last job while the file is PROCESSING.
    pub fn active_job(&self) -> Option<&FileJob> {
        self.last_job()
            .filter(|_| self.status.state == ProcessingState::Processing)
    }
}

impl<H> std::fmt::Debug for FileRow<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileRow")
            .field("id", &self.id)
            .field("drive_id", &self.drive_id)
            .field("path", &self.path)
            .field("name", &self.name)
            .field("processing_state", &self.status.state)
            .finish_non_exhaustive()
    }
}

// Every gate asks the host first and the file's state second, and a refusal
// about a file the principal cannot see is `FILE_NOT_FOUND` — exactly what an
// unknown id answers. A principal the host refuses therefore learns neither
// the state nor the existence of a file it may not see; one who sees the file
// learns the host's reason, then the state.

/// The host's answer about `file`, told as not found to a principal whose
/// visible drives do not hold the file.
pub(crate) fn host_gate<H: DriveHost>(
    file: &FileRow<H>,
    principal: &H,
    request: &DriveRequest<'_, H>,
) -> Gate {
    let host = principal.drive_gate(request);
    if host.is_allowed() || principal.visible_drives().contains(&file.drive_id) {
        host
    } else {
        Gate::blocked(codes::FILE_NOT_FOUND)
    }
}

fn host_then<H: DriveHost>(
    file: &FileRow<H>,
    principal: &H,
    request: DriveRequest<'_, H>,
    state: impl FnOnce() -> Option<Reason>,
) -> Gate {
    let host = host_gate(file, principal, &request);
    if !host.is_allowed() {
        return host;
    }
    match state() {
        Some(reason) => Gate::blocked(reason),
        None => host,
    }
}

fn unprotected<H: DriveHost>(
    file: &FileRow<H>,
    principal: &H,
    request: DriveRequest<'_, H>,
) -> Gate {
    host_then(file, principal, request, || {
        file.protected.then_some(codes::FILE_PROTECTED)
    })
}

fn ready<H: DriveHost>(file: &FileRow<H>, principal: &H, request: DriveRequest<'_, H>) -> Gate {
    host_then(file, principal, request, || match file.status.state {
        ProcessingState::Ready => None,
        ProcessingState::Processing => Some(codes::FILE_PROCESSING),
        ProcessingState::Pending | ProcessingState::Failed => Some(codes::FILE_NOT_READY),
    })
}

fn landed<H: DriveHost>(file: &FileRow<H>, principal: &H, request: DriveRequest<'_, H>) -> Gate {
    host_then(file, principal, request, || {
        (file.status.state == ProcessingState::Pending).then_some(codes::FILE_NOT_READY)
    })
}

fn settled<H: DriveHost>(file: &FileRow<H>, principal: &H, request: DriveRequest<'_, H>) -> Gate {
    host_then(file, principal, request, || match file.status.state {
        ProcessingState::Ready | ProcessingState::Failed => None,
        ProcessingState::Processing => Some(codes::FILE_PROCESSING),
        ProcessingState::Pending => Some(codes::FILE_NOT_READY),
    })
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
        landed(this, principal, DriveRequest::ReadFile { file: this })
    }
    "editPage" => fn edit_page_gate(this, principal) {
        ready(this, principal, DriveRequest::EditPage { file: this })
    }
    "process" => fn process_gate(this, principal) {
        settled(this, principal, DriveRequest::Process { file: this })
    }
    "setLabels" => fn set_labels_gate(this, principal) {
        host_gate(this, principal, &DriveRequest::SetFileLabels { file: this })
    }
    "commit" => fn commit_gate(this, principal) {
        host_then(this, principal, DriveRequest::CommitUpload { file: this }, || {
            (this.status.state != ProcessingState::Pending).then_some(codes::FILE_NOT_PENDING)
        })
    }
    "setMetadata" => fn set_metadata_gate(this, principal) {
        host_gate(this, principal, &DriveRequest::SetMetadata { file: this })
    }
    "retitle" => fn retitle_gate(this, principal) {
        host_gate(this, principal, &DriveRequest::RetitleFile { file: this })
    }
    "cancelProcessing" => fn cancel_processing_gate(this, principal) {
        host_then(this, principal, DriveRequest::CancelProcessing { file: this }, || {
            (this.status.state != ProcessingState::Processing)
                .then_some(codes::FILE_NOT_PROCESSING)
        })
    }
}

/// Stages the impact of a change on `file`: its own views, and the host object
/// of its drive (`DriveHost::DRIVE_OWNER_NOUN`).
pub(crate) fn file_changed<H: DriveHost>(
    ops: &mut service_engine::pipeline::Ops<'_>,
    file: &FileRow<H>,
    cause: FileCause,
) -> Result<(), service_engine::error::EngineError> {
    crate::owner::touch::<H>(ops, file.drive_id)?;
    ops.impact_caused::<File, _>(&file.id, cause)
}

/// Stages an impact on `file` without a cause: its views recompute, and a live
/// session receives a delta only if what it shows changed — for a job log
/// entry that may or may not show (a queued job, a late fact of an old job).
/// The host object is not touched: such an entry never changes the file's
/// state, so never the drive's counts.
pub(crate) fn file_touched<H: DriveHost>(
    ops: &mut service_engine::pipeline::Ops<'_>,
    file: &FileRow<H>,
) -> Result<(), service_engine::error::EngineError> {
    ops.impact::<File>(&file.id, service_engine::impact::Dims::ALL)
}

impl<H: DriveHost> FileRow<H> {
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

    /// The host's `Import` gate, then a READY file: an import never races a
    /// running chain nor lands on an upload that is not confirmed.
    pub fn import_gate(&self, principal: &H) -> Gate {
        ready(self, principal, DriveRequest::Import { file: self })
    }

    /// The host's `ImportCommit` gate, then a PENDING file: committing an
    /// upload without processing is an import's privilege (`<p>ImportCommit`).
    pub fn import_commit_gate(&self, principal: &H) -> Gate {
        host_then(
            self,
            principal,
            DriveRequest::ImportCommit { file: self },
            || (self.status.state != ProcessingState::Pending).then_some(codes::FILE_NOT_PENDING),
        )
    }

    pub fn read_gate(&self, principal: &H) -> Gate {
        host_gate(self, principal, &DriveRequest::ReadFile { file: self })
    }

    pub fn regenerate_page_gate(&self, principal: &H, number: i32) -> Gate {
        ready(
            self,
            principal,
            DriveRequest::RegeneratePage { file: self, number },
        )
    }

    pub fn require_active_job(&self, job_id: Uuid) -> Result<(), Reason> {
        if self.active_job().is_some_and(|job| job.job_id == job_id) {
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
        drive_memberships(principal)
    }
}

pub(crate) fn drive_memberships<H: DriveHost>(principal: &H) -> Cohorts {
    principal
        .visible_drives()
        .into_iter()
        .map(|drive| Cohort::uuid(DRIVE_DIM, drive))
        .collect()
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
