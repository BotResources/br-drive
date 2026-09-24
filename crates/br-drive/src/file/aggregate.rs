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
use crate::processing::Initiator;
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
    Moved { from_drive: Uuid },
    FolderMoved,
    ProtectionChanged { protected: bool },
    MetadataChanged,
    ImageRequested { name: String },
    ImageAvailable { name: String },
    ImagesDropped { names: Vec<String> },
    ReportStored { job_id: Uuid, done: bool },
    RenditionImported,
    ProcessingStarted { job_id: Uuid, step: i32 },
    ProgressChanged,
    ProcessingFinished,
    ProcessingFailed { reason: String },
    LabelsChanged { detached: Option<Uuid> },
    Erased,
    Deleted,
    FolderDeleted,
    DriveDeleted,
    LaunchDeferred { step: i32 },
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
    pub processing_state: ProcessingState,
    pub processing_error: Option<String>,
    pub metadata: serde_json::Value,
    pub summary: Option<String>,
    pub page_count: Option<i32>,
    pub estimated_tokens: Option<i64>,
    pub ruleset_id: Option<Uuid>,
    pub steps: Option<Vec<RulesetStep>>,
    pub step_index: Option<i32>,
    pub step_count: Option<i32>,
    pub step_runner_type: Option<String>,
    pub job_id: Option<Uuid>,
    pub plan: Option<Vec<String>>,
    pub progress_index: Option<i32>,
    pub progress_label: Option<String>,
    pub progress_at: Option<DateTime<Utc>>,
    pub triggered_by: Option<Initiator>,
    pub done_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub step_entered_at: Option<DateTime<Utc>>,
    pub step_alive_at: Option<DateTime<Utc>>,
    /// When the step's current job first showed a started run: the pickup
    /// deadline gives way to the run-silence deadline from then on.
    pub run_started_at: Option<DateTime<Utc>>,
    pub stray_job_id: Option<Uuid>,
    pub created_by: Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub(crate) host: PhantomData<fn() -> H>,
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
            processing_state: self.processing_state,
            processing_error: self.processing_error.clone(),
            metadata: self.metadata.clone(),
            summary: self.summary.clone(),
            page_count: self.page_count,
            estimated_tokens: self.estimated_tokens,
            ruleset_id: self.ruleset_id,
            steps: self.steps.clone(),
            step_index: self.step_index,
            step_count: self.step_count,
            step_runner_type: self.step_runner_type.clone(),
            job_id: self.job_id,
            plan: self.plan.clone(),
            progress_index: self.progress_index,
            progress_label: self.progress_label.clone(),
            progress_at: self.progress_at,
            triggered_by: self.triggered_by.clone(),
            done_at: self.done_at,
            completed_at: self.completed_at,
            step_entered_at: self.step_entered_at,
            step_alive_at: self.step_alive_at,
            run_started_at: self.run_started_at,
            stray_job_id: self.stray_job_id,
            created_by: self.created_by,
            created_at: self.created_at,
            updated_at: self.updated_at,
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
    host_then(file, principal, request, || match file.processing_state {
        ProcessingState::Ready => None,
        ProcessingState::Processing => Some(codes::FILE_PROCESSING),
        ProcessingState::Pending | ProcessingState::Failed => Some(codes::FILE_NOT_READY),
    })
}

fn landed<H: DriveHost>(file: &FileRow<H>, principal: &H, request: DriveRequest<'_, H>) -> Gate {
    host_then(file, principal, request, || {
        (file.processing_state == ProcessingState::Pending).then_some(codes::FILE_NOT_READY)
    })
}

fn settled<H: DriveHost>(file: &FileRow<H>, principal: &H, request: DriveRequest<'_, H>) -> Gate {
    host_then(file, principal, request, || match file.processing_state {
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
            (this.processing_state != ProcessingState::Pending).then_some(codes::FILE_NOT_PENDING)
        })
    }
    "setMetadata" => fn set_metadata_gate(this, principal) {
        host_gate(this, principal, &DriveRequest::SetMetadata { file: this })
    }
    "retitle" => fn retitle_gate(this, principal) {
        host_gate(this, principal, &DriveRequest::RetitleFile { file: this })
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
            || {
                (self.processing_state != ProcessingState::Pending)
                    .then_some(codes::FILE_NOT_PENDING)
            },
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
        if self.processing_state == ProcessingState::Processing && self.job_id == Some(job_id) {
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
