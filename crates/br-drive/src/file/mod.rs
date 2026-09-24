mod aggregate;
mod curation;
mod delta;
mod gestures;
pub(crate) mod images;
pub(crate) mod pages;
pub(crate) mod store;
mod view;

pub use aggregate::{
    File, FileCause, FileRow, FileVisibility, PageOrigin, ProcessingState, UnknownDbValue,
};
pub use curation::{drive_of, set_metadata, set_protected};
pub use delta::{DriveDelta, DriveRemove, DriveReset, DriveUpsert, DriveView};
pub use gestures::{
    DeleteFile, EditPage, Process, RegeneratePage, RetitleFile, UpdateFile, delete_file, edit_page,
    process, regenerate_page, retitle_file, update_file,
};
pub use images::{
    IMAGE_LANDED_AGGREGATE, IMAGE_LANDED_DURABLE, IMAGE_LANDED_VERB, ImageKey, ImageLanded,
    ImageRecord, references_image,
};
pub use pages::{
    DrivePage, DrivePages, EDIT_PAGE_ACTION, Page, PageCause, PageKey, PageWindow,
    REGENERATE_PAGE_ACTION, RunnerPage,
};
pub use view::{ByteCount, DriveFile, DriveFiles, DriveImage, DriveProgress, DriveWindow};

#[cfg(test)]
pub(crate) mod tests_support {
    use std::marker::PhantomData;

    use chrono::{DateTime, Utc};
    use uuid::Uuid;

    use super::{FileRow, ProcessingState};
    use crate::media::MediaType;
    use crate::path::{DrivePath, FileName};

    /// A file inside step `step`, entered at `entered`, for the unit tests of
    /// the rules that read a file's step.
    pub fn processing_file<H>(step: i32, entered: DateTime<Utc>) -> FileRow<H> {
        FileRow {
            id: Uuid::now_v7(),
            drive_id: Uuid::now_v7(),
            path: DrivePath::root(),
            name: FileName::parse("a.txt").expect("a sound name"),
            title: crate::title::FileTitle::parse("a").expect("a sound title"),
            protected: false,
            media_type: MediaType::parse("text/plain").expect("a sound media type"),
            size_bytes: 1,
            sha256: [0; 32],
            blob_ref: Uuid::now_v7(),
            processing_state: ProcessingState::Processing,
            processing_error: None,
            metadata: serde_json::json!({}),
            summary: None,
            page_count: None,
            estimated_tokens: None,
            ruleset_id: None,
            steps: None,
            step_index: Some(step),
            step_count: Some(step + 1),
            step_runner_type: None,
            job_id: None,
            plan: None,
            progress_index: None,
            progress_label: None,
            progress_at: None,
            triggered_by: None,
            done_at: None,
            completed_at: None,
            step_entered_at: Some(entered),
            step_alive_at: Some(entered),
            stray_job_id: None,
            created_by: Uuid::now_v7(),
            created_at: entered,
            updated_at: entered,
            host: PhantomData,
        }
    }
}
