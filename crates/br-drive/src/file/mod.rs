mod aggregate;
mod curation;
mod delta;
mod gestures;
pub(crate) mod images;
pub(crate) mod pages;
pub(crate) mod rendition;
pub(crate) mod store;
mod view;

pub use aggregate::{
    File, FileCause, FileRow, FileStatus, FileVisibility, PageOrigin, ProcessingState,
    UnknownDbValue,
};
pub(crate) use aggregate::{file_changed, file_touched};
pub use curation::{drive_of, set_metadata, set_protected};
pub use delta::{DriveDelta, DriveRemove, DriveReset, DriveUpsert, DriveView};
pub use gestures::{
    CancelProcessing, DeleteFile, EditPage, Process, RegeneratePage, RetitleFile, UpdateFile,
    cancel_processing, delete_file, edit_page, process, regenerate_page, retitle_file, update_file,
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

    use chrono::Utc;
    use uuid::Uuid;

    use super::{FileRow, FileStatus, ProcessingState};
    use crate::media::MediaType;
    use crate::path::{DrivePath, FileName};

    /// A committed file in `state`, with no job, for the unit tests of the
    /// rules that read a file row.
    pub fn a_file<H>(state: ProcessingState) -> FileRow<H> {
        let now = Utc::now();
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
            committed_at: Some(now),
            metadata: serde_json::json!({}),
            summary: None,
            page_count: None,
            estimated_tokens: None,
            ruleset_id: None,
            steps: None,
            created_by: Uuid::now_v7(),
            created_at: now,
            updated_at: now,
            status: FileStatus {
                state,
                error: None,
                last_job: None,
            },
            host: PhantomData,
        }
    }
}
