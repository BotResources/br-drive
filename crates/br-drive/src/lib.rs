mod admission;
mod blob;
mod catalogue;
mod drive;
mod erase;
mod facts;
mod fault;
mod file;
mod folders;
mod host;
mod host_window;
mod image;
mod label;
mod media;
mod owner;
mod path;
mod processing;
mod register;
mod ruleset;
mod runner;
mod slice;
mod title;
mod upload;

use std::ops::RangeInclusive;

use service_engine::LibraryMigrations;

#[doc(hidden)]
pub use admission::{admitted_mutation, admitted_query, admitted_subscription};
pub use blob::{DriveImage as DriveImageBlob, DriveSource};
pub use catalogue::{
    CatalogueWatch, DriveRunnerType, DriveRunnerTypeLifecycle, DriveRunnerTypes, KnownRunnerType,
    RunnerTypeAccess, RunnerTypeStore, known_runner_types, watch_runner_types,
    watch_runner_types_of,
};
pub use drive::{
    DriveDeleted, DriveFrozen, create_drive, create_unowned_drive, delete_drive,
    delete_drive_in_reaction, freeze_drive,
};
pub use erase::{DriveErasure, EraseMode, REDACTED_PERSON};
pub use facts::{ActorKind, DriveFact, FactMeta};
pub use fault::{DriveFault, DriveReactionFault, codes};
pub use file::{
    ByteCount, CancelProcessing, DeleteFile, DriveDelta, DriveFile, DriveFiles, DriveImage,
    DrivePage, DrivePages, DriveProgress, DriveRemove, DriveReset, DriveUpsert, DriveView,
    DriveWindow, EDIT_PAGE_ACTION, EditPage, FILE_EVENT_VERSION, File, FileCause, FileEvent,
    FilePlace, FileRow, FileStatus, FileVisibility, IMAGE_LANDED_AGGREGATE, IMAGE_LANDED_DURABLE,
    IMAGE_LANDED_VERB, ImageKey, ImageLanded, ImageRecord, PAGE_EVENT_VERSION, Page, PageCause,
    PageEvent, PageKey, PageOrigin, PageWindow, ProcessFile, ProcessingState,
    REGENERATE_PAGE_ACTION, RegeneratePage, RetitleFile, RunnerPage, UnknownDbValue, UpdateFile,
    drive_of, references_image, set_metadata,
};
pub use folders::{DeleteFolder, MoveFolder};
pub use host::{DRIVE_DIM, DriveHost, DriveRequest, SCOPES_CLAIM};
pub use image::{ImageName, InvalidImageName, MAX_IMAGE_NAME_BYTES};
pub use label::{
    CreateLabel, DeleteLabel, DriveLabel, DriveLabels, LABEL_EVENT_VERSION, Label, LabelCause,
    LabelEvent, LabelRecord, LabelRow, LabelStore, LabelWindow, MAX_LABEL_DESCRIPTION_BYTES,
    MAX_LABEL_NAME_CHARS, SetFileLabels, UpdateLabel,
};
pub use media::{InvalidMediaType, MAX_MEDIA_TYPE_BYTES, MediaType};
pub use owner::{
    DriveOwnerNoun, DriveOwnerObject, FileCounts, NoDriveOwner, OwnerObject, ProcessingCounts,
    Unowned, file_counts, processing_counts,
};
pub use path::{DrivePath, FileName, MAX_PATH_BYTES, MAX_SEGMENT_BYTES, PathError};
pub use processing::{
    CANCELLED, ChainPlan, FileJob, FileProcessing, FileProcessingNoun, FileProcessingStore,
    Initiator, PROCESSING_EVENT_VERSION, ProcessingEvent, RootNames, RunProgress, durable,
    ignored_because, job_event,
};
pub use register::register;
pub use ruleset::{
    ANY_MEDIA_TYPE, CreateRuleset, DeleteRuleset, DriveRuleset, DriveRulesets, DriveStep,
    MAX_RULESET_NAME_BYTES, MAX_RULESET_STEPS, MAX_RUNNER_TYPE_BYTES, RULESET_EVENT_VERSION,
    RulesetCause, RulesetEvent, RulesetRecord, RulesetRow, RulesetSaved, RulesetStep,
    RulesetStepInput, RulesetStore, Trigger, UpdateRuleset,
};
pub use runner::{
    MAX_FAILURE_MESSAGE_BYTES, MAX_FAILURE_REASON_BYTES, MAX_REPORT_PAGES, ReportedPage,
    ReportedPageInput, RunnerContext, RunnerReport, RunnerReportFailure, RunnerRequestImageUpload,
    RunnerSource, RunnerSources, RunnerWindow, runner_context, scoped_to_job,
};
pub use title::{FileTitle, InvalidTitle, MAX_TITLE_CHARS};
pub use upload::{CommitUpload, RequestUpload, RequestUploadWithRuleset, UploadTicket};

pub const NAME: &str = "drive";
pub const SCHEMA: &str = "drive";
pub const BAND: RangeInclusive<i64> = 9_121_000_001..=9_121_999_999;
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const DOWNLOAD_ACTION: &str = "download";

pub fn migrations() -> LibraryMigrations {
    LibraryMigrations {
        name: NAME,
        schema: SCHEMA,
        band: BAND,
        migrator: sqlx::migrate!("./migrations"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_declared_migrations_sit_inside_the_declared_band() {
        let library = migrations();
        let versions: Vec<i64> = library.migrator.iter().map(|m| m.version).collect();
        assert!(!versions.is_empty(), "the drive library ships migrations");
        for version in versions {
            assert!(
                BAND.contains(&version),
                "migration {version} escapes the drive band"
            );
        }
    }

    #[test]
    fn the_band_is_disjoint_from_the_engine_reserved_range_and_the_roster_example_band() {
        let engine = service_engine::schema::RESERVED_VERSION_MIN
            ..=service_engine::schema::RESERVED_VERSION_MAX;
        let roster = 9_120_000_001..=9_120_999_999;
        for other in [engine, roster] {
            assert!(BAND.start() > other.end() || BAND.end() < other.start());
        }
    }
}
