mod blob;
mod catalogue;
mod drive;
mod erase;
mod fault;
mod file;
mod folders;
mod host;
mod image;
mod label;
mod media;
mod path;
mod processing;
mod register;
mod ruleset;
mod runner;
mod slice;
mod upload;

use std::ops::RangeInclusive;

use service_engine::LibraryMigrations;

pub use blob::{DriveImage as DriveImageBlob, DriveSource};
pub use catalogue::{CatalogueWatch, watch_runner_types};
pub use drive::{DriveDeleted, create_drive, delete_drive};
pub use erase::{DriveErasure, EraseMode, REDACTED_PERSON};
pub use fault::{DriveFault, DriveReactionFault, codes};
pub use file::{
    ByteCount, DeleteFile, DriveDelta, DriveFile, DriveFiles, DriveImage, DrivePage, DrivePages,
    DriveProgress, DriveRemove, DriveReset, DriveUpsert, DriveView, DriveWindow, EDIT_PAGE_ACTION,
    EditPage, File, FileCause, FileRow, FileVisibility, IMAGE_LANDED_AGGREGATE,
    IMAGE_LANDED_DURABLE, IMAGE_LANDED_VERB, ImageKey, ImageLanded, ImageRecord, Page, PageCause,
    PageKey, PageOrigin, PageWindow, Process, ProcessingState, REGENERATE_PAGE_ACTION,
    RegeneratePage, RunnerPage, UnknownDbValue, UpdateFile, drive_of, references_image,
    set_metadata, set_protected,
};
pub use folders::{DeleteFolder, MoveFolder};
pub use host::{DRIVE_DIM, DriveHost, DriveRequest, SCOPES_CLAIM};
pub use image::{ImageName, InvalidImageName, MAX_IMAGE_NAME_BYTES};
pub use label::{
    CreateLabel, DeleteLabel, DriveLabel, DriveLabels, Label, LabelCause, LabelRow, LabelWindow,
    MAX_LABEL_DESCRIPTION_BYTES, MAX_LABEL_NAME_CHARS, SetFileLabels, UpdateLabel,
};
pub use media::{InvalidMediaType, MAX_MEDIA_TYPE_BYTES, MediaType};
pub use path::{DrivePath, FileName, MAX_PATH_BYTES, MAX_SEGMENT_BYTES, PathError};
#[allow(deprecated)]
pub use processing::{
    CANCELLED, CATALOGUE_NOT_WATCHED, ChainPlan, Initiator, LAUNCH_RETRY_AFTER, LAUNCH_RETRY_CAP,
    RUNNER_TYPE_UNAVAILABLE, RootNames, TIMED_OUT, durable, retry_delay,
};
pub use register::register;
pub use ruleset::{
    ANY_MEDIA_TYPE, CreateRuleset, DeleteRuleset, DriveRuleset, DriveRulesets, DriveStep,
    MAX_RULESET_NAME_BYTES, MAX_RULESET_STEPS, MAX_RUNNER_TYPE_BYTES, RulesetCause, RulesetRow,
    RulesetSaved, RulesetStep, RulesetStepInput, Trigger, UpdateRuleset,
};
pub use runner::{
    MAX_REPORT_PAGES, ReportedPage, ReportedPageInput, RunnerContext, RunnerReport,
    RunnerRequestImageUpload, RunnerSource, RunnerSources, RunnerWindow, runner_context,
    scoped_to_job,
};
pub use upload::{CommitUpload, RequestUpload, UploadTicket};

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
