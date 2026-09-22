mod blob;
mod drive;
mod fault;
mod file;
mod folders;
mod host;
mod path;
mod register;
mod slice;
mod upload;

use std::ops::RangeInclusive;

use service_engine::LibraryMigrations;

pub use blob::DriveSource;
pub use drive::{DriveDeleted, create_drive, delete_drive, drive_of, set_protected};
pub use fault::{DriveFault, DriveReactionFault, codes};
pub use file::{
    DriveDelta, DriveFile, DriveFileUnion, DriveFiles, DriveRemove, DriveReset, DriveUpsert,
    DriveWindow, File, FileCause, FileRow, FileVisibility, ProcessingState,
};
pub use folders::{DeleteFile, DeleteFolder, MoveFolder, UpdateFile};
pub use host::{DRIVE_DIM, DriveHost, DriveRequest};
pub use path::{DrivePath, FileName, PathError};
pub use register::register;
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
    fn the_band_is_disjoint_from_the_engine_reserved_range() {
        let engine = service_engine::schema::RESERVED_VERSION_MIN
            ..=service_engine::schema::RESERVED_VERSION_MAX;
        assert!(BAND.start() > engine.end() || BAND.end() < engine.start());
    }
}
