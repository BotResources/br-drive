use std::time::Duration;

use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::gate::Gate;
use service_engine::impact::Deps;
use service_engine::pipeline::Ops;
use service_engine::principal::Principal;
use uuid::Uuid;

use crate::erase::EraseMode;
use crate::file::FileRow;
use crate::media::MediaType;
use crate::owner::DriveOwnerNoun;
use crate::path::{DrivePath, FileName};

pub const DRIVE_DIM: &str = "drive";
pub const SCOPES_CLAIM: &str = "scopes";

pub enum DriveRequest<'a, H> {
    CreateFile {
        drive: Uuid,
        path: &'a DrivePath,
        name: &'a FileName,
        media_type: &'a MediaType,
        size: u64,
    },
    /// Confirming a pending upload: the row carries its uploader
    /// (`created_by`), so a host can reserve the commit to them.
    CommitUpload {
        file: &'a FileRow<H>,
    },
    ReadFile {
        file: &'a FileRow<H>,
    },
    UpdateFile {
        file: &'a FileRow<H>,
        target_drive: Uuid,
    },
    DeleteFile {
        file: &'a FileRow<H>,
    },
    /// Changing a file's title; never its name, path or drive.
    RetitleFile {
        file: &'a FileRow<H>,
    },
    MoveFolder {
        drive: Uuid,
        old_prefix: &'a DrivePath,
        new_prefix: &'a DrivePath,
    },
    DeleteFolder {
        drive: Uuid,
        prefix: &'a DrivePath,
    },
    Process {
        file: &'a FileRow<H>,
    },
    EditPage {
        file: &'a FileRow<H>,
    },
    RegeneratePage {
        file: &'a FileRow<H>,
        number: i32,
    },
    ManageRulesets,
    ReadRulesets,
    ManageLabels,
    SetFileLabels {
        file: &'a FileRow<H>,
    },
    /// Writing a rendition or an image into a READY file without a runner or
    /// a job (`<p>ImportPages`, `<p>ImportImage`): the host decides who may.
    Import {
        file: &'a FileRow<H>,
    },
    /// Reading the host's label catalogue, live included.
    ReadLabels,
    /// Writing a file's free `metadata` through `br_drive::set_metadata`.
    SetMetadata {
        file: &'a FileRow<H>,
    },
}

impl<H> DriveRequest<'_, H> {
    pub fn drive(&self) -> Option<Uuid> {
        match self {
            Self::CreateFile { drive, .. }
            | Self::MoveFolder { drive, .. }
            | Self::DeleteFolder { drive, .. } => Some(*drive),
            Self::CommitUpload { file }
            | Self::ReadFile { file }
            | Self::SetMetadata { file }
            | Self::UpdateFile { file, .. }
            | Self::DeleteFile { file }
            | Self::RetitleFile { file }
            | Self::Import { file }
            | Self::Process { file }
            | Self::EditPage { file }
            | Self::RegeneratePage { file, .. }
            | Self::SetFileLabels { file } => Some(file.drive_id),
            Self::ManageRulesets | Self::ReadRulesets | Self::ManageLabels | Self::ReadLabels => {
                None
            }
        }
    }
}

pub trait DriveHost: Principal {
    const SERVICE: &'static str;

    const RUNNER_SCOPE: &'static str;

    const VISIBILITY_DEPS: Deps = Deps::ALL;

    const SOURCE_MAX_BYTES: u64 = 1 << 30;

    const SOURCE_ORPHAN_AFTER: Duration = Duration::from_secs(24 * 60 * 60);

    const BULK_RESET_THRESHOLD: usize = 256;

    const IMAGE_MAX_BYTES: u64 = 64 << 20;

    const IMAGE_ORPHAN_AFTER: Duration = Duration::from_secs(24 * 60 * 60);

    /// The host's own noun whose objects are keyed by the drive's id (the
    /// documented convention: a drive's id is its host object's id), or
    /// `br_drive::NoDriveOwner`. Every file change the library stages also
    /// impacts that key, so a host view bound to its own noun — an object
    /// showing file counts, say — recomputes and republishes. The noun is a
    /// type: its name and its UUID key are checked by the compiler.
    type DriveOwner: DriveOwnerNoun;

    /// How long a step of a processing chain may stay silent before the
    /// library cancels its job and fails the file with `timed_out`. Silence is
    /// measured from the step's last sign of life: its entry, then each run
    /// start, plan, step and runner report — so time spent queued counts until
    /// the first run starts. Jobs never fails a job no live runner picked up,
    /// so without it a file whose runner type has no live instance would sit in
    /// PROCESSING for good. The default is Jobs' own longest run (72 h), so the
    /// library never gives up on work Jobs still allows; a host that wants its
    /// users to learn sooner lowers it. Checked at registration: positive and
    /// within the scheduler's range.
    const STEP_TIMEOUT: Duration = Duration::from_secs(72 * 60 * 60);

    fn drive_gate(&self, request: &DriveRequest<'_, Self>) -> Gate;

    fn visible_drives(&self) -> Vec<Uuid>;

    /// The scope a service account must hold to import a rendition into a
    /// file (`<p>ImportPages`, `<p>ImportImage`); `None` (the default) means
    /// the host offers no import. The host's `DriveRequest::Import` gate then
    /// decides file by file.
    const IMPORT_SCOPE: Option<&'static str> = None;

    fn is_importer(&self) -> bool {
        let Some(import_scope) = Self::IMPORT_SCOPE else {
            return false;
        };
        let passport = self.passport();
        passport.service_account_id().is_some()
            && passport
                .claim::<Vec<String>>(SCOPES_CLAIM)
                .is_some_and(|scopes| scopes.iter().any(|scope| scope == import_scope))
    }

    fn is_runner(&self) -> bool {
        let passport = self.passport();
        passport.service_account_id().is_some()
            && passport
                .claim::<Vec<String>>(SCOPES_CLAIM)
                .is_some_and(|scopes| scopes.iter().any(|scope| scope == Self::RUNNER_SCOPE))
    }

    fn display_name(&self) -> Option<String> {
        None
    }

    /// What the engine's erase pipeline does with the rows a person created.
    fn erase_mode() -> EraseMode {
        EraseMode::Anonymise
    }

    fn upload_window(&self) -> Duration {
        Duration::from_secs(15 * 60)
    }

    fn folder_moved<'a, 'o>(
        _ops: &'a mut Ops<'o>,
        _drive: Uuid,
        _old_prefix: &'a DrivePath,
        _new_prefix: &'a DrivePath,
    ) -> BoxFuture<'a, Result<(), EngineError>>
    where
        'o: 'a,
    {
        Box::pin(async { Ok(()) })
    }

    fn folder_deleted<'a, 'o>(
        _ops: &'a mut Ops<'o>,
        _drive: Uuid,
        _prefix: &'a DrivePath,
    ) -> BoxFuture<'a, Result<(), EngineError>>
    where
        'o: 'a,
    {
        Box::pin(async { Ok(()) })
    }
}
