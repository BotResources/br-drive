use std::time::Duration;

use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::gate::Gate;
use service_engine::impact::Deps;
use service_engine::pipeline::Ops;
use service_engine::principal::Principal;
use uuid::Uuid;

use crate::file::FileRow;
use crate::media::MediaType;
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
    ManageLabels,
    SetFileLabels {
        file: &'a FileRow<H>,
    },
}

impl<H> DriveRequest<'_, H> {
    pub fn drive(&self) -> Option<Uuid> {
        match self {
            Self::CreateFile { drive, .. }
            | Self::MoveFolder { drive, .. }
            | Self::DeleteFolder { drive, .. } => Some(*drive),
            Self::ReadFile { file }
            | Self::UpdateFile { file, .. }
            | Self::DeleteFile { file }
            | Self::Process { file }
            | Self::EditPage { file }
            | Self::SetFileLabels { file } => Some(file.drive_id),
            Self::ManageLabels => None,
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

    fn drive_gate(&self, request: &DriveRequest<'_, Self>) -> Gate;

    fn visible_drives(&self) -> Vec<Uuid>;

    fn is_runner(&self) -> bool {
        let passport = self.passport();
        passport.service_account_id().is_some()
            && passport
                .claim::<Vec<String>>(SCOPES_CLAIM)
                .is_some_and(|scopes| scopes.iter().any(|scope| scope == Self::RUNNER_SCOPE))
    }

    fn active_job(file: &FileRow<Self>) -> Option<Uuid>;

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
