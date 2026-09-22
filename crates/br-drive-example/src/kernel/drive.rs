use std::time::Duration;

use br_drive::{DriveHost, DrivePath, DriveRequest};
use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::gate::{Gate, Reason};
use service_engine::impact::Deps;
use service_engine::pipeline::Ops;
use uuid::Uuid;

use crate::kernel::facts::HostSettings;
use crate::kernel::{AppPrincipal, OWNERSHIP_DEP};

pub const NOT_THE_OWNER: Reason = Reason::new("NOT_THE_WORKSPACE_OWNER");
pub const UNRENDERABLE_MEDIA_TYPE: Reason = Reason::new("UNRENDERABLE_MEDIA_TYPE");
pub const FORBIDDEN_FOLDER: Reason = Reason::new("FORBIDDEN_FOLDER");

pub const UNRENDERABLE: &str = "application/x-unrenderable";
pub const FORBIDDEN_PREFIX: &str = "forbidden";

impl DriveHost for AppPrincipal {
    const SERVICE: &'static str = crate::SERVICE;

    const RUNNER_SCOPE: &'static str = "workspace:runner";

    const VISIBILITY_DEPS: Deps = Deps::from_bits(1 << OWNERSHIP_DEP);

    fn drive_gate(&self, request: &DriveRequest<'_, Self>) -> Gate {
        if let DriveRequest::CreateFile { media_type, .. } = request
            && *media_type == UNRENDERABLE
        {
            return Gate::blocked(UNRENDERABLE_MEDIA_TYPE);
        }
        let owned = request
            .drive()
            .is_some_and(|drive| self.owns_workspace(drive));
        let owned_target = match request {
            DriveRequest::UpdateFile { target_drive, .. } => self.owns_workspace(*target_drive),
            _ => true,
        };
        if owned && owned_target {
            Gate::allowed()
        } else {
            Gate::blocked(NOT_THE_OWNER)
        }
    }

    fn visible_drives(&self) -> Vec<Uuid> {
        self.owned_workspaces()
    }

    fn upload_window(&self) -> Duration {
        self.facts()
            .get::<HostSettings>()
            .map(|settings| settings.upload_window)
            .unwrap_or(HostSettings::DEFAULT_UPLOAD_WINDOW)
    }

    fn folder_moved<'a, 'o>(
        ops: &'a mut Ops<'o>,
        drive: Uuid,
        old_prefix: &'a DrivePath,
        new_prefix: &'a DrivePath,
    ) -> BoxFuture<'a, Result<(), EngineError>>
    where
        'o: 'a,
    {
        Box::pin(async move {
            refuse_forbidden(new_prefix)?;
            log_folder_gesture(
                ops,
                drive,
                "moved",
                old_prefix.as_str(),
                Some(new_prefix.as_str()),
            )
            .await
        })
    }

    fn folder_deleted<'a, 'o>(
        ops: &'a mut Ops<'o>,
        drive: Uuid,
        prefix: &'a DrivePath,
    ) -> BoxFuture<'a, Result<(), EngineError>>
    where
        'o: 'a,
    {
        Box::pin(async move {
            refuse_forbidden(prefix)?;
            log_folder_gesture(ops, drive, "deleted", prefix.as_str(), None).await
        })
    }
}

fn refuse_forbidden(prefix: &DrivePath) -> Result<(), EngineError> {
    if prefix.as_str() == FORBIDDEN_PREFIX {
        return Err(EngineError::PolicyRefused {
            code: FORBIDDEN_FOLDER.code(),
        });
    }
    Ok(())
}

async fn log_folder_gesture(
    ops: &mut Ops<'_>,
    drive: Uuid,
    gesture: &str,
    prefix: &str,
    new_prefix: Option<&str>,
) -> Result<(), EngineError> {
    let now = ops.now().as_datetime();
    sqlx::query(
        "INSERT INTO workspace_folder_gesture (id, workspace_id, gesture, prefix, new_prefix, at) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(Uuid::now_v7())
    .bind(drive)
    .bind(gesture)
    .bind(prefix)
    .bind(new_prefix)
    .bind(now)
    .execute(ops.connection())
    .await?;
    Ok(())
}
