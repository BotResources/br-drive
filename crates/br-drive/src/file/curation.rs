use service_engine::pipeline::Ops;
use uuid::Uuid;

use super::aggregate::{FileCause, FileRow};
use super::store;
use crate::fault::{DriveFault, codes};
use crate::host::{DriveHost, DriveRequest};

pub async fn drive_of(ops: &mut Ops<'_>, file_id: Uuid) -> Result<Option<Uuid>, DriveFault> {
    Ok(store::drive_of(ops.connection(), file_id).await?)
}

pub async fn set_protected<H: DriveHost>(
    ops: &mut Ops<'_>,
    file_id: Uuid,
    protected: bool,
) -> Result<(), DriveFault> {
    let mut file = ops
        .load::<FileRow<H>>(&file_id)
        .await?
        .ok_or(DriveFault::Refused(codes::FILE_NOT_FOUND))?;
    if file.protected == protected {
        return Err(DriveFault::Refused(codes::NOTHING_TO_CHANGE));
    }
    file.protected = protected;
    file.updated_at = ops.now().as_datetime();
    ops.save(&file).await?;
    crate::file::file_changed::<H>(ops, &file, FileCause::ProtectionChanged { protected })?;
    Ok(())
}

/// Writes the host's free JSON on a file, asking the host's gate
/// (`DriveRequest::SetMetadata`) on behalf of `principal` first.
pub async fn set_metadata<H: DriveHost>(
    ops: &mut Ops<'_>,
    principal: &H,
    file_id: Uuid,
    metadata: serde_json::Value,
) -> Result<(), DriveFault> {
    let mut file = ops
        .load::<FileRow<H>>(&file_id)
        .await?
        .ok_or(DriveFault::Refused(codes::FILE_NOT_FOUND))?;
    principal
        .drive_gate(&DriveRequest::SetMetadata { file: &file })
        .require()?;
    if file.metadata == metadata {
        return Err(DriveFault::Refused(codes::NOTHING_TO_CHANGE));
    }
    file.metadata = metadata;
    file.updated_at = ops.now().as_datetime();
    ops.save(&file).await?;
    crate::file::file_changed::<H>(ops, &file, FileCause::MetadataChanged)?;
    Ok(())
}
