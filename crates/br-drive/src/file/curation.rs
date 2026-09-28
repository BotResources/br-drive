use service_engine::pipeline::Ops;
use uuid::Uuid;

use super::aggregate::{FileCause, FileRow};
use super::store;
use crate::fact::Author;
use crate::fault::{DriveFault, codes};
use crate::host::DriveHost;

pub async fn drive_of(ops: &mut Ops<'_>, file_id: Uuid) -> Result<Option<Uuid>, DriveFault> {
    Ok(store::drive_of(ops.connection(), file_id).await?)
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
    file.set_metadata_gate(principal).require()?;
    if file.metadata == metadata {
        return Err(DriveFault::Refused(codes::NOTHING_TO_CHANGE));
    }
    file.metadata = metadata;
    ops.save(&file).await?;
    let author = Author::of(principal);
    crate::file::file_recorded::<H>(ops, &author, &mut file, FileCause::MetadataChanged).await?;
    Ok(())
}
