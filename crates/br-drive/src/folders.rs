use std::collections::HashSet;

use futures_util::future::BoxFuture;
use serde::Deserialize;
use service_engine::BlobRef;
use service_engine::gate::Reason;
use service_engine::persistence::Aggregate;
use service_engine::pipeline::{Bulk, MutationInput};
use uuid::Uuid;

use crate::drive::DriveRow;
use crate::fault::{DriveFault, codes};
use crate::file::store;
use crate::file::{DriveFiles, DrivePages, File, FileCause, FileRow};
use crate::host::{DriveHost, DriveRequest};
use crate::path::DrivePath;

pub(crate) async fn folder_members<H: DriveHost>(
    cx: &mut Bulk<'_, H>,
    drive: Uuid,
    prefix: &DrivePath,
) -> Result<Vec<FileRow<H>>, DriveFault> {
    let ids = store::ids_under_prefix(cx.connection(), drive, prefix).await?;
    let files = cx.load_many::<FileRow<H>>(&ids).await?;
    if files.is_empty() {
        return Err(DriveFault::Refused(codes::FOLDER_NOT_FOUND));
    }
    if files.iter().any(|file| file.protected) {
        return Err(DriveFault::Refused(codes::FILE_PROTECTED));
    }
    Ok(files)
}

pub(crate) async fn delete_rows<H: DriveHost>(
    cx: &mut Bulk<'_, H>,
    files: &[FileRow<H>],
) -> Result<(), DriveFault> {
    let ids: Vec<Uuid> = files.iter().map(|file| file.id).collect();
    let images = store::image_refs_of_files(cx.connection(), &ids).await?;
    store::delete_many(cx.connection(), &ids).await?;
    for file in files {
        crate::processing::cancel_active_job(cx, file)?;
        for reference in file.blob_refs() {
            cx.release_blob(reference)?;
        }
    }
    for reference in images {
        cx.release_blob(BlobRef(reference))?;
    }
    Ok(())
}

pub(crate) fn impact_rows<H: DriveHost>(
    cx: &mut Bulk<'_, H>,
    drive: Uuid,
    ids: &[Uuid],
    cause: FileCause,
) -> Result<(), DriveFault> {
    crate::owner::touch::<H, _>(cx, drive, &cause)?;
    if ids.len() > H::BULK_RESET_THRESHOLD {
        cx.impact_all_view::<DriveFiles<H>>();
        cx.impact_all_view::<DrivePages<H>>();
        return Ok(());
    }
    for id in ids {
        cx.impact_caused::<File, _>(id, &cause)?;
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
pub struct MoveFolder {
    pub drive_id: Uuid,
    pub old_prefix: String,
    pub new_prefix: String,
}

impl MutationInput for MoveFolder {
    type Output = ();
    type Error = DriveFault;
    const NAME: &'static str = "drive_move_folder";
}

pub fn move_folder<'m, H: DriveHost>(
    cx: &'m mut Bulk<'m, H>,
    input: MoveFolder,
) -> BoxFuture<'m, Result<(), DriveFault>> {
    Box::pin(async move {
        let old_prefix = DrivePath::parse(&input.old_prefix).map_err(Reason::from)?;
        let new_prefix = DrivePath::parse(&input.new_prefix).map_err(Reason::from)?;
        if old_prefix.is_root() {
            return Err(DriveFault::Refused(codes::INVALID_PATH));
        }
        if old_prefix == new_prefix {
            return Err(DriveFault::Refused(codes::NOTHING_TO_CHANGE));
        }
        if new_prefix.is_within(&old_prefix) {
            return Err(DriveFault::Refused(codes::FOLDER_INTO_ITSELF));
        }
        cx.principal()
            .drive_gate(&DriveRequest::MoveFolder {
                drive: input.drive_id,
                old_prefix: &old_prefix,
                new_prefix: &new_prefix,
            })
            .require()?;
        cx.load::<DriveRow>(&input.drive_id)
            .await?
            .ok_or(DriveFault::Refused(codes::DRIVE_NOT_FOUND))?;
        let files = folder_members::<H>(cx, input.drive_id, &old_prefix).await?;
        let moving: HashSet<Uuid> = files.iter().map(|file| file.id).collect();
        let occupied: HashSet<(String, String)> =
            store::entries_under_prefix(cx.connection(), input.drive_id, &new_prefix)
                .await?
                .into_iter()
                .filter(|(id, _, _)| !moving.contains(id))
                .map(|(_, path, name)| (path, name))
                .collect();
        for file in &files {
            let landing = file
                .path
                .rebased(&old_prefix, &new_prefix)
                .ok_or(DriveFault::Refused(codes::INVALID_PATH))?;
            if occupied.contains(&(landing.into_string(), file.name.as_str().to_string())) {
                return Err(DriveFault::Refused(codes::NAME_TAKEN));
            }
        }
        let ids: Vec<Uuid> = files.iter().map(|file| file.id).collect();
        let now = cx.now().as_datetime();
        store::rebase_paths(cx.connection(), &ids, &old_prefix, &new_prefix, now).await?;
        H::folder_moved(cx, input.drive_id, &old_prefix, &new_prefix).await?;
        impact_rows(cx, input.drive_id, &ids, FileCause::FolderMoved)
    })
}

#[derive(Debug, Deserialize)]
pub struct DeleteFolder {
    pub drive_id: Uuid,
    pub prefix: String,
}

impl MutationInput for DeleteFolder {
    type Output = ();
    type Error = DriveFault;
    const NAME: &'static str = "drive_delete_folder";
}

pub fn delete_folder<'m, H: DriveHost>(
    cx: &'m mut Bulk<'m, H>,
    input: DeleteFolder,
) -> BoxFuture<'m, Result<(), DriveFault>> {
    Box::pin(async move {
        let prefix = DrivePath::parse(&input.prefix).map_err(Reason::from)?;
        if prefix.is_root() {
            return Err(DriveFault::Refused(codes::INVALID_PATH));
        }
        cx.principal()
            .drive_gate(&DriveRequest::DeleteFolder {
                drive: input.drive_id,
                prefix: &prefix,
            })
            .require()?;
        cx.load::<DriveRow>(&input.drive_id)
            .await?
            .ok_or(DriveFault::Refused(codes::DRIVE_NOT_FOUND))?;
        let files = folder_members::<H>(cx, input.drive_id, &prefix).await?;
        delete_rows(cx, &files).await?;
        H::folder_deleted(cx, input.drive_id, &prefix).await?;
        let ids: Vec<Uuid> = files.iter().map(|file| file.id).collect();
        impact_rows(cx, input.drive_id, &ids, FileCause::FolderDeleted)
    })
}
