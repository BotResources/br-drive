use std::collections::HashSet;

use futures_util::future::BoxFuture;
use serde::Deserialize;
use service_engine::gate::Reason;
use service_engine::pipeline::{Mutation, MutationInput, Ops};
use uuid::Uuid;

use crate::drive::DriveRow;
use crate::fault::{DriveFault, codes};
use crate::file::store;
use crate::file::{File, FileCause, FileRow};
use crate::host::{DriveHost, DriveRequest};
use crate::path::{DrivePath, FileName};

#[derive(Debug, Deserialize)]
pub struct UpdateFile {
    pub file_id: Uuid,
    pub name: Option<String>,
    pub path: Option<String>,
    pub drive_id: Option<Uuid>,
}

impl MutationInput for UpdateFile {
    type Output = ();
    type Error = DriveFault;
    const NAME: &'static str = "drive_update_file";
}

pub fn update_file<'m, H: DriveHost>(
    cx: &'m mut Mutation<'m, H>,
    input: UpdateFile,
) -> BoxFuture<'m, Result<(), DriveFault>> {
    Box::pin(async move {
        let name = input
            .name
            .as_deref()
            .map(FileName::parse)
            .transpose()
            .map_err(Reason::from)?;
        let path = input
            .path
            .as_deref()
            .map(DrivePath::parse)
            .transpose()
            .map_err(Reason::from)?;
        if let Some(target) = input.drive_id {
            cx.load::<DriveRow>(&target)
                .await?
                .ok_or(DriveFault::Refused(codes::DRIVE_NOT_FOUND))?;
        }
        let mut file = cx
            .load::<FileRow<H>>(&input.file_id)
            .await?
            .ok_or(DriveFault::Refused(codes::FILE_NOT_FOUND))?;
        let from_drive = file.drive_id;
        let target_drive = input.drive_id.unwrap_or(from_drive);
        file.move_to_gate(cx.principal(), target_drive).require()?;
        let target_path = path.unwrap_or_else(|| file.path.clone());
        let target_name = name.unwrap_or_else(|| file.name.clone());
        let unchanged =
            target_drive == from_drive && target_path == file.path && target_name == file.name;
        if unchanged {
            return Err(DriveFault::Refused(codes::NOTHING_TO_CHANGE));
        }
        let taken = store::sibling_names(cx.connection(), target_drive, &target_path).await?;
        if taken.iter().any(|taken| taken == target_name.as_str()) {
            return Err(DriveFault::Refused(codes::NAME_TAKEN));
        }
        let cause = if target_drive != from_drive {
            FileCause::Moved { from_drive }
        } else {
            FileCause::Renamed
        };
        file.drive_id = target_drive;
        file.path = target_path;
        file.name = target_name;
        file.updated_at = cx.now().as_datetime();
        cx.save(&file).await?;
        cx.impact_caused::<File, _>(&file.id, cause)?;
        Ok(())
    })
}

#[derive(Debug, Deserialize)]
pub struct DeleteFile {
    pub file_id: Uuid,
}

impl MutationInput for DeleteFile {
    type Output = ();
    type Error = DriveFault;
    const NAME: &'static str = "drive_delete_file";
}

pub fn delete_file<'m, H: DriveHost>(
    cx: &'m mut Mutation<'m, H>,
    input: DeleteFile,
) -> BoxFuture<'m, Result<(), DriveFault>> {
    Box::pin(async move {
        let file = cx
            .load::<FileRow<H>>(&input.file_id)
            .await?
            .ok_or(DriveFault::Refused(codes::FILE_NOT_FOUND))?;
        file.delete_gate(cx.principal()).require()?;
        cx.delete(&file).await?;
        cx.impact_caused::<File, _>(&file.id, FileCause::Deleted)?;
        Ok(())
    })
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

async fn folder_members<H: DriveHost>(
    ops: &mut Ops<'_>,
    drive: Uuid,
    prefix: &DrivePath,
) -> Result<Vec<FileRow<H>>, DriveFault> {
    let ids = store::ids_under_prefix(ops.connection(), drive, prefix).await?;
    let files = ops.load_many::<FileRow<H>>(&ids).await?;
    if files.is_empty() {
        return Err(DriveFault::Refused(codes::FOLDER_NOT_FOUND));
    }
    if files.iter().any(|file| file.protected) {
        return Err(DriveFault::Refused(codes::FILE_PROTECTED));
    }
    Ok(files)
}

pub fn move_folder<'m, H: DriveHost>(
    cx: &'m mut Mutation<'m, H>,
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
        cx.load::<DriveRow>(&input.drive_id)
            .await?
            .ok_or(DriveFault::Refused(codes::DRIVE_NOT_FOUND))?;
        cx.principal()
            .drive_gate(&DriveRequest::MoveFolder {
                drive: input.drive_id,
                old_prefix: &old_prefix,
                new_prefix: &new_prefix,
            })
            .require()?;
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
        for id in &ids {
            cx.impact_caused::<File, _>(id, FileCause::FolderMoved)?;
        }
        Ok(())
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
    cx: &'m mut Mutation<'m, H>,
    input: DeleteFolder,
) -> BoxFuture<'m, Result<(), DriveFault>> {
    Box::pin(async move {
        let prefix = DrivePath::parse(&input.prefix).map_err(Reason::from)?;
        if prefix.is_root() {
            return Err(DriveFault::Refused(codes::INVALID_PATH));
        }
        cx.load::<DriveRow>(&input.drive_id)
            .await?
            .ok_or(DriveFault::Refused(codes::DRIVE_NOT_FOUND))?;
        cx.principal()
            .drive_gate(&DriveRequest::DeleteFolder {
                drive: input.drive_id,
                prefix: &prefix,
            })
            .require()?;
        let files = folder_members::<H>(cx, input.drive_id, &prefix).await?;
        for file in &files {
            cx.delete(file).await?;
        }
        H::folder_deleted(cx, input.drive_id, &prefix).await?;
        for file in &files {
            cx.impact_caused::<File, _>(&file.id, FileCause::FolderDeleted)?;
        }
        Ok(())
    })
}
