use std::collections::HashSet;

use futures_util::future::BoxFuture;
use serde::Deserialize;
use service_engine::BlobRef;
use service_engine::gate::{Gate, Reason};
use service_engine::persistence::Aggregate;
use service_engine::pipeline::{Bulk, MutationInput};
use uuid::Uuid;

use crate::drive::DriveRow;
use crate::fault::{DriveFault, codes};
use crate::file::store;
use crate::file::{DriveFiles, DrivePages, File, FileCause, FileRow};
use crate::host::{DriveHost, DriveRequest};
use crate::path::DrivePath;

/// The files under `prefix`, each one allowed to the principal by the same
/// decision the per-file gesture asks (`gate`: `UpdateFile` for a move,
/// `DeleteFile` for a delete, protection included) — all or nothing, so a
/// folder gesture never reaches a file its principal could not move or delete
/// one by one (`folder_verdict`). The decisions are in memory over the rows
/// the gesture loads anyway: they add no statement, whatever the folder's size.
pub(crate) async fn folder_members<H: DriveHost>(
    cx: &mut Bulk<'_, H>,
    drive: Uuid,
    prefix: &DrivePath,
    gate: impl Fn(&FileRow<H>, &H) -> Gate,
) -> Result<Vec<FileRow<H>>, DriveFault> {
    let ids = store::ids_under_prefix(cx.connection(), drive, prefix).await?;
    let mut files = cx.load_many::<FileRow<H>>(&ids).await?;
    files.sort_by(|a, b| {
        (a.path.as_str(), a.name.as_str()).cmp(&(b.path.as_str(), b.name.as_str()))
    });
    let principal = cx.principal();
    folder_verdict(files.iter().map(|file| gate(file, principal).reason()))
        .map_err(DriveFault::Refused)?;
    Ok(files)
}

/// The verdict of a folder gesture from the per-file refusals, in path order.
/// An empty folder is `FOLDER_NOT_FOUND`. A file refused as not found (the
/// principal cannot see it) makes the whole folder `FOLDER_NOT_FOUND` too,
/// wherever it sorts, so neither an invisible file nor its place is ever
/// disclosed. Otherwise the first refusal answers: the host's code, or
/// `FILE_PROTECTED`.
pub(crate) fn folder_verdict(
    refusals: impl IntoIterator<Item = Option<Reason>>,
) -> Result<(), Reason> {
    let mut any = false;
    let mut first = None;
    for refusal in refusals {
        any = true;
        match refusal {
            Some(reason) if reason == codes::FILE_NOT_FOUND => {
                return Err(codes::FOLDER_NOT_FOUND);
            }
            Some(reason) => {
                first.get_or_insert(reason);
            }
            None => {}
        }
    }
    match (any, first) {
        (false, _) => Err(codes::FOLDER_NOT_FOUND),
        (true, Some(reason)) => Err(reason),
        (true, None) => Ok(()),
    }
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
    crate::owner::touch::<H>(cx, drive)?;
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
        let files = folder_members::<H>(cx, input.drive_id, &old_prefix, |file, principal| {
            file.move_gate(principal)
        })
        .await?;
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
        let files = folder_members::<H>(cx, input.drive_id, &prefix, |file, principal| {
            file.delete_gate(principal)
        })
        .await?;
        delete_rows(cx, &files).await?;
        H::folder_deleted(cx, input.drive_id, &prefix).await?;
        let ids: Vec<Uuid> = files.iter().map(|file| file.id).collect();
        impact_rows(cx, input.drive_id, &ids, FileCause::FolderDeleted)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const HELD: Reason = Reason::new("FILE_ON_HOLD");

    #[test]
    fn an_empty_folder_is_not_found() {
        assert_eq!(folder_verdict([]), Err(codes::FOLDER_NOT_FOUND));
    }

    #[test]
    fn every_file_allowed_lets_the_folder_through() {
        assert_eq!(folder_verdict([None, None, None]), Ok(()));
    }

    #[test]
    fn the_first_refusal_in_path_order_answers_for_the_folder() {
        assert_eq!(
            folder_verdict([None, Some(codes::FILE_PROTECTED), Some(HELD)]),
            Err(codes::FILE_PROTECTED)
        );
        assert_eq!(
            folder_verdict([Some(HELD), Some(codes::FILE_PROTECTED)]),
            Err(HELD)
        );
    }

    #[test]
    fn an_invisible_file_anywhere_makes_the_folder_not_found() {
        assert_eq!(
            folder_verdict([Some(HELD), None, Some(codes::FILE_NOT_FOUND)]),
            Err(codes::FOLDER_NOT_FOUND),
            "the order of a hidden file never changes the answer"
        );
    }
}
