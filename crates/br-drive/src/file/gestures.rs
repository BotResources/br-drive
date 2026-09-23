use futures_util::future::BoxFuture;
use serde::Deserialize;
use service_engine::BlobRef;
use service_engine::gate::Reason;
use service_engine::pipeline::{Mutation, MutationInput};
use uuid::Uuid;

use super::aggregate::{File, FileCause, FileRow, PageOrigin};
use super::pages::{Page, PageCause, PageKey};
use super::store::{self, PageWrite};
use crate::drive::DriveRow;
use crate::fault::{DriveFault, codes};
use crate::host::DriveHost;
use crate::path::{DrivePath, FileName};

#[derive(Debug, Deserialize)]
pub struct EditPage {
    pub file_id: Uuid,
    pub number: i32,
    pub markdown: String,
}

impl MutationInput for EditPage {
    type Output = ();
    type Error = DriveFault;
    const NAME: &'static str = "drive_edit_page";
}

pub fn edit_page<'m, H: DriveHost>(
    cx: &'m mut Mutation<'m, H>,
    input: EditPage,
) -> BoxFuture<'m, Result<(), DriveFault>> {
    Box::pin(async move {
        let file = cx
            .load::<FileRow<H>>(&input.file_id)
            .await?
            .ok_or(DriveFault::Refused(codes::FILE_NOT_FOUND))?;
        file.edit_page_gate(cx.principal()).require()?;
        if !store::page_exists(cx.connection(), file.id, input.number).await? {
            return Err(DriveFault::Refused(codes::PAGE_NOT_FOUND));
        }
        let by = cx.principal().id().as_uuid();
        let now = cx.now().as_datetime();
        store::upsert_pages(
            cx.connection(),
            file.id,
            &[PageWrite {
                number: input.number,
                markdown: &input.markdown,
                origin: PageOrigin::Edited,
            }],
            by,
            now,
        )
        .await?;
        cx.impact_caused::<Page, _>(
            &PageKey {
                file_id: file.id,
                number: input.number,
            },
            PageCause::Edited,
        )?;
        Ok(())
    })
}

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
        let Some(current_drive) = store::drive_of(cx.connection(), input.file_id).await? else {
            return Err(DriveFault::Refused(codes::FILE_NOT_FOUND));
        };
        let target_drive = input.drive_id.unwrap_or(current_drive);
        let locked = cx
            .load_many::<DriveRow>(&[current_drive, target_drive])
            .await?;
        if !locked.iter().any(|drive| drive.id == target_drive) {
            return Err(DriveFault::Refused(codes::DRIVE_NOT_FOUND));
        }
        let mut file = cx
            .load::<FileRow<H>>(&input.file_id)
            .await?
            .ok_or(DriveFault::Refused(codes::FILE_NOT_FOUND))?;
        let from_drive = file.drive_id;
        if from_drive != current_drive {
            cx.load::<DriveRow>(&from_drive).await?;
        }
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
        let images = store::image_refs_of_files(cx.connection(), &[file.id]).await?;
        cx.delete(&file).await?;
        for reference in images {
            cx.release_blob(BlobRef(reference))?;
        }
        cx.impact_caused::<File, _>(&file.id, FileCause::Deleted)?;
        Ok(())
    })
}
