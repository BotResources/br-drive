use futures_util::future::BoxFuture;
use serde::Deserialize;
use service_engine::BlobRef;
use service_engine::gate::Reason;
use service_engine::pipeline::{Mutation, MutationInput};
use uuid::Uuid;

use super::aggregate::{File, FileCause, FileRow, PageOrigin, ProcessingState};
use super::pages::{Page, PageCause, PageKey};
use super::store::{self, PageWrite};
use crate::drive::DriveRow;
use crate::fault::{DriveFault, codes};
use crate::host::DriveHost;
use crate::path::{DrivePath, FileName};
use crate::processing;
use crate::ruleset::{Trigger, select_ruleset};
use crate::title::FileTitle;

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
pub struct RetitleFile {
    pub file_id: Uuid,
    pub title: String,
}

impl MutationInput for RetitleFile {
    type Output = ();
    type Error = DriveFault;
    const NAME: &'static str = "drive_retitle_file";
}

/// Changes the title and nothing else: the name, the path, the drive and the
/// processing state are untouched.
pub fn retitle_file<'m, H: DriveHost>(
    cx: &'m mut Mutation<'m, H>,
    input: RetitleFile,
) -> BoxFuture<'m, Result<(), DriveFault>> {
    Box::pin(async move {
        let title = FileTitle::parse(&input.title)
            .map_err(|_| DriveFault::Refused(codes::INVALID_TITLE))?;
        let mut file = cx
            .load::<FileRow<H>>(&input.file_id)
            .await?
            .ok_or(DriveFault::Refused(codes::FILE_NOT_FOUND))?;
        file.retitle_gate(cx.principal()).require()?;
        if file.title == title {
            return Err(DriveFault::Refused(codes::NOTHING_TO_CHANGE));
        }
        file.title = title;
        file.updated_at = cx.now().as_datetime();
        cx.save(&file).await?;
        cx.impact_caused::<File, _>(&file.id, FileCause::Retitled)?;
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
        processing::cancel_active_job(cx, &file)?;
        let images = store::image_refs_of_files(cx.connection(), &[file.id]).await?;
        cx.delete(&file).await?;
        for reference in images {
            cx.release_blob(BlobRef(reference))?;
        }
        cx.impact_caused::<File, _>(&file.id, FileCause::Deleted)?;
        Ok(())
    })
}

#[derive(Debug, Deserialize)]
pub struct Process {
    pub file_id: Uuid,
    pub ruleset_id: Option<Uuid>,
}

impl MutationInput for Process {
    type Output = ();
    type Error = DriveFault;
    const NAME: &'static str = "drive_process";
}

pub fn process<'m, H: DriveHost>(
    cx: &'m mut Mutation<'m, H>,
    input: Process,
) -> BoxFuture<'m, Result<(), DriveFault>> {
    Box::pin(async move {
        let mut file = cx
            .load::<FileRow<H>>(&input.file_id)
            .await?
            .ok_or(DriveFault::Refused(codes::FILE_NOT_FOUND))?;
        file.process_gate(cx.principal()).require()?;
        // The given rule, else the default `reprocess` rule of the media type,
        // else the file's own snapshot (a replay of what last ran).
        let plan = match select_ruleset(
            cx.connection(),
            Trigger::Reprocess,
            &file.media_type,
            input.ruleset_id,
        )
        .await?
        {
            Some(ruleset) => processing::ChainPlan::from_ruleset(&ruleset, None),
            None => processing::ChainPlan::replay(&file)
                .ok_or(DriveFault::Refused(codes::NO_RULESET_MATCHES))?,
        };
        processing::wipe_rendition(cx, &mut file).await?;
        let initiator = processing::Initiator::of(cx.principal());
        processing::start_chain(cx, &mut file, plan, initiator).await?;
        Ok(())
    })
}

#[derive(Debug, Deserialize)]
pub struct RegeneratePage {
    pub file_id: Uuid,
    pub number: i32,
    pub comment: Option<String>,
    pub ruleset_id: Option<Uuid>,
}

impl MutationInput for RegeneratePage {
    type Output = ();
    type Error = DriveFault;
    const NAME: &'static str = "drive_regenerate_page";
}

pub fn regenerate_page<'m, H: DriveHost>(
    cx: &'m mut Mutation<'m, H>,
    input: RegeneratePage,
) -> BoxFuture<'m, Result<(), DriveFault>> {
    Box::pin(async move {
        let mut file = cx
            .load::<FileRow<H>>(&input.file_id)
            .await?
            .ok_or(DriveFault::Refused(codes::FILE_NOT_FOUND))?;
        file.regenerate_page_gate(cx.principal(), input.number)
            .require()?;
        if !store::page_exists(cx.connection(), file.id, input.number).await? {
            return Err(DriveFault::Refused(codes::PAGE_NOT_FOUND));
        }
        let ruleset = select_ruleset(
            cx.connection(),
            Trigger::RegeneratePage,
            &file.media_type,
            input.ruleset_id,
        )
        .await?
        .ok_or(DriveFault::Refused(codes::NO_RULESET_MATCHES))?;
        let mut options = serde_json::Map::new();
        options.insert("page".into(), serde_json::Value::from(input.number));
        if let Some(comment) = input.comment {
            options.insert("comment".into(), serde_json::Value::String(comment));
        }
        debug_assert_eq!(file.processing_state, ProcessingState::Ready);
        let initiator = processing::Initiator::of(cx.principal());
        let plan = processing::ChainPlan::from_ruleset(
            &ruleset,
            Some(&serde_json::Value::Object(options)),
        );
        processing::start_chain(cx, &mut file, plan, initiator).await?;
        Ok(())
    })
}
