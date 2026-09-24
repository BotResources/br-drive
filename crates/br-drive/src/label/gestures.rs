use std::collections::BTreeSet;

use futures_util::future::BoxFuture;
use serde::Deserialize;
use service_engine::pipeline::{Mutation, MutationInput};
use uuid::Uuid;

use service_engine::pipeline::Bulk;

use super::store::{
    existing_ids, files_with_label, labels_of_file, name_taken, replace_file_labels,
    serialize_labels,
};
use super::{Label, LabelCause, LabelRow, validate_color, validate_description, validate_name};
use crate::fault::{DriveFault, codes};
use crate::file::{DriveFiles, File, FileCause, FileRow};
use crate::host::{DriveHost, DriveRequest};

fn manage_gate<H: DriveHost>(principal: &H) -> Result<(), DriveFault> {
    principal
        .drive_gate(&DriveRequest::ManageLabels)
        .require()?;
    Ok(())
}

#[derive(Debug, Deserialize)]
pub struct CreateLabel {
    pub id: Uuid,
    pub name: String,
    pub color: String,
    pub description: Option<String>,
}

impl MutationInput for CreateLabel {
    type Output = ();
    type Error = DriveFault;
    const NAME: &'static str = "drive_create_label";
}

pub fn create_label<'m, H: DriveHost>(
    cx: &'m mut Mutation<'m, H>,
    input: CreateLabel,
) -> BoxFuture<'m, Result<(), DriveFault>> {
    Box::pin(async move {
        manage_gate(cx.principal())?;
        let name = validate_name(&input.name)?;
        let color = validate_color(&input.color)?;
        let description = validate_description(input.description.as_deref().unwrap_or(""))?;
        serialize_labels(cx.connection()).await?;
        if name_taken(cx.connection(), &name, None).await? {
            return Err(DriveFault::Refused(codes::LABEL_NAME_TAKEN));
        }
        let now = cx.now().as_datetime();
        let label = LabelRow {
            id: input.id,
            name,
            color,
            description,
            created_by: cx.principal().id().as_uuid(),
            created_at: now,
            updated_at: now,
        };
        cx.create(&label).await?;
        cx.impact_caused::<Label, _>(&label.id, LabelCause::Created)?;
        Ok(())
    })
}

#[derive(Debug, Deserialize)]
pub struct UpdateLabel {
    pub id: Uuid,
    pub name: Option<String>,
    pub color: Option<String>,
    pub description: Option<String>,
}

impl MutationInput for UpdateLabel {
    type Output = ();
    type Error = DriveFault;
    const NAME: &'static str = "drive_update_label";
}

pub fn update_label<'m, H: DriveHost>(
    cx: &'m mut Mutation<'m, H>,
    input: UpdateLabel,
) -> BoxFuture<'m, Result<(), DriveFault>> {
    Box::pin(async move {
        manage_gate(cx.principal())?;
        serialize_labels(cx.connection()).await?;
        let mut label = cx
            .load::<LabelRow>(&input.id)
            .await?
            .ok_or(DriveFault::Refused(codes::LABEL_NOT_FOUND))?;
        let name = match input.name.as_deref() {
            Some(name) => validate_name(name)?,
            None => label.name.clone(),
        };
        let color = match input.color.as_deref() {
            Some(color) => validate_color(color)?,
            None => label.color.clone(),
        };
        let description =
            validate_description(input.description.as_deref().unwrap_or(&label.description))?;
        if name == label.name && color == label.color && description == label.description {
            return Err(DriveFault::Refused(codes::NOTHING_TO_CHANGE));
        }
        if name_taken(cx.connection(), &name, Some(label.id)).await? {
            return Err(DriveFault::Refused(codes::LABEL_NAME_TAKEN));
        }
        label.name = name;
        label.color = color;
        label.description = description;
        label.updated_at = cx.now().as_datetime();
        cx.save(&label).await?;
        cx.impact_caused::<Label, _>(&label.id, LabelCause::Updated)?;
        Ok(())
    })
}

#[derive(Debug, Deserialize)]
pub struct DeleteLabel {
    pub id: Uuid,
}

impl MutationInput for DeleteLabel {
    type Output = ();
    type Error = DriveFault;
    const NAME: &'static str = "drive_delete_label";
}

/// Deleting a label detaches every file it was on: the bulk pipeline, so a
/// label on any number of files can go — one `LabelsChanged` per file up to
/// the host's threshold, a `DriveFiles` reset beyond it.
pub fn delete_label<'m, H: DriveHost>(
    cx: &'m mut Bulk<'m, H>,
    input: DeleteLabel,
) -> BoxFuture<'m, Result<(), DriveFault>> {
    Box::pin(async move {
        manage_gate(cx.principal())?;
        let label = cx
            .load::<LabelRow>(&input.id)
            .await?
            .ok_or(DriveFault::Refused(codes::LABEL_NOT_FOUND))?;
        let detached = files_with_label(cx.connection(), label.id).await?;
        if crate::owner::refreshes::<H>() {
            for drive in crate::file::store::drives_of(cx.connection(), &detached).await? {
                crate::owner::touch::<H>(cx, drive)?;
            }
        }
        cx.delete(&label).await?;
        cx.impact_caused::<Label, _>(&label.id, LabelCause::Deleted)?;
        if detached.len() > H::BULK_RESET_THRESHOLD {
            cx.impact_all_view::<DriveFiles<H>>();
            return Ok(());
        }
        for file in detached {
            cx.impact_caused::<File, _>(
                &file,
                FileCause::LabelsChanged {
                    detached: Some(label.id),
                },
            )?;
        }
        Ok(())
    })
}

#[derive(Debug, Deserialize)]
pub struct SetFileLabels {
    pub file_id: Uuid,
    pub label_ids: Vec<Uuid>,
}

impl MutationInput for SetFileLabels {
    type Output = ();
    type Error = DriveFault;
    const NAME: &'static str = "drive_set_file_labels";
}

pub fn set_file_labels<'m, H: DriveHost>(
    cx: &'m mut Mutation<'m, H>,
    input: SetFileLabels,
) -> BoxFuture<'m, Result<(), DriveFault>> {
    Box::pin(async move {
        let file = cx
            .load::<FileRow<H>>(&input.file_id)
            .await?
            .ok_or(DriveFault::Refused(codes::FILE_NOT_FOUND))?;
        file.set_labels_gate(cx.principal()).require()?;
        let wanted: BTreeSet<Uuid> = input.label_ids.iter().copied().collect();
        let wanted: Vec<Uuid> = wanted.into_iter().collect();
        let known = existing_ids(cx.connection(), &wanted).await?;
        if known.len() != wanted.len() {
            return Err(DriveFault::Refused(codes::LABEL_NOT_FOUND));
        }
        let current: BTreeSet<Uuid> = labels_of_file(cx.connection(), file.id)
            .await?
            .into_iter()
            .collect();
        if current.iter().copied().eq(wanted.iter().copied()) {
            return Err(DriveFault::Refused(codes::NOTHING_TO_CHANGE));
        }
        let by = cx.principal().id().as_uuid();
        let now = cx.now().as_datetime();
        replace_file_labels(cx.connection(), file.id, &wanted, by, now).await?;
        crate::file::file_changed::<H>(cx, &file, FileCause::LabelsChanged { detached: None })?;
        Ok(())
    })
}
