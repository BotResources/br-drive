use futures_util::future::BoxFuture;
use serde::Deserialize;
use service_engine::impact::Deps;
use service_engine::pipeline::{Bulk, Mutation, MutationInput};
use service_engine::principal::PrincipalId;
use uuid::Uuid;

use super::aggregate::{Workspace, WorkspaceCause, WorkspaceRow};
use crate::kernel::error::WORKSPACE_NOT_FOUND;
use crate::kernel::{AppFault, AppPrincipal, OWNERSHIP_DEP};

fn ownership_dep() -> Deps {
    Deps::bit(OWNERSHIP_DEP).expect("a declared dependency fits the bit set")
}

#[derive(Debug, Deserialize)]
pub struct CreateWorkspace {
    pub id: Uuid,
    pub name: String,
}

impl MutationInput for CreateWorkspace {
    type Output = ();
    type Error = AppFault;
    const NAME: &'static str = "create_workspace";
}

pub fn create_workspace<'m>(
    cx: &'m mut Mutation<'m, AppPrincipal>,
    input: CreateWorkspace,
) -> BoxFuture<'m, Result<(), AppFault>> {
    Box::pin(async move {
        let owner = cx.principal().user();
        let workspace = WorkspaceRow {
            id: input.id,
            owner_id: owner,
            name: input.name,
            created_at: cx.now().as_datetime(),
            file_count: 0,
            ready_file_count: 0,
        };
        cx.create(&workspace).await?;
        #[cfg(feature = "drive")]
        br_drive::create_drive(cx, workspace.id, owner).await?;
        cx.impact_caused::<Workspace, _>(&workspace.id, WorkspaceCause::Created)?;
        cx.impact_principal_facts(PrincipalId::from(owner), ownership_dep());
        Ok(())
    })
}

#[derive(Debug, Deserialize)]
pub struct DeleteWorkspace {
    pub id: Uuid,
}

impl MutationInput for DeleteWorkspace {
    type Output = ();
    type Error = AppFault;
    const NAME: &'static str = "delete_workspace";
}

pub fn delete_workspace<'m>(
    cx: &'m mut Bulk<'m, AppPrincipal>,
    input: DeleteWorkspace,
) -> BoxFuture<'m, Result<(), AppFault>> {
    Box::pin(async move {
        let workspace = cx
            .load::<WorkspaceRow>(&input.id)
            .await?
            .ok_or(AppFault::Refused(WORKSPACE_NOT_FOUND))?;
        workspace.delete_gate(cx.principal()).require()?;
        #[cfg(feature = "drive")]
        br_drive::delete_drive::<AppPrincipal>(cx, workspace.id).await?;
        cx.delete(&workspace).await?;
        cx.impact_caused::<Workspace, _>(&workspace.id, WorkspaceCause::Deleted)?;
        cx.impact_principal_facts(PrincipalId::from(workspace.owner_id), ownership_dep());
        Ok(())
    })
}

#[derive(Debug, Deserialize)]
pub struct TransferWorkspace {
    pub id: Uuid,
    pub to: Uuid,
}

impl MutationInput for TransferWorkspace {
    type Output = ();
    type Error = AppFault;
    const NAME: &'static str = "transfer_workspace";
}

pub fn transfer_workspace<'m>(
    cx: &'m mut Mutation<'m, AppPrincipal>,
    input: TransferWorkspace,
) -> BoxFuture<'m, Result<(), AppFault>> {
    Box::pin(async move {
        let mut workspace = cx
            .load::<WorkspaceRow>(&input.id)
            .await?
            .ok_or(AppFault::Refused(WORKSPACE_NOT_FOUND))?;
        let from = workspace.owner_id;
        let cause = workspace.transfer(cx.principal(), input.to)?;
        cx.save(&workspace).await?;
        cx.impact_caused::<Workspace, _>(&workspace.id, cause)?;
        cx.impact_principal_facts(PrincipalId::from(from), ownership_dep());
        cx.impact_principal_facts(PrincipalId::from(input.to), ownership_dep());
        Ok(())
    })
}

#[cfg(feature = "drive")]
#[derive(Debug, Deserialize)]
pub struct ProtectFile {
    pub file_id: Uuid,
    pub protected: bool,
}

#[cfg(feature = "drive")]
impl MutationInput for ProtectFile {
    type Output = ();
    type Error = AppFault;
    const NAME: &'static str = "protect_file";
}

#[cfg(feature = "drive")]
pub fn protect_file<'m>(
    cx: &'m mut Mutation<'m, AppPrincipal>,
    input: ProtectFile,
) -> BoxFuture<'m, Result<(), AppFault>> {
    Box::pin(async move {
        let drive = br_drive::drive_of(cx, input.file_id)
            .await?
            .ok_or(AppFault::Refused(br_drive::codes::FILE_NOT_FOUND))?;
        let workspace = cx
            .load::<WorkspaceRow>(&drive)
            .await?
            .ok_or(AppFault::Refused(WORKSPACE_NOT_FOUND))?;
        workspace.transfer_gate(cx.principal()).require()?;
        br_drive::set_protected::<AppPrincipal>(cx, input.file_id, input.protected).await?;
        Ok(())
    })
}

#[cfg(feature = "drive")]
#[derive(Debug, Deserialize)]
pub struct AnnotateFile {
    pub file_id: Uuid,
    pub metadata: serde_json::Value,
}

#[cfg(feature = "drive")]
impl MutationInput for AnnotateFile {
    type Output = ();
    type Error = AppFault;
    const NAME: &'static str = "annotate_file";
}

#[cfg(feature = "drive")]
pub fn annotate_file<'m>(
    cx: &'m mut Mutation<'m, AppPrincipal>,
    input: AnnotateFile,
) -> BoxFuture<'m, Result<(), AppFault>> {
    Box::pin(async move {
        // The library asks the host's gate (`DriveRequest::SetMetadata`): the
        // owner-only rule of the drive applies with no check of its own here.
        let principal = cx.principal().clone();
        br_drive::set_metadata::<AppPrincipal>(cx, &principal, input.file_id, input.metadata)
            .await?;
        Ok(())
    })
}
