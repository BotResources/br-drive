use futures_util::future::BoxFuture;
use serde::Deserialize;
use service_engine::impact::Deps;
use service_engine::pipeline::{Mutation, MutationInput};
use service_engine::principal::PrincipalId;
use uuid::Uuid;

use super::aggregate::{Workspace, WorkspaceCause, WorkspaceRow};
use crate::kernel::error::WORKSPACE_NOT_FOUND;
use crate::kernel::{AppFault, AppPrincipal};

pub const OWNERSHIP_DEP: u8 = 0;

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
        };
        cx.create(&workspace).await?;
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
    cx: &'m mut Mutation<'m, AppPrincipal>,
    input: DeleteWorkspace,
) -> BoxFuture<'m, Result<(), AppFault>> {
    Box::pin(async move {
        let workspace = cx
            .load::<WorkspaceRow>(&input.id)
            .await?
            .ok_or(AppFault::Refused(WORKSPACE_NOT_FOUND))?;
        workspace.delete_gate(cx.principal()).require()?;
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
