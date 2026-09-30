use futures_util::future::BoxFuture;
use serde::Deserialize;
use service_engine::impact::Deps;
use service_engine::pipeline::{Bulk, Mutation, MutationInput};
#[cfg(feature = "drive")]
use service_engine::pipeline::Reaction;
use service_engine::principal::PrincipalId;
use uuid::Uuid;

use super::aggregate::{Workspace, WorkspaceCause, WorkspaceRow};
use crate::kernel::error::WORKSPACE_NOT_FOUND;
#[cfg(feature = "drive")]
use crate::kernel::ReactionFault;
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
            pending_file_count: 0,
            processing_file_count: 0,
            failed_file_count: 0,
        };
        cx.create(&workspace).await?;
        #[cfg(feature = "drive")]
        br_drive::create_drive::<AppPrincipal>(cx, &workspace, owner).await?;
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

/// Closes a workspace's drive: its running processings end, cancelled, and
/// its pending uploads are abandoned (`br_drive::freeze_drive`), in this
/// gesture's transaction. The example host stores no closed state of its own:
/// the gesture shows the library's side only.
#[cfg(feature = "drive")]
#[derive(Debug, Deserialize)]
pub struct FreezeWorkspace {
    pub id: Uuid,
}

#[cfg(feature = "drive")]
impl MutationInput for FreezeWorkspace {
    type Output = ();
    type Error = AppFault;
    const NAME: &'static str = "freeze_workspace";
}

#[cfg(feature = "drive")]
pub fn freeze_workspace<'m>(
    cx: &'m mut Mutation<'m, AppPrincipal>,
    input: FreezeWorkspace,
) -> BoxFuture<'m, Result<(), AppFault>> {
    Box::pin(async move {
        let workspace = cx
            .load::<WorkspaceRow>(&input.id)
            .await?
            .ok_or(AppFault::Refused(WORKSPACE_NOT_FOUND))?;
        workspace.freeze_gate(cx.principal()).require()?;
        let meta = br_drive::FactMeta::of(cx.principal(), cx.now().as_datetime());
        br_drive::freeze_drive::<AppPrincipal>(cx, &meta, workspace.id).await?;
        Ok(())
    })
}

/// The verb of the host command that retracts a workspace — what a roster
/// sends when the person a personal workspace belongs to leaves.
#[cfg(feature = "drive")]
pub const RETRACT_VERB: &str = "retract";
#[cfg(feature = "drive")]
pub const RETRACT_DURABLE: &str = "workspace-retract";

/// `integration.cmd.workspace.workspace.retract.v1 { workspace_id }`: deletes
/// the workspace and its drive from a **reaction**
/// (`br_drive::delete_drive_in_reaction`). An unknown workspace is a no-op,
/// so a redelivered command changes nothing.
#[cfg(feature = "drive")]
#[derive(Debug, serde::Serialize, Deserialize)]
pub struct RetractWorkspace {
    pub workspace_id: Uuid,
}

#[cfg(feature = "drive")]
impl service_engine::inbound::ReactionMessage for RetractWorkspace {
    fn coordinates() -> service_engine::inbound::ReactionCoordinates {
        use br_core_integration::{Aggregate, Bc, CommandCoords, Verb};
        service_engine::inbound::ReactionCoordinates::Command(CommandCoords {
            receiver: Bc::new(crate::SERVICE).expect("the host service name is a valid bc"),
            aggregate: Aggregate::new("workspace").expect("a static aggregate segment"),
            verb: Verb::new(RETRACT_VERB).expect("a static verb segment"),
            version: 1,
        })
    }

    fn decode(payload: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(payload)
    }
}

#[cfg(feature = "drive")]
pub fn retract_workspace<'r>(
    cx: &'r mut Reaction<'r>,
    message: RetractWorkspace,
) -> BoxFuture<'r, Result<(), ReactionFault>> {
    Box::pin(async move {
        let Some(workspace) = cx.load::<WorkspaceRow>(&message.workspace_id).await? else {
            return Ok(());
        };
        br_drive::delete_drive_in_reaction::<AppPrincipal>(cx, workspace.id).await?;
        cx.delete(&workspace).await?;
        cx.impact_caused::<Workspace, _>(&workspace.id, WorkspaceCause::Deleted)?;
        cx.impact_principal_facts(PrincipalId::from(workspace.owner_id), ownership_dep());
        Ok(())
    })
}
