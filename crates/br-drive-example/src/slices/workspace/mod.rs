mod aggregate;
pub mod graphql;
mod mutations;
mod principal;
mod store;
mod view;

use service_engine::Engine;
use service_engine::error::EngineError;

use crate::kernel::AppPrincipal;

pub use aggregate::{Workspace, WorkspaceCause, WorkspaceRow};
pub use view::{WorkspaceView, WorkspacesView};

pub fn register(engine: &mut Engine<AppPrincipal>) -> Result<(), EngineError> {
    engine.register_principal_fact(principal::load_owned_workspaces)?;
    engine.register_view(view::WorkspacesView)?;
    engine.register_mutation::<mutations::CreateWorkspace, _>(mutations::create_workspace)?;
    engine.register_mutation::<mutations::DeleteWorkspace, _>(mutations::delete_workspace)?;
    engine.register_mutation::<mutations::TransferWorkspace, _>(mutations::transfer_workspace)?;
    #[cfg(feature = "drive")]
    engine.register_mutation::<mutations::ProtectFile, _>(mutations::protect_file)?;
    engine.register_schema_slice(
        service_engine::graphql::SliceFragment::derive::<
            graphql::WorkspaceQuery,
            graphql::WorkspaceMutation,
            graphql::WorkspaceSubscription,
        >("workspace"),
    )?;
    Ok(())
}
