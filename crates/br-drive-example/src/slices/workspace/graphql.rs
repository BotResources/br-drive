use async_graphql::{Context, Object, Result, Subscription};
use futures_util::{Stream, StreamExt};
use service_engine::session::WindowSpec;
use service_engine::{MutationAck, Query};
use uuid::Uuid;

use super::mutations::{CreateWorkspace, DeleteWorkspace, TransferWorkspace};
use super::view::{WorkspaceView, WorkspacesView};
use crate::kernel::AppPrincipal;

service_engine::subscription_union! {
    view = WorkspaceViewUnion;
    delta = WorkspaceDelta { reset = WorkspaceReset, upsert = WorkspaceUpsert, remove = WorkspaceRemove };
    Workspace => service_engine::view::ViewProjector<WorkspacesView> => WorkspaceView,
}

#[derive(Default)]
pub struct WorkspaceQuery;

#[Object]
impl WorkspaceQuery {
    async fn workspace_workspace(
        &self,
        ctx: &Context<'_>,
        id: Uuid,
    ) -> Result<Option<WorkspaceView>> {
        Query::<AppPrincipal>::new(ctx)?
            .fetch_view::<WorkspacesView>(&id)
            .await
    }

    async fn workspace_workspaces(&self, ctx: &Context<'_>) -> Result<Vec<WorkspaceView>> {
        Query::<AppPrincipal>::new(ctx)?
            .fetch_view_window::<WorkspacesView>(&())
            .await
    }
}

#[derive(Default)]
pub struct WorkspaceMutation;

#[Object]
impl WorkspaceMutation {
    async fn workspace_create(
        &self,
        ctx: &Context<'_>,
        id: Uuid,
        name: String,
    ) -> Result<MutationAck> {
        service_engine::ack::<AppPrincipal, CreateWorkspace>(ctx, CreateWorkspace { id, name })
            .await
    }

    async fn workspace_delete(&self, ctx: &Context<'_>, id: Uuid) -> Result<MutationAck> {
        service_engine::ack_bulk::<AppPrincipal, DeleteWorkspace>(ctx, DeleteWorkspace { id })
            .await
    }

    async fn workspace_transfer(
        &self,
        ctx: &Context<'_>,
        id: Uuid,
        to: Uuid,
    ) -> Result<MutationAck> {
        service_engine::ack::<AppPrincipal, TransferWorkspace>(ctx, TransferWorkspace { id, to })
            .await
    }

    #[cfg(feature = "drive")]
    async fn workspace_protect_file(
        &self,
        ctx: &Context<'_>,
        file_id: Uuid,
        protected: bool,
    ) -> Result<MutationAck> {
        use super::mutations::ProtectFile;
        service_engine::ack::<AppPrincipal, ProtectFile>(ctx, ProtectFile { file_id, protected })
            .await
    }

    #[cfg(feature = "drive")]
    async fn workspace_annotate_file(
        &self,
        ctx: &Context<'_>,
        file_id: Uuid,
        metadata: service_engine::JsonScalar,
    ) -> Result<MutationAck> {
        use super::mutations::AnnotateFile;
        service_engine::ack::<AppPrincipal, AnnotateFile>(
            ctx,
            AnnotateFile {
                file_id,
                metadata: metadata.0,
            },
        )
        .await
    }
}

#[derive(Default)]
pub struct WorkspaceSubscription;

#[Subscription]
impl WorkspaceSubscription {
    async fn workspace_deltas(
        &self,
        ctx: &Context<'_>,
    ) -> Result<impl Stream<Item = Result<WorkspaceDelta>>> {
        let stream = service_engine::attach::<AppPrincipal>(
            ctx,
            vec![WindowSpec::view::<WorkspacesView>(&(), false)?],
        )
        .await?;
        Ok(stream.map(|delta| WorkspaceDelta::from_delta(&delta)))
    }
}
