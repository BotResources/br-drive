use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use sqlx::PgPool;

use crate::kernel::{AppPrincipal, OwnedWorkspaces};

pub fn load_owned_workspaces<'a>(
    pg: &'a PgPool,
    principal: &'a mut AppPrincipal,
) -> BoxFuture<'a, Result<(), EngineError>> {
    Box::pin(async move {
        let owned = super::store::workspaces_of(pg, principal.user()).await?;
        principal.facts_mut().insert(OwnedWorkspaces(owned));
        Ok(())
    })
}
