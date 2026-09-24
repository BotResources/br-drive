use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::gate::{Affordances, Gated};
use service_engine::name::ProjectorName;
use service_engine::population::Population;
use service_engine::view::{Populate, Projector, cohort_window};
use uuid::Uuid;

use super::aggregate::{Workspace, WorkspaceRow};
use super::store::WorkspaceStore;
use crate::kernel::AppPrincipal;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct WorkspaceView {
    pub id: Uuid,
    pub name: String,
    pub owner_id: Uuid,
    pub created_at: DateTime<Utc>,
    pub file_count: i64,
    pub ready_file_count: i64,
    pub affordances: Affordances,
}

#[derive(Default)]
pub struct WorkspacesView;

impl WorkspacesView {
    pub const NAME: ProjectorName = ProjectorName::from_static("workspaces");
}

impl Projector for WorkspacesView {
    type Principal = AppPrincipal;
    type Noun = Workspace;
    type Store = WorkspaceStore;
    type Query = ();
    type Out = WorkspaceView;
    type Visibility = Workspace;

    const NAME: ProjectorName = Self::NAME;

    async fn populate(
        cx: &Populate<'_, AppPrincipal>,
        _query: &(),
    ) -> Result<Population<Uuid>, EngineError> {
        cohort_window::<Self>(cx).await
    }

    fn project(row: &WorkspaceRow, principal: &AppPrincipal) -> Result<WorkspaceView, EngineError> {
        Ok(WorkspaceView {
            id: row.id,
            name: row.name.clone(),
            owner_id: row.owner_id,
            created_at: row.created_at,
            file_count: row.file_count,
            ready_file_count: row.ready_file_count,
            affordances: row.affordances(principal),
        })
    }
}
