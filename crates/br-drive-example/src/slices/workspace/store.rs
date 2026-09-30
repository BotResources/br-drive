use futures_util::future::BoxFuture;
use service_engine::{Cohort, KeyCeiling};
use service_engine::error::EngineError;
use service_engine::persistence::{Aggregate, CohortIndex, Persistence, PersistenceStyle};
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use super::aggregate::{OWNER_DIM, WorkspaceRow};

const COLUMNS: &str = "id, owner_id, name, created_at";

fn row_to_workspace(row: &sqlx::postgres::PgRow) -> WorkspaceRow {
    WorkspaceRow {
        id: row.get("id"),
        owner_id: row.get("owner_id"),
        name: row.get("name"),
        created_at: row.get("created_at"),
        file_count: 0,
        ready_file_count: 0,
        pending_file_count: 0,
        processing_file_count: 0,
        failed_file_count: 0,
    }
}

/// Fills the read-side file counts of the drive each workspace owns, in one
/// statement for the whole batch.
#[cfg(feature = "drive")]
async fn with_file_counts(
    conn: &mut PgConnection,
    workspaces: &mut [WorkspaceRow],
) -> Result<(), EngineError> {
    let ids: Vec<Uuid> = workspaces.iter().map(|workspace| workspace.id).collect();
    let counts = br_drive::file_counts(conn, &ids).await?;
    let states = br_drive::processing_counts(conn, &ids).await?;
    for workspace in workspaces {
        let counted = counts.get(&workspace.id).copied().unwrap_or_default();
        workspace.file_count = counted.files;
        workspace.ready_file_count = counted.ready;
        let states = states.get(&workspace.id).copied().unwrap_or_default();
        workspace.pending_file_count = states.pending;
        workspace.processing_file_count = states.processing;
        workspace.failed_file_count = states.failed;
    }
    Ok(())
}

#[cfg(not(feature = "drive"))]
async fn with_file_counts(
    _conn: &mut PgConnection,
    _workspaces: &mut [WorkspaceRow],
) -> Result<(), EngineError> {
    Ok(())
}

pub struct WorkspaceStore;

impl Persistence for WorkspaceStore {
    type Aggregate = WorkspaceRow;
    type Key = Uuid;
    type Event = ();

    const STYLE: PersistenceStyle = PersistenceStyle::Crud;

    fn lock<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Self::row_lock(conn, "workspace", key)
    }

    fn read_many<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [Uuid],
    ) -> BoxFuture<'a, Result<Vec<(Uuid, WorkspaceRow)>, EngineError>> {
        Box::pin(async move {
            let rows = sqlx::query(&format!(
                "SELECT {COLUMNS} FROM workspace WHERE id = ANY($1)"
            ))
            .bind(keys)
            .fetch_all(&mut *conn)
            .await?;
            let mut workspaces: Vec<WorkspaceRow> = rows.iter().map(row_to_workspace).collect();
            with_file_counts(conn, &mut workspaces).await?;
            Ok(workspaces
                .into_iter()
                .map(|workspace| (workspace.id, workspace))
                .collect())
        })
    }

    fn save<'a>(
        conn: &'a mut PgConnection,
        workspace: &'a WorkspaceRow,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(
                "UPDATE workspace SET owner_id = $2, name = $3 WHERE id = $1",
            )
            .bind(workspace.id)
            .bind(workspace.owner_id)
            .bind(&workspace.name)
            .execute(conn)
            .await?;
            Ok(())
        })
    }

    fn create<'a>(
        conn: &'a mut PgConnection,
        workspace: &'a WorkspaceRow,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(
                "INSERT INTO workspace (id, owner_id, name, created_at) VALUES ($1, $2, $3, $4)",
            )
            .bind(workspace.id)
            .bind(workspace.owner_id)
            .bind(&workspace.name)
            .bind(workspace.created_at)
            .execute(conn)
            .await?;
            Ok(())
        })
    }

    fn delete<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query("DELETE FROM workspace WHERE id = $1")
                .bind(key)
                .execute(conn)
                .await?;
            Ok(())
        })
    }
}

impl CohortIndex for WorkspaceStore {
    fn keys_in_cohorts<'a>(
        conn: &'a mut PgConnection,
        cohorts: &'a [Cohort],
        ceiling: KeyCeiling,
    ) -> BoxFuture<'a, Result<Vec<Uuid>, EngineError>> {
        Box::pin(async move {
            let owners = Cohort::uuids(cohorts, OWNER_DIM);
            let rows = sqlx::query("SELECT id FROM workspace WHERE owner_id = ANY($1) LIMIT $2")
                .bind(&owners)
                .bind(ceiling.limit())
                .fetch_all(conn)
                .await?;
            Ok(rows.iter().map(|row| row.get::<Uuid, _>("id")).collect())
        })
    }
}

impl Aggregate for WorkspaceRow {
    type Store = WorkspaceStore;

    fn key(&self) -> Uuid {
        self.id
    }
}

pub async fn workspaces_of(pg: &PgPool, owner: Uuid) -> Result<Vec<Uuid>, EngineError> {
    let rows = sqlx::query("SELECT id FROM workspace WHERE owner_id = $1")
        .bind(owner)
        .fetch_all(pg)
        .await?;
    Ok(rows.iter().map(|row| row.get::<Uuid, _>("id")).collect())
}
