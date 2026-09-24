use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::persistence::{Aggregate, Persistence, PersistenceStyle};
use service_engine::pipeline::{Bulk, Ops};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::fault::{DriveFault, codes};
use crate::file::{FileCause, FileRow, store};
use crate::folders::{delete_rows, impact_rows};
use crate::host::DriveHost;
use crate::owner::{DriveOwnerObject, NoDriveOwner, OwnerObject};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DriveRow {
    pub id: Uuid,
    pub created_by: Uuid,
    pub created_at: DateTime<Utc>,
}

pub struct DriveStore;

impl Persistence for DriveStore {
    type Aggregate = DriveRow;
    type Key = Uuid;
    type Event = ();

    const STYLE: PersistenceStyle = PersistenceStyle::Crud;

    fn load<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<Option<DriveRow>, EngineError>> {
        Box::pin(async move {
            let row =
                sqlx::query("SELECT id, created_by, created_at FROM drive.drive WHERE id = $1")
                    .bind(key)
                    .fetch_optional(conn)
                    .await?;
            Ok(row.map(|row| DriveRow {
                id: row.get("id"),
                created_by: row.get("created_by"),
                created_at: row.get("created_at"),
            }))
        })
    }

    fn lock<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Self::row_lock(conn, "drive.drive", key)
    }

    fn read_many<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [Uuid],
    ) -> BoxFuture<'a, Result<Vec<(Uuid, DriveRow)>, EngineError>> {
        Box::pin(async move {
            let rows = sqlx::query(
                "SELECT id, created_by, created_at FROM drive.drive WHERE id = ANY($1)",
            )
            .bind(keys)
            .fetch_all(conn)
            .await?;
            Ok(rows
                .iter()
                .map(|row| {
                    let drive = DriveRow {
                        id: row.get("id"),
                        created_by: row.get("created_by"),
                        created_at: row.get("created_at"),
                    };
                    (drive.id, drive)
                })
                .collect())
        })
    }

    fn save<'a>(
        _conn: &'a mut PgConnection,
        _drive: &'a DriveRow,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async { Ok(()) })
    }

    fn create<'a>(
        conn: &'a mut PgConnection,
        drive: &'a DriveRow,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query("INSERT INTO drive.drive (id, created_by, created_at) VALUES ($1, $2, $3)")
                .bind(drive.id)
                .bind(drive.created_by)
                .bind(drive.created_at)
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
            sqlx::query("DELETE FROM drive.drive WHERE id = $1")
                .bind(key)
                .execute(conn)
                .await?;
            Ok(())
        })
    }
}

impl Aggregate for DriveRow {
    type Store = DriveStore;

    fn key(&self) -> Uuid {
        self.id
    }
}

/// Creates the drive of a host object, in the host's transaction: the drive
/// takes the object's key, so a host view keyed by that object refreshes when
/// the drive's files change (`DriveHost::DriveOwner`). A host whose owner is
/// `NoDriveOwner` has no such object and calls `create_unowned_drive`.
pub async fn create_drive<H: DriveHost>(
    ops: &mut Ops<'_>,
    owner: &OwnerObject<H>,
    created_by: Uuid,
) -> Result<(), DriveFault> {
    insert_drive(ops, owner.drive_id(), created_by).await
}

/// Creates a drive of a host that declared no owner noun
/// (`type DriveOwner = NoDriveOwner`), under an id the host chooses: nothing
/// is refreshed from it, so nothing relies on it matching another object.
pub async fn create_unowned_drive<H>(
    ops: &mut Ops<'_>,
    id: Uuid,
    created_by: Uuid,
) -> Result<(), DriveFault>
where
    H: DriveHost<DriveOwner = NoDriveOwner>,
{
    insert_drive(ops, id, created_by).await
}

async fn insert_drive(ops: &mut Ops<'_>, id: Uuid, created_by: Uuid) -> Result<(), DriveFault> {
    let drive = DriveRow {
        id,
        created_by,
        created_at: ops.now().as_datetime(),
    };
    ops.create(&drive).await?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DriveDeleted {
    pub files: usize,
}

pub async fn delete_drive<H: DriveHost>(
    cx: &mut Bulk<'_, H>,
    id: Uuid,
) -> Result<DriveDeleted, DriveFault> {
    let drive = cx
        .load::<DriveRow>(&id)
        .await?
        .ok_or(DriveFault::Refused(codes::DRIVE_NOT_FOUND))?;
    let ids = store::ids_in_drive(cx.connection(), id).await?;
    let files = cx.load_many::<FileRow<H>>(&ids).await?;
    delete_rows(cx, &files).await?;
    cx.delete(&drive).await?;
    let ids: Vec<Uuid> = files.iter().map(|file| file.id).collect();
    impact_rows(cx, id, &ids, FileCause::DriveDeleted)?;
    Ok(DriveDeleted { files: files.len() })
}
