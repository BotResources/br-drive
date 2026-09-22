use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::persistence::{Aggregate, Persistence, PersistenceStyle};
use service_engine::pipeline::Ops;
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::fault::{DriveFault, codes};
use crate::file::{File, FileCause, FileRow, store};
use crate::host::DriveHost;

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

pub async fn create_drive(ops: &mut Ops<'_>, id: Uuid, created_by: Uuid) -> Result<(), DriveFault> {
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
    ops: &mut Ops<'_>,
    id: Uuid,
) -> Result<DriveDeleted, DriveFault> {
    let drive = ops
        .load::<DriveRow>(&id)
        .await?
        .ok_or(DriveFault::Refused(codes::DRIVE_NOT_FOUND))?;
    let ids = store::ids_in_drive(ops.connection(), id).await?;
    let files = ops.load_many::<FileRow<H>>(&ids).await?;
    for file in &files {
        ops.delete(file).await?;
        ops.impact_caused::<File, _>(&file.id, FileCause::DriveDeleted)?;
    }
    ops.delete(&drive).await?;
    Ok(DriveDeleted { files: files.len() })
}

pub async fn drive_of(ops: &mut Ops<'_>, file_id: Uuid) -> Result<Option<Uuid>, DriveFault> {
    Ok(store::drive_of(ops.connection(), file_id).await?)
}

pub async fn set_protected<H: DriveHost>(
    ops: &mut Ops<'_>,
    file_id: Uuid,
    protected: bool,
) -> Result<(), DriveFault> {
    let mut file = ops
        .load::<FileRow<H>>(&file_id)
        .await?
        .ok_or(DriveFault::Refused(codes::FILE_NOT_FOUND))?;
    if file.protected == protected {
        return Ok(());
    }
    file.protected = protected;
    file.updated_at = ops.now().as_datetime();
    ops.save(&file).await?;
    ops.impact_caused::<File, _>(&file.id, FileCause::ProtectionChanged { protected })?;
    Ok(())
}
