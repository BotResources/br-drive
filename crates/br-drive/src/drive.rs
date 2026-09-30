use chrono::{DateTime, Utc};
use contract_jobs::command::CancelJob;
use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::persistence::{Aggregate, Persistence, PersistenceStyle};
use service_engine::pipeline::{Bulk, Ops, Reaction};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::facts::{self, FactMeta};
use crate::fault::{DriveFault, codes};
use crate::file::{File, FileCause, FileEvent, FileRow, store};
use crate::folders::{delete_rows, delete_rows_as, impact_rows};
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
    let (drive, files) = drive_and_files::<H>(cx, id).await?;
    delete_rows(cx, &files, FileEvent::DriveDeleted).await?;
    cx.delete(&drive).await?;
    let ids: Vec<Uuid> = files.iter().map(|file| file.id).collect();
    impact_rows(cx, id, &ids, FileCause::DriveDeleted)?;
    Ok(DriveDeleted { files: files.len() })
}

/// `delete_drive` from a **reaction** of the host (a roster retraction that
/// removes a person's collection, say), in the reaction's transaction: the
/// same effects — every file deleted with its pages and images, every object
/// released, the running job of each file cancelled (`job.cancel`), each
/// file's `DriveDeleted` fact handed to the host under the reaction's
/// message (`FactMeta::of_reaction`) — and `DRIVE_NOT_FOUND` for an unknown
/// drive. The engine offers no bulk pipeline to a reaction, and a reaction
/// cannot stage a projector reset: live sessions are told file by file up to
/// `BULK_RESET_THRESHOLD`, and past it catch up on their next repopulation —
/// the one a host's visibility change stages (`impact_principal_facts`), or
/// their next reset. Since 0.5.1.
pub async fn delete_drive_in_reaction<H: DriveHost>(
    cx: &mut Reaction<'_>,
    id: Uuid,
) -> Result<DriveDeleted, DriveFault> {
    let meta = FactMeta::of_reaction(cx);
    let (drive, files) = drive_and_files::<H>(cx, id).await?;
    delete_rows_as::<H>(cx, &meta, &files, FileEvent::DriveDeleted).await?;
    cx.delete(&drive).await?;
    crate::owner::touch::<H>(cx, id)?;
    if files.len() <= H::BULK_RESET_THRESHOLD {
        for file in &files {
            cx.impact_caused::<File, _>(&file.id, &FileCause::DriveDeleted)?;
        }
    }
    Ok(DriveDeleted { files: files.len() })
}

/// The drive, locked, and every file of it, locked, in the engine's order.
async fn drive_and_files<H: DriveHost>(
    cx: &mut Ops<'_>,
    id: Uuid,
) -> Result<(DriveRow, Vec<FileRow<H>>), DriveFault> {
    let drive = cx
        .load::<DriveRow>(&id)
        .await?
        .ok_or(DriveFault::Refused(codes::DRIVE_NOT_FOUND))?;
    let ids = store::ids_in_drive(cx.connection(), id).await?;
    let files = cx.load_many::<FileRow<H>>(&ids).await?;
    Ok((drive, files))
}

/// What a drive freeze ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DriveFrozen {
    /// Running processings ended, cancelled.
    pub cancelled: usize,
    /// Pending uploads abandoned.
    pub abandoned: usize,
}

/// Freezes a drive, in the host's own gesture and transaction — a mutation,
/// a bulk mutation or a reaction, `meta` its hand (`FactMeta::of` for a
/// principal, `FactMeta::of_reaction` for a reaction): so a host can close a
/// collection with no work landing on it afterwards.
///
/// - Every running processing of the drive's files ends, as a cancel:
///   `job.cancel` is staged for the job and the job ends here, cancelled —
///   `JobCancelSent` then `JobCancelledOnFreeze` on the file's processing,
///   the file `FAILED` `cancelled` at once. The runner's next call about the
///   job meets `JOB_NOT_ACTIVE`, the next step of its chain is never
///   launched, and Jobs' own `cancelled` (or any later end) is recorded as
///   `JobFactIgnored` and changes nothing — even when Jobs drops the cancel.
/// - Every pending upload of the drive (`committed_at` unset) is abandoned
///   as its upload deadline would: `UploadAbandoned`, the row deleted, its
///   blob released; a later commit meets `FILE_NOT_FOUND`.
///
/// Every fact reaches the host through `record_facts`, in this transaction.
/// Stored, processed and failed files are untouched, and the library stores
/// no "frozen" flag: the host's gate keeps refusing new gestures on the
/// drive. A gesture that passed that gate before the freeze committed and
/// runs after it (a `ProcessFile`, a `RequestUpload` crossing the freeze) is
/// the host's to refuse: its facts reach `record_facts` in its own
/// transaction, where the host can check its own closed state under its own
/// lock and refuse (an `EngineError::PolicyRefused`). Freezing again ends
/// nothing more. `DRIVE_NOT_FOUND` for an unknown drive. One impact per file
/// it changes (a drive's in-flight files are few). Since 0.5.1.
pub async fn freeze_drive<H: DriveHost>(
    ops: &mut Ops<'_>,
    meta: &FactMeta,
    id: Uuid,
) -> Result<DriveFrozen, DriveFault> {
    ops.load::<DriveRow>(&id)
        .await?
        .ok_or(DriveFault::Refused(codes::DRIVE_NOT_FOUND))?;
    let ids = store::ids_in_flight(ops.connection(), id).await?;
    let mut files = ops.load_many::<FileRow<H>>(&ids).await?;
    // Each file's processing is locked after every file, in key order.
    files.sort_by_key(|file| file.id);
    let mut frozen = DriveFrozen::default();
    for mut file in files {
        if file.committed_at.is_none() {
            store::hand_gone::<H>(ops.connection(), meta, &file, FileEvent::UploadAbandoned)
                .await?;
            ops.delete(&file).await?;
            crate::file::file_changed::<H>(ops, &file, FileCause::UploadAbandoned)?;
            frozen.abandoned += 1;
            continue;
        }
        let Some(job_id) = file.active_job().map(|job| job.job_id) else {
            // It settled between the read and the lock.
            continue;
        };
        let mut processing = crate::processing::load_processing(ops, &file).await?;
        processing.cancel_on_freeze(meta);
        ops.command(crate::processing::JobCancel {
            payload: CancelJob { job_id },
        })?;
        facts::save(ops, &mut processing).await?;
        file.status = processing.status();
        file.updated_at = file.updated_at.max(processing.updated_at);
        crate::file::file_changed::<H>(
            ops,
            &file,
            FileCause::ProcessingFailed {
                reason: crate::processing::CANCELLED.to_string(),
            },
        )?;
        frozen.cancelled += 1;
    }
    Ok(frozen)
}
