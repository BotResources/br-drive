//! The host's own objects refresh when a drive's files change. A host names
//! the noun of the object a drive hangs off (`DriveHost::DriveOwner`), keyed
//! by the drive's id; every file impact the library stages then also
//! impacts that key, so the host's views bound to its own noun — an object
//! carrying file counts, say — recompute and republish. The library never
//! calls the host. A drive is created from its host object and takes its key
//! (`create_drive`): the drive's id is the key of the object the host
//! declares as its `DriveOwnerNoun::Object`.

use std::collections::HashMap;

use service_engine::error::EngineError;
use service_engine::impact::Dims;
use service_engine::name::NounName;
use service_engine::persistence::{Aggregate, Persistence};
use service_engine::pipeline::Ops;
use service_engine::wire::Noun;
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::file::ProcessingState;
use crate::host::DriveHost;

/// A host noun whose objects are keyed by a drive's id — the object a drive
/// hangs off. Named by `DriveHost::DriveOwner`, typed, so the compiler checks
/// both the noun and its key; the host writes
/// `impl DriveOwnerNoun for Its { type Object = ItsRow; }`.
pub trait DriveOwnerNoun: Noun<Key = Uuid> {
    /// The host object a drive hangs off: `create_drive` takes one and the
    /// drive takes its key, so a drive's id is its host object's id by
    /// construction. `NoDriveOwner` names `Unowned`, which has no value: such
    /// a host creates its drives with `create_unowned_drive`.
    type Object: DriveOwnerObject;

    /// Whether the library impacts this noun at all; only `NoDriveOwner` says no.
    const REFRESHED: bool = true;
}

mod sealed {
    pub trait Sealed {}
}

/// What a drive is created from: the id it takes. Sealed: every engine
/// aggregate keyed by a UUID is one — the drive takes its key — and nothing
/// else is, so the id is the key of the object the host declared.
pub trait DriveOwnerObject: sealed::Sealed {
    fn drive_id(&self) -> Uuid;
}

impl<A> sealed::Sealed for A
where
    A: Aggregate,
    A::Store: Persistence<Key = Uuid>,
{
}

impl sealed::Sealed for Unowned {}

impl<A> DriveOwnerObject for A
where
    A: Aggregate,
    A::Store: Persistence<Key = Uuid>,
{
    fn drive_id(&self) -> Uuid {
        self.key()
    }
}

/// The owner object of a host with no owner noun: it has no value, so such a
/// host never calls `create_drive` and names its drives' ids itself
/// (`create_unowned_drive`) — nothing in the library relies on them then.
#[derive(Debug, Clone, Copy)]
pub enum Unowned {}

impl DriveOwnerObject for Unowned {
    fn drive_id(&self) -> Uuid {
        match *self {}
    }
}

/// The owner noun of a host whose objects need no refresh from the drives.
pub struct NoDriveOwner;

impl Noun for NoDriveOwner {
    type Key = Uuid;
    const NAME: NounName = NounName::from_static("drive_no_owner");
}

impl DriveOwnerNoun for NoDriveOwner {
    type Object = Unowned;
    const REFRESHED: bool = false;
}

/// The host object a drive of `H` is created from.
pub type OwnerObject<H> = <<H as DriveHost>::DriveOwner as DriveOwnerNoun>::Object;

/// Whether the host asked for its objects to refresh with their drive's files.
pub(crate) fn refreshes<H: DriveHost>() -> bool {
    <H::DriveOwner as DriveOwnerNoun>::REFRESHED
}

/// Stages an impact on the host object of `drive`, when the host declared one.
/// No cause: the host's deltas keep speaking the host's own causes.
pub(crate) fn touch<H: DriveHost>(ops: &mut Ops<'_>, drive: Uuid) -> Result<(), EngineError> {
    if refreshes::<H>() {
        ops.impact::<H::DriveOwner>(&drive, Dims::ALL)?;
    }
    Ok(())
}

/// How many files a drive holds, and how many of them are READY.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FileCounts {
    pub files: i64,
    pub ready: i64,
}

/// The file counts of several drives in one statement — for a host view that
/// shows them on its own object. A drive with no file is absent from the map.
pub async fn file_counts(
    conn: &mut PgConnection,
    drives: &[Uuid],
) -> Result<HashMap<Uuid, FileCounts>, EngineError> {
    // One pass over the drives' files through `file_drive_idx`, each file's
    // processing by its primary key. A file never processed is READY once
    // its upload is confirmed.
    let rows = sqlx::query(
        "SELECT f.drive_id, count(*) AS files, \
                count(*) FILTER (WHERE COALESCE(s.state, \
                  CASE WHEN f.committed_at IS NULL THEN 'pending' ELSE 'ready' END) = $2) AS ready \
         FROM drive.file f LEFT JOIN drive.file_processing s ON s.file_id = f.id \
         WHERE f.drive_id = ANY($1) GROUP BY f.drive_id",
    )
    .bind(drives)
    .bind(ProcessingState::Ready.as_str())
    .fetch_all(conn)
    .await?;
    Ok(rows
        .iter()
        .map(|row| {
            (
                row.get("drive_id"),
                FileCounts {
                    files: row.get("files"),
                    ready: row.get("ready"),
                },
            )
        })
        .collect())
}

/// How many files of a drive are in each processing state — what
/// `processingState` reads on each of them. Since 0.5.1.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProcessingCounts {
    /// Uploads not confirmed yet.
    pub pending: i64,
    /// Files whose chain runs.
    pub processing: i64,
    /// Files stored, processed or never processed.
    pub ready: i64,
    /// Files whose last job failed or was cancelled.
    pub failed: i64,
}

impl ProcessingCounts {
    /// Every file of the drive, whatever its state.
    pub fn files(&self) -> i64 {
        self.pending + self.processing + self.ready + self.failed
    }
}

/// The processing counts of several drives in one statement, served by the
/// same index as [`file_counts`] — for a host view that shows how many files
/// are processing or failed without reading `drive.file_processing` itself.
/// A drive with no file is absent from the map. Since 0.5.1.
pub async fn processing_counts(
    conn: &mut PgConnection,
    drives: &[Uuid],
) -> Result<HashMap<Uuid, ProcessingCounts>, EngineError> {
    let rows = sqlx::query(
        "SELECT drive_id, \
                count(*) FILTER (WHERE state = $2) AS pending, \
                count(*) FILTER (WHERE state = $3) AS processing, \
                count(*) FILTER (WHERE state = $4) AS ready, \
                count(*) FILTER (WHERE state = $5) AS failed \
         FROM (SELECT f.drive_id, COALESCE(s.state, \
                 CASE WHEN f.committed_at IS NULL THEN $2 ELSE $4 END) AS state \
               FROM drive.file f LEFT JOIN drive.file_processing s ON s.file_id = f.id \
               WHERE f.drive_id = ANY($1)) files \
         GROUP BY drive_id",
    )
    .bind(drives)
    .bind(ProcessingState::Pending.as_str())
    .bind(ProcessingState::Processing.as_str())
    .bind(ProcessingState::Ready.as_str())
    .bind(ProcessingState::Failed.as_str())
    .fetch_all(conn)
    .await?;
    Ok(rows
        .iter()
        .map(|row| {
            (
                row.get("drive_id"),
                ProcessingCounts {
                    pending: row.get("pending"),
                    processing: row.get("processing"),
                    ready: row.get("ready"),
                    failed: row.get("failed"),
                },
            )
        })
        .collect())
}
