//! The host's own objects refresh when a drive's files change. A host names
//! the noun of the object a drive hangs off (`DriveHost::DriveOwner`), keyed
//! by the drive's id; every file impact the library stages then also
//! impacts that key, so the host's views bound to its own noun — an object
//! carrying file counts, say — recompute and republish. The library never
//! calls the host. A drive is created from its host object and takes its key
//! (`create_drive`), so the two ids cannot differ.

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

/// What a drive is created from: the id it takes. Every engine aggregate keyed
/// by a UUID is one — the drive takes its key.
pub trait DriveOwnerObject {
    fn drive_id(&self) -> Uuid;
}

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
    let rows = sqlx::query(
        "SELECT drive_id, count(*) AS files, \
                count(*) FILTER (WHERE processing_state = $2) AS ready \
         FROM drive.file WHERE drive_id = ANY($1) GROUP BY drive_id",
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
