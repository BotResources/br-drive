//! The host's own objects refresh when a drive's files change. A host names
//! the noun of the object a drive hangs off (`DriveHost::DRIVE_OWNER_NOUN`),
//! keyed by the drive's id; every file impact the library stages then also
//! impacts that key, so the host's views bound to its own noun — an object
//! carrying file counts, say — recompute and republish. The library never
//! calls the host.

use std::collections::HashMap;
use std::marker::PhantomData;

use serde::Serialize;
use service_engine::error::EngineError;
use service_engine::name::NounName;
use service_engine::pipeline::Ops;
use service_engine::wire::Noun;
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::host::DriveHost;

/// The host noun named by `DriveHost::DRIVE_OWNER_NOUN`, keyed by drive id.
pub struct DriveOwner<H>(PhantomData<fn() -> H>);

impl<H: DriveHost> Noun for DriveOwner<H> {
    type Key = Uuid;
    const NAME: NounName = NounName::from_static(match H::DRIVE_OWNER_NOUN {
        Some(noun) => noun,
        // Never staged: `touch` stages nothing when the host names no noun.
        None => "drive_owner_unbound",
    });
}

/// Stages an impact on the host object of `drive`, when the host declared one.
pub(crate) fn touch<H: DriveHost, C: Serialize>(
    ops: &mut Ops<'_>,
    drive: Uuid,
    cause: C,
) -> Result<(), EngineError> {
    if H::DRIVE_OWNER_NOUN.is_some() {
        ops.impact_caused::<DriveOwner<H>, _>(&drive, cause)?;
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
                count(*) FILTER (WHERE processing_state = 'ready') AS ready \
         FROM drive.file WHERE drive_id = ANY($1) GROUP BY drive_id",
    )
    .bind(drives)
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
