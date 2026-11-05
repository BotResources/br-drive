//! A chain's first step waits for the engine to promote the file's source:
//! the engine serves no download URL for a verified blob before its reaper
//! verified the landed object (`pending` -> `uploaded`), so a job created
//! before that would meet a runner context with no source to read. The
//! source's post-upload policy commands [`SourcePromoted`] in the promotion's
//! transaction, and its reaction asks Jobs for the awaited step (since 0.8.2).

use std::marker::PhantomData;

use br_core_integration::{Aggregate as BcAggregate, Bc, CommandCoords, Verb};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::inbound::{ReactionCoordinates, ReactionMessage};
use service_engine::pipeline::{OutboundCommand, Reaction};
use service_engine::schema::TABLE_BLOB;
use sqlx::PgConnection;
use uuid::Uuid;

use super::chain::release_awaited_step;
use super::state::FileProcessing;
use crate::facts::FactMeta;
use crate::fault::DriveReactionFault;
use crate::file::FileRow;
use crate::host::DriveHost;

pub const SOURCE_PROMOTED_AGGREGATE: &str = "drive_source";
pub const SOURCE_PROMOTED_VERB: &str = "promoted";
pub const SOURCE_PROMOTED_DURABLE: &str = "source-promoted";

/// Whether the source blob `reference` still awaits the engine's promotion
/// (`pending`: the reaper has not verified it yet), read on the gesture's own
/// connection. Any other state starts a chain at once, as before 0.8.2: a
/// promoted source is readable, and a source with no row (data older than
/// the engine's blob store) or a failed one would never be promoted.
pub(crate) async fn source_pending(
    conn: &mut PgConnection,
    reference: Uuid,
) -> Result<bool, EngineError> {
    let state: Option<String> =
        sqlx::query_scalar(&format!("SELECT state FROM {TABLE_BLOB} WHERE id = $1"))
            .bind(reference)
            .fetch_optional(conn)
            .await?;
    Ok(state.as_deref() == Some("pending"))
}

/// The engine promoted the source of `file_id`: a chain step awaiting it
/// can be asked of Jobs.
#[derive(Serialize, Deserialize)]
pub struct SourcePromoted<H> {
    pub file_id: Uuid,
    #[serde(skip)]
    host: PhantomData<fn() -> H>,
}

impl<H> SourcePromoted<H> {
    pub fn new(file_id: Uuid) -> Self {
        Self {
            file_id,
            host: PhantomData,
        }
    }
}

fn promoted_coords<H: DriveHost>() -> CommandCoords {
    CommandCoords {
        receiver: Bc::new(H::SERVICE).expect("the host service name is a valid bc"),
        aggregate: BcAggregate::new(SOURCE_PROMOTED_AGGREGATE).expect("a static aggregate segment"),
        verb: Verb::new(SOURCE_PROMOTED_VERB).expect("a static verb segment"),
        version: 1,
    }
}

impl<H: DriveHost> ReactionMessage for SourcePromoted<H> {
    fn coordinates() -> ReactionCoordinates {
        ReactionCoordinates::Command(promoted_coords::<H>())
    }

    fn decode(payload: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(payload)
    }
}

impl<H: DriveHost> OutboundCommand for SourcePromoted<H> {
    fn coords(&self) -> CommandCoords {
        promoted_coords::<H>()
    }

    fn command_id(&self) -> Uuid {
        Uuid::now_v7()
    }
}

/// Asks Jobs for the file's first step when it awaits the source — the file
/// loaded first, so locked: a commit or a `ProcessFile` still deciding on the
/// same file is waited for, and then seen awaiting. Anything else (no chain,
/// a step already created, a chain cancelled meanwhile, a deleted file, a
/// redelivery) is acknowledged and changes nothing.
pub fn source_promoted_reaction<'r, H: DriveHost>(
    cx: &'r mut Reaction<'r>,
    message: SourcePromoted<H>,
) -> BoxFuture<'r, Result<(), DriveReactionFault>> {
    Box::pin(async move {
        let Some(mut file) = cx.load::<FileRow<H>>(&message.file_id).await? else {
            return Ok(());
        };
        let Some(mut processing) = cx.load::<FileProcessing<H>>(&file.id).await? else {
            return Ok(());
        };
        if !processing.awaits_source() {
            return Ok(());
        }
        let meta = FactMeta::of_reaction(cx);
        release_awaited_step(cx, &meta, &mut file, &mut processing).await?;
        Ok(())
    })
}
