//! The read of the local runner-type catalogue copy: a closed view over the
//! table the watch writes, so the query reads it through the engine's view
//! path (no raw pool in a resolver) and under its window capacity.

use std::collections::BTreeSet;
use std::marker::PhantomData;

use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::name::{NounName, ProjectorName};
use service_engine::persistence::{Persistence, PersistenceStyle};
use service_engine::population::Population;
use service_engine::view::{Populate, Projector};
use service_engine::visibility::Unrestricted;
use service_engine::wire::Noun;
use sqlx::{PgConnection, Row};

use super::{DriveRunnerType, DriveRunnerTypeLifecycle};
use crate::host::{DriveHost, DriveRequest};

/// A runner type of the local catalogue copy, keyed by its name.
pub struct KnownRunnerType;

impl Noun for KnownRunnerType {
    type Key = String;
    const NAME: NounName = NounName::from_static("drive_known_runner_type");
}

service_engine::open_access!(
    pub RunnerTypeAccess = "the runner-type catalogue copy belongs to the host service as a whole; it is gated on the host's ReadRulesets gate, never on a cohort"
);

/// The read-only store of `drive.known_runner_type`: the catalogue watch is
/// its only writer, so `save` and `create` refuse.
pub struct RunnerTypeStore;

fn watched_only() -> EngineError {
    EngineError::Service(
        "drive.known_runner_type is written by the runner-type catalogue watch, never by a \
         mutation"
            .into(),
    )
}

fn row_to_type(row: &sqlx::postgres::PgRow) -> Result<DriveRunnerType, EngineError> {
    let lifecycle: String = row.get("lifecycle");
    Ok(DriveRunnerType {
        runner_type: row.get("runner_type"),
        lifecycle: DriveRunnerTypeLifecycle::from_db_str(&lifecycle)
            .map_err(|e| EngineError::Config(e.to_string()))?,
        seen_at: row.get::<DateTime<Utc>, _>("seen_at"),
    })
}

impl Persistence for RunnerTypeStore {
    type Aggregate = DriveRunnerType;
    type Key = String;
    type Event = ();

    const STYLE: PersistenceStyle = PersistenceStyle::Crud;

    fn read_many<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [String],
    ) -> BoxFuture<'a, Result<Vec<(String, DriveRunnerType)>, EngineError>> {
        Box::pin(async move {
            let rows = sqlx::query(
                "SELECT runner_type, lifecycle, seen_at FROM drive.known_runner_type \
                 WHERE runner_type = ANY($1)",
            )
            .bind(keys)
            .fetch_all(conn)
            .await?;
            rows.iter()
                .map(|row| row_to_type(row).map(|known| (known.runner_type.clone(), known)))
                .collect()
        })
    }

    fn save<'a>(
        _conn: &'a mut PgConnection,
        _known: &'a DriveRunnerType,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async { Err(watched_only()) })
    }

    fn create<'a>(
        _conn: &'a mut PgConnection,
        _known: &'a DriveRunnerType,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async { Err(watched_only()) })
    }
}

/// The whole catalogue copy, in name order, for a principal the host's
/// `ReadRulesets` gate admits; empty for any other.
pub struct DriveRunnerTypes<H>(PhantomData<fn() -> H>);

impl<H> Default for DriveRunnerTypes<H> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<H> DriveRunnerTypes<H> {
    pub const NAME: ProjectorName = ProjectorName::from_static("drive_runner_types");
}

fn may_read<H: DriveHost>(principal: &H) -> bool {
    principal
        .drive_gate(&DriveRequest::ReadRulesets)
        .is_allowed()
}

impl<H: DriveHost> Projector for DriveRunnerTypes<H> {
    type Principal = H;
    type Noun = KnownRunnerType;
    type Store = RunnerTypeStore;
    type Query = ();
    type Out = DriveRunnerType;
    type Visibility = Unrestricted<DriveRunnerType, H, RunnerTypeAccess>;

    const NAME: ProjectorName = Self::NAME;

    async fn populate(
        cx: &Populate<'_, H>,
        _query: &(),
    ) -> Result<Population<String>, EngineError> {
        if !may_read(cx.principal()) {
            return Ok(Population::Keys(BTreeSet::new()));
        }
        let names: Vec<String> = sqlx::query_scalar(
            "SELECT runner_type FROM drive.known_runner_type ORDER BY runner_type LIMIT $1",
        )
        .bind(cx.limit_all())
        .fetch_all(cx.pool())
        .await?;
        Ok(Population::Ordered {
            keys: names,
            open_head: false,
        })
    }

    fn project(known: &DriveRunnerType, _principal: &H) -> Result<DriveRunnerType, EngineError> {
        Ok(known.clone())
    }

    fn visible(_known: &DriveRunnerType, principal: &H) -> bool {
        may_read(principal)
    }
}
