//! A local copy of Jobs' runner-type catalogue, kept for information only.
//!
//! `drive.known_runner_type` is filled by the optional watch
//! (`watch_runner_types`) and read twice: to warn at a rule save about the
//! steps whose runner type is not known `ACTIVE` (`RulesetSaved.unknownRunnerTypes`),
//! and to list the known types on a host's rule-editing screen
//! (`known_runner_types`). Nothing else reads it: a step's job is created
//! whatever the copy says, and Jobs alone judges the runner type.

mod view;
mod watch;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use service_engine::Query;
use service_engine::error::EngineError;
use sqlx::PgConnection;

use crate::file::UnknownDbValue;
use crate::host::DriveHost;

pub use view::{DriveRunnerTypes, KnownRunnerType, RunnerTypeAccess, RunnerTypeStore};
pub use watch::{CatalogueWatch, watch_runner_types, watch_runner_types_of};

/// A runner type's lifecycle as Jobs publishes it. A retired type is not
/// published at all, so it is absent from the copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, async_graphql::Enum)]
pub enum DriveRunnerTypeLifecycle {
    Active,
    Deprecated,
}

impl DriveRunnerTypeLifecycle {
    pub(crate) fn as_db(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Deprecated => "deprecated",
        }
    }

    fn from_db_str(text: &str) -> Result<Self, UnknownDbValue> {
        match text {
            "active" => Ok(Self::Active),
            "deprecated" => Ok(Self::Deprecated),
            other => Err(UnknownDbValue("lifecycle", other.to_string())),
        }
    }
}

/// One runner type of the local catalogue copy, as the watch last saw it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct DriveRunnerType {
    pub runner_type: String,
    pub lifecycle: DriveRunnerTypeLifecycle,
    /// When the watch last wrote the entry (database clock).
    pub seen_at: DateTime<Utc>,
}

/// The whole local catalogue copy — every runner type the watch last saw,
/// not only the ones the rules name — in name order, read through the
/// [`DriveRunnerTypes`] view. Gated by the host's `ReadRulesets` (the gate of
/// `<p>Rulesets`): a principal it refuses gets an empty list. Empty too on a
/// host that never started the watch.
pub async fn known_runner_types<H: DriveHost>(
    query: &Query<'_, H>,
) -> async_graphql::Result<Vec<DriveRunnerType>> {
    query.fetch_view_window::<DriveRunnerTypes<H>>(&()).await
}

/// The runner types among `runner_types` the copy does not know as `ACTIVE`
/// (absent, or deprecated), sorted and deduplicated: a save's warning, never
/// a refusal.
pub(crate) async fn not_known_active(
    conn: &mut PgConnection,
    runner_types: &[String],
) -> Result<Vec<String>, EngineError> {
    let active: Vec<String> = sqlx::query_scalar(
        "SELECT runner_type FROM drive.known_runner_type \
         WHERE runner_type = ANY($1) AND lifecycle = $2",
    )
    .bind(runner_types)
    .bind(DriveRunnerTypeLifecycle::Active.as_db())
    .fetch_all(conn)
    .await?;
    let mut unknown: Vec<String> = runner_types
        .iter()
        .filter(|runner_type| !active.contains(runner_type))
        .cloned()
        .collect();
    unknown.sort();
    unknown.dedup();
    Ok(unknown)
}
