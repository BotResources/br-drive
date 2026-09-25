//! A local copy of Jobs' runner-type catalogue, kept for information only.
//!
//! `drive.known_runner_type` is filled by the optional watch
//! (`watch_runner_types`) and read twice: to warn at a rule save about the
//! steps whose runner type is not known `ACTIVE` (`RulesetSaved.unknownRunnerTypes`),
//! and to list the known types on a host's rule-editing screen
//! (`known_runner_types`). Nothing else reads it: a step's job is created
//! whatever the copy says, and Jobs alone judges the runner type.

mod watch;

use chrono::{DateTime, Utc};
use service_engine::error::EngineError;
use sqlx::{PgConnection, PgPool};

use crate::fault::DriveFault;
use crate::file::UnknownDbValue;
use crate::host::{DriveHost, DriveRequest};

pub use watch::{CatalogueWatch, watch_runner_types};

/// A runner type's lifecycle as Jobs publishes it. A retired type is not
/// published at all, so it is absent from the copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, async_graphql::Enum)]
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
#[derive(Debug, Clone, PartialEq, Eq, async_graphql::SimpleObject)]
pub struct DriveRunnerType {
    pub runner_type: String,
    pub lifecycle: DriveRunnerTypeLifecycle,
    /// The wire version the catalogue entry declared.
    pub version: i32,
    /// When the watch last wrote the entry (database clock).
    pub seen_at: DateTime<Utc>,
}

/// The known runner types in name order, for a principal the host's
/// `ReadRulesets` gate allows — the people who read the rules they would
/// edit; empty for anyone else, as `<p>Rulesets` is. Empty too on a host that
/// never started the watch.
pub async fn known_runner_types<H: DriveHost>(
    pool: &PgPool,
    principal: &H,
) -> Result<Vec<DriveRunnerType>, DriveFault> {
    if !principal
        .drive_gate(&DriveRequest::ReadRulesets)
        .is_allowed()
    {
        return Ok(Vec::new());
    }
    let rows: Vec<(String, String, i32, DateTime<Utc>)> = sqlx::query_as(
        "SELECT runner_type, lifecycle, version, seen_at FROM drive.known_runner_type \
         ORDER BY runner_type",
    )
    .fetch_all(pool)
    .await
    .map_err(EngineError::from)?;
    let mut known = Vec::with_capacity(rows.len());
    for (runner_type, lifecycle, version, seen_at) in rows {
        let lifecycle = DriveRunnerTypeLifecycle::from_db_str(&lifecycle)
            .map_err(|e| EngineError::Config(e.to_string()))?;
        known.push(DriveRunnerType {
            runner_type,
            lifecycle,
            version,
            seen_at,
        });
    }
    Ok(known)
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
         WHERE runner_type = ANY($1) AND lifecycle = 'active'",
    )
    .bind(runner_types)
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
