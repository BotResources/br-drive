use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::persistence::{Aggregate, Persistence, PersistenceStyle};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::{RulesetRow, Trigger};
use crate::fault::{DriveFault, codes};
use crate::media::MediaType;

const COLUMNS: &str =
    "id, name, trigger, media_types, steps, is_default, created_by, created_at, updated_at";

fn row_to_ruleset(row: &sqlx::postgres::PgRow) -> Result<RulesetRow, EngineError> {
    let trigger: String = row.get("trigger");
    let steps: serde_json::Value = row.get("steps");
    Ok(RulesetRow {
        id: row.get("id"),
        name: row.get("name"),
        trigger: Trigger::from_db_str(&trigger).map_err(|e| EngineError::Config(e.to_string()))?,
        media_types: row.get("media_types"),
        steps: serde_json::from_value(steps)
            .map_err(|e| EngineError::Config(format!("a ruleset's steps do not decode: {e}")))?,
        is_default: row.get("is_default"),
        created_by: row.get("created_by"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

pub struct RulesetStore;

impl Persistence for RulesetStore {
    type Aggregate = RulesetRow;
    type Key = Uuid;
    type Event = ();

    const STYLE: PersistenceStyle = PersistenceStyle::Crud;

    fn load<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<Option<RulesetRow>, EngineError>> {
        Box::pin(async move {
            let row = sqlx::query(&format!(
                "SELECT {COLUMNS} FROM drive.ruleset WHERE id = $1"
            ))
            .bind(key)
            .fetch_optional(conn)
            .await?;
            row.as_ref().map(row_to_ruleset).transpose()
        })
    }

    fn lock<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Self::row_lock(conn, "drive.ruleset", key)
    }

    fn read_many<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [Uuid],
    ) -> BoxFuture<'a, Result<Vec<(Uuid, RulesetRow)>, EngineError>> {
        Box::pin(async move {
            let rows = sqlx::query(&format!(
                "SELECT {COLUMNS} FROM drive.ruleset WHERE id = ANY($1)"
            ))
            .bind(keys)
            .fetch_all(conn)
            .await?;
            rows.iter()
                .map(|row| row_to_ruleset(row).map(|ruleset| (ruleset.id, ruleset)))
                .collect()
        })
    }

    fn save<'a>(
        conn: &'a mut PgConnection,
        ruleset: &'a RulesetRow,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(
                "UPDATE drive.ruleset SET name = $2, trigger = $3, media_types = $4, steps = $5, \
                   is_default = $6, updated_at = $7 WHERE id = $1",
            )
            .bind(ruleset.id)
            .bind(&ruleset.name)
            .bind(ruleset.trigger.as_str())
            .bind(&ruleset.media_types)
            .bind(
                serde_json::to_value(&ruleset.steps).map_err(|source| EngineError::Encode {
                    what: "ruleset steps",
                    source,
                })?,
            )
            .bind(ruleset.is_default)
            .bind(ruleset.updated_at)
            .execute(conn)
            .await?;
            Ok(())
        })
    }

    fn create<'a>(
        conn: &'a mut PgConnection,
        ruleset: &'a RulesetRow,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(&format!(
                "INSERT INTO drive.ruleset ({COLUMNS}) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
            ))
            .bind(ruleset.id)
            .bind(&ruleset.name)
            .bind(ruleset.trigger.as_str())
            .bind(&ruleset.media_types)
            .bind(
                serde_json::to_value(&ruleset.steps).map_err(|source| EngineError::Encode {
                    what: "ruleset steps",
                    source,
                })?,
            )
            .bind(ruleset.is_default)
            .bind(ruleset.created_by)
            .bind(ruleset.created_at)
            .bind(ruleset.updated_at)
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
            sqlx::query("DELETE FROM drive.ruleset WHERE id = $1")
                .bind(key)
                .execute(conn)
                .await?;
            Ok(())
        })
    }
}

impl Aggregate for RulesetRow {
    type Store = RulesetStore;

    fn key(&self) -> Uuid {
        self.id
    }
}

pub async fn all_ids(conn: &mut PgConnection) -> Result<Vec<Uuid>, EngineError> {
    let rows = sqlx::query("SELECT id FROM drive.ruleset ORDER BY name")
        .fetch_all(conn)
        .await?;
    Ok(rows.iter().map(|row| row.get::<Uuid, _>("id")).collect())
}

async fn defaults_of(
    conn: &mut PgConnection,
    trigger: Trigger,
) -> Result<Vec<RulesetRow>, EngineError> {
    let rows = sqlx::query(&format!(
        "SELECT {COLUMNS} FROM drive.ruleset WHERE is_default AND trigger = $1"
    ))
    .bind(trigger.as_str())
    .fetch_all(conn)
    .await?;
    rows.iter().map(row_to_ruleset).collect()
}

pub(super) async fn serialize_rulesets(conn: &mut PgConnection) -> Result<(), EngineError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('drive.ruleset'))")
        .execute(conn)
        .await?;
    Ok(())
}

/// The given rule, checked against the trigger and the media type, or the
/// default rule of that trigger with the best pattern — exact over `type/*`
/// over `*` — for the media type; `None` when no default matches.
pub async fn select_ruleset(
    conn: &mut PgConnection,
    trigger: Trigger,
    media_type: &MediaType,
    explicit: Option<Uuid>,
) -> Result<Option<RulesetRow>, DriveFault> {
    if let Some(id) = explicit {
        let ruleset = <RulesetStore as Persistence>::load(conn, &id)
            .await?
            .ok_or(DriveFault::Refused(codes::RULESET_NOT_FOUND))?;
        if ruleset.trigger != trigger || ruleset.best_match(media_type.as_str()).is_none() {
            return Err(DriveFault::Refused(codes::RULESET_MISMATCH));
        }
        return Ok(Some(ruleset));
    }
    let candidates = defaults_of(conn, trigger).await?;
    Ok(candidates
        .into_iter()
        .filter_map(|ruleset| {
            ruleset
                .best_match(media_type.as_str())
                .map(|rank| (rank, ruleset))
        })
        .min_by_key(|(rank, ruleset)| (*rank, ruleset.name.clone()))
        .map(|(_, ruleset)| ruleset))
}
