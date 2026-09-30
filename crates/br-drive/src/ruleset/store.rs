use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::persistence::{Aggregate, Persistence, PersistenceStyle};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use std::marker::PhantomData;

use super::{RulesetEvent, RulesetRow, RulesetStep, Trigger};
use crate::facts::{self, FactMeta, Pending, SoftEda, Stamped};
use crate::fault::{DriveFault, codes};
use crate::host::DriveHost;
use crate::media::MediaType;

/// The columns of a rule.
const COLUMNS: &str = "id, name, trigger, media_types, steps, is_default, created_by, created_at, \
     version, updated_at";

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

/// A rule as its store keeps it: its row, its version, its pending events.
pub struct RulesetRecord<H> {
    pub row: RulesetRow,
    pub(crate) version: i64,
    pending: Pending<RulesetEvent>,
    host: PhantomData<fn() -> H>,
}

impl<H> Clone for RulesetRecord<H> {
    fn clone(&self) -> Self {
        Self {
            row: self.row.clone(),
            version: self.version,
            pending: self.pending.clone(),
            host: PhantomData,
        }
    }
}

impl<H> RulesetRecord<H> {
    /// A new rule, created now.
    pub(crate) fn create(row: RulesetRow, meta: &FactMeta) -> Self {
        let event = RulesetEvent::Created {
            name: row.name.clone(),
            trigger: row.trigger,
            media_types: row.media_types.clone(),
            steps: row.steps.clone(),
            is_default: row.is_default,
        };
        let mut ruleset = Self {
            row,
            version: 0,
            pending: Pending::default(),
            host: PhantomData,
        };
        ruleset.pending.push(&mut ruleset.version, event, meta);
        ruleset
    }

    /// The rule now reads as given.
    pub(crate) fn save(
        &mut self,
        name: String,
        media_types: Vec<String>,
        steps: Vec<RulesetStep>,
        is_default: bool,
        meta: &FactMeta,
    ) {
        self.row.name = name.clone();
        self.row.media_types = media_types.clone();
        self.row.steps = steps.clone();
        self.row.is_default = is_default;
        self.row.updated_at = meta.occurred_at;
        self.pending.push(
            &mut self.version,
            RulesetEvent::Saved {
                name,
                media_types,
                steps,
                is_default,
            },
            meta,
        );
    }

    fn base_version(&self) -> i64 {
        self.version - self.pending.len()
    }
}

fn record<H>(row: &sqlx::postgres::PgRow) -> Result<RulesetRecord<H>, EngineError> {
    Ok(RulesetRecord {
        row: row_to_ruleset(row)?,
        version: row.get("version"),
        pending: Pending::default(),
        host: PhantomData,
    })
}

/// One rule, by id.
async fn load_ruleset(
    conn: &mut PgConnection,
    id: Uuid,
) -> Result<Option<RulesetRow>, EngineError> {
    let row = sqlx::query(&format!(
        "SELECT {COLUMNS} FROM drive.ruleset WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(conn)
    .await?;
    row.as_ref().map(row_to_ruleset).transpose()
}

pub struct RulesetStore<H>(PhantomData<fn() -> H>);

impl<H: DriveHost> Persistence for RulesetStore<H> {
    type Aggregate = RulesetRecord<H>;
    type Key = Uuid;
    type Event = Stamped<RulesetEvent>;

    const STYLE: PersistenceStyle = PersistenceStyle::SoftEda;

    fn load<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<Option<RulesetRecord<H>>, EngineError>> {
        Box::pin(async move {
            let row = sqlx::query(&format!(
                "SELECT {COLUMNS} FROM drive.ruleset WHERE id = $1"
            ))
            .bind(key)
            .fetch_optional(conn)
            .await?;
            row.as_ref().map(record).transpose()
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
    ) -> BoxFuture<'a, Result<Vec<(Uuid, RulesetRecord<H>)>, EngineError>> {
        Box::pin(async move {
            let rows = sqlx::query(&format!(
                "SELECT {COLUMNS} FROM drive.ruleset WHERE id = ANY($1)"
            ))
            .bind(keys)
            .fetch_all(conn)
            .await?;
            rows.iter()
                .map(|row| record(row).map(|ruleset| (ruleset.row.id, ruleset)))
                .collect()
        })
    }

    fn save<'a>(
        conn: &'a mut PgConnection,
        ruleset: &'a RulesetRecord<H>,
        events: &'a [Stamped<RulesetEvent>],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            upsert(conn, ruleset).await?;
            hand_ruleset_facts::<H>(conn, ruleset, events).await
        })
    }

    fn create<'a>(
        conn: &'a mut PgConnection,
        ruleset: &'a RulesetRecord<H>,
        events: &'a [Stamped<RulesetEvent>],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            upsert(conn, ruleset).await?;
            hand_ruleset_facts::<H>(conn, ruleset, events).await
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

async fn upsert<H>(conn: &mut PgConnection, ruleset: &RulesetRecord<H>) -> Result<(), EngineError> {
    let row = &ruleset.row;
    sqlx::query(&format!(
        "INSERT INTO drive.ruleset ({COLUMNS}) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
         ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, trigger = EXCLUDED.trigger, \
           media_types = EXCLUDED.media_types, steps = EXCLUDED.steps, \
           is_default = EXCLUDED.is_default, version = EXCLUDED.version, \
           updated_at = EXCLUDED.updated_at"
    ))
    .bind(row.id)
    .bind(&row.name)
    .bind(row.trigger.as_str())
    .bind(&row.media_types)
    .bind(
        serde_json::to_value(&row.steps).map_err(|source| EngineError::Encode {
            what: "ruleset steps",
            source,
        })?,
    )
    .bind(row.is_default)
    .bind(row.created_by)
    .bind(row.created_at)
    .bind(ruleset.version)
    .bind(row.updated_at)
    .execute(conn)
    .await?;
    Ok(())
}

async fn hand_ruleset_facts<H: DriveHost>(
    conn: &mut PgConnection,
    ruleset: &RulesetRecord<H>,
    events: &[Stamped<RulesetEvent>],
) -> Result<(), EngineError> {
    let facts = facts::facts_of(
        &facts::uuid_key(ruleset.row.id),
        ruleset.base_version(),
        events,
    )?;
    facts::hand::<H>(conn, &facts).await
}

/// Hands the last fact of `ruleset`, about to be deleted by the caller.
pub(crate) async fn hand_deleted<H: DriveHost>(
    conn: &mut PgConnection,
    meta: &FactMeta,
    ruleset: &RulesetRecord<H>,
) -> Result<(), EngineError> {
    let fact = facts::fact_at(
        facts::uuid_key(ruleset.row.id),
        ruleset.version + 1,
        &RulesetEvent::Deleted,
        meta,
    )?;
    facts::hand::<H>(conn, &[fact]).await
}

impl<H: DriveHost> Aggregate for RulesetRecord<H> {
    type Store = RulesetStore<H>;

    fn key(&self) -> Uuid {
        self.row.id
    }

    fn pending_events(&self) -> &[Stamped<RulesetEvent>] {
        self.pending.as_slice()
    }
}

impl<H: DriveHost> SoftEda for RulesetRecord<H> {
    fn clear_pending(&mut self) {
        self.pending.clear();
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

/// Serializes the name and default-overlap checks of two concurrent saves, so
/// a collision is answered with its code and never a constraint error. Not a
/// row lock: what a save claims (a name, a default's patterns) has no
/// aggregate to load, and the engine offers no pipeline lock for it. It is
/// always taken before any aggregate of the gesture.
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
        let ruleset = load_ruleset(conn, id)
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
