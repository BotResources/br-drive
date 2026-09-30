use std::collections::HashMap;

use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::persistence::{Aggregate, Persistence, PersistenceStyle};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use std::marker::PhantomData;

use super::{LabelEvent, LabelRow};
use crate::facts::{self, FactMeta, Pending, SoftEda, Stamped};
use crate::host::DriveHost;

/// The columns of a label.
const COLUMNS: &str = "id, name, color, description, created_by, created_at, version, updated_at";

fn row_to_label(row: &sqlx::postgres::PgRow) -> LabelRow {
    LabelRow {
        id: row.get("id"),
        name: row.get("name"),
        color: row.get("color"),
        description: row.get("description"),
        created_by: row.get("created_by"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

/// A label as its store keeps it: its row, its version, its pending events.
pub struct LabelRecord<H> {
    pub row: LabelRow,
    pub(crate) version: i64,
    pending: Pending<LabelEvent>,
    host: PhantomData<fn() -> H>,
}

impl<H> Clone for LabelRecord<H> {
    fn clone(&self) -> Self {
        Self {
            row: self.row.clone(),
            version: self.version,
            pending: self.pending.clone(),
            host: PhantomData,
        }
    }
}

impl<H> LabelRecord<H> {
    /// A new label, created now by `meta`'s hand.
    pub(crate) fn create(
        id: Uuid,
        name: String,
        color: String,
        description: String,
        meta: &FactMeta,
    ) -> Self {
        let mut label = Self {
            row: LabelRow {
                id,
                name: name.clone(),
                color: color.clone(),
                description: description.clone(),
                created_by: meta.actor_id,
                created_at: meta.occurred_at,
                updated_at: meta.occurred_at,
            },
            version: 0,
            pending: Pending::default(),
            host: PhantomData,
        };
        label.pending.push(
            &mut label.version,
            LabelEvent::Created {
                name,
                color,
                description,
            },
            meta,
        );
        label
    }

    /// The label now reads as given.
    pub(crate) fn update(
        &mut self,
        name: String,
        color: String,
        description: String,
        meta: &FactMeta,
    ) {
        self.row.name = name.clone();
        self.row.color = color.clone();
        self.row.description = description.clone();
        self.row.updated_at = meta.occurred_at;
        self.pending.push(
            &mut self.version,
            LabelEvent::Updated {
                name,
                color,
                description,
            },
            meta,
        );
    }

    fn base_version(&self) -> i64 {
        self.version - self.pending.len()
    }
}

fn record<H>(row: &sqlx::postgres::PgRow) -> LabelRecord<H> {
    LabelRecord {
        row: row_to_label(row),
        version: row.get("version"),
        pending: Pending::default(),
        host: PhantomData,
    }
}

pub struct LabelStore<H>(PhantomData<fn() -> H>);

impl<H: DriveHost> Persistence for LabelStore<H> {
    type Aggregate = LabelRecord<H>;
    type Key = Uuid;
    type Event = Stamped<LabelEvent>;

    const STYLE: PersistenceStyle = PersistenceStyle::SoftEda;

    fn load<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<Option<LabelRecord<H>>, EngineError>> {
        Box::pin(async move {
            let row = sqlx::query(&format!("SELECT {COLUMNS} FROM drive.label WHERE id = $1"))
                .bind(key)
                .fetch_optional(conn)
                .await?;
            Ok(row.as_ref().map(record))
        })
    }

    fn lock<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Self::row_lock(conn, "drive.label", key)
    }

    fn read_many<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [Uuid],
    ) -> BoxFuture<'a, Result<Vec<(Uuid, LabelRecord<H>)>, EngineError>> {
        Box::pin(async move {
            let rows = sqlx::query(&format!(
                "SELECT {COLUMNS} FROM drive.label WHERE id = ANY($1)"
            ))
            .bind(keys)
            .fetch_all(conn)
            .await?;
            Ok(rows
                .iter()
                .map(|row| {
                    let label = record(row);
                    (label.row.id, label)
                })
                .collect())
        })
    }

    fn save<'a>(
        conn: &'a mut PgConnection,
        label: &'a LabelRecord<H>,
        events: &'a [Stamped<LabelEvent>],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            upsert(conn, label).await?;
            hand_label_facts::<H>(conn, label, events).await
        })
    }

    fn create<'a>(
        conn: &'a mut PgConnection,
        label: &'a LabelRecord<H>,
        events: &'a [Stamped<LabelEvent>],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            upsert(conn, label).await?;
            hand_label_facts::<H>(conn, label, events).await
        })
    }

    fn delete<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query("DELETE FROM drive.label WHERE id = $1")
                .bind(key)
                .execute(conn)
                .await?;
            Ok(())
        })
    }
}

async fn upsert<H>(conn: &mut PgConnection, label: &LabelRecord<H>) -> Result<(), EngineError> {
    let row = &label.row;
    sqlx::query(&format!(
        "INSERT INTO drive.label ({COLUMNS}) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
         ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, color = EXCLUDED.color, \
           description = EXCLUDED.description, version = EXCLUDED.version, \
           updated_at = EXCLUDED.updated_at"
    ))
    .bind(row.id)
    .bind(&row.name)
    .bind(&row.color)
    .bind(&row.description)
    .bind(row.created_by)
    .bind(row.created_at)
    .bind(label.version)
    .bind(row.updated_at)
    .execute(conn)
    .await?;
    Ok(())
}

async fn hand_label_facts<H: DriveHost>(
    conn: &mut PgConnection,
    label: &LabelRecord<H>,
    events: &[Stamped<LabelEvent>],
) -> Result<(), EngineError> {
    let facts = facts::facts_of(&facts::uuid_key(label.row.id), label.base_version(), events)?;
    facts::hand::<H>(conn, &facts).await
}

/// Hands the last fact of `label`, about to be deleted by the caller.
pub(crate) async fn hand_deleted<H: DriveHost>(
    conn: &mut PgConnection,
    meta: &FactMeta,
    label: &LabelRecord<H>,
) -> Result<(), EngineError> {
    let fact = facts::fact_at(
        facts::uuid_key(label.row.id),
        label.version + 1,
        &LabelEvent::Deleted,
        meta,
    )?;
    facts::hand::<H>(conn, &[fact]).await
}

impl<H: DriveHost> Aggregate for LabelRecord<H> {
    type Store = LabelStore<H>;

    fn key(&self) -> Uuid {
        self.row.id
    }

    fn pending_events(&self) -> &[Stamped<LabelEvent>] {
        self.pending.as_slice()
    }
}

impl<H: DriveHost> SoftEda for LabelRecord<H> {
    fn clear_pending(&mut self) {
        self.pending.clear();
    }
}

pub async fn all_ids(conn: &mut PgConnection) -> Result<Vec<Uuid>, EngineError> {
    let rows = sqlx::query("SELECT id FROM drive.label")
        .fetch_all(conn)
        .await?;
    Ok(rows.iter().map(|row| row.get::<Uuid, _>("id")).collect())
}

/// Serializes the name checks of two concurrent saves, so a collision is
/// answered `LABEL_NAME_TAKEN` and never the unique index's error.
pub async fn serialize_labels(conn: &mut PgConnection) -> Result<(), EngineError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('drive.label'))")
        .execute(conn)
        .await?;
    Ok(())
}

pub async fn name_taken(
    conn: &mut PgConnection,
    name: &str,
    except: Option<Uuid>,
) -> Result<bool, EngineError> {
    let taken: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM drive.label WHERE lower(name) = lower($1) AND id <> $2)",
    )
    .bind(name)
    .bind(except.unwrap_or(Uuid::nil()))
    .fetch_one(conn)
    .await?;
    Ok(taken)
}

pub async fn existing_ids(conn: &mut PgConnection, ids: &[Uuid]) -> Result<Vec<Uuid>, EngineError> {
    let rows = sqlx::query("SELECT id FROM drive.label WHERE id = ANY($1)")
        .bind(ids)
        .fetch_all(conn)
        .await?;
    Ok(rows.iter().map(|row| row.get::<Uuid, _>("id")).collect())
}

/// The label ids of every file asked for, set-based.
pub async fn label_ids_of_files(
    conn: &mut PgConnection,
    files: &[Uuid],
) -> Result<HashMap<Uuid, Vec<Uuid>>, EngineError> {
    let rows = sqlx::query(
        "SELECT fl.file_id, fl.label_id FROM drive.file_label fl \
         JOIN drive.label l ON l.id = fl.label_id \
         WHERE fl.file_id = ANY($1) ORDER BY lower(l.name)",
    )
    .bind(files)
    .fetch_all(conn)
    .await?;
    let mut by_file: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
    for row in &rows {
        by_file
            .entry(row.get("file_id"))
            .or_default()
            .push(row.get("label_id"));
    }
    Ok(by_file)
}

pub async fn labels_of_file(conn: &mut PgConnection, file: Uuid) -> Result<Vec<Uuid>, EngineError> {
    let rows = sqlx::query("SELECT label_id FROM drive.file_label WHERE file_id = $1")
        .bind(file)
        .fetch_all(conn)
        .await?;
    Ok(rows
        .iter()
        .map(|row| row.get::<Uuid, _>("label_id"))
        .collect())
}

pub async fn replace_file_labels(
    conn: &mut PgConnection,
    file: Uuid,
    labels: &[Uuid],
    by: Uuid,
    at: DateTime<Utc>,
) -> Result<(), EngineError> {
    sqlx::query("DELETE FROM drive.file_label WHERE file_id = $1 AND NOT (label_id = ANY($2))")
        .bind(file)
        .bind(labels)
        .execute(&mut *conn)
        .await?;
    sqlx::query(
        "INSERT INTO drive.file_label (file_id, label_id, created_by, created_at) \
         SELECT $1, label_id, $3, $4 FROM unnest($2::uuid[]) AS wanted(label_id) \
         ON CONFLICT (file_id, label_id) DO NOTHING",
    )
    .bind(file)
    .bind(labels)
    .bind(by)
    .bind(at)
    .execute(conn)
    .await?;
    Ok(())
}

/// The files a label is on, before it is deleted, so they can be impacted.
pub async fn files_with_label(
    conn: &mut PgConnection,
    label: Uuid,
) -> Result<Vec<Uuid>, EngineError> {
    let rows = sqlx::query("SELECT file_id FROM drive.file_label WHERE label_id = $1")
        .bind(label)
        .fetch_all(conn)
        .await?;
    Ok(rows
        .iter()
        .map(|row| row.get::<Uuid, _>("file_id"))
        .collect())
}

/// Locks the files a label is on, in id order.
pub async fn lock_files_with_label(
    conn: &mut PgConnection,
    label: Uuid,
) -> Result<(), EngineError> {
    sqlx::query(
        "SELECT id FROM drive.file WHERE id IN \
           (SELECT file_id FROM drive.file_label WHERE label_id = $1) \
         ORDER BY id FOR UPDATE",
    )
    .bind(label)
    .execute(conn)
    .await?;
    Ok(())
}
