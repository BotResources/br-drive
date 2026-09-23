use std::collections::HashMap;

use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::persistence::{Aggregate, Persistence, PersistenceStyle};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::LabelRow;

const COLUMNS: &str = "id, name, color, description, created_by, created_at, updated_at";

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

pub struct LabelStore;

impl Persistence for LabelStore {
    type Aggregate = LabelRow;
    type Key = Uuid;
    type Event = ();

    const STYLE: PersistenceStyle = PersistenceStyle::Crud;

    fn load<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<Option<LabelRow>, EngineError>> {
        Box::pin(async move {
            let row = sqlx::query(&format!("SELECT {COLUMNS} FROM drive.label WHERE id = $1"))
                .bind(key)
                .fetch_optional(conn)
                .await?;
            Ok(row.as_ref().map(row_to_label))
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
    ) -> BoxFuture<'a, Result<Vec<(Uuid, LabelRow)>, EngineError>> {
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
                    let label = row_to_label(row);
                    (label.id, label)
                })
                .collect())
        })
    }

    fn save<'a>(
        conn: &'a mut PgConnection,
        label: &'a LabelRow,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(
                "UPDATE drive.label SET name = $2, color = $3, description = $4, updated_at = $5 \
                 WHERE id = $1",
            )
            .bind(label.id)
            .bind(&label.name)
            .bind(&label.color)
            .bind(&label.description)
            .bind(label.updated_at)
            .execute(conn)
            .await?;
            Ok(())
        })
    }

    fn create<'a>(
        conn: &'a mut PgConnection,
        label: &'a LabelRow,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(&format!(
                "INSERT INTO drive.label ({COLUMNS}) VALUES ($1, $2, $3, $4, $5, $6, $7)"
            ))
            .bind(label.id)
            .bind(&label.name)
            .bind(&label.color)
            .bind(&label.description)
            .bind(label.created_by)
            .bind(label.created_at)
            .bind(label.updated_at)
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
            sqlx::query("DELETE FROM drive.label WHERE id = $1")
                .bind(key)
                .execute(conn)
                .await?;
            Ok(())
        })
    }
}

impl Aggregate for LabelRow {
    type Store = LabelStore;

    fn key(&self) -> Uuid {
        self.id
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
