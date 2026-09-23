use std::sync::Arc;
use std::time::Duration;

use contract_jobs::catalog::{RUNNER_TYPE_PREFIX, RunnerType, RunnerTypeLifecycle};
use contract_jobs::runner::WIRE_VERSION;
use service_engine::error::EngineError;
use service_engine::nats::{KvEvent, KvPrefix, Nats, Watched};
use sqlx::{PgConnection, PgPool};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

const RETRY_AFTER: Duration = Duration::from_secs(1);

pub fn lifecycle_str(lifecycle: RunnerTypeLifecycle) -> &'static str {
    match lifecycle {
        RunnerTypeLifecycle::Active => "active",
        RunnerTypeLifecycle::Deprecated => "deprecated",
    }
}

/// Whether a catalogue watch has ever completed its boot scan on this host's
/// database: until then no runner type is known and a chain cannot start.
pub async fn scanned(conn: &mut PgConnection) -> Result<bool, EngineError> {
    let scanned: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM drive.catalogue_scan)")
        .fetch_one(conn)
        .await?;
    Ok(scanned)
}

pub async fn is_active(conn: &mut PgConnection, runner_type: &str) -> Result<bool, EngineError> {
    let lifecycle: Option<String> =
        sqlx::query_scalar("SELECT lifecycle FROM drive.known_runner_type WHERE runner_type = $1")
            .bind(runner_type)
            .fetch_optional(conn)
            .await?;
    Ok(lifecycle.as_deref() == Some("active"))
}

pub async fn inactive_among(
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

async fn mark_scanned(conn: &mut PgConnection) -> Result<(), EngineError> {
    sqlx::query(
        "INSERT INTO drive.catalogue_scan (singleton, scanned_at) VALUES (true, now()) \
         ON CONFLICT (singleton) DO UPDATE SET scanned_at = EXCLUDED.scanned_at",
    )
    .execute(conn)
    .await?;
    Ok(())
}

async fn upsert(conn: &mut PgConnection, entry: &RunnerType) -> Result<(), EngineError> {
    sqlx::query(
        "INSERT INTO drive.known_runner_type (runner_type, lifecycle, version, seen_at) \
         VALUES ($1, $2, $3, now()) \
         ON CONFLICT (runner_type) DO UPDATE SET lifecycle = EXCLUDED.lifecycle, \
           version = EXCLUDED.version, seen_at = EXCLUDED.seen_at",
    )
    .bind(&entry.runner_type)
    .bind(lifecycle_str(entry.lifecycle))
    .bind(i32::from(entry.version))
    .execute(conn)
    .await?;
    Ok(())
}

async fn forget(conn: &mut PgConnection, runner_type: &str) -> Result<(), EngineError> {
    sqlx::query("DELETE FROM drive.known_runner_type WHERE runner_type = $1")
        .bind(runner_type)
        .execute(conn)
        .await?;
    Ok(())
}

async fn retain(conn: &mut PgConnection, runner_types: &[String]) -> Result<(), EngineError> {
    sqlx::query("DELETE FROM drive.known_runner_type WHERE NOT (runner_type = ANY($1))")
        .bind(runner_types)
        .execute(conn)
        .await?;
    Ok(())
}

/// Decodes one catalogue entry; anything the mirror cannot trust — an entry that
/// does not decode, one that names another type than its key, one of a wire
/// version this library does not speak — is reported and treated as unknown.
fn parse(key: &str, value: &serde_json::Value) -> Option<RunnerType> {
    let name = key.strip_prefix(RUNNER_TYPE_PREFIX)?;
    match serde_json::from_value::<RunnerType>(value.clone()) {
        Ok(entry) if entry.version != WIRE_VERSION => {
            tracing::error!(
                key,
                found = entry.version,
                supported = WIRE_VERSION,
                "a runner-type catalogue entry speaks a wire version this library does not; \
                 the type is treated as unknown"
            );
            None
        }
        Ok(entry) if entry.runner_type == name => Some(entry),
        Ok(entry) => {
            tracing::warn!(
                key,
                declared = %entry.runner_type,
                "a runner-type catalogue entry names another type than its key; ignored"
            );
            None
        }
        Err(error) => {
            tracing::warn!(key, %error, "a runner-type catalogue entry does not decode; ignored");
            None
        }
    }
}

async fn apply_put(
    conn: &mut PgConnection,
    key: &str,
    value: &serde_json::Value,
) -> Result<Option<String>, EngineError> {
    match parse(key, value) {
        Some(entry) => {
            upsert(conn, &entry).await?;
            Ok(Some(entry.runner_type))
        }
        None => {
            if let Some(name) = key.strip_prefix(RUNNER_TYPE_PREFIX) {
                forget(conn, name).await?;
            }
            Ok(None)
        }
    }
}

async fn sync_once(nats: &Nats, pool: &PgPool, stop: &Notify) -> Result<(), EngineError> {
    let prefix =
        KvPrefix::new(RUNNER_TYPE_PREFIX).map_err(|e| EngineError::Config(e.to_string()))?;
    let bucket = nats.published_language::<serde_json::Value>().await?;
    let (entries, revision) = bucket.entries_with_revision(&prefix).await?;
    let mut conn = pool.acquire().await.map_err(EngineError::from)?;
    let mut present = Vec::new();
    for (key, value) in &entries {
        if let Some(name) = apply_put(&mut conn, key.as_str(), value).await? {
            present.push(name);
        }
    }
    retain(&mut conn, &present).await?;
    mark_scanned(&mut conn).await?;
    drop(conn);
    let mut watch = bucket.watch_all_from(revision + 1).await?;
    loop {
        let next = tokio::select! {
            _ = stop.notified() => return Ok(()),
            next = watch.next_under::<serde_json::Value>(&prefix) => next,
        };
        let Some(event) = next else {
            return Err(EngineError::Config(
                "the runner-type catalogue watch ended".into(),
            ));
        };
        let Watched::Event(event) = event? else {
            continue;
        };
        let mut conn = pool.acquire().await.map_err(EngineError::from)?;
        match event {
            KvEvent::Put { key, value, .. } => {
                apply_put(&mut conn, key.as_str(), &value).await?;
            }
            KvEvent::Delete { key, .. } => {
                if let Some(name) = key.as_str().strip_prefix(RUNNER_TYPE_PREFIX) {
                    forget(&mut conn, name).await?;
                }
            }
        }
    }
}

pub struct CatalogueWatch {
    stop: Arc<Notify>,
    task: JoinHandle<()>,
}

impl CatalogueWatch {
    pub async fn stop(self) {
        self.stop.notify_waiters();
        self.task.abort();
        let _ = self.task.await;
    }
}

pub fn watch_runner_types(nats: Nats, pool: PgPool) -> CatalogueWatch {
    let stop = Arc::new(Notify::new());
    let stopper = stop.clone();
    let task = tokio::spawn(async move {
        let stop = stopper;
        loop {
            let outcome = tokio::select! {
                _ = stop.notified() => return,
                outcome = sync_once(&nats, &pool, &stop) => outcome,
            };
            match outcome {
                Ok(()) => return,
                Err(error) => {
                    tracing::warn!(%error, "the runner-type catalogue watch faulted; retrying");
                    tokio::select! {
                        _ = stop.notified() => return,
                        _ = tokio::time::sleep(RETRY_AFTER) => {}
                    }
                }
            }
        }
    });
    CatalogueWatch { stop, task }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_entry_of_another_wire_version_is_unknown_not_active() {
        let key = "jobs.runner_type.render";
        let current = serde_json::json!({ "runner_type": "render", "lifecycle": "ACTIVE", "version": WIRE_VERSION });
        assert!(parse(key, &current).is_some());
        let newer = serde_json::json!({ "runner_type": "render", "lifecycle": "ACTIVE", "version": WIRE_VERSION + 1 });
        assert!(parse(key, &newer).is_none());
        let misnamed = serde_json::json!({ "runner_type": "other", "lifecycle": "ACTIVE" });
        assert!(parse(key, &misnamed).is_none());
        assert!(parse(key, &serde_json::json!("garbled")).is_none());
        assert!(parse("jobs.something_else", &current).is_none());
    }
}
