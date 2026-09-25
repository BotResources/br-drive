//! The optional watch filling `drive.known_runner_type` from Jobs' catalogue:
//! a scan of `PUBLISHED_LANGUAGE` under `jobs.runner_type.`, then a KV watch.
//! No gesture or reaction waits for it.

use std::sync::Arc;
use std::time::Duration;

use contract_jobs::catalog::{RUNNER_TYPE_PREFIX, RunnerType, RunnerTypeLifecycle};
use contract_jobs::runner::WIRE_VERSION;
use service_engine::error::EngineError;
use service_engine::nats::{KvEvent, KvPrefix, Nats, Watched};
use sqlx::{PgConnection, PgPool};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use super::DriveRunnerTypeLifecycle;

const RETRY_AFTER: Duration = Duration::from_secs(1);

impl From<RunnerTypeLifecycle> for DriveRunnerTypeLifecycle {
    fn from(lifecycle: RunnerTypeLifecycle) -> Self {
        match lifecycle {
            RunnerTypeLifecycle::Active => Self::Active,
            RunnerTypeLifecycle::Deprecated => Self::Deprecated,
        }
    }
}

async fn upsert(conn: &mut PgConnection, entry: &RunnerType) -> Result<(), EngineError> {
    sqlx::query(
        "INSERT INTO drive.known_runner_type (runner_type, lifecycle, version, seen_at) \
         VALUES ($1, $2, $3, now()) \
         ON CONFLICT (runner_type) DO UPDATE SET lifecycle = EXCLUDED.lifecycle, \
           version = EXCLUDED.version, seen_at = EXCLUDED.seen_at",
    )
    .bind(&entry.runner_type)
    .bind(DriveRunnerTypeLifecycle::from(entry.lifecycle).as_db())
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

/// Decodes one catalogue entry; anything the copy cannot trust — an entry that
/// does not decode, one that names another type than its key, one of a wire
/// version this library does not speak — is reported and left out.
fn parse(key: &str, value: &serde_json::Value) -> Option<RunnerType> {
    let name = key.strip_prefix(RUNNER_TYPE_PREFIX)?;
    match serde_json::from_value::<RunnerType>(value.clone()) {
        Ok(entry) if entry.version != WIRE_VERSION => {
            tracing::error!(
                key,
                found = entry.version,
                supported = WIRE_VERSION,
                "a runner-type catalogue entry speaks a wire version this library does not; \
                 it is left out of the copy"
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

/// The running watch; the host stops it at shutdown.
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

/// Starts the optional watch copying Jobs' runner-type catalogue into
/// `drive.known_runner_type` (restarted after a fault). The copy is
/// information only — a save's `unknownRunnerTypes` warning and
/// `<p>RunnerTypes` — never a condition on a launch.
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
    fn an_entry_of_another_wire_version_or_name_is_left_out() {
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
