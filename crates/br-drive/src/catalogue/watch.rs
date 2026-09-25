//! The optional watch filling `drive.known_runner_type` from Jobs' catalogue:
//! a KV watch of `PUBLISHED_LANGUAGE` opened first, a scan under
//! `jobs.runner_type.`, then the watch drained. No gesture or reaction waits
//! for it.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use contract_jobs::catalog::{RUNNER_TYPE_PREFIX, RunnerType, RunnerTypeLifecycle};
use contract_jobs::runner::WIRE_VERSION;
use service_engine::error::EngineError;
use service_engine::nats::{KvEvent, KvPrefix, Nats, NatsError, Watched};
use service_engine::{Engine, Principal};
use sqlx::{PgConnection, PgPool};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use super::DriveRunnerTypeLifecycle;

/// The first pause after a fault; doubled on each fault in a row.
const FIRST_RETRY: Duration = Duration::from_secs(1);
/// The longest pause between two attempts.
const MAX_RETRY: Duration = Duration::from_secs(60);
/// `drive.known_runner_type.runner_type`'s limit (migration `9121000004`).
const MAX_RUNNER_TYPE_BYTES: usize = 128;

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

/// Decodes one catalogue entry; anything the copy cannot trust — a JSON value
/// that is not a runner-type entry, one that names another type than its key,
/// a name the copy cannot hold (empty, over 128 bytes), one of a wire version
/// this library does not speak — is reported and left out.
fn parse(key: &str, value: &serde_json::Value) -> Option<RunnerType> {
    let name = key.strip_prefix(RUNNER_TYPE_PREFIX)?;
    if name.is_empty() || name.len() > MAX_RUNNER_TYPE_BYTES {
        tracing::warn!(
            key,
            bytes = name.len(),
            max = MAX_RUNNER_TYPE_BYTES,
            "a runner-type catalogue entry's name does not fit the copy; ignored"
        );
        return None;
    }
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

/// One attempt: the watch, the scan, then the watch followed until a fault.
/// `live` is set once the scan is applied.
///
/// The watch is opened before the scan (it delivers the changes made after
/// it opens) and drained after it: a put or a delete landing while the scan
/// runs is in the watch whether the scan saw it or not, and applying it again
/// is harmless — upsert and forget are idempotent and the watch replays the
/// changes in order, so the copy ends on the bucket's last value per key.
async fn sync_once(nats: &Nats, pool: &PgPool, live: &mut bool) -> Result<Infallible, EngineError> {
    let prefix =
        KvPrefix::new(RUNNER_TYPE_PREFIX).map_err(|e| EngineError::Config(e.to_string()))?;
    let bucket = nats.published_language::<serde_json::Value>().await?;
    let mut watch = bucket.watch_all().await?;
    let entries = bucket.entries(&prefix).await?;
    let mut conn = pool.acquire().await.map_err(EngineError::from)?;
    let mut present = Vec::new();
    for (key, value) in &entries {
        if let Some(name) = apply_put(&mut conn, key.as_str(), value).await? {
            present.push(name);
        }
    }
    retain(&mut conn, &present).await?;
    drop(conn);
    *live = true;
    loop {
        let Some(next) = watch.next_under::<serde_json::Value>(&prefix).await else {
            return Err(EngineError::Config(
                "the runner-type catalogue watch ended".into(),
            ));
        };
        let event = match next {
            Ok(Watched::Event(event)) => event,
            Ok(Watched::Boundary(_)) => continue,
            Err(NatsError::Decode { key, source }) => {
                tracing::warn!(
                    key,
                    error = %source,
                    "a runner-type catalogue value is not JSON; its type is left out"
                );
                if let Some(name) = key.strip_prefix(RUNNER_TYPE_PREFIX) {
                    let mut conn = pool.acquire().await.map_err(EngineError::from)?;
                    forget(&mut conn, name).await?;
                }
                continue;
            }
            Err(error) => return Err(error.into()),
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

/// The running watch. Stop it at shutdown, or detach it to run until the
/// process exits.
#[must_use = "dropping the handle detaches the watch; call `stop` or `detach`"]
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

    /// Lets the watch run until the process exits — for a host that boots
    /// through the engine's `run_service` and holds no handle at shutdown.
    pub fn detach(self) {}
}

/// Starts the optional watch copying Jobs' runner-type catalogue into
/// `drive.known_runner_type` (restarted after a fault, with a pause doubling
/// from 1 s to 1 min). The copy is information only — a save's
/// `unknownRunnerTypes` warning and `<p>RunnerTypes` — never a condition on a
/// launch, and never a readiness reason.
pub fn watch_runner_types(nats: Nats, pool: PgPool) -> CatalogueWatch {
    let stop = Arc::new(Notify::new());
    let stopper = stop.clone();
    let task = tokio::spawn(async move {
        let stop = stopper;
        let mut delay = FIRST_RETRY;
        loop {
            let mut live = false;
            let outcome = tokio::select! {
                _ = stop.notified() => return,
                outcome = sync_once(&nats, &pool, &mut live) => outcome,
            };
            let error = match outcome {
                Ok(never) => match never {},
                Err(error) => error,
            };
            if live {
                delay = FIRST_RETRY;
            }
            tracing::warn!(
                %error,
                retry_in_ms = delay.as_millis(),
                "the runner-type catalogue watch faulted; retrying"
            );
            tokio::select! {
                _ = stop.notified() => return,
                _ = tokio::time::sleep(delay) => {}
            }
            delay = (delay * 2).min(MAX_RETRY);
        }
    });
    CatalogueWatch { stop, task }
}

/// [`watch_runner_types`] on the engine's own NATS and PostgreSQL handles —
/// the call for a host's `register(&mut Engine)` closure (`BootPlan`).
pub fn watch_runner_types_of<P: Principal>(engine: &Engine<P>) -> CatalogueWatch {
    let pool = engine.accumulator_handle().reader().pool().clone();
    watch_runner_types(engine.nats().clone(), pool)
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

    #[test]
    fn a_name_the_copy_cannot_hold_is_left_out() {
        let entry = |name: &str| serde_json::json!({ "runner_type": name, "lifecycle": "ACTIVE", "version": WIRE_VERSION });
        let longest = "r".repeat(MAX_RUNNER_TYPE_BYTES);
        let key = format!("{RUNNER_TYPE_PREFIX}{longest}");
        assert!(parse(&key, &entry(&longest)).is_some());
        let too_long = "r".repeat(MAX_RUNNER_TYPE_BYTES + 1);
        let key = format!("{RUNNER_TYPE_PREFIX}{too_long}");
        assert!(parse(&key, &entry(&too_long)).is_none());
        assert!(parse(RUNNER_TYPE_PREFIX, &entry("")).is_none());
    }
}
