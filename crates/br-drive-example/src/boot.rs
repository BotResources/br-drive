use std::sync::Arc;
use std::time::{Duration, Instant};

use service_engine::config::EngineConfig;
use service_engine::error::EngineError;
use service_engine::nats::Nats;
use service_engine::{BlobReader, Engine, Settle};
use service_engine::{Readiness, ReadinessHandle};
use sqlx::PgPool;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::kernel::{AppPrincipal, HostSettings};

pub struct Service {
    pub base_url: String,
    settle: Settle<AppPrincipal>,
    blob_reader: BlobReader,
    readiness: ReadinessHandle,
    stop: Arc<Notify>,
    handle: JoinHandle<Result<(), EngineError>>,
    #[cfg(feature = "drive")]
    catalogue: Option<br_drive::CatalogueWatch>,
    eraser: service_engine::Eraser<AppPrincipal>,
}

impl Service {
    pub fn http(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    pub fn ws(&self, path: &str) -> String {
        format!("{}{path}", self.base_url.replacen("http://", "ws://", 1))
    }

    pub fn blob_reader(&self) -> BlobReader {
        self.blob_reader.clone()
    }

    pub fn readiness(&self) -> &ReadinessHandle {
        &self.readiness
    }

    /// Erases a person through the engine's erase pipeline — the host's
    /// gesture, never a GraphQL root of the library.
    pub async fn erase(
        &self,
        person: uuid::Uuid,
    ) -> Result<service_engine::EraseOutcome, EngineError> {
        self.eraser.erase(service_engine::PersonId(person)).await
    }

    pub async fn settle(&self) {
        self.settle
            .settle(Duration::from_millis(150), Duration::from_secs(5))
            .await;
    }

    pub async fn shutdown(self) {
        #[cfg(feature = "drive")]
        if let Some(watch) = self.catalogue {
            watch.stop().await;
        }
        self.stop.notify_one();
        let _ = self.handle.await;
    }
}

pub struct BootOptions {
    pub await_ready: bool,
    pub settings: HostSettings,
    /// Start the optional runner-type catalogue watch (information only: the
    /// save's `unknownRunnerTypes` and `workspaceRunnerTypes`); `false` models
    /// a host that does not.
    pub watch_catalogue: bool,
}

impl Default for BootOptions {
    fn default() -> Self {
        Self {
            await_ready: true,
            settings: HostSettings::default(),
            watch_catalogue: true,
        }
    }
}

pub async fn assemble(
    config: EngineConfig,
    pool: PgPool,
    nats: Nats,
    readiness: ReadinessHandle,
    settings: HostSettings,
) -> Result<(Engine<AppPrincipal>, axum::Router), EngineError> {
    let mut engine = Engine::<AppPrincipal>::boot(config, pool, nats, readiness.clone()).await?;
    crate::register::all_with(Arc::new(settings))(&mut engine)?;
    let app = crate::graphql::build(&mut engine, readiness);
    Ok((engine, app))
}

pub async fn boot(
    config: EngineConfig,
    pool: PgPool,
    nats: Nats,
    options: BootOptions,
) -> Result<Service, EngineError> {
    let http_addr = config.http_addr;
    let readiness = ReadinessHandle::not_ready("booting");
    let (engine, app) = assemble(
        config,
        pool.clone(),
        nats,
        readiness.clone(),
        options.settings,
    )
    .await?;
    #[cfg(feature = "drive")]
    let catalogue = options
        .watch_catalogue
        .then(|| br_drive::watch_runner_types(engine.nats().clone(), pool));
    #[cfg(not(feature = "drive"))]
    let _ = (pool, options.watch_catalogue);
    let stop = engine.shutdown_handle();
    let settle = engine.settle_handle();
    let blob_reader = engine.blob_reader();
    let eraser = engine.eraser();

    let listener = TcpListener::bind(http_addr)
        .await
        .map_err(|source| EngineError::Http {
            addr: http_addr,
            source,
        })?;
    let addr = listener.local_addr().map_err(|source| EngineError::Http {
        addr: http_addr,
        source,
    })?;
    let handle = tokio::spawn(engine.run_with_listener(listener, app));

    if options.await_ready {
        let deadline = Instant::now() + Duration::from_secs(30);
        while readiness.snapshot() != Readiness::Ready {
            if handle.is_finished() {
                let outcome = handle.await.expect("the engine task joined");
                return Err(EngineError::Config(format!(
                    "the engine task ended during boot: {outcome:?}"
                )));
            }
            if Instant::now() >= deadline {
                let reason = format!("{:?}", readiness.snapshot());
                stop.notify_one();
                return Err(EngineError::Config(format!(
                    "the engine never reached readiness UP within the boot deadline; last state: {reason}"
                )));
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    Ok(Service {
        base_url: format!("http://{addr}"),
        settle,
        blob_reader,
        readiness,
        stop,
        handle,
        #[cfg(feature = "drive")]
        catalogue,
        eraser,
    })
}
