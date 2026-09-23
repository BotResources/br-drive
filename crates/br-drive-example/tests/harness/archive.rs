//! A second, minimal host embedding the drive slice under another service name
//! and a two-word root prefix. It shares the NATS broker with the example host
//! so a scenario can prove that two hosts on one cluster each receive every
//! Jobs fact about their own jobs and both boot.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use br_core_auth::Passport;
use br_core_integration::Actor;
use br_drive::{DriveHost, DriveRequest};
use futures_util::future::BoxFuture;
use service_engine::config::EngineConfig;
use service_engine::error::EngineError;
use service_engine::gate::{Gate, Reason};
use service_engine::impact::Deps;
use service_engine::name::{ChannelName, PodId};
use service_engine::principal::{Principal, PrincipalId, PrincipalResolver};
use service_engine::{
    Engine, PassportPrincipal, PrincipalRejected, Readiness, ReadinessHandle, engine_schema,
};
use sqlx::PgPool;
use tokio::net::TcpListener;
use uuid::Uuid;

use super::pg::TestDb;
use super::{World, ok};

pub const ARCHIVE_SERVICE: &str = "archive";
pub const ARCHIVE_RUNNER_SCOPE: &str = "archive:runner";
pub const NOT_THE_ARCHIVIST: Reason = Reason::new("NOT_THE_ARCHIVIST");

#[derive(Debug, Clone)]
pub struct ArchivePrincipal {
    id: PrincipalId,
    passport: Passport,
    drives: Vec<Uuid>,
}

async fn drives_of(pg: &PgPool, person: Uuid) -> Result<Vec<Uuid>, sqlx::Error> {
    sqlx::query_scalar("SELECT id FROM drive.drive WHERE created_by = $1")
        .bind(person)
        .fetch_all(pg)
        .await
}

impl Principal for ArchivePrincipal {
    fn id(&self) -> PrincipalId {
        self.id
    }

    fn passport(&self) -> &Passport {
        &self.passport
    }
}

impl PassportPrincipal for ArchivePrincipal {
    fn from_passport(
        pg: &PgPool,
        passport: Passport,
    ) -> BoxFuture<'_, Result<Self, PrincipalRejected>> {
        Box::pin(async move {
            let id = passport
                .user_id()
                .or_else(|| passport.service_account_id())
                .ok_or_else(|| PrincipalRejected::new("the passport names no subject"))?;
            let drives = drives_of(pg, id)
                .await
                .map_err(|e| PrincipalRejected::new(e.to_string()))?;
            Ok(Self {
                id: PrincipalId::from(id),
                passport,
                drives,
            })
        })
    }
}

struct ArchiveResolver;

impl PrincipalResolver<ArchivePrincipal> for ArchiveResolver {
    fn resolve<'a>(
        &'a self,
        pg: &'a PgPool,
        current: &'a ArchivePrincipal,
    ) -> BoxFuture<'a, Result<Option<ArchivePrincipal>, EngineError>> {
        Box::pin(async move {
            let drives = drives_of(pg, current.id.as_uuid()).await?;
            Ok(Some(ArchivePrincipal {
                id: current.id,
                passport: current.passport.clone(),
                drives,
            }))
        })
    }
}

impl DriveHost for ArchivePrincipal {
    const SERVICE: &'static str = ARCHIVE_SERVICE;
    const RUNNER_SCOPE: &'static str = ARCHIVE_RUNNER_SCOPE;
    const VISIBILITY_DEPS: Deps = Deps::ALL;
    const SOURCE_ORPHAN_AFTER: Duration = Duration::from_secs(8);
    const IMAGE_ORPHAN_AFTER: Duration = Duration::from_secs(8);

    fn drive_gate(&self, request: &DriveRequest<'_, Self>) -> Gate {
        let human = self.passport.user_id().is_some();
        let allowed = match request {
            DriveRequest::ManageRulesets | DriveRequest::ReadRulesets => human,
            other => other
                .drive()
                .is_some_and(|drive| self.drives.contains(&drive)),
        };
        if allowed {
            Gate::allowed()
        } else {
            Gate::blocked(NOT_THE_ARCHIVIST)
        }
    }

    fn visible_drives(&self) -> Vec<Uuid> {
        self.drives.clone()
    }

    fn display_name(&self) -> Option<String> {
        self.passport.claim::<String>("name")
    }
}

pub mod schema {
    service_engine::compose_service! {
        principal = crate::harness::archive::ArchivePrincipal;
        prefix = archive_vault;
        slice drive ["drive"] from br_drive::drive_slice { query = drive::DriveQuery, mutation = drive::DriveMutation, subscription = drive::DriveSubscription }
    }
}

pub struct ArchiveHost {
    pub db: TestDb,
    pub base_url: String,
    stop: Arc<tokio::sync::Notify>,
    handle: tokio::task::JoinHandle<Result<(), EngineError>>,
    catalogue: br_drive::CatalogueWatch,
}

impl ArchiveHost {
    /// Boots the archive host on the example world's broker and object store,
    /// with its own database.
    pub async fn start(world: &World, pod: &str) -> Self {
        world.nats_server.provision(ARCHIVE_SERVICE).await;
        let db = TestDb::fresh().await;
        let bucket = format!("archive-{}", Uuid::now_v7().simple());
        world.minio.create_bucket(&bucket).await;
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let config = EngineConfig::new(
            ChannelName::new(ARCHIVE_SERVICE).expect("valid channel"),
            PodId::new(pod).expect("valid pod id"),
        )
        .with_service(ARCHIVE_SERVICE)
        .with_window(Duration::from_millis(30))
        .with_beat(Duration::from_millis(80))
        .with_lease(Duration::from_secs(5))
        .with_lock_timeout(Duration::from_millis(400))
        .with_http_addr(addr)
        .with_blob_storage(world.minio.config(&bucket))
        .with_blob_reaper_interval(Duration::from_millis(150));

        let readiness = ReadinessHandle::not_ready("booting");
        let nats = world.nats_server.nats().await;
        let mut engine =
            Engine::<ArchivePrincipal>::boot(config, db.app.clone(), nats, readiness.clone())
                .await
                .expect("the archive host's engine boots");
        engine
            .register_principal_resolver(ArchiveResolver)
            .expect("register the resolver");
        engine
            .register_reaction_principal(
                |pg, actor: Actor| -> BoxFuture<'_, Result<ArchivePrincipal, EngineError>> {
                    Box::pin(async move {
                        let passport = match actor {
                            Actor::Service(id) => {
                                Passport::service(id.as_uuid(), br_core_auth::PassportClaims::new())
                            }
                            Actor::Human(id) => Passport::human(
                                id.as_uuid(),
                                false,
                                true,
                                br_core_auth::AuthMethod::Jwt,
                                None,
                                br_core_auth::PassportClaims::new(),
                            ),
                        };
                        let drives = drives_of(pg, actor.id()).await?;
                        Ok(ArchivePrincipal {
                            id: PrincipalId::from(actor.id()),
                            passport,
                            drives,
                        })
                    })
                },
            )
            .expect("register the reaction principal");
        schema::register(&mut engine).expect("the drive slice registers for the archive host");
        let state = Arc::new(engine.graphql_state());
        let graphql = engine_schema(
            schema::QueryRoot::default(),
            schema::MutationRoot::default(),
            schema::SubscriptionRoot::default(),
            state.clone(),
        );
        engine.set_schema_sdl(graphql.sdl());
        let app = service_engine::app(graphql, state, readiness.clone());
        let stop = engine.shutdown_handle();
        let catalogue = br_drive::watch_runner_types(engine.nats().clone(), db.app.clone());
        let listener = TcpListener::bind(addr)
            .await
            .expect("bind the archive host");
        let bound = listener.local_addr().expect("the bound address");
        let handle = tokio::spawn(engine.run_with_listener(listener, app));
        let deadline = Instant::now() + Duration::from_secs(30);
        while readiness.snapshot() != Readiness::Ready {
            assert!(
                !handle.is_finished(),
                "the archive host's engine ended during boot"
            );
            assert!(
                Instant::now() < deadline,
                "the archive host never became ready: {:?}",
                readiness.snapshot()
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        Self {
            db,
            base_url: format!("http://{bound}"),
            stop,
            handle,
            catalogue,
        }
    }

    pub async fn gql(
        &self,
        world: &World,
        passport: &str,
        query: &str,
        variables: serde_json::Value,
    ) -> serde_json::Value {
        let response = world
            .http
            .post(format!("{}/graphql", self.base_url))
            .header("x-passport", passport)
            .json(&serde_json::json!({ "query": query, "variables": variables }))
            .send()
            .await
            .expect("the graphql request reaches the archive host");
        response.json().await.expect("a json graphql response")
    }

    /// The archive host has no host object of its own: the drive row is the
    /// fixture, created the way the host's own mutation would.
    pub async fn drive_for(&self, person: Uuid) -> Uuid {
        let id = Uuid::now_v7();
        sqlx::query("INSERT INTO drive.drive (id, created_by, created_at) VALUES ($1, $2, now())")
            .bind(id)
            .bind(person)
            .execute(&self.db.app)
            .await
            .expect("insert the archive drive");
        id
    }

    pub async fn job_of(&self, file_id: Uuid) -> Option<Uuid> {
        sqlx::query_scalar("SELECT job_id FROM drive.file WHERE id = $1")
            .bind(file_id)
            .fetch_one(&self.db.app)
            .await
            .expect("the archive file row")
    }

    pub async fn await_known_runner_type(&self, runner_type: &str) {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let found: Option<String> = sqlx::query_scalar(
                "SELECT lifecycle FROM drive.known_runner_type WHERE runner_type = $1",
            )
            .bind(runner_type)
            .fetch_optional(&self.db.app)
            .await
            .expect("read the archive catalogue mirror");
            if found.as_deref() == Some("active") {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "the archive mirror never showed {runner_type} active"
            );
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
    }

    pub async fn file_state(
        &self,
        world: &World,
        passport: &str,
        file_id: Uuid,
    ) -> serde_json::Value {
        let response = self
            .gql(
                world,
                passport,
                "query($id:UUID!){archiveVaultFile(fileId:$id){id processingState processingError rulesetId progress{stepIndex stepCount runnerType plan currentIndex currentLabel}}}",
                serde_json::json!({ "id": file_id }),
            )
            .await;
        ok(&response)["archiveVaultFile"].clone()
    }

    pub async fn shutdown(self) {
        self.catalogue.stop().await;
        self.stop.notify_one();
        let _ = self.handle.await;
        self.db.cleanup().await;
    }
}
