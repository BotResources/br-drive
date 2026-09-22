pub mod minio;
pub mod nats;
pub mod pg;
pub mod upload;
pub mod ws;

use std::net::SocketAddr;
use std::time::Duration;

use br_core_auth::{AuthMethod, Passport, PassportClaims, PassportHeader};
use br_drive_example::boot::{BootOptions, Service, boot};
use br_drive_example::kernel::HostSettings;
use service_engine::config::EngineConfig;
use service_engine::name::{ChannelName, PodId};
use uuid::Uuid;

pub use minio::TestMinio;
pub use nats::TestNats;
pub use pg::TestDb;
pub use ws::Subscription;

pub struct World {
    pub db: TestDb,
    pub service: Service,
    pub http: reqwest::Client,
    pub minio: Option<TestMinio>,
    pub blob_bucket: Option<String>,
    _nats_server: TestNats,
}

pub struct WorldOptions {
    pub blobs: bool,
    pub reaper_interval: Option<Duration>,
    pub upload_window: Duration,
}

impl Default for WorldOptions {
    fn default() -> Self {
        Self {
            blobs: false,
            reaper_interval: None,
            upload_window: HostSettings::DEFAULT_UPLOAD_WINDOW,
        }
    }
}

impl World {
    pub async fn start(pod: &str) -> World {
        World::start_with(pod, WorldOptions::default()).await
    }

    pub async fn start_blobs(pod: &str) -> World {
        World::start_with(
            pod,
            WorldOptions {
                blobs: true,
                reaper_interval: Some(Duration::from_millis(150)),
                ..WorldOptions::default()
            },
        )
        .await
    }

    pub async fn start_with(pod: &str, options: WorldOptions) -> World {
        install_log_capture();
        let db = TestDb::fresh().await;
        let nats_server = TestNats::spawn().await;
        nats_server.provision(br_drive_example::SERVICE).await;

        let (minio, blob_bucket, blob_config) = if options.blobs {
            let minio = TestMinio::spawn().await;
            let bucket = format!("drive-{}", Uuid::now_v7().simple());
            minio.create_bucket(&bucket).await;
            let config = minio.config(&bucket);
            (Some(minio), Some(bucket), Some(config))
        } else {
            (None, None, None)
        };

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let mut config = base_config(pod, addr);
        if let Some(blob_config) = blob_config {
            config = config.with_blob_storage(blob_config);
        }
        if let Some(interval) = options.reaper_interval {
            config = config.with_blob_reaper_interval(interval);
        }

        let service = boot(
            config,
            db.app.clone(),
            nats_server.nats().await,
            BootOptions {
                await_ready: true,
                settings: HostSettings {
                    upload_window: options.upload_window,
                },
            },
        )
        .await
        .expect("the example host boots and reaches readiness");

        World {
            db,
            service,
            http: reqwest::Client::new(),
            minio,
            blob_bucket,
            _nats_server: nats_server,
        }
    }

    pub fn subscription_url(&self) -> String {
        self.service.ws("/graphql/ws")
    }

    pub async fn gql(
        &self,
        passport: &str,
        query: &str,
        variables: serde_json::Value,
    ) -> serde_json::Value {
        let response = self
            .http
            .post(self.service.http("/graphql"))
            .header("x-passport", passport)
            .json(&serde_json::json!({ "query": query, "variables": variables }))
            .send()
            .await
            .expect("the graphql request reaches the service");
        response.json().await.expect("a json graphql response")
    }

    pub async fn create_workspace(&self, passport: &str, name: &str) -> Uuid {
        let id = Uuid::now_v7();
        ok(&self
            .gql(
                passport,
                "mutation($id:UUID!,$n:String!){workspaceCreate(id:$id,name:$n){success}}",
                serde_json::json!({ "id": id, "n": name }),
            )
            .await);
        id
    }

    pub async fn file(&self, passport: &str, file_id: Uuid) -> serde_json::Value {
        let response = self
            .gql(
                passport,
                "query($id:UUID!){workspaceFile(fileId:$id){id driveId path name protected \
                 mediaType sizeBytes sha256 processingState affordances}}",
                serde_json::json!({ "id": file_id }),
            )
            .await;
        ok(&response)["workspaceFile"].clone()
    }

    pub async fn drive_files(&self, passport: &str, drive: Uuid) -> Vec<serde_json::Value> {
        let response = self
            .gql(
                passport,
                "query($d:UUID!){workspaceDriveFiles(driveId:$d){id path name protected processingState}}",
                serde_json::json!({ "d": drive }),
            )
            .await;
        ok(&response)["workspaceDriveFiles"]
            .as_array()
            .expect("a list of files")
            .clone()
    }

    pub async fn file_access(&self, passport: &str, file_id: Uuid) -> serde_json::Value {
        self.gql(
            passport,
            "query($id:UUID!){workspaceFileAccess(fileId:$id)}",
            serde_json::json!({ "id": file_id }),
        )
        .await
    }

    pub async fn blob_state(&self, reference: Uuid) -> Option<String> {
        sqlx::query_scalar("SELECT state FROM service_engine.blob WHERE id = $1")
            .bind(reference)
            .fetch_optional(&self.db.app)
            .await
            .expect("read the blob row")
    }

    pub async fn source_of(&self, file_id: Uuid) -> Uuid {
        sqlx::query_scalar("SELECT blob_ref FROM drive.file WHERE id = $1")
            .bind(file_id)
            .fetch_one(&self.db.app)
            .await
            .expect("the file row carries its source reference")
    }

    pub async fn folder_gestures(&self, workspace: Uuid) -> Vec<(String, String, Option<String>)> {
        sqlx::query_as(
            "SELECT gesture, prefix, new_prefix FROM workspace_folder_gesture \
             WHERE workspace_id = $1 ORDER BY at",
        )
        .bind(workspace)
        .fetch_all(&self.db.app)
        .await
        .expect("read the host's folder gesture log")
    }

    pub async fn cleanup(self) {
        self.service.shutdown().await;
        self.db.cleanup().await;
    }
}

fn install_log_capture() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        use tracing_subscriber::EnvFilter;
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("error"));
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .try_init();
    });
}

fn base_config(pod: &str, addr: SocketAddr) -> EngineConfig {
    EngineConfig::new(
        ChannelName::new("workspace").expect("valid channel"),
        PodId::new(pod).expect("valid pod id"),
    )
    .with_service(br_drive_example::SERVICE)
    .with_window(Duration::from_millis(30))
    .with_beat(Duration::from_millis(80))
    .with_lease(Duration::from_secs(5))
    .with_lock_timeout(Duration::from_millis(400))
    .with_http_addr(addr)
}

pub fn passport(user: Uuid) -> String {
    Passport::human(
        user,
        false,
        true,
        AuthMethod::Jwt,
        None,
        PassportClaims::new(),
    )
    .to_header()
}

#[macro_export]
macro_rules! poll_until {
    ($within:expr, $probe:block) => {{
        let deadline = std::time::Instant::now() + $within;
        loop {
            if let Some(value) = $probe {
                break value;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the observed effect never appeared within {:?}",
                $within
            );
            tokio::time::sleep(std::time::Duration::from_millis(40)).await;
        }
    }};
}

pub fn ok(response: &serde_json::Value) -> &serde_json::Value {
    assert!(
        response.get("errors").is_none(),
        "unexpected graphql errors: {response}"
    );
    &response["data"]
}

pub fn error_code(response: &serde_json::Value) -> String {
    response["errors"][0]["extensions"]["code"]
        .as_str()
        .unwrap_or_default()
        .to_string()
}

pub const DRIVE_DELTAS: &str = "subscription($d:UUID!){workspaceDriveChanged(driveId:$d){\
    __typename \
    ... on DriveReset{revision views{... on DriveFile{id path name processingState}}} \
    ... on DriveUpsert{revision cause view{... on DriveFile{id path name processingState affordances}}} \
    ... on DriveRemove{revision projector key cause}}}";

pub async fn drive_subscription(world: &World, passport: &str, drive: Uuid) -> Subscription {
    let mut sub = Subscription::open_with(
        &world.subscription_url(),
        passport,
        DRIVE_DELTAS,
        serde_json::json!({ "d": drive }),
    )
    .await;
    let reset = sub.next_payload(Duration::from_secs(10)).await;
    assert_eq!(
        reset["workspaceDriveChanged"]["__typename"], "DriveReset",
        "the first delta on attach is a Reset: {reset}"
    );
    sub
}

pub async fn next_drive_delta(
    sub: &mut Subscription,
    matches: impl Fn(&serde_json::Value) -> bool,
) -> serde_json::Value {
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let delta = sub.next_payload(Duration::from_secs(15)).await;
        let node = &delta["workspaceDriveChanged"];
        if std::env::var("DRIVE_TRACE_DELTAS").is_ok() {
            eprintln!("delta: {node}");
        }
        if matches(node) {
            break node.clone();
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the expected drive delta never arrived"
        );
    }
}
