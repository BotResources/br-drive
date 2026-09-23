pub mod archive;
pub mod jobs;
pub mod minio;
pub mod nats;
pub mod pg;
pub mod runner;
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

pub use archive::ArchiveHost;
pub use jobs::JobsStandIn;
pub use minio::TestMinio;
pub use nats::TestNats;
pub use pg::TestDb;
pub use ws::Subscription;

pub struct World {
    pub db: TestDb,
    pub service: Service,
    pub http: reqwest::Client,
    pub minio: TestMinio,
    pub blob_bucket: String,
    pub nats_server: TestNats,
}

pub struct WorldOptions {
    pub reaper_interval: Duration,
    pub upload_window: Duration,
}

impl Default for WorldOptions {
    fn default() -> Self {
        Self {
            reaper_interval: Duration::from_millis(150),
            upload_window: HostSettings::DEFAULT_UPLOAD_WINDOW,
        }
    }
}

impl World {
    pub async fn start(pod: &str) -> World {
        World::start_with(pod, WorldOptions::default()).await
    }

    pub async fn start_with(pod: &str, options: WorldOptions) -> World {
        install_log_capture();
        let db = TestDb::fresh().await;
        let nats_server = TestNats::spawn().await;
        nats_server.provision(br_drive_example::SERVICE).await;

        let minio = TestMinio::spawn().await;
        let blob_bucket = format!("drive-{}", Uuid::now_v7().simple());
        minio.create_bucket(&blob_bucket).await;
        let blob_config = minio.config(&blob_bucket);

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let config = base_config(pod, addr)
            .with_blob_storage(blob_config)
            .with_blob_reaper_interval(options.reaper_interval);

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
            nats_server,
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
                 mediaType sizeBytes sha256 processingState processingError summary pageCount \
                 estimatedTokens images{name mediaType sizeBytes page} rulesetId \
                 steps{runnerType options} progress{stepIndex stepCount runnerType plan \
                 currentIndex currentLabel at} affordances updatedAt}}",
                serde_json::json!({ "id": file_id }),
            )
            .await;
        ok(&response)["workspaceFile"].clone()
    }

    pub async fn file_pages(&self, passport: &str, file_id: Uuid) -> Vec<serde_json::Value> {
        let response = self
            .gql(
                passport,
                "query($f:UUID!){workspacePages(fileId:$f){fileId number markdown origin updatedBy affordances}}",
                serde_json::json!({ "f": file_id }),
            )
            .await;
        ok(&response)["workspacePages"]
            .as_array()
            .expect("a list of pages")
            .clone()
    }

    pub async fn image_row(&self, file_id: Uuid, name: &str) -> Option<(Uuid, Option<Uuid>, bool)> {
        sqlx::query_as(
            "SELECT blob_ref, pending_blob_ref, landed_at IS NOT NULL FROM drive.file_image \
             WHERE file_id = $1 AND name = $2",
        )
        .bind(file_id)
        .bind(name)
        .fetch_optional(&self.db.app)
        .await
        .expect("read the image row")
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

    pub async fn image_access(
        &self,
        passport: &str,
        file_id: Uuid,
        name: &str,
    ) -> serde_json::Value {
        self.gql(
            passport,
            "query($id:UUID!,$n:String){workspaceFileAccess(fileId:$id,name:$n)}",
            serde_json::json!({ "id": file_id, "n": name }),
        )
        .await
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

    pub async fn job_of(&self, file_id: Uuid) -> Option<Uuid> {
        sqlx::query_scalar("SELECT job_id FROM drive.file WHERE id = $1")
            .bind(file_id)
            .fetch_one(&self.db.app)
            .await
            .expect("the file row carries its job")
    }

    pub async fn await_job(&self, file_id: Uuid) -> Uuid {
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            if let Some(job) = self.job_of(file_id).await {
                return job;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "no job was minted for {file_id}"
            );
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
    }

    pub async fn await_state(
        &self,
        passport: &str,
        file_id: Uuid,
        state: &str,
    ) -> serde_json::Value {
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            let file = self.file(passport, file_id).await;
            if file["processingState"] == state {
                return file;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{file_id} never reached {state}: {file}"
            );
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
    }

    pub async fn await_known_runner_type(&self, runner_type: &str, lifecycle: Option<&str>) {
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            let found: Option<String> = sqlx::query_scalar(
                "SELECT lifecycle FROM drive.known_runner_type WHERE runner_type = $1",
            )
            .bind(runner_type)
            .fetch_optional(&self.db.app)
            .await
            .expect("read the catalogue mirror");
            if found.as_deref() == lifecycle {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the catalogue mirror never showed {runner_type} as {lifecycle:?} (found {found:?})"
            );
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
    }

    pub async fn rulesets(&self, passport: &str) -> Vec<serde_json::Value> {
        let response = self
            .gql(
                passport,
                "query{workspaceRulesets{id name trigger mediaTypes steps{runnerType options} isDefault}}",
                serde_json::json!({}),
            )
            .await;
        ok(&response)["workspaceRulesets"]
            .as_array()
            .expect("a list of rulesets")
            .clone()
    }

    pub async fn await_source_promoted(&self, file_id: Uuid) {
        let source = self.source_of(file_id).await;
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        while self.blob_state(source).await.as_deref() != Some("uploaded") {
            assert!(
                std::time::Instant::now() < deadline,
                "the engine reaper never promoted the source of {file_id}"
            );
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
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

    pub async fn object_exists(&self, object_key: &str) -> bool {
        self.minio
            .object_exists(&self.blob_bucket, object_key)
            .await
    }

    pub async fn object_key_of(&self, reference: Uuid) -> Option<String> {
        sqlx::query_scalar("SELECT object_key FROM service_engine.blob WHERE id = $1")
            .bind(reference)
            .fetch_optional(&self.db.app)
            .await
            .expect("read the blob row")
    }

    pub async fn send_upload_deadline(&self, file_id: Uuid) {
        use br_core_integration::{Actor, EventMetadata, IntegrationCommand, ServiceAccountId};
        let command = IntegrationCommand::new(
            Uuid::now_v7(),
            "drive_file.upload-deadline",
            1,
            chrono::Utc::now(),
            EventMetadata::new(
                Actor::Service(ServiceAccountId::from(Uuid::now_v7())),
                Uuid::now_v7(),
            ),
            serde_json::json!({ "file_id": file_id }),
        );
        let bytes = serde_json::to_vec(&command).expect("the command encodes");
        let client = async_nats::connect(self.nats_server.url())
            .await
            .expect("dial the ephemeral broker");
        let js = async_nats::jetstream::new(client);
        js.publish(
            format!(
                "integration.cmd.{}.drive_file.upload-deadline.v1",
                br_drive_example::SERVICE
            ),
            bytes.into(),
        )
        .await
        .expect("publish the redelivered deadline")
        .await
        .expect("the stream acks the redelivered deadline");
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

pub fn service_passport(scopes: &[&str]) -> String {
    let mut map = serde_json::Map::new();
    map.insert(
        "scopes".to_string(),
        serde_json::Value::Array(
            scopes
                .iter()
                .map(|scope| serde_json::Value::String((*scope).to_string()))
                .collect(),
        ),
    );
    Passport::service(Uuid::now_v7(), PassportClaims::from_map(map)).to_header()
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

pub fn manager_passport(user: Uuid, display_name: &str) -> String {
    let mut map = serde_json::Map::new();
    map.insert(
        "scopes".to_string(),
        serde_json::json!(["workspace:manage"]),
    );
    map.insert("name".to_string(), serde_json::json!(display_name));
    Passport::human(
        user,
        false,
        true,
        AuthMethod::Jwt,
        None,
        PassportClaims::from_map(map),
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
    ... on DriveUpsert{revision cause view{... on DriveFile{id path name processingState processingError affordances summary pageCount estimatedTokens images{name page} rulesetId progress{stepIndex stepCount runnerType plan currentIndex currentLabel}}}} \
    ... on DriveRemove{revision projector key cause}}}";

pub const PAGE_DELTAS: &str = "subscription($f:UUID!){workspaceFilePages(fileId:$f){\
    __typename \
    ... on DriveReset{revision views{... on DrivePage{fileId number markdown origin affordances}}} \
    ... on DriveUpsert{revision cause view{... on DrivePage{fileId number markdown origin updatedBy affordances}}} \
    ... on DriveRemove{revision projector key cause}}}";

pub async fn pages_subscription(world: &World, passport: &str, file_id: Uuid) -> Subscription {
    let mut sub = Subscription::open_with(
        &world.subscription_url(),
        passport,
        PAGE_DELTAS,
        serde_json::json!({ "f": file_id }),
    )
    .await;
    let reset = sub.next_payload(Duration::from_secs(10)).await;
    assert_eq!(
        reset["workspaceFilePages"]["__typename"], "DriveReset",
        "the first delta on attach is a Reset: {reset}"
    );
    sub
}

pub async fn pages_reset(world: &World, passport: &str, file_id: Uuid) -> Vec<serde_json::Value> {
    let mut sub = Subscription::open_with(
        &world.subscription_url(),
        passport,
        PAGE_DELTAS,
        serde_json::json!({ "f": file_id }),
    )
    .await;
    let reset = sub.next_payload(Duration::from_secs(10)).await;
    reset["workspaceFilePages"]["views"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

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

pub async fn drain_with_a_rename(
    world: &World,
    passport: &str,
    sub: &mut Subscription,
    file_id: Uuid,
    name: &str,
) {
    ok(&world
        .gql(
            passport,
            "mutation($f:UUID!,$n:String){workspaceUpdateFile(fileId:$f,name:$n){success}}",
            serde_json::json!({ "f": file_id, "n": name }),
        )
        .await);
    next_drive_delta(sub, |node| {
        node["__typename"] == "DriveUpsert"
            && node["cause"]["kind"] == "Renamed"
            && node["view"]["id"] == file_id.to_string()
    })
    .await;
}

pub async fn next_drive_delta(
    sub: &mut Subscription,
    matches: impl Fn(&serde_json::Value) -> bool,
) -> serde_json::Value {
    next_delta(sub, "workspaceDriveChanged", matches).await
}

pub async fn next_page_delta(
    sub: &mut Subscription,
    matches: impl Fn(&serde_json::Value) -> bool,
) -> serde_json::Value {
    next_delta(sub, "workspaceFilePages", matches).await
}

pub async fn next_delta(
    sub: &mut Subscription,
    root: &str,
    matches: impl Fn(&serde_json::Value) -> bool,
) -> serde_json::Value {
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let delta = sub.next_payload(Duration::from_secs(15)).await;
        let node = &delta[root];
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
