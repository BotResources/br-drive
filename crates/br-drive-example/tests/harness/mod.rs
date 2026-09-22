pub mod nats;
pub mod pg;
pub mod ws;

use std::net::SocketAddr;
use std::time::Duration;

use br_core_auth::{AuthMethod, Passport, PassportClaims, PassportHeader};
use br_drive_example::boot::{BootOptions, Service, boot};
use service_engine::config::EngineConfig;
use service_engine::name::{ChannelName, PodId};
use uuid::Uuid;

pub use nats::TestNats;
pub use pg::TestDb;
pub use ws::Subscription;

pub struct World {
    pub db: TestDb,
    pub service: Service,
    pub http: reqwest::Client,
    _nats_server: TestNats,
}

impl World {
    pub async fn start(pod: &str) -> World {
        install_log_capture();
        let db = TestDb::fresh().await;
        let nats_server = TestNats::spawn().await;
        nats_server.provision(br_drive_example::SERVICE).await;

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let config = base_config(pod, addr);

        let service = boot(
            config,
            db.app.clone(),
            nats_server.nats().await,
            BootOptions { await_ready: true },
        )
        .await
        .expect("the example host boots and reaches readiness");

        World {
            db,
            service,
            http: reqwest::Client::new(),
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
