use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use service_engine::nats::{
    INTEGRATION_CMD, INTEGRATION_EVT, KV_PUBLISHED_LANGUAGE, Nats, streaming_filter,
    streaming_stream,
};
use uuid::Uuid;

pub const STREAMING_MAX_AGE: Duration = Duration::from_secs(300);
pub const INTEGRATION_MAX_AGE: Duration = Duration::from_secs(1800);

pub struct TestNats {
    child: Child,
    port: u16,
    store: PathBuf,
    name: String,
}

impl TestNats {
    pub async fn spawn() -> Self {
        for _ in 0..5 {
            if let Some(server) = Self::try_spawn().await {
                return server;
            }
        }
        panic!("nats-server did not come up");
    }

    async fn try_spawn() -> Option<Self> {
        let port = free_port();
        let name = format!("drive-nats-{}", Uuid::now_v7().simple());
        let store = std::env::temp_dir().join(&name);
        std::fs::create_dir_all(&store).expect("create the ephemeral JetStream store");
        let child = Command::new("nats-server")
            .args([
                "-js",
                "-a",
                "127.0.0.1",
                "-p",
                &port.to_string(),
                "-n",
                &name,
                "-sd",
                store.to_str().expect("utf-8 store path"),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("nats-server must be on PATH");
        let mut server = Self {
            child,
            port,
            store,
            name,
        };
        server.await_ready().await.then_some(server)
    }

    pub fn url(&self) -> String {
        format!("nats://127.0.0.1:{}", self.port)
    }

    pub async fn nats(&self) -> Nats {
        Nats::connect(&self.url())
            .await
            .expect("dial the ephemeral broker")
    }

    pub async fn provision(&self, service: &str) {
        let js = self.jetstream().await;
        for (name, subject) in [
            (INTEGRATION_CMD, "integration.cmd.>"),
            (INTEGRATION_EVT, "integration.evt.>"),
        ] {
            js.create_stream(async_nats::jetstream::stream::Config {
                name: name.to_string(),
                subjects: vec![subject.to_string()],
                duplicate_window: Duration::from_secs(120),
                max_age: INTEGRATION_MAX_AGE,
                ..Default::default()
            })
            .await
            .unwrap_or_else(|e| panic!("declare {name}: {e}"));
        }
        js.create_stream(async_nats::jetstream::stream::Config {
            name: streaming_stream(service),
            subjects: vec![streaming_filter(service)],
            max_age: STREAMING_MAX_AGE,
            ..Default::default()
        })
        .await
        .unwrap_or_else(|e| {
            panic!(
                "declare the {} lane-A stream: {e}",
                streaming_stream(service)
            )
        });
        js.create_key_value(async_nats::jetstream::kv::Config {
            bucket: KV_PUBLISHED_LANGUAGE.to_string(),
            history: 1,
            ..Default::default()
        })
        .await
        .expect("declare PUBLISHED_LANGUAGE");
        js.create_key_value(async_nats::jetstream::kv::Config {
            bucket: format!("EPHEMERAL_{service}"),
            history: 1,
            max_age: Duration::from_secs(300),
            limit_markers: Some(Duration::from_secs(300)),
            ..Default::default()
        })
        .await
        .expect("declare the EPHEMERAL presence bucket");
    }

    async fn jetstream(&self) -> async_nats::jetstream::Context {
        let client = async_nats::connect(&self.url())
            .await
            .expect("connect broker");
        async_nats::jetstream::new(client)
    }

    async fn await_ready(&mut self) -> bool {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if self.child.try_wait().expect("read nats state").is_some() {
                return false;
            }
            if let Ok(client) = async_nats::connect(&self.url()).await {
                return client.server_info().server_name == self.name;
            }
            if Instant::now() >= deadline {
                panic!("nats-server never accepted connections");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

impl Drop for TestNats {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.store);
    }
}

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener.local_addr().expect("read port").port();
    drop(listener);
    port
}
