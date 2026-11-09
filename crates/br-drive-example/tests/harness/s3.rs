//! A throwaway SeaweedFS S3 store per world: one `weed server -s3` process
//! (master, volume, filer and S3 gateway in one process, the shape the
//! services run against) over a fresh directory, with a static identity file.
//! SeaweedFS enforces every condition of the engine's presigned POST policy —
//! the pinned `x-amz-checksum-sha256` against the received bytes, the exact
//! `content-length-range`, key, bucket, `Content-Type`, expiry and signature —
//! and answers `x-amz-checksum-sha256` on a `HEAD` with checksum mode, so the
//! refusals the scenarios assert are the store's own.

use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use service_engine::BlobConfig;
use uuid::Uuid;

const BOOT_TIMEOUT: Duration = Duration::from_secs(60);
const ACCESS_KEY: &str = "e2eaccesskey";
const SECRET_KEY: &str = "e2esecretkey";
const REGION: &str = "us-east-1";

/// The listeners `weed` also serves on a Unix socket under `/tmp`, named after
/// their port (`/tmp/seaweedfs-<role>-<port>.sock`); a killed process leaves
/// them behind.
const SOCKETS: [&str; 6] = [
    "s3",
    "filer",
    "master-grpc",
    "volume-grpc",
    "filer-grpc",
    "s3-grpc",
];

pub struct TestS3 {
    child: Child,
    port: u16,
    master: u16,
    socket_ports: [u16; 6],
    store: PathBuf,
    http: reqwest::Client,
}

impl TestS3 {
    pub async fn spawn() -> Self {
        for _ in 0..5 {
            if let Some(server) = Self::try_spawn().await {
                return server;
            }
        }
        panic!("weed did not come up");
    }

    async fn try_spawn() -> Option<Self> {
        let [port, master, volume, filer] = [free_port(), free_port(), free_port(), free_port()];
        let [master_grpc, volume_grpc, filer_grpc, s3_grpc] =
            [free_port(), free_port(), free_port(), free_port()];
        let name = format!("drive-s3-{}", Uuid::now_v7().simple());
        let store = std::env::temp_dir().join(&name);
        let data = store.join("data");
        std::fs::create_dir_all(&data).expect("create the ephemeral object store");
        let identities = store.join("s3.json");
        std::fs::write(
            &identities,
            serde_json::json!({
                "identities": [{
                    "name": "e2e",
                    "credentials": [{ "accessKey": ACCESS_KEY, "secretKey": SECRET_KEY }],
                    "actions": ["Admin", "Read", "Write", "List", "Tagging"],
                }]
            })
            .to_string(),
        )
        .expect("write the S3 identities");
        let flag = |name: &str, value: u16| format!("-{name}={value}");
        let child = Command::new("weed")
            .arg("server")
            .arg(format!(
                "-dir={}",
                data.to_str().expect("a utf-8 store path")
            ))
            .args(["-ip=127.0.0.1", "-ip.bind=127.0.0.1"])
            .args([
                flag("master.port", master),
                flag("master.port.grpc", master_grpc),
                flag("volume.port", volume),
                flag("volume.port.grpc", volume_grpc),
                flag("filer.port", filer),
                flag("filer.port.grpc", filer_grpc),
            ])
            .args(["-master.volumeSizeLimitMB=64", "-volume.max=64"])
            .args([
                "-s3",
                &flag("s3.port", port),
                &flag("s3.port.grpc", s3_grpc),
            ])
            .arg(format!(
                "-s3.config={}",
                identities.to_str().expect("a utf-8 identities path")
            ))
            .args(["-s3.iam=false", "-s3.port.iceberg=0", "-s3.port.lance=0"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("weed (SeaweedFS) must be on PATH for the blob scenarios");
        let mut server = Self {
            child,
            port,
            master,
            socket_ports: [port, filer, master_grpc, volume_grpc, filer_grpc, s3_grpc],
            store,
            http: reqwest::Client::new(),
        };
        server.await_ready().await.then_some(server)
    }

    pub fn endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    pub fn config(&self, bucket: &str) -> BlobConfig {
        BlobConfig::new(self.endpoint(), REGION, bucket, ACCESS_KEY, SECRET_KEY)
            .with_upload_ttl(Duration::from_secs(6))
            .with_download_ttl(Duration::from_secs(600))
    }

    fn s3_bucket(&self, bucket: &str) -> Bucket {
        Bucket::new(
            self.endpoint().parse().expect("a valid endpoint url"),
            UrlStyle::Path,
            bucket.to_string(),
            REGION.to_string(),
        )
        .expect("a valid bucket")
    }

    pub async fn create_bucket(&self, bucket: &str) {
        let s3 = self.s3_bucket(bucket);
        let credentials = Credentials::new(ACCESS_KEY, SECRET_KEY);
        let deadline = Instant::now() + BOOT_TIMEOUT;
        loop {
            let url = s3.create_bucket(&credentials).sign(Duration::from_secs(60));
            let status = self
                .http
                .put(url)
                .send()
                .await
                .expect("reach the object store to create the bucket")
                .status();
            if status.is_success() || status.as_u16() == 409 {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "creating {bucket} answered {status}"
            );
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
    }

    pub async fn object_exists(&self, bucket: &str, object_key: &str) -> bool {
        let s3 = self.s3_bucket(bucket);
        let credentials = Credentials::new(ACCESS_KEY, SECRET_KEY);
        let url = s3
            .head_object(Some(&credentials), object_key)
            .sign(Duration::from_secs(60));
        self.http
            .head(url)
            .send()
            .await
            .expect("reach the object store to HEAD the object")
            .status()
            .is_success()
    }

    pub async fn put_object(&self, bucket: &str, object_key: &str, bytes: &[u8]) {
        let s3 = self.s3_bucket(bucket);
        let credentials = Credentials::new(ACCESS_KEY, SECRET_KEY);
        let url = s3
            .put_object(Some(&credentials), object_key)
            .sign(Duration::from_secs(60));
        let status = self
            .http
            .put(url)
            .body(bytes.to_vec())
            .send()
            .await
            .expect("reach the object store to PUT the object")
            .status();
        assert!(status.is_success(), "the PUT answered {status}");
    }

    /// Ready once the S3 gateway answers healthy AND the master counts the
    /// volume server: the volume server's first heartbeat races the master's
    /// leader election and is only retried five seconds later, so a gateway
    /// that is up can still have nowhere to write — and an upload in that
    /// window would outlast a scenario's short upload window.
    async fn await_ready(&mut self) -> bool {
        let deadline = Instant::now() + BOOT_TIMEOUT;
        let health = format!("{}/healthz", self.endpoint());
        let topology = format!("http://127.0.0.1:{}/dir/status", self.master);
        loop {
            if self.child.try_wait().expect("read weed state").is_some() {
                return false;
            }
            if self.answers(&health).await && self.has_volume_slots(&topology).await {
                return true;
            }
            if Instant::now() >= deadline {
                panic!("weed never became ready");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn answers(&self, url: &str) -> bool {
        matches!(self.http.get(url).send().await, Ok(response) if response.status().is_success())
    }

    async fn has_volume_slots(&self, url: &str) -> bool {
        let Ok(response) = self.http.get(url).send().await else {
            return false;
        };
        let Ok(status) = response.json::<serde_json::Value>().await else {
            return false;
        };
        status["Topology"]["Max"]
            .as_u64()
            .is_some_and(|max| max > 0)
    }
}

impl Drop for TestS3 {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.store);
        let tmp = std::path::Path::new("/tmp");
        for (socket, port) in SOCKETS.iter().zip(self.socket_ports) {
            let _ = std::fs::remove_file(tmp.join(format!("seaweedfs-{socket}-{port}.sock")));
        }
    }
}

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
    let port = listener.local_addr().expect("read the bound port").port();
    drop(listener);
    port
}
