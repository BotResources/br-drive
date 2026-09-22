use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use service_engine::BlobConfig;
use uuid::Uuid;

const BOOT_TIMEOUT: Duration = Duration::from_secs(30);
const ACCESS_KEY: &str = "minioadmin";
const SECRET_KEY: &str = "minioadmin";
const REGION: &str = "us-east-1";

pub struct TestMinio {
    child: Child,
    port: u16,
    store: PathBuf,
    http: reqwest::Client,
}

impl TestMinio {
    pub async fn spawn() -> Self {
        for _ in 0..5 {
            if let Some(server) = Self::try_spawn().await {
                return server;
            }
        }
        panic!("minio did not come up");
    }

    async fn try_spawn() -> Option<Self> {
        let port = free_port();
        let console = free_port();
        let name = format!("drive-minio-{}", Uuid::now_v7().simple());
        let store = std::env::temp_dir().join(&name);
        std::fs::create_dir_all(&store).expect("create the ephemeral object store");
        let child = Command::new("minio")
            .args([
                "server",
                store.to_str().expect("a utf-8 store path"),
                "--address",
                &format!("127.0.0.1:{port}"),
                "--console-address",
                &format!("127.0.0.1:{console}"),
            ])
            .env("MINIO_ROOT_USER", ACCESS_KEY)
            .env("MINIO_ROOT_PASSWORD", SECRET_KEY)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("minio must be on PATH for the blob scenarios");
        let mut server = Self {
            child,
            port,
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
                .expect("reach minio to create the bucket")
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
            .expect("reach minio to HEAD the object")
            .status()
            .is_success()
    }

    async fn await_ready(&mut self) -> bool {
        let deadline = Instant::now() + BOOT_TIMEOUT;
        let health = format!("{}/minio/health/ready", self.endpoint());
        loop {
            if self.child.try_wait().expect("read minio state").is_some() {
                return false;
            }
            if let Ok(response) = self.http.get(&health).send().await
                && response.status().is_success()
            {
                return true;
            }
            if Instant::now() >= deadline {
                panic!("minio never became ready");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

impl Drop for TestMinio {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.store);
    }
}

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
    let port = listener.local_addr().expect("read the bound port").port();
    drop(listener);
    port
}
