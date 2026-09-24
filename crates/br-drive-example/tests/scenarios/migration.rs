//! The one-way migration from 0.1's stored processing state to the job log.
//! The only place a starting state is built in SQL: that state is the old
//! schema, which no gesture of this version can produce.

use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

use crate::harness::pg::admin_url;

const JOB_LOG_MIGRATION: i64 = 9_121_000_007;

/// A 0.1 file row: name, stored state, stored error, job, step.
type OldFile<'a> = (&'a str, &'a str, Option<&'a str>, Option<Uuid>, Option<i32>);

async fn apply(pool: &sqlx::PgPool, versions: impl Fn(i64) -> bool) {
    for migration in br_drive::migrations().migrator.iter() {
        if versions(migration.version) {
            sqlx::raw_sql(&migration.sql)
                .execute(pool)
                .await
                .unwrap_or_else(|e| panic!("migration {}: {e}", migration.version));
        }
    }
}

#[tokio::test]
async fn the_job_log_migration_keeps_every_state_and_error_of_a_0_1_database() {
    // Given: a database on 0.1's schema (and the title), with one file in each state
    let admin = PgPoolOptions::new()
        .max_connections(2)
        .connect(&admin_url())
        .await
        .expect("connect as the admin");
    let database = format!("drv_{}_db", Uuid::now_v7().simple());
    sqlx::query(&format!("CREATE DATABASE \"{database}\""))
        .execute(&admin)
        .await
        .expect("create the database");
    let url = format!(
        "{}/{database}",
        admin_url()
            .rsplit_once('/')
            .expect("a database in the url")
            .0
    );
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("connect to the database");
    apply(&pool, |version| version < JOB_LOG_MIGRATION).await;
    let drive = Uuid::now_v7();
    sqlx::query("INSERT INTO drive.drive (id, created_by, created_at) VALUES ($1, $2, now())")
        .bind(drive)
        .bind(Uuid::now_v7())
        .execute(&pool)
        .await
        .unwrap();
    let in_flight_job = Uuid::now_v7();
    let initiator = serde_json::json!({ "id": Uuid::now_v7(), "display_name": "Ada" });
    let rows: [OldFile<'_>; 5] = [
        ("pending.txt", "pending", None, None, None),
        ("ready.txt", "ready", None, None, None),
        (
            "running.txt",
            "processing",
            None,
            Some(in_flight_job),
            Some(1),
        ),
        ("orphan.txt", "processing", None, None, Some(0)),
        ("failed.txt", "failed", Some("timed_out"), None, None),
    ];
    let mut ids = Vec::new();
    for (name, state, error, job, step) in rows {
        let id = Uuid::now_v7();
        ids.push((name, id));
        sqlx::query(
            "INSERT INTO drive.file (id, drive_id, path, name, title, media_type, size_bytes, \
             sha256, blob_ref, processing_state, processing_error, job_id, step_index, \
             triggered_by, created_by, created_at, updated_at) \
             VALUES ($1, $2, '', $3, $3, 'text/plain', 1, $4, $5, $6, $7, $8, $9, $10, $11, \
             now(), now())",
        )
        .bind(id)
        .bind(drive)
        .bind(name)
        .bind(vec![0u8; 32])
        .bind(Uuid::now_v7())
        .bind(state)
        .bind(error)
        .bind(job)
        .bind(step)
        .bind(job.map(|_| initiator.clone()))
        .bind(Uuid::now_v7())
        .execute(&pool)
        .await
        .unwrap_or_else(|e| panic!("seed {name}: {e}"));
    }

    // When: the job log migration runs
    apply(&pool, |version| version == JOB_LOG_MIGRATION).await;

    // Then: every file reads the state and the error it had, from the view
    let status = |name: &str| {
        let id = ids.iter().find(|(n, _)| *n == name).unwrap().1;
        let pool = pool.clone();
        async move {
            sqlx::query_as::<_, (String, Option<String>, Option<Uuid>, Option<i32>)>(
                "SELECT processing_state, processing_error, last_job_id, last_job_step \
                 FROM drive.file_status WHERE file_id = $1",
            )
            .bind(id)
            .fetch_one(&pool)
            .await
            .expect("the file's status")
        }
    };
    assert_eq!(
        status("pending.txt").await,
        ("pending".into(), None, None, None)
    );
    assert_eq!(
        status("ready.txt").await,
        ("ready".into(), None, None, None)
    );
    assert_eq!(
        status("running.txt").await,
        ("processing".into(), None, Some(in_flight_job), Some(1)),
        "a job in flight becomes the file's log, and its next fact lands in it"
    );
    let orphan = status("orphan.txt").await;
    assert_eq!(
        (orphan.0.as_str(), orphan.1.as_deref()),
        ("failed", Some("interrupted"))
    );
    let failed = status("failed.txt").await;
    assert_eq!(
        (failed.0.as_str(), failed.1.as_deref()),
        ("failed", Some("timed_out"))
    );
    let carried: serde_json::Value =
        sqlx::query_scalar("SELECT triggered_by FROM drive.file_job WHERE job_id = $1")
            .bind(in_flight_job)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(carried, initiator, "the initiator moves with the job");
    let committed: Vec<(String, bool)> =
        sqlx::query_as("SELECT name, committed_at IS NOT NULL FROM drive.file ORDER BY name")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        committed,
        vec![
            ("failed.txt".into(), true),
            ("orphan.txt".into(), true),
            ("pending.txt".into(), false),
            ("ready.txt".into(), true),
            ("running.txt".into(), true),
        ]
    );
    let gone: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'drive' \
         AND table_name = 'file' AND column_name IN ('processing_state', 'job_id', 'triggered_by')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(gone, 0, "the stored state is gone");

    pool.close().await;
    let _ = sqlx::query(&format!(
        "DROP DATABASE IF EXISTS \"{database}\" WITH (FORCE)"
    ))
    .execute(&admin)
    .await;
}
