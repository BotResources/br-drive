//! The upgrade of a 0.6 database to 0.7.0, whose chains act under a stored
//! Passport: a chain running at the upgrade has none, so its next step is
//! never sent and the file ends `FAILED` `passport_missing`. The Given is
//! built in SQL — a processing 0.6's binary started, which no gesture of this
//! version can produce; everything after the upgrade is observed through the
//! booted host.

use std::time::Duration;

use chrono::{SubsecRound, TimeDelta, Utc};
use futures_util::FutureExt;
use uuid::Uuid;

use crate::harness::pg::TestDb;
use crate::harness::runner::{RUNNER_SCOPE, Report, report};
use crate::harness::{JobsStandIn, World, WorldOptions, ok, passport, service_passport};

/// The last drive migration released in v0.6.5.
const RELEASED_0_6: i64 = 9_121_000_012;

#[tokio::test]
async fn a_chain_running_at_the_0_7_upgrade_ends_failed_passport_missing_at_its_next_step() {
    // Given: a database exactly as 0.6.5 left it — the drive migrations that
    // release shipped, 01 to 12 — cleaned up whatever happens
    let db = TestDb::at_drive_version(RELEASED_0_6).await;
    let database = db.database.clone();
    let admin = db.admin.clone();
    let owner_role = db.owner_role.clone();
    let app_role = db.app_role.clone();
    let outcome = std::panic::AssertUnwindSafe(upgrade_and_run(db))
        .catch_unwind()
        .await;
    let _ = sqlx::query(&format!(
        "DROP DATABASE IF EXISTS \"{database}\" WITH (FORCE)"
    ))
    .execute(&admin)
    .await;
    for role in [app_role, owner_role] {
        let _ = sqlx::query(&format!("DROP ROLE IF EXISTS \"{role}\""))
            .execute(&admin)
            .await;
    }
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

/// A committed file of `workspace` with a two-step chain pinned, and its
/// processing as 0.6 wrote it: `state` on `job_id`, step `step_index`.
async fn seed_file(
    pool: &sqlx::PgPool,
    workspace: Uuid,
    owner: Uuid,
    name: &str,
    state: &str,
    job_id: Uuid,
    at: chrono::DateTime<Utc>,
) -> Uuid {
    let id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO drive.file (id, drive_id, path, name, title, media_type, size_bytes, \
         sha256, blob_ref, steps, committed_at, created_by, created_at, updated_at, version) \
         VALUES ($1, $2, '', $3, $3, 'text/plain', 1, $4, $5, $6, $7, $8, $7, $7, 1)",
    )
    .bind(id)
    .bind(workspace)
    .bind(name)
    .bind(vec![0u8; 32])
    .bind(Uuid::now_v7())
    .bind(serde_json::json!([
        { "runner_type": "render", "options": {} },
        { "runner_type": "index", "options": {} },
    ]))
    .bind(at)
    .bind(owner)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("seed {name}: {e}"));
    sqlx::query(
        "INSERT INTO drive.file_processing (file_id, version, state, job_id, step_index, \
         trigger, triggered_by_id, triggered_by_name, job_created_at, updated_at) \
         VALUES ($1, 1, $2, $3, $4, 'upload', $5, 'Ada', $6, $6)",
    )
    .bind(id)
    .bind(state)
    .bind(job_id)
    .bind(if state == "processing" { 0 } else { 1 })
    .bind(owner)
    .bind(at)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("seed the processing of {name}: {e}"));
    id
}

async fn upgrade_and_run(db: TestDb) {
    let owner_id = Uuid::now_v7();
    let owner = passport(owner_id);
    let workspace = Uuid::now_v7();
    let t0 = (Utc::now() - TimeDelta::minutes(10)).trunc_subsecs(6);
    let pool = db.owner().await;
    sqlx::query("INSERT INTO workspace (id, owner_id, name) VALUES ($1, $2, 'upgraded')")
        .bind(workspace)
        .bind(owner_id)
        .execute(&pool)
        .await
        .expect("seed the host object");
    sqlx::query("INSERT INTO drive.drive (id, created_by, created_at) VALUES ($1, $2, $3)")
        .bind(workspace)
        .bind(owner_id)
        .bind(t0)
        .execute(&pool)
        .await
        .expect("seed the drive");
    // And: a two-step chain 0.6 started, its first step running; and a file
    // whose chain 0.6 finished
    let first = Uuid::now_v7();
    let running = seed_file(
        &pool,
        workspace,
        owner_id,
        "running.txt",
        "processing",
        first,
        t0,
    )
    .await;
    let done = seed_file(
        &pool,
        workspace,
        owner_id,
        "done.txt",
        "ready",
        Uuid::now_v7(),
        t0,
    )
    .await;

    // When: the host's upgrade applies this version's migrations
    db.migrate(br_drive_example::db::libraries()).await;

    // Then: the check passes on the running chain's row, which holds no
    // Passport — and it holds for every write after: a settled processing
    // cannot hold one
    let held: Option<serde_json::Value> =
        sqlx::query_scalar("SELECT passport FROM drive.file_processing WHERE file_id = $1")
            .bind(running)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(held, None, "a chain started by 0.6 has no stored passport");
    let refused =
        sqlx::query("UPDATE drive.file_processing SET passport = '{}'::jsonb WHERE file_id = $1")
            .bind(done)
            .execute(&pool)
            .await
            .expect_err("a settled processing never holds a passport");
    assert_eq!(
        refused
            .as_database_error()
            .and_then(|error| error.constraint()),
        Some("file_processing_passport_only_while_processing"),
        "{refused}"
    );
    pool.close().await;

    // When: the host boots on the upgraded database, and the running step's
    // runner sends its final report
    let world = World::start_on(db, "pod-upgraded-0-7", WorldOptions::default()).await;
    let jobs = JobsStandIn::attach(&world).await;
    let runner = service_passport(&[RUNNER_SCOPE]);
    assert_eq!(
        world.file(&owner, running).await["processingState"],
        "PROCESSING"
    );
    ok(&report(
        &world,
        &runner,
        running,
        Report {
            job_id: first,
            pages: vec![(1, "rendered before the upgrade")],
            origin: None,
            indexer: None,
            done: true,
        },
    )
    .await);

    // Then: the step is done — Jobs is told — but the next one is never asked
    // of Jobs without a passport: the file is FAILED `passport_missing`,
    // visibly, and the chain's stop is a fact
    let file = world.await_state(&owner, running, "FAILED").await;
    assert_eq!(file["processingError"], br_drive::PASSPORT_MISSING);
    assert_eq!(jobs.await_finish(first).await.job_id, first);
    jobs.expect_no_command(Duration::from_secs(1)).await;
    let abandoned = world
        .processing_facts(running)
        .await
        .into_iter()
        .find(|fact| fact.event_type == "StepAbandoned")
        .expect("the chain stopped as StepAbandoned");
    assert_eq!(abandoned.payload["step_index"], 1);
    assert_eq!(abandoned.payload["runner_type"], "index");
    assert_eq!(abandoned.payload["after_job_id"], first.to_string());
    assert_eq!(abandoned.payload["reason_code"], br_drive::PASSPORT_MISSING);
    assert_eq!(world.held_passport(running).await, None);
    // And: the settled file reads as before
    assert_eq!(world.file(&owner, done).await["processingState"], "READY");

    world.service.shutdown().await;
}
