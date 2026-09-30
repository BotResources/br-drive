//! The one-way upgrade of a 0.1.0 database. The only scenario whose Given is
//! built in SQL: that state is the 0.1 schema, which no gesture of this
//! version can produce. Everything after the upgrade is observed the way a
//! user and a runner observe it — the host booted on the upgraded database.

use std::time::Duration;

use futures_util::FutureExt;
use uuid::Uuid;

use crate::harness::pg::TestDb;
use crate::harness::runner::{RUNNER_SCOPE, Report, context, report};
use crate::harness::{
    JobsStandIn, World, WorldOptions, error_code, ok, passport, service_passport,
};

/// The last drive migration released in v0.1.0.
const RELEASED_0_1: i64 = 9_121_000_005;

/// A 0.1 file: its name and its stored processing columns.
struct OldFile {
    name: &'static str,
    state: &'static str,
    error: Option<&'static str>,
    job: Option<Uuid>,
    step: Option<i32>,
    done: bool,
    completed: bool,
}

impl OldFile {
    const fn settled(name: &'static str, state: &'static str, error: Option<&'static str>) -> Self {
        Self {
            name,
            state,
            error,
            job: None,
            step: None,
            done: false,
            completed: false,
        }
    }

    const fn running(
        name: &'static str,
        job: Uuid,
        step: i32,
        done: bool,
        completed: bool,
    ) -> Self {
        Self {
            name,
            state: "processing",
            error: None,
            job: Some(job),
            step: Some(step),
            done,
            completed,
        }
    }
}

async fn seed(
    owner_pool: &sqlx::PgPool,
    owner: Uuid,
    workspace: Uuid,
    files: &[OldFile],
) -> Vec<(&'static str, Uuid)> {
    sqlx::query("INSERT INTO workspace (id, owner_id, name) VALUES ($1, $2, 'upgraded')")
        .bind(workspace)
        .bind(owner)
        .execute(owner_pool)
        .await
        .expect("seed the host object");
    sqlx::query("INSERT INTO drive.drive (id, created_by, created_at) VALUES ($1, $2, now())")
        .bind(workspace)
        .bind(owner)
        .execute(owner_pool)
        .await
        .expect("seed the drive");
    let steps = serde_json::json!([
        { "runner_type": "render", "options": {} },
        { "runner_type": "index", "options": {} },
    ]);
    let initiator = serde_json::json!({ "id": owner, "display_name": "Ada" });
    let mut ids = Vec::new();
    for file in files {
        let id = Uuid::now_v7();
        ids.push((file.name, id));
        sqlx::query(
            "INSERT INTO drive.file (id, drive_id, path, name, media_type, size_bytes, sha256, \
             blob_ref, processing_state, processing_error, steps, job_id, step_index, \
             step_count, triggered_by, done_at, completed_at, created_by, created_at, updated_at) \
             VALUES ($1, $2, '', $3, 'text/plain', 1, $4, $5, $6, $7, $8, $9, $10, $11, $12, \
             CASE WHEN $13 THEN now() END, CASE WHEN $14 THEN now() END, $15, now(), now())",
        )
        .bind(id)
        .bind(workspace)
        .bind(file.name)
        .bind(vec![0u8; 32])
        .bind(Uuid::now_v7())
        .bind(file.state)
        .bind(file.error)
        .bind(file.job.map(|_| steps.clone()))
        .bind(file.job)
        .bind(file.step)
        .bind(file.job.map(|_| 2))
        .bind(file.job.map(|_| initiator.clone()))
        .bind(file.done)
        .bind(file.completed)
        .bind(owner)
        .execute(owner_pool)
        .await
        .unwrap_or_else(|e| panic!("seed {}: {e}", file.name));
    }
    ids
}

async fn drive_indexes(pool: &sqlx::PgPool) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT indexname::text FROM pg_indexes WHERE schemaname = 'drive' \
         AND tablename IN ('file', 'file_processing', 'file_processed', 'file_page', 'file_image') \
         ORDER BY indexname",
    )
    .fetch_all(pool)
    .await
    .expect("list the drive indexes")
}

#[tokio::test]
async fn a_0_1_database_upgrades_and_its_running_job_ends_through_the_booted_host() {
    // Given: a database exactly as 0.1.0 left it — only the drive migrations
    // that release shipped — with one file in every state 0.1 could store
    let db = TestDb::at_drive_version(RELEASED_0_1).await;
    let database = db.database.clone();
    let admin = db.admin.clone();
    let owner_role = db.owner_role.clone();
    let app_role = db.app_role.clone();
    let outcome = std::panic::AssertUnwindSafe(upgrade_and_drive(db))
        .catch_unwind()
        .await;
    // Cleaned up whatever happened: the database and both roles go.
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

async fn upgrade_and_drive(db: TestDb) {
    let owner_id = Uuid::now_v7();
    let owner = passport(owner_id);
    let workspace = Uuid::now_v7();
    let running_job = Uuid::now_v7();
    let reported_job = Uuid::now_v7();
    let interrupted_job = Uuid::now_v7();
    let completed_job = Uuid::now_v7();
    let owner_pool = db.owner().await;
    let before = drive_indexes(&owner_pool).await;
    assert!(
        before.contains(&"file_job_idx".to_string()),
        "0.1's job index: {before:?}"
    );
    let ids = seed(
        &owner_pool,
        owner_id,
        workspace,
        &[
            OldFile::settled("pending.txt", "pending", None),
            OldFile::settled("ready.txt", "ready", None),
            OldFile::settled("failed.txt", "failed", Some("runner_type_unavailable")),
            // The last step runs; no final report yet.
            OldFile::running("running.txt", running_job, 1, false, false),
            // The last step's final report received, Jobs' completion not yet.
            OldFile::running("reported.txt", reported_job, 1, true, false),
            // The first step's final report received: the next step was never asked.
            OldFile::running("interrupted.txt", interrupted_job, 0, true, false),
            // Jobs' completion without the final report (unreachable with the real Jobs).
            OldFile::running("completed.txt", completed_job, 1, false, true),
        ],
    )
    .await;
    let id = |name: &str| {
        ids.iter()
            .find(|(n, _)| *n == name)
            .expect("a seeded file")
            .1
    };
    // The ready file was last changed an hour after its creation.
    let last_change: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(
        "UPDATE drive.file SET updated_at = created_at + interval '1 hour' \
         WHERE id = $1 RETURNING updated_at",
    )
    .bind(id("ready.txt"))
    .fetch_one(&owner_pool)
    .await
    .expect("seed a last change");
    // The ready file carries 0.1's results: an indexing, a page, an image.
    sqlx::query(
        "UPDATE drive.file SET summary = 'Old summary.', page_count = 1, estimated_tokens = 5 \
         WHERE id = $1",
    )
    .bind(id("ready.txt"))
    .execute(&owner_pool)
    .await
    .expect("seed the indexing");
    sqlx::query(
        "INSERT INTO drive.file_page (file_id, number, markdown, origin, updated_by, updated_at) \
         VALUES ($1, 1, 'old page ![i](p001-img01.png)', 'runner', $2, now())",
    )
    .bind(id("ready.txt"))
    .bind(owner_id)
    .execute(&owner_pool)
    .await
    .expect("seed a page");
    sqlx::query(
        "INSERT INTO drive.file_image (file_id, name, page, blob_ref, media_type, size_bytes, \
         sha256, landed_at, requested_at) \
         VALUES ($1, 'p001-img01.png', 1, $2, 'image/png', 3, $3, now(), now())",
    )
    .bind(id("ready.txt"))
    .bind(Uuid::now_v7())
    .bind(vec![0u8; 32])
    .execute(&owner_pool)
    .await
    .expect("seed an image");

    // When: the host's upgrade applies every migration of this version
    db.migrate(br_drive_example::db::libraries()).await;

    // Then: the indexes changed as the migrations say — 0.1's per-file job
    // index went with its column, the processing and the results have theirs
    let after = drive_indexes(&owner_pool).await;
    assert!(
        !after.contains(&"file_job_idx".to_string()),
        "0.1's job index is gone: {after:?}"
    );
    for kept in [
        "file_drive_idx",
        "file_processing_job_id_key",
        "file_processing_pkey",
        "file_processed_pkey",
        "file_page_pkey",
        "file_image_pkey",
    ] {
        assert!(
            after.contains(&kept.to_string()),
            "{kept} exists: {after:?}"
        );
    }
    owner_pool.close().await;

    // When: the host boots on the upgraded database
    let world = World::start_on(db, "pod-upgraded", WorldOptions::default()).await;
    let jobs = JobsStandIn::attach(&world).await;
    let runner = service_passport(&[RUNNER_SCOPE]);

    // Then: every file reads the state 0.1 stored, through the host
    let state = |name: &'static str| {
        let world = &world;
        let owner = owner.clone();
        async move {
            let file = world.file(&owner, id(name)).await;
            (
                file["processingState"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
                file["processingError"].as_str().map(str::to_string),
            )
        }
    };
    let expect = |state: &str, error: Option<&str>| (state.to_string(), error.map(str::to_string));
    assert_eq!(state("pending.txt").await, expect("PENDING", None));
    assert_eq!(state("ready.txt").await, expect("READY", None));
    assert_eq!(
        state("failed.txt").await,
        expect("FAILED", Some("runner_type_unavailable"))
    );
    assert_eq!(state("running.txt").await, expect("PROCESSING", None));
    assert_eq!(
        state("reported.txt").await,
        expect("READY", None),
        "a final report received by 0.1 ended its last step"
    );
    assert_eq!(
        state("interrupted.txt").await,
        expect("FAILED", Some("interrupted")),
        "the next step was never asked of Jobs: the chain is interrupted, open to a reprocess"
    );
    assert_eq!(
        state("completed.txt").await,
        expect("FAILED", Some("interrupted"))
    );
    let interrupted = world.file(&owner, id("interrupted.txt")).await;
    assert_eq!(interrupted["affordances"]["process"]["allowed"], true);
    // And: the results moved with their file
    let ready = world.file(&owner, id("ready.txt")).await;
    assert_eq!(ready["summary"], "Old summary.");
    assert_eq!(ready["pageCount"], 1);
    assert_eq!(ready["estimatedTokens"], 5);
    assert_eq!(ready["images"][0]["name"], "p001-img01.png");
    let pages = world.file_pages(&owner, id("ready.txt")).await;
    assert_eq!(pages.len(), 1);
    assert_eq!(pages[0]["markdown"], "old page ![i](p001-img01.png)");
    // And: the last changes carried over: the page's writer, and the file's
    // last change, read as before
    assert_eq!(pages[0]["updatedBy"], owner_id.to_string());
    let carried: chrono::DateTime<chrono::Utc> =
        serde_json::from_value(ready["updatedAt"].clone()).unwrap();
    assert_eq!(
        carried.timestamp_micros(),
        last_change.timestamp_micros(),
        "the file's last change survives the upgrade"
    );
    // And: the host object counts its drive's files through their processing
    let counted = world
        .gql(
            &owner,
            "query($id:UUID!){workspaceWorkspace(id:$id){fileCount readyFileCount}}",
            serde_json::json!({ "id": workspace }),
        )
        .await;
    assert_eq!(
        ok(&counted)["workspaceWorkspace"],
        serde_json::json!({ "fileCount": 7, "readyFileCount": 2 })
    );

    // When: the runner of the job 0.1 left running sends its final report
    let running = id("running.txt");
    assert_eq!(
        error_code(&context(&world, &runner, running, completed_job).await),
        "JOB_NOT_ACTIVE",
        "another file's job never opens this one"
    );
    ok(&report(
        &world,
        &runner,
        running,
        Report {
            job_id: running_job,
            pages: vec![(1, "finished after the upgrade")],
            origin: None,
            indexer: Some(("Upgraded.", 1, 3)),
            done: true,
        },
    )
    .await);

    // Then: it lands on the backfilled job and ends the chain — it was the
    // last step — and Jobs is told
    let file = world.await_state(&owner, running, "READY").await;
    assert_eq!(file["summary"], "Upgraded.");
    assert_eq!(file["steps"].as_array().map(Vec::len), Some(2));
    assert_eq!(jobs.await_finish(running_job).await.job_id, running_job);
    assert_eq!(
        world.job_end(running_job).await,
        Some(("reported_done".to_string(), None)),
        "the runner's end is recorded on the job 0.1 was running"
    );
    jobs.expect_no_command(Duration::from_millis(500)).await;

    world.service.shutdown().await;
}
