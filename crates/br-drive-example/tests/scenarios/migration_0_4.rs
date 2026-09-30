//! The upgrade of a 0.4.0 database: its job fact tables and its `drive.fact`
//! give way to the state tables. The Given is built in SQL — that state is
//! 0.4.0's schema, which no gesture of this version can produce; everything
//! after the upgrade is observed through the booted host.

use chrono::{DateTime, SubsecRound, TimeDelta, Utc};
use futures_util::FutureExt;
use uuid::Uuid;

use crate::harness::pg::TestDb;
use crate::harness::runner::{RUNNER_SCOPE, Report, report};
use crate::harness::{JobsStandIn, World, WorldOptions, ok, passport, service_passport};

/// The last drive migration released in v0.4.0.
const RELEASED_0_4: i64 = 9_121_000_010;

struct Seeded {
    pool: sqlx::PgPool,
    workspace: Uuid,
    owner: Uuid,
    t0: DateTime<Utc>,
}

impl Seeded {
    fn t(&self, seconds: i64) -> DateTime<Utc> {
        self.t0 + TimeDelta::seconds(seconds)
    }

    async fn file(&self, name: &str, committed: bool, steps: &[&str]) -> Uuid {
        let id = Uuid::now_v7();
        let steps: Vec<serde_json::Value> = steps
            .iter()
            .map(|runner| serde_json::json!({ "runner_type": runner, "options": {} }))
            .collect();
        sqlx::query(
            "INSERT INTO drive.file (id, drive_id, path, name, title, media_type, size_bytes, \
             sha256, blob_ref, steps, committed_at, created_by, created_at) \
             VALUES ($1, $2, '', $3, $3, 'text/plain', 1, $4, $5, $6, \
             CASE WHEN $7 THEN $9 END, $8, $9)",
        )
        .bind(id)
        .bind(self.workspace)
        .bind(name)
        .bind(vec![0u8; 32])
        .bind(Uuid::now_v7())
        .bind(serde_json::Value::Array(steps))
        .bind(committed)
        .bind(self.owner)
        .bind(self.t0)
        .execute(&self.pool)
        .await
        .unwrap_or_else(|e| panic!("seed {name}: {e}"));
        id
    }

    /// A 0.4.0 job of `file`, its next number, created at `at`.
    async fn job(&self, file: Uuid, number: i32, step: i32, at: i64) -> Uuid {
        let job = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO drive.file_job (file_id, number, job_id, step_index, trigger, \
             triggered_by_id, triggered_by_name, created_at) \
             VALUES ($1, $2, $3, $4, 'upload', $5, 'Ada', $6)",
        )
        .bind(file)
        .bind(number)
        .bind(job)
        .bind(step)
        .bind(self.owner)
        .bind(self.t(at))
        .execute(&self.pool)
        .await
        .expect("seed a 0.4.0 job");
        job
    }

    async fn end(
        &self,
        job: Uuid,
        kind: &str,
        reason: Option<&str>,
        message: Option<&str>,
        at: i64,
    ) {
        sqlx::query(
            "INSERT INTO drive.file_job_end (job_id, kind, reason_code, message, at) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(job)
        .bind(kind)
        .bind(reason)
        .bind(message)
        .bind(self.t(at))
        .execute(&self.pool)
        .await
        .expect("seed a 0.4.0 job end");
    }

    /// A 0.4.0 fact of the file, page, label or rule `id`.
    async fn fact(
        &self,
        aggregate: &str,
        id: Uuid,
        page: Option<i32>,
        kind: &str,
        by: Uuid,
        at: i64,
    ) {
        sqlx::query(
            "INSERT INTO drive.fact (id, aggregate_type, aggregate_id, page_number, \
             correlation_id, fact_type, payload, actor_id, actor_kind, impersonator_id, \
             occurred_at) VALUES ($1, $2, $3, $4, $1, $5, jsonb_build_object('kind', $5::text), \
             $6, 'human', NULL, $7)",
        )
        .bind(Uuid::now_v7())
        .bind(aggregate)
        .bind(id)
        .bind(page)
        .bind(kind)
        .bind(by)
        .bind(self.t(at))
        .execute(&self.pool)
        .await
        .expect("seed a 0.4.0 fact");
    }
}

#[tokio::test]
async fn a_0_4_database_upgrades_to_the_state_tables_and_reads_as_before() {
    // Given: a database exactly as 0.4.0 left it — the drive migrations that
    // release shipped, 01 to 10 — cleaned up whatever happens
    let db = TestDb::at_drive_version(RELEASED_0_4).await;
    let database = db.database.clone();
    let admin = db.admin.clone();
    let owner_role = db.owner_role.clone();
    let app_role = db.app_role.clone();
    let outcome = std::panic::AssertUnwindSafe(upgrade_and_drive(db))
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

async fn upgrade_and_drive(db: TestDb) {
    let owner_id = Uuid::now_v7();
    let owner = passport(owner_id);
    let runner_id = Uuid::now_v7();
    let workspace = Uuid::now_v7();
    // At the database's precision (microseconds), so an instant read back
    // compares equal on any clock.
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
    let seeded = Seeded {
        pool,
        workspace,
        owner: owner_id,
        t0,
    };

    // And: one file in each shape 0.4.0 could hold, each with its last change
    let pending = seeded.file("pending.txt", false, &[]).await;
    let stored = seeded.file("stored.txt", true, &[]).await;
    seeded
        .fact("file", stored, None, "UploadCommitted", owner_id, 1)
        .await;
    // A two-step chain: its first step reported done, its second one runs,
    // with a plan, a step started and a cancel asked.
    let running = seeded.file("running.txt", true, &["render", "index"]).await;
    let first = seeded.job(running, 1, 0, 2).await;
    seeded.end(first, "reported_done", None, None, 3).await;
    let running_job = seeded.job(running, 2, 1, 3).await;
    let run = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO drive.file_job_plan (job_id, number, run_id, labels, declared_at) \
         VALUES ($1, 1, $2, ARRAY['read', 'write'], $3)",
    )
    .bind(running_job)
    .bind(run)
    .bind(seeded.t(4))
    .execute(&seeded.pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO drive.file_job_step (job_id, run_id, plan_index, label, started_at) \
         VALUES ($1, $2, 1, 'write', $3)",
    )
    .bind(running_job)
    .bind(run)
    .bind(seeded.t(5))
    .execute(&seeded.pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO drive.file_job_cancel (job_id, number, requested_by, at) \
         VALUES ($1, 1, $2, $3)",
    )
    .bind(running_job)
    .bind(owner_id)
    .bind(seeded.t(6))
    .execute(&seeded.pool)
    .await
    .unwrap();
    seeded
        .fact("file", running, None, "CancelRequested", owner_id, 6)
        .await;
    let done = seeded.file("done.txt", true, &["render"]).await;
    let done_job = seeded.job(done, 1, 0, 7).await;
    seeded.end(done_job, "reported_done", None, None, 8).await;
    seeded
        .fact("file", done, None, "ProcessingFinished", runner_id, 8)
        .await;
    sqlx::query("INSERT INTO drive.file_processed (file_id, page_count) VALUES ($1, 1)")
        .bind(done)
        .execute(&seeded.pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO drive.file_page (file_id, number, markdown, origin) \
         VALUES ($1, 1, 'kept', 'runner')",
    )
    .bind(done)
    .execute(&seeded.pool)
    .await
    .unwrap();
    seeded
        .fact("page", done, Some(1), "Reported", runner_id, 8)
        .await;
    let failed = seeded.file("failed.txt", true, &["render"]).await;
    let failed_job = seeded.job(failed, 1, 0, 9).await;
    seeded
        .end(
            failed_job,
            "reported_failed",
            Some("unreadable_scan"),
            Some("page 3 is blank"),
            10,
        )
        .await;
    seeded
        .fact("file", failed, None, "ProcessingFailed", runner_id, 10)
        .await;
    let cancelled = seeded.file("cancelled.txt", true, &["render"]).await;
    let cancelled_job = seeded.job(cancelled, 1, 0, 11).await;
    seeded.end(cancelled_job, "cancelled", None, None, 12).await;
    // A chain a cancel stopped before its second step.
    let stopped = seeded.file("stopped.txt", true, &["render", "index"]).await;
    let stopped_first = seeded.job(stopped, 1, 0, 13).await;
    seeded
        .end(stopped_first, "reported_done", None, None, 14)
        .await;
    let never = seeded.job(stopped, 2, 1, 14).await;
    seeded.end(never, "cancelled", None, None, 14).await;
    let label = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO drive.label (id, name, color, created_by, created_at) \
         VALUES ($1, 'Kept', '#112233', $2, $3)",
    )
    .bind(label)
    .bind(owner_id)
    .bind(t0)
    .execute(&seeded.pool)
    .await
    .unwrap();
    seeded
        .fact("label", label, None, "Updated", owner_id, 15)
        .await;
    let pool = seeded.pool.clone();

    // When: the host's upgrade applies this version's migrations
    db.migrate(br_drive_example::db::libraries()).await;

    // Then: the job fact tables, their view and the fact table are gone
    let left: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'drive' \
         AND table_name IN ('fact', 'file_job', 'file_job_end', 'file_job_cancel', \
           'file_job_plan', 'file_job_step', 'file_status')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(left, 0, "no library fact table survives");
    let versions: Vec<i64> =
        sqlx::query_scalar("SELECT DISTINCT version FROM drive.file_processing")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(versions, vec![1], "every processing starts at version 1");
    let earlier: Vec<Uuid> =
        sqlx::query_scalar("SELECT past_job_ids FROM drive.file_processing WHERE file_id = $1")
            .bind(stopped)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(earlier, vec![stopped_first], "the earlier jobs are known");
    pool.close().await;

    // When: the host boots on the upgraded database
    let world = World::start_on(db, "pod-upgraded-0-4", WorldOptions::default()).await;
    let jobs = JobsStandIn::attach(&world).await;
    let runner = service_passport(&[RUNNER_SCOPE]);

    // Then: every file reads its state, its error and its last change as
    // 0.4.0 showed them
    let t = |seconds: i64| t0 + TimeDelta::seconds(seconds);
    for (file, state, error, last) in [
        (pending, "PENDING", None, t0),
        (stored, "READY", None, t(1)),
        (running, "PROCESSING", None, t(6)),
        (done, "READY", None, t(8)),
        (failed, "FAILED", Some("unreadable_scan"), t(10)),
        (cancelled, "FAILED", Some("cancelled"), t0),
        (stopped, "FAILED", Some("cancelled"), t0),
    ] {
        let read = world.file(&owner, file).await;
        assert_eq!(read["processingState"], state, "{read}");
        assert_eq!(read["processingError"].as_str(), error, "{read}");
        let updated: DateTime<Utc> = serde_json::from_value(read["updatedAt"].clone()).unwrap();
        assert_eq!(
            updated.timestamp_micros(),
            last.timestamp_micros(),
            "the last change survives the upgrade: {read}"
        );
    }
    // And: the running file shows where its runner is, and its cancel request
    let shown = world.file(&owner, running).await;
    assert_eq!(shown["progress"]["stepIndex"], 1);
    assert_eq!(shown["progress"]["stepCount"], 2);
    assert_eq!(shown["progress"]["runnerType"], "index");
    assert_eq!(
        shown["progress"]["plan"],
        serde_json::json!(["read", "write"])
    );
    assert_eq!(shown["progress"]["currentIndex"], 1);
    assert_eq!(shown["progress"]["currentLabel"], "write");
    let at: DateTime<Utc> = serde_json::from_value(shown["progress"]["at"].clone()).unwrap();
    assert_eq!(at.timestamp_micros(), t(5).timestamp_micros());
    for settled in [done, failed, cancelled] {
        assert!(world.file(&owner, settled).await["progress"].is_null());
    }
    // And: the page keeps its last writer and instant
    let pages = world.file_pages(&owner, done).await;
    assert_eq!(pages[0]["updatedBy"], runner_id.to_string());
    // And: the label its last change
    let labels = world
        .gql(
            &owner,
            "query{workspaceLabels{id updatedAt}}",
            serde_json::json!({}),
        )
        .await;
    let updated: DateTime<Utc> =
        serde_json::from_value(ok(&labels)["workspaceLabels"][0]["updatedAt"].clone()).unwrap();
    assert_eq!(updated.timestamp_micros(), t(15).timestamp_micros());
    // And: the counts come through the processing
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

    // When: the runner of the running job sends its final report
    ok(&report(
        &world,
        &runner,
        running,
        Report {
            job_id: running_job,
            pages: vec![(1, "indexed after the upgrade")],
            origin: None,
            indexer: Some(("Upgraded.", 1, 3)),
            done: true,
        },
    )
    .await);

    // Then: the cancel 0.4.0 recorded crossed the last step's report: nothing
    // is left to stop and the file is READY; Jobs is told, and the facts start
    // after the migrated version
    let file = world.await_state(&owner, running, "READY").await;
    assert_eq!(file["summary"], "Upgraded.");
    assert_eq!(jobs.await_finish(running_job).await.job_id, running_job);
    let facts = world.processing_facts(running).await;
    assert_eq!(facts.first().map(|fact| fact.seq), Some(2), "{facts:?}");
    assert_eq!(
        world.job_end(running_job).await,
        Some(("reported_done".to_string(), None))
    );

    world.service.shutdown().await;
}
