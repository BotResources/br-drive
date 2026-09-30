//! The upgrade of a 0.2.0 database: its per-job `events` log exploded into
//! the fact tables. The Given is built in SQL — that state is 0.2.0's schema,
//! which no gesture of this version can produce; everything after the upgrade
//! is observed through the booted host, and the tables the migration wrote.

use chrono::{DateTime, SubsecRound, TimeDelta, Utc};
use futures_util::FutureExt;
use uuid::Uuid;

use crate::harness::pg::TestDb;
use crate::harness::runner::{RUNNER_SCOPE, Report, report};
use crate::harness::{JobsStandIn, World, WorldOptions, ok, passport, service_passport};

/// A `drive.file_job_end` row: job, kind, reason, message, instant.
type EndRow = (Uuid, String, Option<String>, Option<String>, DateTime<Utc>);

/// The last drive migration released in v0.2.0.
const RELEASED_0_2: i64 = 9_121_000_008;
/// The last drive migration released in v0.4.0: the job fact tables this
/// scenario reads before the upgrade to the state tables drops them.
const RELEASED_0_4: i64 = 9_121_000_010;

fn at(instant: DateTime<Utc>) -> serde_json::Value {
    serde_json::json!(instant)
}

struct Seeded {
    owner_pool: sqlx::PgPool,
    workspace: Uuid,
    owner: Uuid,
    t0: DateTime<Utc>,
}

impl Seeded {
    async fn file(&self, name: &str, committed: bool, two_steps: bool) -> Uuid {
        let id = Uuid::now_v7();
        let steps = if two_steps {
            serde_json::json!([
                { "runner_type": "render", "options": {} },
                { "runner_type": "index", "options": {} },
            ])
        } else {
            serde_json::json!([{ "runner_type": "render", "options": {} }])
        };
        sqlx::query(
            "INSERT INTO drive.file (id, drive_id, path, name, title, media_type, size_bytes, \
             sha256, blob_ref, steps, committed_at, created_by, created_at, updated_at) \
             VALUES ($1, $2, '', $3, $3, 'text/plain', 1, $4, $5, $6, \
             CASE WHEN $7 THEN now() END, $8, now(), now())",
        )
        .bind(id)
        .bind(self.workspace)
        .bind(name)
        .bind(vec![0u8; 32])
        .bind(Uuid::now_v7())
        .bind(steps)
        .bind(committed)
        .bind(self.owner)
        .execute(&self.owner_pool)
        .await
        .unwrap_or_else(|e| panic!("seed {name}: {e}"));
        id
    }

    /// A 0.2.0 job row: inserted in order, so its `seq` follows.
    async fn job(&self, file: Uuid, step: i32, events: serde_json::Value) -> Uuid {
        let job = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO drive.file_job (job_id, file_id, step_index, trigger, triggered_by, \
             events, created_at) VALUES ($1, $2, $3, 'upload', $4, $5, $6)",
        )
        .bind(job)
        .bind(file)
        .bind(step)
        .bind(serde_json::json!({ "id": self.owner, "display_name": "Ada" }))
        .bind(events)
        .bind(self.t0)
        .execute(&self.owner_pool)
        .await
        .expect("seed a 0.2.0 job");
        job
    }
}

#[tokio::test]
async fn a_0_2_database_upgrades_through_the_fact_tables_and_its_running_job_ends_through_the_host()
{
    // Given: a database exactly as 0.2.0 left it — the drive migrations that
    // release shipped, 01 to 08 — cleaned up whatever happens
    let db = TestDb::at_drive_version(RELEASED_0_2).await;
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
    let workspace = Uuid::now_v7();
    // At the database's precision (microseconds), so an instant read back
    // compares equal on any clock.
    let t0 = (Utc::now() - TimeDelta::minutes(10)).trunc_subsecs(6);
    let t = |seconds: i64| t0 + TimeDelta::seconds(seconds);
    let owner_pool = db.owner().await;
    sqlx::query("INSERT INTO workspace (id, owner_id, name) VALUES ($1, $2, 'upgraded')")
        .bind(workspace)
        .bind(owner_id)
        .execute(&owner_pool)
        .await
        .expect("seed the host object");
    sqlx::query("INSERT INTO drive.drive (id, created_by, created_at) VALUES ($1, $2, now())")
        .bind(workspace)
        .bind(owner_id)
        .execute(&owner_pool)
        .await
        .expect("seed the drive");
    let seeded = Seeded {
        owner_pool,
        workspace,
        owner: owner_id,
        t0,
    };

    // And: one file in each shape 0.2.0 could store
    let pending = seeded.file("pending.txt", false, false).await;
    let stored = seeded.file("stored.txt", true, false).await;
    // A two-step chain whose first step ended and whose second one runs,
    // with a plan declared and two steps started (one redelivered).
    let running = seeded.file("running.txt", true, true).await;
    let run = Uuid::now_v7();
    let first_step = seeded
        .job(
            running,
            0,
            serde_json::json!([
                { "kind": "queued", "at": at(t(1)), "job_id": Uuid::nil(), "runner_type": "render" },
                { "kind": "reported_done", "at": at(t(2)) },
                { "kind": "completed", "at": at(t(3)) },
            ]),
        )
        .await;
    let running_job = seeded
        .job(
            running,
            1,
            serde_json::json!([
                { "kind": "queued", "at": at(t(4)) },
                { "kind": "started", "at": at(t(5)), "run_id": run },
                { "kind": "plan_declared", "at": at(t(6)), "run_id": run, "steps": ["read", "write"] },
                { "kind": "step_started", "at": at(t(7)), "run_id": run, "index": 0, "label": "read", "started_at": at(t(7)) },
                { "kind": "step_started", "at": at(t(8)), "run_id": run, "index": 1, "label": "write", "started_at": at(t(8)) },
                { "kind": "step_started", "at": at(t(9)), "run_id": run, "index": 1, "label": "write", "started_at": at(t(8)) },
            ]),
        )
        .await;
    let done = seeded.file("done.txt", true, false).await;
    let done_job = seeded
        .job(
            done,
            0,
            serde_json::json!([
                { "kind": "reported_done", "at": at(t(10)) },
                { "kind": "completed", "at": at(t(11)) },
            ]),
        )
        .await;
    let runner_failed = seeded.file("runner-failed.txt", true, false).await;
    let runner_failed_job = seeded
        .job(
            runner_failed,
            0,
            serde_json::json!([
                { "kind": "reported_failed", "at": at(t(12)), "reason_code": "unreadable_scan", "message": "page 3 is blank" },
                { "kind": "failed", "at": at(t(13)), "failure_cause": "DECLARED_BY_OWNER" },
            ]),
        )
        .await;
    let jobs_failed = seeded.file("jobs-failed.txt", true, false).await;
    let jobs_failed_job = seeded
        .job(
            jobs_failed,
            0,
            serde_json::json!([
                { "kind": "failed", "at": at(t(14)), "failure_cause": "TERMINAL_RUN_FAILURE",
                  "failure_report": { "kind": "PERMANENT", "reason_code": "ocr_timeout", "params": {}, "diagnostic": {} },
                  "note": "the runner gave up" },
            ]),
        )
        .await;
    let cancelled = seeded.file("cancelled.txt", true, false).await;
    let cancelled_job = seeded
        .job(
            cancelled,
            0,
            serde_json::json!([
                { "kind": "cancel_requested", "at": at(t(15)) },
                { "kind": "cancel_requested", "at": at(t(16)) },
                { "kind": "cancelled", "at": at(t(17)) },
            ]),
        )
        .await;
    // A cancel that crossed the first step's final report: the second step
    // recorded as never started.
    let crossed = seeded.file("crossed.txt", true, true).await;
    let crossed_first = seeded
        .job(
            crossed,
            0,
            serde_json::json!([
                { "kind": "cancel_requested", "at": at(t(18)) },
                { "kind": "reported_done", "at": at(t(19)) },
            ]),
        )
        .await;
    let crossed_never = seeded
        .job(
            crossed,
            1,
            serde_json::json!([{ "kind": "cancelled", "at": at(t(19)), "before_start": true }]),
        )
        .await;
    // A terminal entry after the first one, which changed nothing.
    let later = seeded.file("later.txt", true, false).await;
    let later_job = seeded
        .job(
            later,
            0,
            serde_json::json!([
                { "kind": "reported_done", "at": at(t(20)) },
                { "kind": "failed", "at": at(t(21)), "failure_cause": "RUN_TIMED_OUT" },
                { "kind": "cancelled", "at": at(t(22)) },
            ]),
        )
        .await;
    let rejected = seeded.file("rejected.txt", true, false).await;
    let rejected_job = seeded
        .job(
            rejected,
            0,
            serde_json::json!([
                { "kind": "creation_rejected", "at": at(t(23)), "reason_code": "runner_type_retired", "params": {} },
            ]),
        )
        .await;
    let pool = seeded.owner_pool;

    // When: a 0.4.0 host's upgrade applies that release's migrations
    db.migrate_drive_until(RELEASED_0_4).await;

    // Then: each job is numbered in its file's order, ended by its FIRST
    // terminal entry, with its reason; the later ones are not kept
    let jobs_of: Vec<(Uuid, Uuid, i32, i32)> = sqlx::query_as(
        "SELECT file_id, job_id, number, step_index FROM drive.file_job ORDER BY file_id, number",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    let numbered = |file: Uuid| -> Vec<(Uuid, i32)> {
        jobs_of
            .iter()
            .filter(|row| row.0 == file)
            .map(|row| (row.1, row.2))
            .collect()
    };
    assert_eq!(numbered(running), vec![(first_step, 1), (running_job, 2)]);
    assert_eq!(
        numbered(crossed),
        vec![(crossed_first, 1), (crossed_never, 2)]
    );
    assert_eq!(numbered(done), vec![(done_job, 1)]);
    let ends: Vec<EndRow> =
        sqlx::query_as("SELECT job_id, kind, reason_code, message, at FROM drive.file_job_end")
            .fetch_all(&pool)
            .await
            .unwrap();
    let end = |job: Uuid| {
        ends.iter()
            .find(|row| row.0 == job)
            .map(|row| (row.1.clone(), row.2.clone(), row.3.clone()))
    };
    let s = |text: &str| Some(text.to_string());
    assert_eq!(end(first_step), Some(("reported_done".into(), None, None)));
    assert_eq!(end(running_job), None, "the running job has no end");
    assert_eq!(end(done_job), Some(("reported_done".into(), None, None)));
    assert_eq!(
        end(runner_failed_job),
        Some((
            "reported_failed".into(),
            s("unreadable_scan"),
            s("page 3 is blank")
        ))
    );
    assert_eq!(
        end(jobs_failed_job),
        Some(("failed".into(), s("ocr_timeout"), s("the runner gave up")))
    );
    assert_eq!(end(cancelled_job), Some(("cancelled".into(), None, None)));
    assert_eq!(
        end(crossed_first),
        Some(("reported_done".into(), None, None))
    );
    assert_eq!(end(crossed_never), Some(("cancelled".into(), None, None)));
    assert_eq!(
        end(later_job),
        Some(("reported_done".into(), None, None)),
        "the first end wins"
    );
    assert_eq!(
        end(rejected_job),
        Some(("creation_rejected".into(), s("runner_type_retired"), None))
    );
    let ended_at = ends.iter().find(|row| row.0 == later_job).unwrap().4;
    assert_eq!(ended_at.timestamp_micros(), t(20).timestamp_micros());
    // And: the cancel requests, the plan and the steps have their tables
    let cancels: Vec<(Uuid, i32, Option<Uuid>)> = sqlx::query_as(
        "SELECT job_id, number, requested_by FROM drive.file_job_cancel ORDER BY job_id, number",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        cancels,
        {
            let mut expected = vec![
                (cancelled_job, 1, None),
                (cancelled_job, 2, None),
                (crossed_first, 1, None),
            ];
            expected.sort();
            expected
        },
        "0.2.0 did not record who asked"
    );
    let plans: Vec<(Uuid, i32, Uuid, Vec<String>)> =
        sqlx::query_as("SELECT job_id, number, run_id, labels FROM drive.file_job_plan")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        plans,
        vec![(
            running_job,
            1,
            run,
            vec!["read".to_string(), "write".to_string()]
        )]
    );
    let steps: Vec<(Uuid, Uuid, i32, String)> = sqlx::query_as(
        "SELECT job_id, run_id, plan_index, label FROM drive.file_job_step ORDER BY plan_index",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        steps,
        vec![
            (running_job, run, 0, "read".to_string()),
            (running_job, run, 1, "write".to_string()),
        ],
        "a redelivered step is one step"
    );
    // And: the stored log is gone
    let gone: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'drive' \
         AND table_name = 'file_job' AND column_name IN ('seq', 'events', 'outcome', 'triggered_by')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(gone, 0, "no stored log, no derived outcome");
    let initiators: Vec<(Option<Uuid>, Option<String>)> =
        sqlx::query_as("SELECT DISTINCT triggered_by_id, triggered_by_name FROM drive.file_job")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(initiators, vec![(Some(owner_id), s("Ada"))]);
    pool.close().await;

    // When: the upgrade goes on to this version's state tables
    db.migrate(br_drive_example::db::libraries()).await;

    // When: the host boots on the upgraded database
    let world = World::start_on(db, "pod-upgraded-0-2", WorldOptions::default()).await;
    let jobs = JobsStandIn::attach(&world).await;
    let runner = service_passport(&[RUNNER_SCOPE]);

    // Then: every file reads the state 0.2.0 computed for it
    for (file, state, error) in [
        (pending, "PENDING", None),
        (stored, "READY", None),
        (running, "PROCESSING", None),
        (done, "READY", None),
        (runner_failed, "FAILED", Some("unreadable_scan")),
        (jobs_failed, "FAILED", Some("ocr_timeout")),
        (cancelled, "FAILED", Some("cancelled")),
        (crossed, "FAILED", Some("cancelled")),
        (later, "READY", None),
        (rejected, "FAILED", Some("runner_type_retired")),
    ] {
        let read = world.file(&owner, file).await;
        assert_eq!(read["processingState"], state, "{read}");
        assert_eq!(read["processingError"].as_str(), error, "{read}");
    }
    // And: the running file shows where its runner is
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
    assert_eq!(shown["affordances"]["cancelProcessing"]["allowed"], true);
    // And: the counts come through the status view
    let counted = world
        .gql(
            &owner,
            "query($id:UUID!){workspaceWorkspace(id:$id){fileCount readyFileCount}}",
            serde_json::json!({ "id": workspace }),
        )
        .await;
    assert_eq!(
        ok(&counted)["workspaceWorkspace"],
        serde_json::json!({ "fileCount": 10, "readyFileCount": 3 })
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

    // Then: the job ends there, the chain with it (it was the last step), and
    // Jobs is told
    let file = world.await_state(&owner, running, "READY").await;
    assert_eq!(file["summary"], "Upgraded.");
    assert!(file["progress"].is_null());
    assert_eq!(jobs.await_finish(running_job).await.job_id, running_job);
    assert_eq!(
        world.job_end(running_job).await,
        Some(("reported_done".to_string(), None))
    );

    // When: the owner reprocesses a failed file
    ok(&crate::harness::upload::process(&world, &owner, jobs_failed).await);

    // Then: its next job is its running one, the migrated job an earlier one
    let create = jobs.await_create(jobs_failed).await;
    let log = world.job_log(jobs_failed).await;
    assert_eq!(log, vec![(create.job_id, 0, None)]);
    let earlier: Vec<Uuid> =
        sqlx::query_scalar("SELECT past_job_ids FROM drive.file_processing WHERE file_id = $1")
            .bind(jobs_failed)
            .fetch_one(&world.db.app)
            .await
            .unwrap();
    assert_eq!(earlier, vec![jobs_failed_job]);

    // When: a late fact of the migrated job arrives
    let late = jobs.cancel(jobs_failed_job).await;
    world.await_consumed(late).await;

    // Then: it is kept, as ignored, and the new job still runs
    let kept = world.job_facts(jobs_failed_job).await;
    assert_eq!(kept.len(), 1, "{kept:?}");
    assert_eq!(kept[0].event_type, "JobFactIgnored");
    assert_eq!(kept[0].payload["why"], "not_the_current_job");
    assert_eq!(
        world.file(&owner, jobs_failed).await["processingState"],
        "PROCESSING"
    );

    world.service.shutdown().await;
}
