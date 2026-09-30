//! What reaches the host's fact table, and what never moves the state twice:
//! a Jobs fact that no longer changes anything is still recorded — as
//! `JobFactIgnored` — a redelivered one applies once, and a bulk gesture hands
//! one fact per row it changes.

use std::time::Duration;

use uuid::Uuid;

use crate::harness::runner::{RUNNER_SCOPE, Report, install_render_rule, report};
use crate::harness::upload::{UploadRequest, upload, upload_processed};
use crate::harness::{
    Fact, JobsStandIn, World, drive_subscription, manager_passport, ok, passport, quiet,
    service_passport,
};

const BYTES: &[u8] = b"a document whose facts the host keeps";

fn of_kind<'a>(facts: &'a [Fact], kind: &str) -> Vec<&'a Fact> {
    facts
        .iter()
        .filter(|fact| fact.event_type == kind)
        .collect()
}

#[tokio::test]
async fn a_jobs_failure_after_the_final_report_leaves_the_file_ready_and_is_kept_as_ignored() {
    // Given: a one-step rule, and a file its runner reported done
    let world = World::start("pod-facts-late-failure").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    install_render_rule(&world, &manager).await;
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = upload_processed(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "done.txt", BYTES),
    )
    .await;
    let job = jobs.await_create(file_id).await.job_id;
    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id: job,
            pages: vec![(1, "the only page")],
            origin: None,
            indexer: Some(("One page.", 1, 7)),
            done: true,
        },
    )
    .await);
    let ready = world.await_state(&owner, file_id, "READY").await;
    jobs.await_finish(job).await;
    let mut files = drive_subscription(&world, &owner, drive).await;
    quiet(&mut files).await;

    // When: Jobs says the job failed
    let late = jobs.fail(job, "RUN_TIMED_OUT", Some("too_slow")).await;
    world.await_consumed(late).await;

    // Then: the file stays READY, untouched
    files.expect_silence(Duration::from_millis(800)).await;
    let file = world.file(&owner, file_id).await;
    assert_eq!(file["processingState"], "READY");
    assert!(file["processingError"].is_null());
    assert_eq!(file["updatedAt"], ready["updatedAt"]);
    // And: the failure is on record, as ignored — the job had already ended —
    // caused by Jobs' message
    let facts = world.job_facts(job).await;
    assert_eq!(
        of_kind(&facts, "JobReportedDone").len(),
        1,
        "the runner's end"
    );
    assert!(of_kind(&facts, "JobFailed").is_empty(), "{facts:?}");
    let ignored = of_kind(&facts, "JobFactIgnored");
    assert_eq!(ignored.len(), 1, "{facts:?}");
    assert_eq!(ignored[0].payload["why"], "job_already_ended");
    assert_eq!(ignored[0].payload["received"]["kind"], "JobFailed");
    assert_eq!(ignored[0].payload["received"]["reason_code"], "too_slow");
    assert_eq!(ignored[0].causation_id, Some(late));
    assert_eq!(ignored[0].actor_kind, "service");
    assert_eq!(
        world.job_end(job).await,
        Some(("reported_done".to_string(), None))
    );

    world.cleanup().await;
}

#[tokio::test]
async fn a_redelivered_jobs_fact_applies_once() {
    // Given: a file whose only job runs
    let world = World::start("pod-facts-redelivered").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    install_render_rule(&world, &manager).await;
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = upload_processed(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "twice.txt", BYTES),
    )
    .await;
    let job = jobs.await_create(file_id).await.job_id;

    // When: the broker delivers Jobs' `failed` twice, then Jobs sends it again
    // as a message of its own
    let delivered = jobs.fail_delivered_twice(job, "RUNNER_LOST").await;
    world.await_consumed(delivered).await;
    let failed = world.await_state(&owner, file_id, "FAILED").await;
    let again = jobs.fail(job, "RUNNER_LOST", None).await;
    world.await_consumed(again).await;

    // Then: the failure applied once — one `JobFailed`, the file FAILED once —
    // the redelivered message was consumed once, and the second message is kept
    // as ignored
    let facts = world.job_facts(job).await;
    assert_eq!(of_kind(&facts, "JobFailed").len(), 1, "{facts:?}");
    assert_eq!(
        of_kind(&facts, "JobFailed")[0].causation_id,
        Some(delivered)
    );
    let ignored = of_kind(&facts, "JobFactIgnored");
    assert_eq!(ignored.len(), 1, "{facts:?}");
    assert_eq!(ignored[0].causation_id, Some(again));
    assert_eq!(ignored[0].payload["why"], "job_already_ended");
    let file = world.file(&owner, file_id).await;
    assert_eq!(file["processingState"], "FAILED");
    assert_eq!(file["processingError"], "RUNNER_LOST");
    assert_eq!(file["updatedAt"], failed["updatedAt"]);
    let version: i64 =
        sqlx::query_scalar("SELECT version FROM drive.file_processing WHERE file_id = $1")
            .bind(file_id)
            .fetch_one(&world.db.app)
            .await
            .unwrap();
    let processing = world.processing_facts(file_id).await;
    assert_eq!(
        version,
        processing.len() as i64,
        "one version per fact: {processing:?}"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn a_folder_move_hands_one_fact_per_moved_file() {
    // Given: two files under a folder and one beside it
    let world = World::start("pod-facts-folder").await;
    let owner_id = Uuid::now_v7();
    let owner = passport(owner_id);
    let drive = world.create_workspace(&owner, "library").await;
    let moved = [
        upload(
            &world,
            &owner,
            &UploadRequest::text(drive, "reports", "a.txt", BYTES),
        )
        .await,
        upload(
            &world,
            &owner,
            &UploadRequest::text(drive, "reports/2026", "b.txt", BYTES),
        )
        .await,
    ];
    let beside = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "notes", "c.txt", BYTES),
    )
    .await;
    let before_beside = world.facts("drive_file", serde_json::json!(beside)).await;

    // When: the owner moves the folder
    ok(&world
        .gql(
            &owner,
            "mutation($d:UUID!,$o:String!,$n:String!){workspaceMoveFolder(driveId:$d,oldPrefix:$o,newPrefix:$n){success}}",
            serde_json::json!({ "d": drive, "o": "reports", "n": "archive/reports" }),
        )
        .await);

    // Then: each moved file has one `FolderMoved`, at its next version, with
    // its own paths — every one in the gesture's correlation, the owner's
    let mut correlations = Vec::new();
    for (file, from, to) in [
        (moved[0], "reports", "archive/reports"),
        (moved[1], "reports/2026", "archive/reports/2026"),
    ] {
        let facts = world.facts("drive_file", serde_json::json!(file)).await;
        let last = facts.last().expect("a fact");
        assert_eq!(last.event_type, "FolderMoved");
        assert_eq!(last.seq, facts.len() as i64);
        assert_eq!(
            last.payload,
            serde_json::json!({ "kind": "FolderMoved", "from_path": from, "to_path": to })
        );
        assert_eq!(last.actor_id, owner_id);
        assert_eq!(of_kind(&facts, "FolderMoved").len(), 1);
        let version: i64 = sqlx::query_scalar("SELECT version FROM drive.file WHERE id = $1")
            .bind(file)
            .fetch_one(&world.db.app)
            .await
            .unwrap();
        assert_eq!(version, last.seq, "the row's version is its last fact's");
        correlations.push(last.correlation_id);
        let shown = world.file(&owner, file).await;
        assert_eq!(shown["path"], to);
        let at: chrono::DateTime<chrono::Utc> =
            serde_json::from_value(shown["updatedAt"].clone()).unwrap();
        assert_eq!(at, last.occurred_at, "the move is the file's last change");
    }
    assert_eq!(correlations[0], correlations[1], "one gesture");
    // And: the file beside the folder has no new fact
    let after_beside = world.facts("drive_file", serde_json::json!(beside)).await;
    assert_eq!(after_beside.len(), before_beside.len());

    world.cleanup().await;
}
