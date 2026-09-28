//! Storing and processing are two gestures: a commit only confirms the upload
//! (the file is READY, stored and unprocessed), and `ProcessFile` is the only
//! way to start a chain — right after the commit, later, or never.

use std::time::Duration;

use uuid::Uuid;

use crate::harness::runner::{RENDER, RUNNER_SCOPE, install_render_rule};
use crate::harness::upload::{UploadRequest, process, request, ticket, upload};
use crate::harness::{
    JobsStandIn, World, drive_subscription, error_code, manager_passport, next_drive_delta, ok,
    passport, quiet, refute_delta, service_passport,
};

const BYTES: &[u8] = b"a document to store, then maybe to process";

#[tokio::test]
async fn a_commit_stores_the_file_ready_with_no_results_and_asks_jobs_nothing() {
    // Given: an upload rule that matches text files, and the owner watching
    let world = World::start("pod-commit-only").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    install_render_rule(&world, &manager).await;
    let drive = world.create_workspace(&owner, "library").await;
    let mut files = drive_subscription(&world, &owner, drive).await;
    quiet(&mut files).await;

    // When: a file is uploaded and committed
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "kept.txt", BYTES),
    )
    .await;

    // Then: it is READY — stored, not processed: no rule ran, no result exists,
    // and the process gesture is offered
    let committed = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "UploadCommitted"
    })
    .await;
    let view = &committed["view"];
    assert_eq!(view["id"], file_id.to_string());
    assert_eq!(view["processingState"], "READY");
    assert!(view["processingError"].is_null());
    assert!(view["rulesetId"].is_null());
    assert!(view["progress"].is_null());
    assert!(view["summary"].is_null());
    assert!(view["pageCount"].is_null());
    assert!(view["estimatedTokens"].is_null());
    assert_eq!(view["images"], serde_json::json!([]));
    assert_eq!(view["affordances"]["process"]["allowed"], true);
    assert_eq!(
        view["affordances"]["cancelProcessing"]["reason"],
        "FILE_NOT_PROCESSING"
    );
    assert!(world.file_pages(&owner, file_id).await.is_empty());
    let file = world.file(&owner, file_id).await;
    assert!(file["steps"].is_null(), "no plan was ever taken");

    // And: nothing more happens — no job is asked of Jobs, the file stays READY
    jobs.expect_no_command(Duration::from_secs(1)).await;
    refute_delta(
        &mut files,
        "workspaceDriveChanged",
        Duration::from_millis(600),
        |node| node["view"]["processingState"] != "READY",
    )
    .await;

    world.cleanup().await;
}

#[tokio::test]
async fn a_stored_file_is_processed_later_by_the_process_gesture_and_only_by_its_owner() {
    // Given: a file committed a while ago, READY, and an upload rule declared since
    let world = World::start("pod-process-later").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner_id = Uuid::now_v7();
    let owner = manager_passport(owner_id, "Ada Owner");
    let stranger = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "later.txt", BYTES),
    )
    .await;
    let rule = install_render_rule(&world, &manager).await;
    jobs.expect_no_command(Duration::from_millis(600)).await;
    let mut files = drive_subscription(&world, &owner, drive).await;
    quiet(&mut files).await;

    // When: someone who cannot see the file, and a runner, ask to process it
    for intruder in [&stranger, &runner] {
        // Then: they learn nothing of it, and nothing happens
        assert_eq!(
            error_code(&process(&world, intruder, file_id).await),
            "FILE_NOT_FOUND"
        );
    }
    files.expect_silence(Duration::from_millis(600)).await;
    jobs.expect_no_command(Duration::from_millis(300)).await;

    // When: the owner processes it
    let acked = process(&world, &owner, file_id).await;

    // Then: the answer is an ack, nothing more
    assert_eq!(
        ok(&acked),
        &serde_json::json!({ "workspaceProcessFile": { "success": true } })
    );
    // And: the chain starts with the file's upload rule — it never had a job
    let started = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "ProcessingStarted"
    })
    .await;
    assert_eq!(started["cause"]["step"], 0);
    assert_eq!(started["view"]["processingState"], "PROCESSING");
    assert_eq!(started["view"]["rulesetId"], rule.to_string());
    assert_eq!(started["view"]["progress"]["runnerType"], RENDER);
    assert_eq!(
        started["view"]["affordances"]["process"]["reason"],
        "FILE_PROCESSING"
    );
    assert_eq!(
        started["view"]["affordances"]["cancelProcessing"]["allowed"],
        true
    );
    let create = jobs.await_create(file_id).await;
    assert_eq!(create.runner_type, RENDER);
    assert_eq!(create.source_entity_id, Some(file_id));
    assert_eq!(
        started["cause"]["job_id"],
        create.job_id.to_string(),
        "the job the file shows is the one asked of Jobs"
    );
    jobs.expect_no_command(Duration::from_millis(500)).await;

    world.cleanup().await;
}

#[tokio::test]
async fn the_process_gesture_is_refused_on_a_pending_and_on_a_processing_file() {
    // Given: an upload rule, one upload not confirmed yet and one file processing
    let world = World::start("pod-process-refused").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    install_render_rule(&world, &manager).await;
    let drive = world.create_workspace(&owner, "library").await;
    let pending = Uuid::now_v7();
    ticket(
        &request(
            &world,
            &owner,
            pending,
            &UploadRequest::text(drive, "", "pending.txt", BYTES),
        )
        .await,
    );
    let running = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "running.txt", BYTES),
    )
    .await;
    ok(&process(&world, &owner, running).await);
    let job = jobs.await_create(running).await.job_id;
    let mut files = drive_subscription(&world, &owner, drive).await;
    quiet(&mut files).await;

    // Then: each file already says the gesture is not open, and why
    let shown = world.file(&owner, pending).await;
    assert_eq!(
        shown["affordances"]["process"],
        serde_json::json!({ "allowed": false, "reason": "FILE_NOT_READY" })
    );
    let shown = world.file(&owner, running).await;
    assert_eq!(
        shown["affordances"]["process"],
        serde_json::json!({ "allowed": false, "reason": "FILE_PROCESSING" })
    );

    // When: the owner asks anyway
    // Then: the pending upload is not ready, the running one is processing
    assert_eq!(
        error_code(&process(&world, &owner, pending).await),
        "FILE_NOT_READY"
    );
    assert_eq!(
        error_code(&process(&world, &owner, running).await),
        "FILE_PROCESSING"
    );

    // And: nothing changed — no delta, no second job, the same running job
    files.expect_silence(Duration::from_millis(800)).await;
    jobs.expect_no_command(Duration::from_millis(500)).await;
    assert_eq!(world.job_of(running).await, Some(job));
    assert_eq!(
        world.file(&owner, pending).await["processingState"],
        "PENDING"
    );

    world.cleanup().await;
}
