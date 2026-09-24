//! The chain's contract with the real Jobs service, and the way out of a file
//! stuck in PROCESSING: the user's cancel. The library keeps no deadline of
//! its own — a job Jobs accepted but never dispatches (no live instance, or a
//! runner type Jobs does not know) waits until someone cancels it.

use std::time::Duration;

use contract_jobs::command::TriggeredBy;
use uuid::Uuid;

use crate::harness::runner::{
    INDEX, RENDER, RUNNER_SCOPE, Report, RuleSpec, context, create_ruleset, finish_job,
    install_render_rule, report,
};
use crate::harness::upload::{UploadRequest, commit, post_bytes, request, ticket, upload};
use crate::harness::{
    JobsStandIn, World, drive_subscription, error_code, manager_passport, next_drive_delta, ok,
    passport, refute_delta, service_passport,
};

const BYTES: &[u8] = b"a document the runners never pick up";
const PROCESS: &str = "mutation($f:UUID!){workspaceProcess(fileId:$f){success}}";
const CANCEL: &str = "mutation($f:UUID!){workspaceCancelProcessing(fileId:$f){success}}";

async fn done(world: &World, jobs: &JobsStandIn, runner: &str, file_id: Uuid, job_id: Uuid) {
    ok(&report(
        world,
        runner,
        file_id,
        Report {
            job_id,
            pages: vec![(1, "done")],
            origin: None,
            indexer: None,
            done: true,
        },
    )
    .await);
    finish_job(world, jobs, job_id).await;
}

async fn cancel(world: &World, passport: &str, file_id: Uuid) -> serde_json::Value {
    world
        .gql(passport, CANCEL, serde_json::json!({ "f": file_id }))
        .await
}

#[tokio::test]
async fn a_job_no_runner_picks_up_waits_until_the_user_cancels_it_then_can_be_reprocessed() {
    // Given: a render rule whose runner type has no live instance
    let world = World::start("pod-user-cancel").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    let stranger = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    install_render_rule(&world, &manager).await;
    let drive = world.create_workspace(&owner, "library").await;

    // When: a file is uploaded and Jobs queues its job, which nobody picks up
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "stuck.txt", BYTES),
    )
    .await;
    let stuck = jobs.await_create(file_id).await.job_id;
    let mut files = drive_subscription(&world, &owner, drive).await;

    // Then: the file waits in PROCESSING — no deadline fails it — and offers
    // the cancel to its owner only
    refute_delta(
        &mut files,
        "workspaceDriveChanged",
        Duration::from_secs(2),
        |node| node["view"]["processingState"] != "PROCESSING",
    )
    .await;
    let file = world.await_state(&owner, file_id, "PROCESSING").await;
    assert_eq!(file["affordances"]["cancelProcessing"]["allowed"], true);
    assert_eq!(file["affordances"]["process"]["allowed"], false);
    assert_eq!(file["progress"]["stepIndex"], 0);
    assert_eq!(file["progress"]["runnerType"], RENDER);
    assert_eq!(
        error_code(&cancel(&world, &stranger, file_id).await),
        "FILE_NOT_FOUND",
        "a principal who cannot see the file learns nothing of it"
    );
    assert_eq!(
        error_code(&cancel(&world, &runner, file_id).await),
        "FILE_NOT_FOUND",
        "a runner sees no drive, so it cancels nothing"
    );
    assert_eq!(
        error_code(&cancel(&world, &owner, Uuid::now_v7()).await),
        "FILE_NOT_FOUND"
    );

    crate::poll_until!(Duration::from_secs(15), {
        (world.job_events(stuck).await == ["queued"]).then_some(())
    });

    // When: the owner cancels it
    ok(&cancel(&world, &owner, file_id).await);

    // Then: the request is logged and the file shows it, still PROCESSING until
    // Jobs confirms; Jobs is asked to cancel that very job
    let requested = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "CancelRequested"
    })
    .await;
    assert_eq!(requested["cause"]["job_id"], stuck.to_string());
    assert_eq!(requested["view"]["processingState"], "PROCESSING");
    assert_eq!(
        requested["view"]["affordances"]["cancelProcessing"]["allowed"], true,
        "until Jobs confirms, the cancel may be asked again"
    );
    jobs.await_cancel(stuck).await;

    // And: Jobs' `cancelled` lands the file FAILED `cancelled`, open to a reprocess
    let failed = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "ProcessingFailed"
    })
    .await;
    assert_eq!(failed["cause"]["reason"], "cancelled");
    assert_eq!(failed["view"]["processingState"], "FAILED");
    assert_eq!(failed["view"]["processingError"], "cancelled");
    assert!(failed["view"]["progress"].is_null());
    assert_eq!(failed["view"]["affordances"]["process"]["allowed"], true);
    assert_eq!(
        failed["view"]["affordances"]["cancelProcessing"],
        serde_json::json!({ "allowed": false, "reason": "FILE_NOT_PROCESSING" })
    );
    assert_eq!(
        world.job_events(stuck).await,
        vec!["queued", "cancel_requested", "cancelled"],
        "the job's log holds every fact and the request, in arrival order"
    );
    assert_eq!(
        error_code(&context(&world, &runner, file_id, stuck).await),
        "JOB_NOT_ACTIVE",
        "the cancelled job no longer opens the file"
    );
    // A `queued` redelivered after the cancellation reopens nothing.
    jobs.queue(stuck, RENDER).await;
    refute_delta(
        &mut files,
        "workspaceDriveChanged",
        Duration::from_millis(800),
        |node| node["view"]["processingState"] != "FAILED",
    )
    .await;
    assert_eq!(
        world.file(&owner, file_id).await["processingState"],
        "FAILED"
    );
    assert_eq!(
        error_code(&cancel(&world, &owner, file_id).await),
        "FILE_NOT_PROCESSING"
    );

    // When: the owner reprocesses it
    ok(&world
        .gql(&owner, PROCESS, serde_json::json!({ "f": file_id }))
        .await);

    // Then: a new job, which Jobs accepts, carries the chain to READY
    let retry = jobs.await_create(file_id).await.job_id;
    assert_ne!(retry, stuck);
    done(&world, &jobs, &runner, file_id, retry).await;
    let ready = world.await_state(&owner, file_id, "READY").await;
    assert_eq!(
        ready["affordances"]["cancelProcessing"],
        serde_json::json!({ "allowed": false, "reason": "FILE_NOT_PROCESSING" })
    );
    assert_eq!(
        error_code(&cancel(&world, &owner, file_id).await),
        "FILE_NOT_PROCESSING"
    );
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
    assert_eq!(
        world.file(&owner, pending).await["affordances"]["cancelProcessing"],
        serde_json::json!({ "allowed": false, "reason": "FILE_NOT_PROCESSING" })
    );
    assert_eq!(
        error_code(&cancel(&world, &owner, pending).await),
        "FILE_NOT_PROCESSING"
    );
    let log = world.job_log(file_id).await;
    assert_eq!(
        log.iter().map(|(job, _, _)| *job).collect::<Vec<_>>(),
        vec![stuck, retry]
    );
    jobs.expect_no_command(Duration::from_secs(1)).await;

    world.cleanup().await;
}

#[tokio::test]
async fn a_cancel_jobs_dropped_can_be_asked_again() {
    // Given: a file PROCESSING, and a Jobs that drops the first cancel — as it
    // does with a cancel it consumes before the creation of the job it names
    let world = World::start("pod-cancel-again").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    install_render_rule(&world, &manager).await;
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "twice.txt", BYTES),
    )
    .await;
    let job = jobs.await_create(file_id).await.job_id;
    jobs.drop_cancels(true);

    // When: the owner cancels, and Jobs never confirms
    ok(&cancel(&world, &owner, file_id).await);
    jobs.await_cancel(job).await;
    jobs.expect_no_command(Duration::from_millis(500)).await;

    // Then: the file is still PROCESSING and still offers the cancel
    let file = world.file(&owner, file_id).await;
    assert_eq!(file["processingState"], "PROCESSING");
    assert_eq!(file["affordances"]["cancelProcessing"]["allowed"], true);
    assert!(jobs.is_live(job));

    // When: the owner cancels again, and Jobs takes it
    jobs.drop_cancels(false);
    ok(&cancel(&world, &owner, file_id).await);

    // Then: the same job is cancelled and the file lands FAILED
    jobs.await_cancel(job).await;
    let file = world.await_state(&owner, file_id, "FAILED").await;
    assert_eq!(file["processingError"], "cancelled");
    let mut events = world.job_events(job).await;
    events.sort_unstable();
    assert_eq!(
        events,
        vec![
            "cancel_requested",
            "cancel_requested",
            "cancelled",
            "queued"
        ]
    );
    assert_eq!(
        error_code(&cancel(&world, &owner, file_id).await),
        "FILE_NOT_PROCESSING"
    );
    jobs.expect_no_command(Duration::from_millis(500)).await;

    world.cleanup().await;
}

#[tokio::test]
async fn a_cancel_that_crosses_a_steps_completion_stops_the_chain_there() {
    // Given: a two-step rule, and a file whose first step's runner reported done
    let world = World::start("pod-cancel-crossed").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    ok(&create_ruleset(
        &world,
        &manager,
        RuleSpec {
            name: "render then index",
            trigger: "UPLOAD",
            media_types: &["text/plain"],
            steps: &[
                (RENDER, serde_json::json!({})),
                (INDEX, serde_json::json!({})),
            ],
            is_default: true,
        },
    )
    .await);
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "crossed.txt", BYTES),
    )
    .await;
    let first = jobs.await_create(file_id).await.job_id;
    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id: first,
            pages: vec![(1, "done")],
            origin: None,
            indexer: None,
            done: true,
        },
    )
    .await);
    jobs.await_finish(first).await;

    // When: the owner cancels while Jobs finishes the job — Jobs refuses to
    // cancel a job it already finished and says nothing — then Jobs completes it
    jobs.drop_cancels(true);
    ok(&cancel(&world, &owner, file_id).await);
    jobs.await_cancel(first).await;
    let mut files = drive_subscription(&world, &owner, drive).await;
    jobs.complete(first).await;

    // Then: the chain stops there: FAILED `cancelled`, no second job asked for
    let failed = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "ProcessingFailed"
    })
    .await;
    assert_eq!(failed["cause"]["reason"], "cancelled");
    assert_eq!(failed["view"]["processingError"], "cancelled");
    assert_eq!(failed["view"]["affordances"]["process"]["allowed"], true);
    jobs.expect_no_command(Duration::from_secs(1)).await;
    let log = world.job_log(file_id).await;
    assert_eq!(log.len(), 2, "the step never started is recorded as such");
    assert_eq!(log[1].1, 1);
    assert_eq!(log[1].2.len(), 1);
    assert_eq!(log[1].2[0]["kind"], "cancelled");
    assert_eq!(
        world.file_pages(&owner, file_id).await.len(),
        1,
        "the first step's work stays"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn a_runner_type_jobs_retired_fails_the_file_at_creation() {
    // Given: a render rule whose runner type Jobs retired
    let world = World::start("pod-retired-runner").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    install_render_rule(&world, &manager).await;
    jobs.retire_runner_type(RENDER);
    let drive = world.create_workspace(&owner, "library").await;
    let mut files = drive_subscription(&world, &owner, drive).await;

    // When: a file is uploaded
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "retired.txt", BYTES),
    )
    .await;

    // Then: Jobs refuses the job and the file fails with Jobs' own code
    let (create, refusal) = jobs.await_refused_create(file_id).await;
    assert_eq!(refusal.reason_code, "runner_type_retired");
    let failed = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "ProcessingFailed"
    })
    .await;
    assert_eq!(failed["cause"]["reason"], "runner_type_retired");
    assert_eq!(failed["view"]["processingError"], "runner_type_retired");
    assert_eq!(
        world.job_events(create.job_id).await,
        vec!["creation_rejected"]
    );
    jobs.expect_no_command(Duration::from_secs(1)).await;

    world.cleanup().await;
}

/// A committed file whose job Jobs refused because Jobs still holds `stray` on it.
async fn trapped(world: &World, jobs: &JobsStandIn, owner: &str, drive: Uuid) -> (Uuid, Uuid) {
    let upload_request = UploadRequest::text(drive, "", "restored.txt", BYTES);
    let file_id = Uuid::now_v7();
    let upload_ticket = ticket(&request(world, owner, file_id, &upload_request).await);
    let stray = jobs.hold_source(br_drive_example::SERVICE, file_id);
    let posted = post_bytes(world, &upload_ticket, BYTES, "restored.txt").await;
    assert!((200..300).contains(&posted), "the bytes land: {posted}");
    ok(&commit(world, owner, file_id).await);
    let (_, refusal) = jobs.await_refused_create(file_id).await;
    assert_eq!(refusal.reason_code, "duplicate_active_entity");
    assert_eq!(refusal.params["activeJobId"], stray.to_string());
    (file_id, stray)
}

#[tokio::test]
async fn a_create_refused_for_a_forgotten_live_job_cancels_it_and_a_reprocess_gets_through() {
    // Given: Jobs still holds a live job on the file the host is about to commit
    // (a lost `job.finish`, a restored host database)
    let world = World::start("pod-duplicate-trap").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    install_render_rule(&world, &manager).await;
    let drive = world.create_workspace(&owner, "library").await;
    let mut files = drive_subscription(&world, &owner, drive).await;

    // When: the upload is committed and Jobs refuses its job
    let (file_id, stray) = trapped(&world, &jobs, &owner, drive).await;

    // Then: the file fails with the reason, and the forgotten job is cancelled
    let failed = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "ProcessingFailed"
    })
    .await;
    assert_eq!(failed["view"]["processingError"], "duplicate_active_entity");
    assert_eq!(failed["view"]["affordances"]["process"]["allowed"], true);
    jobs.await_cancel(stray).await;
    assert!(!jobs.is_live(stray));

    // And: a reprocess gets a job Jobs accepts at once, and lands READY
    ok(&world
        .gql(&owner, PROCESS, serde_json::json!({ "f": file_id }))
        .await);
    let accepted = jobs.await_create(file_id).await.job_id;
    done(&world, &jobs, &runner, file_id, accepted).await;
    world.await_state(&owner, file_id, "READY").await;
    jobs.expect_no_command(Duration::from_secs(1)).await;

    world.cleanup().await;
}

#[tokio::test]
async fn every_duplicate_rejection_cancels_the_forgotten_job_again_until_one_takes() {
    // Given: a file trapped by a forgotten job whose first cancel Jobs drops
    let world = World::start("pod-duplicate-again").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    install_render_rule(&world, &manager).await;
    let drive = world.create_workspace(&owner, "library").await;
    jobs.drop_cancels(true);
    let (file_id, stray) = trapped(&world, &jobs, &owner, drive).await;
    jobs.await_cancel(stray).await;
    world.await_state(&owner, file_id, "FAILED").await;
    assert!(jobs.is_live(stray));
    jobs.drop_cancels(false);

    // When: the owner reprocesses while the stray is still live
    ok(&world
        .gql(&owner, PROCESS, serde_json::json!({ "f": file_id }))
        .await);

    // Then: Jobs refuses again, naming the same job, and it is cancelled again
    let (_, refusal) = jobs.await_refused_create(file_id).await;
    assert_eq!(refusal.params["activeJobId"], stray.to_string());
    jobs.await_cancel(stray).await;
    world.await_state(&owner, file_id, "FAILED").await;
    assert!(!jobs.is_live(stray));

    // And: the next reprocess gets through to READY
    ok(&world
        .gql(&owner, PROCESS, serde_json::json!({ "f": file_id }))
        .await);
    let accepted = jobs.await_create(file_id).await.job_id;
    done(&world, &jobs, &runner, file_id, accepted).await;
    world.await_state(&owner, file_id, "READY").await;
    jobs.expect_no_command(Duration::from_secs(1)).await;

    world.cleanup().await;
}

#[tokio::test]
async fn a_duplicate_rejection_naming_another_file_cancels_nothing() {
    // Given: a file whose job Jobs refuses as a duplicate, naming a job of
    // another source entity
    let world = World::start("pod-duplicate-foreign").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    install_render_rule(&world, &manager).await;
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "foreign.txt", BYTES),
    )
    .await;
    let job = jobs.await_create(file_id).await.job_id;
    let foreign = Uuid::now_v7();

    // When: Jobs says a live job of another file blocks this one
    jobs.reject_creation_with(
        job,
        "duplicate_active_entity",
        serde_json::json!({
            "activeJobId": foreign,
            "sourceEntityId": Uuid::now_v7(),
            "sourceBc": br_drive_example::SERVICE,
        }),
    )
    .await;

    // Then: the file fails, and no job of anyone else is cancelled
    let file = world.await_state(&owner, file_id, "FAILED").await;
    assert_eq!(file["processingError"], "duplicate_active_entity");
    jobs.expect_no_command(Duration::from_secs(1)).await;

    world.cleanup().await;
}

#[tokio::test]
async fn deleting_a_file_that_failed_asks_jobs_nothing() {
    // Given: a file failed on a job Jobs still held on it, which was cancelled
    let world = World::start("pod-duplicate-trap-delete").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    install_render_rule(&world, &manager).await;
    let drive = world.create_workspace(&owner, "library").await;
    let (file_id, stray) = trapped(&world, &jobs, &owner, drive).await;
    world.await_state(&owner, file_id, "FAILED").await;
    jobs.await_cancel(stray).await;

    // When: the owner deletes it
    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!){workspaceDeleteFile(fileId:$f){success}}",
            serde_json::json!({ "f": file_id }),
        )
        .await);

    // Then: no job runs on it, so nothing is asked of Jobs
    jobs.expect_no_command(Duration::from_secs(1)).await;
    assert!(
        world.job_log(file_id).await.is_empty(),
        "the log goes with the file"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn a_late_fact_of_an_old_job_changes_nothing() {
    // Given: a file whose first job was cancelled and whose reprocess runs a second one
    let world = World::start("pod-stale-facts").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    install_render_rule(&world, &manager).await;
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "busy.txt", BYTES),
    )
    .await;
    let old = jobs.await_create(file_id).await.job_id;
    ok(&cancel(&world, &owner, file_id).await);
    jobs.await_cancel(old).await;
    world.await_state(&owner, file_id, "FAILED").await;
    ok(&world
        .gql(&owner, PROCESS, serde_json::json!({ "f": file_id }))
        .await);
    let current = jobs.await_create(file_id).await.job_id;
    let run = Uuid::now_v7();
    jobs.start(current, run).await;
    jobs.declare_plan(current, run, &["read", "write"]).await;
    jobs.start_step(current, run, 0, "read").await;
    crate::poll_until!(Duration::from_secs(15), {
        (world.job_events(current).await.len() == 4).then_some(())
    });
    let progress = crate::poll_until!(Duration::from_secs(15), {
        let file = world.file(&owner, file_id).await;
        (file["progress"]["currentLabel"] == "read"
            && file["progress"]["plan"] == serde_json::json!(["read", "write"]))
        .then_some(file["progress"].clone())
    });
    let mut files = drive_subscription(&world, &owner, drive).await;

    // When: facts of the old job arrive late — a start, a plan, a step, even a
    // completion and a failure
    let stale_run = Uuid::now_v7();
    jobs.start(old, stale_run).await;
    jobs.declare_plan(old, stale_run, &["stale"]).await;
    jobs.start_step(old, stale_run, 5, "stale").await;
    jobs.complete(old).await;
    jobs.fail(old, "RUNNER_REPORTED", Some("stale_failure"))
        .await;

    // Then: they are logged on the old job only — each fact type rides its own
    // durable, so their arrival order is the broker's — and the file does not move
    let mut expected = vec![
        "queued",
        "cancel_requested",
        "cancelled",
        "completed",
        "failed",
        "plan_declared",
        "started",
        "step_started",
    ];
    expected.sort_unstable();
    crate::poll_until!(Duration::from_secs(15), {
        let mut events = world.job_events(old).await;
        events.sort_unstable();
        (events == expected).then_some(())
    });
    refute_delta(
        &mut files,
        "workspaceDriveChanged",
        Duration::from_millis(800),
        |node| {
            let shown = &node["view"]["progress"];
            node["view"]["processingState"] != "PROCESSING"
                || shown["stepIndex"] != progress["stepIndex"]
                || shown["plan"] != progress["plan"]
                || shown["currentIndex"] != progress["currentIndex"]
                || shown["currentLabel"] != progress["currentLabel"]
        },
    )
    .await;
    let file = world.file(&owner, file_id).await;
    assert_eq!(file["processingState"], "PROCESSING");
    assert_eq!(file["progress"], progress);
    assert_eq!(world.job_of(file_id).await, Some(current));
    let mut current_events = world.job_events(current).await;
    current_events.sort_unstable();
    assert_eq!(
        current_events,
        vec!["plan_declared", "queued", "started", "step_started"],
        "the current job's log is untouched by the old job's facts"
    );
    jobs.expect_no_command(Duration::from_millis(500)).await;

    // When: the current job ends, then one of its own progress facts arrives late
    done(&world, &jobs, &runner, file_id, current).await;
    world.await_state(&owner, file_id, "READY").await;
    jobs.start_step(current, run, 1, "write").await;
    crate::poll_until!(Duration::from_secs(15), {
        (world.job_events(current).await.last().map(String::as_str) == Some("step_started"))
            .then_some(())
    });

    // Then: a READY file stays READY
    assert_eq!(
        world.file(&owner, file_id).await["processingState"],
        "READY"
    );
    jobs.expect_no_command(Duration::from_millis(500)).await;

    world.cleanup().await;
}

#[tokio::test]
async fn two_concurrent_reprocesses_of_one_file_start_exactly_one_job() {
    // Given: a FAILED file
    let world = World::start("pod-concurrent-process").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    install_render_rule(&world, &manager).await;
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "race.txt", BYTES),
    )
    .await;
    let first = jobs.await_create(file_id).await.job_id;
    jobs.fail(first, "RUNNER_REPORTED", Some("boom")).await;
    world.await_state(&owner, file_id, "FAILED").await;

    // When: two reprocesses race
    let (a, b) = tokio::join!(
        world.gql(&owner, PROCESS, serde_json::json!({ "f": file_id })),
        world.gql(&owner, PROCESS, serde_json::json!({ "f": file_id })),
    );

    // Then: exactly one is acked and one job is asked for; the other sees the
    // file PROCESSING
    let acked = [&a, &b]
        .iter()
        .filter(|response| response.get("errors").is_none())
        .count();
    assert_eq!(acked, 1, "{a} / {b}");
    let refused = if a.get("errors").is_some() { &a } else { &b };
    assert_eq!(error_code(refused), "FILE_PROCESSING");
    jobs.await_create(file_id).await;
    jobs.expect_no_command(Duration::from_secs(1)).await;
    assert_eq!(world.job_log(file_id).await.len(), 2);

    world.cleanup().await;
}

#[tokio::test]
async fn what_the_chain_tells_jobs_is_always_something_jobs_accepts() {
    // Given: a two-step rule, and an owner whose passport names them with blanks only
    let world = World::start("pod-jobs-shaped").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner_id = Uuid::now_v7();
    let owner = manager_passport(owner_id, "   ");
    let runner = service_passport(&[RUNNER_SCOPE]);
    ok(&create_ruleset(
        &world,
        &manager,
        RuleSpec {
            name: "render then index",
            trigger: "UPLOAD",
            media_types: &["text/plain"],
            steps: &[
                (RENDER, serde_json::json!({})),
                (INDEX, serde_json::json!({})),
            ],
            is_default: true,
        },
    )
    .await);
    let drive = world.create_workspace(&owner, "library").await;

    // When: a file id that is not a UUIDv7 is offered
    let refused = request(
        &world,
        &owner,
        Uuid::new_v4(),
        &UploadRequest::text(drive, "", "v4.txt", BYTES),
    )
    .await;
    // Then: it is refused before anything exists — Jobs would refuse every job of it
    assert_eq!(error_code(&refused), "INVALID_FILE_ID");
    assert!(world.drive_files(&owner, drive).await.is_empty());

    // When: the owner uploads a file
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "notes.txt", BYTES),
    )
    .await;
    // Then: the blank name is not sent: the owner is named anonymously
    let first = jobs.await_create(file_id).await;
    assert_eq!(first.triggered_by, Some(TriggeredBy::Anonymous(owner_id)));

    // When: the owner is erased while the chain runs, then the first step ends
    world.erase(owner_id).await;
    done(&world, &jobs, &runner, file_id, first.job_id).await;

    // Then: the next step names no initiator at all, and Jobs accepts it
    let second = jobs.await_create(file_id).await;
    assert_eq!(second.runner_type, INDEX);
    assert_eq!(second.triggered_by, None);
    done(&world, &jobs, &runner, file_id, second.job_id).await;
    world.await_state(&owner, file_id, "READY").await;

    world.cleanup().await;
}
