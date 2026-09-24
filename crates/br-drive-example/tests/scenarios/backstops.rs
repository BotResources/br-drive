//! The chain's contract with the real Jobs service, and the backstops that
//! keep a file from sitting in PROCESSING for good.

use std::time::Duration;

use contract_jobs::command::CreateJob;
use uuid::Uuid;

use crate::harness::runner::{
    RENDER, RUNNER_SCOPE, Report, RuleSpec, context, create_ruleset, finish_job,
    install_render_rule, report,
};
use crate::harness::upload::{UploadRequest, commit, post_bytes, request, ticket, upload};
use crate::harness::{
    JobsStandIn, World, WorldOptions, drive_subscription, error_code, manager_passport,
    next_delta_within, next_drive_delta, ok, passport, service_passport,
};

const BYTES: &[u8] = b"a document the runners never pick up";

fn create(source_entity_id: Uuid, parent_job_id: Option<Uuid>) -> CreateJob {
    CreateJob {
        job_id: Uuid::now_v7(),
        runner_type: RENDER.to_string(),
        producer: "another-producer".to_string(),
        config: None,
        parent_job_id,
        triggered_by: None,
        source_bc: Some("another-producer".to_string()),
        source_entity_id: Some(source_entity_id),
        max_attempts: None,
    }
}

#[tokio::test]
async fn the_jobs_double_refuses_what_jobs_refuses_a_settled_or_deleted_parent_and_a_busy_source() {
    // Given: the double judging creates the way Jobs' `create_job` does
    let world = World::start("pod-jobs-double").await;
    let jobs = JobsStandIn::attach(&world).await;

    // When: a job is created, then settles, then is named as a parent
    let parent = create(Uuid::now_v7(), None);
    jobs.submit_create(&parent).await;
    jobs.await_create(parent.source_entity_id.unwrap()).await;
    assert!(jobs.is_live(parent.job_id), "a fresh root job is accepted");
    jobs.complete(parent.job_id).await;
    let child = create(Uuid::now_v7(), Some(parent.job_id));
    jobs.submit_create(&child).await;

    // Then: the child is refused, naming the terminal parent
    let refused = jobs.await_rejection(child.job_id).await;
    assert_eq!(refused.reason_code, "parent_job_terminal");
    assert_eq!(refused.params["parentJobId"], parent.job_id.to_string());
    assert!(!jobs.is_live(child.job_id));

    // And: a deleted parent and an unknown one are refused too
    let deleted = create(Uuid::now_v7(), None);
    jobs.submit_create(&deleted).await;
    jobs.await_create(deleted.source_entity_id.unwrap()).await;
    jobs.delete_job(deleted.job_id);
    let under_deleted = create(Uuid::now_v7(), Some(deleted.job_id));
    jobs.submit_create(&under_deleted).await;
    assert_eq!(
        jobs.await_rejection(under_deleted.job_id).await.reason_code,
        "parent_job_deleted"
    );
    let orphan = create(Uuid::now_v7(), Some(Uuid::now_v7()));
    jobs.submit_create(&orphan).await;
    assert_eq!(
        jobs.await_rejection(orphan.job_id).await.reason_code,
        "parent_job_unknown"
    );

    // And: a second live job on one source entity is refused, naming the first
    let entity = Uuid::now_v7();
    let first = create(entity, None);
    jobs.submit_create(&first).await;
    jobs.await_create(entity).await;
    let second = create(entity, None);
    jobs.submit_create(&second).await;
    let busy = jobs.await_rejection(second.job_id).await;
    assert_eq!(busy.reason_code, "duplicate_active_entity");
    assert_eq!(busy.params["activeJobId"], first.job_id.to_string());

    world.cleanup().await;
}

#[tokio::test]
async fn a_step_no_runner_ever_picks_up_times_out_cancels_its_job_and_can_be_reprocessed() {
    // Given: a render rule and a runner type that is ACTIVE but has no live instance
    let world = World::start("pod-step-timeout").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    install_render_rule(&world, &jobs, &manager).await;
    let drive = world.create_workspace(&owner, "library").await;
    let mut files = drive_subscription(&world, &owner, drive).await;

    // When: a file is uploaded and Jobs never dispatches its job
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "stuck.txt", BYTES),
    )
    .await;
    let stuck = jobs.await_create(file_id).await.job_id;

    // Then: past the host's step timeout the job is cancelled and the file fails `timed_out`
    let timed_out = next_delta_within(
        &mut files,
        "workspaceDriveChanged",
        Duration::from_secs(40),
        |node| node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "ProcessingFailed",
    )
    .await;
    assert_eq!(timed_out["cause"]["reason"], "timed_out");
    assert_eq!(timed_out["view"]["processingState"], "FAILED");
    assert_eq!(timed_out["view"]["processingError"], "timed_out");
    assert!(timed_out["view"]["progress"].is_null());
    assert_eq!(timed_out["view"]["affordances"]["process"]["allowed"], true);
    jobs.await_cancel(stuck).await;
    assert_eq!(
        error_code(&context(&world, &runner, file_id, stuck).await),
        "JOB_NOT_ACTIVE",
        "the timed-out job no longer opens the file"
    );

    // And: a reprocess starts over with a job Jobs accepts
    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!){workspaceProcess(fileId:$f){success}}",
            serde_json::json!({ "f": file_id }),
        )
        .await);
    let retry = jobs.await_create(file_id).await.job_id;
    assert_ne!(retry, stuck);
    assert!(!jobs.rejected(retry));
    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id: retry,
            pages: vec![(1, "picked up at last")],
            origin: None,
            indexer: None,
            done: true,
        },
    )
    .await);
    finish_job(&world, &jobs, retry).await;
    world.await_state(&owner, file_id, "READY").await;

    world.cleanup().await;
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
    install_render_rule(&world, &jobs, &manager).await;
    let drive = world.create_workspace(&owner, "library").await;
    let mut files = drive_subscription(&world, &owner, drive).await;
    let upload_request = UploadRequest::text(drive, "", "restored.txt", BYTES);
    let file_id = Uuid::now_v7();
    let upload_ticket = ticket(&request(&world, &owner, file_id, &upload_request).await);
    let stray = jobs.hold_source(br_drive_example::SERVICE, file_id);
    let posted = post_bytes(&world, &upload_ticket, BYTES, "restored.txt").await;
    assert!((200..300).contains(&posted), "the bytes land: {posted}");

    // When: the upload is committed and its chain asks Jobs for a job
    ok(&commit(&world, &owner, file_id).await);
    let refused = jobs.await_create(file_id).await.job_id;

    // Then: Jobs refuses it, the file fails with the reason, and the forgotten job is cancelled
    assert_eq!(
        jobs.await_rejection(refused).await.reason_code,
        "duplicate_active_entity"
    );
    let failed = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "ProcessingFailed"
    })
    .await;
    assert_eq!(failed["view"]["processingError"], "duplicate_active_entity");
    assert_eq!(failed["view"]["affordances"]["process"]["allowed"], true);
    jobs.await_cancel(stray).await;
    assert!(!jobs.is_live(stray));

    // And: a reprocess gets a job Jobs accepts, and the chain lands READY
    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!){workspaceProcess(fileId:$f){success}}",
            serde_json::json!({ "f": file_id }),
        )
        .await);
    let accepted = jobs.await_create(file_id).await.job_id;
    assert!(!jobs.rejected(accepted), "the source is free again");
    assert!(jobs.is_live(accepted));
    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id: accepted,
            pages: vec![(1, "recovered")],
            origin: None,
            indexer: None,
            done: true,
        },
    )
    .await);
    finish_job(&world, &jobs, accepted).await;
    world.await_state(&owner, file_id, "READY").await;

    world.cleanup().await;
}

#[tokio::test]
async fn a_chain_fired_before_the_first_catalogue_scan_waits_for_it_instead_of_failing() {
    // Given: a fresh host whose catalogue watch has not scanned yet
    let world = World::start_with(
        "pod-unscanned-chain",
        WorldOptions {
            watch_catalogue: false,
            ..WorldOptions::default()
        },
    )
    .await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);

    // When: a manager saves a rule
    let saved = create_ruleset(
        &world,
        &manager,
        RuleSpec {
            name: "render text",
            trigger: "UPLOAD",
            media_types: &["text/plain"],
            steps: &[(RENDER, serde_json::json!({}))],
            is_default: true,
        },
    )
    .await;

    // Then: the rule is kept, its step flagged as not known to be ACTIVE yet
    assert_eq!(
        ok(&saved)["workspaceCreateRuleset"]["unknownRunnerTypes"],
        serde_json::json!([RENDER])
    );

    // When: a file is uploaded before the first scan
    jobs.declare_runner_type(RENDER, contract_jobs::catalog::RunnerTypeLifecycle::Active)
        .await;
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "early.txt", BYTES),
    )
    .await;

    // Then: the file waits in its first step, with no job and no failure
    let waiting = world.file(&owner, file_id).await;
    assert_eq!(waiting["processingState"], "PROCESSING");
    assert!(waiting["processingError"].is_null());
    assert_eq!(waiting["progress"]["stepIndex"], 0);
    assert_eq!(waiting["progress"]["runnerType"], RENDER);
    jobs.expect_no_command(Duration::from_secs(6)).await;
    assert_eq!(
        world.file(&owner, file_id).await["processingState"],
        "PROCESSING",
        "a deferred launch retried before the scan defers again"
    );

    // When: the host starts its catalogue watch
    world.service.start_catalogue_watch().await;

    // Then: the deferred step launches on its own and the chain completes
    let job = jobs.await_create(file_id).await.job_id;
    assert!(!jobs.rejected(job));
    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id: job,
            pages: vec![(1, "late start")],
            origin: None,
            indexer: None,
            done: true,
        },
    )
    .await);
    finish_job(&world, &jobs, job).await;
    world.await_state(&owner, file_id, "READY").await;

    world.cleanup().await;
}
