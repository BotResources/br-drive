//! The chain's contract with the real Jobs service, and the backstops that
//! keep a file from sitting in PROCESSING for good.

use std::time::{Duration, Instant};

use br_drive::DriveHost;
use br_drive_example::kernel::AppPrincipal;

use contract_jobs::command::TriggeredBy;
use uuid::Uuid;

use crate::harness::runner::{
    INDEX, RENDER, RUNNER_SCOPE, Report, RuleSpec, context, create_ruleset, finish_job,
    install_render_rule, report,
};
use crate::harness::upload::{UploadRequest, commit, post_bytes, request, ticket, upload};
use crate::harness::{
    JobsStandIn, World, WorldOptions, drive_subscription, error_code, manager_passport,
    next_delta_within, next_drive_delta, ok, passport, service_passport,
};

const BYTES: &[u8] = b"a document the runners never pick up";
const PROCESS: &str = "mutation($f:UUID!){workspaceProcess(fileId:$f){success}}";

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

/// The next two commands jobs receives are the cancel of `stray`, then a
/// create for `file_id` — in that order, which is the order they were staged.
async fn cancel_then_create(jobs: &JobsStandIn, stray: Uuid, file_id: Uuid) -> Uuid {
    let (first, cancel) = jobs
        .next_command(Duration::from_secs(15))
        .await
        .expect("the relaunch cancels the stray job first");
    assert_eq!(first, contract_jobs::CMD_JOB_CANCEL_V2);
    assert_eq!(cancel["job_id"], stray.to_string());
    let (second, create) = jobs
        .next_command(Duration::from_secs(15))
        .await
        .expect("then asks for a job");
    assert_eq!(second, contract_jobs::CMD_JOB_CREATE_V1);
    assert_eq!(create["source_entity_id"], file_id.to_string());
    let job_id = Uuid::parse_str(create["job_id"].as_str().unwrap()).unwrap();
    assert!(
        !jobs.rejected(job_id),
        "the source is free: Jobs accepts it"
    );
    job_id
}

/// The example host's two deadlines: a job must be picked up within
/// `PICKUP_TIMEOUT` of its creation, a started run may stay silent for
/// `STEP_TIMEOUT`.
const PICKUP_TIMEOUT: Duration = <AppPrincipal as DriveHost>::PICKUP_TIMEOUT;
const STEP_TIMEOUT: Duration = <AppPrincipal as DriveHost>::STEP_TIMEOUT;

#[tokio::test]
async fn a_job_no_runner_picks_up_times_out_at_the_pickup_deadline_and_can_be_reprocessed() {
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
    let created = Instant::now();

    // Then: past the pickup deadline — well before the run-silence one — the
    // job is cancelled and the file fails `timed_out`
    let timed_out = next_delta_within(
        &mut files,
        "workspaceDriveChanged",
        Duration::from_secs(75),
        |node| node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "ProcessingFailed",
    )
    .await;
    let waited = created.elapsed();
    assert!(
        waited + Duration::from_secs(1) >= PICKUP_TIMEOUT && waited < STEP_TIMEOUT,
        "the pickup deadline ({PICKUP_TIMEOUT:?}) fired, not the run-silence one: {waited:?}"
    );
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

    // And: a reprocess cancels that job again before asking for a new one, and lands READY
    ok(&world
        .gql(&owner, PROCESS, serde_json::json!({ "f": file_id }))
        .await);
    let retry = cancel_then_create(&jobs, stuck, file_id).await;
    assert_ne!(retry, stuck);
    done(&world, &jobs, &runner, file_id, retry).await;
    world.await_state(&owner, file_id, "READY").await;
    jobs.expect_no_command(Duration::from_secs(1)).await;

    world.cleanup().await;
}

#[tokio::test]
async fn a_started_run_outlives_the_pickup_deadline_and_times_out_only_once_silent() {
    // Given: a render rule and a file whose job a runner picked up
    let world = World::start("pod-run-silence").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    install_render_rule(&world, &jobs, &manager).await;
    let drive = world.create_workspace(&owner, "library").await;
    let mut files = drive_subscription(&world, &owner, drive).await;
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "slow.txt", BYTES),
    )
    .await;
    let job = jobs.await_create(file_id).await.job_id;

    // When: Jobs reports the run started, and the runner then falls silent
    jobs.start(job, Uuid::now_v7()).await;
    let started = Instant::now();

    // Then: the pickup deadline passes without failing the file; the
    // run-silence deadline, measured from the start, cancels the job and
    // fails it `timed_out`
    let timed_out = next_delta_within(
        &mut files,
        "workspaceDriveChanged",
        Duration::from_secs(90),
        |node| node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "ProcessingFailed",
    )
    .await;
    let silent = started.elapsed();
    assert!(
        silent + Duration::from_secs(1) >= STEP_TIMEOUT,
        "a started run is failed only after {STEP_TIMEOUT:?} of silence, not at the pickup \
         deadline ({PICKUP_TIMEOUT:?}): {silent:?}"
    );
    assert_eq!(timed_out["cause"]["reason"], "timed_out");
    assert_eq!(timed_out["view"]["processingError"], "timed_out");
    jobs.await_cancel(job).await;
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
    install_render_rule(&world, &jobs, &manager).await;
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

    // And: a reprocess cancels it again, then gets a job Jobs accepts, and lands READY
    ok(&world
        .gql(&owner, PROCESS, serde_json::json!({ "f": file_id }))
        .await);
    let accepted = cancel_then_create(&jobs, stray, file_id).await;
    done(&world, &jobs, &runner, file_id, accepted).await;
    world.await_state(&owner, file_id, "READY").await;
    jobs.expect_no_command(Duration::from_secs(1)).await;

    world.cleanup().await;
}

#[tokio::test]
async fn deleting_a_file_trapped_by_a_forgotten_job_cancels_that_job_too() {
    // Given: a file failed on a job Jobs still held on it
    let world = World::start("pod-duplicate-trap-delete").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    install_render_rule(&world, &jobs, &manager).await;
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

    // Then: the job it still knew of is cancelled with it, and nothing else is asked
    jobs.await_cancel(stray).await;
    jobs.expect_no_command(Duration::from_secs(1)).await;

    world.cleanup().await;
}

#[tokio::test]
async fn a_chain_fired_before_the_first_catalogue_scan_waits_for_it_instead_of_failing() {
    // Given: a fresh host whose catalogue watch has not scanned yet, while Jobs
    // already publishes both runner types as ACTIVE
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
    for runner_type in [RENDER, INDEX] {
        jobs.declare_runner_type(
            runner_type,
            contract_jobs::catalog::RunnerTypeLifecycle::Active,
        )
        .await;
    }

    // When: a manager saves a two-step rule
    let saved = create_ruleset(
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
    .await;

    // Then: the rule is kept, and the host vouches for none of its steps yet
    assert_eq!(
        ok(&saved)["workspaceCreateRuleset"]["unknownRunnerTypes"],
        serde_json::json!([INDEX, RENDER])
    );

    // When: a file is uploaded before the first scan, watched by its owner
    let drive = world.create_workspace(&owner, "library").await;
    let mut files = drive_subscription(&world, &owner, drive).await;
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "early.txt", BYTES),
    )
    .await;

    // Then: the file waits in its first step — no job, no failure, no reprocess
    let waiting = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["view"]["processingState"] == "PROCESSING"
    })
    .await;
    assert!(waiting["view"]["processingError"].is_null());
    assert_eq!(waiting["view"]["progress"]["stepIndex"], 0);
    assert_eq!(waiting["view"]["progress"]["runnerType"], RENDER);
    assert_eq!(
        waiting["view"]["affordances"]["process"]["reason"],
        "FILE_PROCESSING"
    );
    // And: a retry that finds no scan yet asks Jobs nothing and changes nothing
    jobs.expect_no_command(Duration::from_secs(6)).await;
    while let Some(delta) = files.try_next_payload(Duration::from_millis(300)).await {
        let node = &delta["workspaceDriveChanged"];
        assert_eq!(
            node["view"]["processingState"], "PROCESSING",
            "the file keeps waiting: {delta}"
        );
        assert!(
            !matches!(
                node["cause"]["kind"].as_str(),
                Some("ProcessingStarted" | "ProcessingFailed")
            ),
            "nothing launches or fails before the scan: {delta}"
        );
    }

    // When: the host starts its catalogue watch
    world.service.start_catalogue_watch().await;

    // Then: the deferred step launches on its own and the chain completes
    let started = next_delta_within(
        &mut files,
        "workspaceDriveChanged",
        Duration::from_secs(20),
        |node| node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "ProcessingStarted",
    )
    .await;
    assert_eq!(started["cause"]["step"], 0);
    let render = jobs.await_create(file_id).await.job_id;
    assert_eq!(started["cause"]["job_id"], render.to_string());
    done(&world, &jobs, &runner, file_id, render).await;
    let index = jobs.await_create(file_id).await.job_id;
    done(&world, &jobs, &runner, file_id, index).await;
    world.await_state(&owner, file_id, "READY").await;

    world.cleanup().await;
}

#[tokio::test]
async fn a_chain_on_a_host_that_never_scans_times_out_without_asking_jobs_anything() {
    // Given: a host that never starts its catalogue watch, and a rule
    let world = World::start_with(
        "pod-never-scanned",
        WorldOptions {
            watch_catalogue: false,
            ..WorldOptions::default()
        },
    )
    .await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    ok(&create_ruleset(
        &world,
        &manager,
        RuleSpec {
            name: "render",
            trigger: "UPLOAD",
            media_types: &["text/plain"],
            steps: &[(RENDER, serde_json::json!({}))],
            is_default: true,
        },
    )
    .await);
    let drive = world.create_workspace(&owner, "library").await;
    let mut files = drive_subscription(&world, &owner, drive).await;

    // When: a file is uploaded
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "forgotten.txt", BYTES),
    )
    .await;

    // Then: the deferred step's deadline bounds the wait: the file fails `timed_out`
    let timed_out = next_delta_within(
        &mut files,
        "workspaceDriveChanged",
        Duration::from_secs(75),
        |node| node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "ProcessingFailed",
    )
    .await;
    assert_eq!(timed_out["view"]["processingError"], "timed_out");
    assert_eq!(world.job_of(file_id).await, None);
    // And: no job was ever asked for, so none is cancelled
    jobs.expect_no_command(Duration::from_secs(1)).await;

    world.cleanup().await;
}

#[tokio::test]
async fn a_step_message_outliving_its_step_or_delivered_twice_changes_nothing() {
    // Given: a file in its first step, its job staged and accepted
    let world = World::start("pod-stale-step-messages").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    install_render_rule(&world, &jobs, &manager).await;
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "busy.txt", BYTES),
    )
    .await;
    let job = jobs.await_create(file_id).await.job_id;
    world.await_source_promoted(file_id).await;
    let (step, entered) = world.step_clock(file_id).await;
    let (step, entered) = (step.expect("in a step"), entered.expect("entered"));
    let mut files = drive_subscription(&world, &owner, drive).await;
    while files
        .try_next_payload(Duration::from_millis(300))
        .await
        .is_some()
    {}

    // When: the step's own deadline arrives early, deadlines for another step,
    // another entry and another file arrive, and the launch retry is redelivered
    let message = |step: i32, entered: chrono::DateTime<chrono::Utc>, file: Uuid| serde_json::json!({ "file_id": file, "step": step, "entered_at": entered });
    let earlier = entered - chrono::TimeDelta::microseconds(1);
    for payload in [
        message(step, entered, file_id),
        message(step + 1, entered, file_id),
        message(step, earlier, file_id),
        message(step, entered, Uuid::now_v7()),
    ] {
        world.send_file_command("step-deadline", payload).await;
    }
    world
        .send_file_command("launch-retry", message(step, entered, file_id))
        .await;

    // Then: nothing moves — no delta, no second job, the step still running
    files.expect_silence(Duration::from_millis(800)).await;
    jobs.expect_no_command(Duration::from_millis(500)).await;
    let file = world.file(&owner, file_id).await;
    assert_eq!(file["processingState"], "PROCESSING");
    assert_eq!(world.job_of(file_id).await, Some(job));

    // When: the chain ends, and the step's deadline is delivered once more
    done(&world, &jobs, &runner, file_id, job).await;
    world.await_state(&owner, file_id, "READY").await;
    world
        .send_file_command("step-deadline", message(step, entered, file_id))
        .await;

    // Then: a READY file stays READY
    jobs.expect_no_command(Duration::from_millis(800)).await;
    assert_eq!(
        world.file(&owner, file_id).await["processingState"],
        "READY"
    );

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
    for runner_type in [RENDER, INDEX] {
        jobs.declare_runner_type(
            runner_type,
            contract_jobs::catalog::RunnerTypeLifecycle::Active,
        )
        .await;
        world
            .await_known_runner_type(runner_type, Some("active"))
            .await;
    }
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
