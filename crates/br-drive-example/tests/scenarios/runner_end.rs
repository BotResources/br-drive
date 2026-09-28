//! The runner declares the end of its job, through the host: its final report
//! (`done`) or its declared failure. The library records it on the job's log,
//! moves the file at once and tells Jobs (`job.finish` / `job.fail`); what
//! Jobs says afterwards about that job is logged and changes nothing.

use std::time::Duration;

use uuid::Uuid;

use crate::harness::runner::{
    INDEX, RENDER, RUNNER_SCOPE, Report, RuleSpec, context, create_ruleset, install_render_rule,
    report,
};
use crate::harness::upload::{UploadRequest, upload_processed};
use crate::harness::{
    JobsStandIn, World, drive_subscription, error_code, manager_passport, next_drive_delta, ok,
    passport, quiet, refute_delta, service_passport,
};

const BYTES: &[u8] = b"a document a runner works on";
const REPORT_FAILURE: &str = "mutation($f:UUID!,$j:UUID!,$c:String!,$m:String){\
     workspaceRunnerReportFailure(fileId:$f,jobId:$j,reasonCode:$c,message:$m){success}}";

async fn report_failure(
    world: &World,
    passport: &str,
    file_id: Uuid,
    job_id: Uuid,
    reason_code: &str,
    message: Option<&str>,
) -> serde_json::Value {
    world
        .gql(
            passport,
            REPORT_FAILURE,
            serde_json::json!({ "f": file_id, "j": job_id, "c": reason_code, "m": message }),
        )
        .await
}

#[tokio::test]
async fn the_runners_final_report_lands_the_file_ready_without_waiting_for_jobs() {
    // Given: a one-step rule, and a file whose only job runs
    let world = World::start("pod-runner-done").await;
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
    let mut files = drive_subscription(&world, &owner, drive).await;
    quiet(&mut files).await;
    let before = world.file(&owner, file_id).await["updatedAt"].clone();

    // When: the runner sends its final report
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

    // Then: the file is READY at once, with its results — Jobs has said nothing yet
    let finished = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "ProcessingFinished"
    })
    .await;
    assert_eq!(finished["view"]["processingState"], "READY");
    assert!(finished["view"]["processingError"].is_null());
    assert!(finished["view"]["progress"].is_null());
    assert_eq!(finished["view"]["summary"], "One page.");
    assert_eq!(finished["view"]["pageCount"], 1);
    assert_eq!(finished["view"]["affordances"]["process"]["allowed"], true);
    let settled = world.file(&owner, file_id).await["updatedAt"].clone();
    assert_ne!(settled, before, "the end of processing moves updatedAt");
    // And: Jobs is told the job is done, and the job no longer opens the file
    jobs.await_finish(job).await;
    assert_eq!(
        error_code(&context(&world, &runner, file_id, job).await),
        "JOB_NOT_ACTIVE"
    );

    // When: Jobs confirms, then says it again
    jobs.complete(job).await;
    jobs.complete(job).await;

    // Then: nothing moves — the confirmation is information only
    crate::poll_until!(Duration::from_secs(15), {
        (world
            .job_events(job)
            .await
            .iter()
            .filter(|kind| *kind == "completed")
            .count()
            == 2)
            .then_some(())
    });
    refute_delta(
        &mut files,
        "workspaceDriveChanged",
        Duration::from_millis(800),
        |node| node["view"]["processingState"] != "READY",
    )
    .await;
    let file = world.file(&owner, file_id).await;
    assert_eq!(file["processingState"], "READY");
    assert_eq!(
        file["updatedAt"], settled,
        "the confirmation touched nothing"
    );
    jobs.expect_no_command(Duration::from_millis(500)).await;

    world.cleanup().await;
}

#[tokio::test]
async fn a_runners_declared_failure_fails_the_file_with_its_code_and_tells_jobs() {
    // Given: a file whose job runs, and a runner that already reported a page
    let world = World::start("pod-runner-failure").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    let other_runner = service_passport(&["archive:runner"]);
    install_render_rule(&world, &manager).await;
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = upload_processed(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "unreadable.txt", BYTES),
    )
    .await;
    let job = jobs.await_create(file_id).await.job_id;
    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id: job,
            pages: vec![(1, "page one, read")],
            origin: None,
            indexer: None,
            done: false,
        },
    )
    .await);
    let mut files = drive_subscription(&world, &owner, drive).await;
    quiet(&mut files).await;

    // When: the failure is declared by the wrong principals, for the wrong
    // job, or without a proper code
    // Then: each is refused and nothing happens
    for (who, job_id, code, expected) in [
        (&owner, job, "unreadable_scan", "RUNNER_SCOPE_REQUIRED"),
        (
            &other_runner,
            job,
            "unreadable_scan",
            "RUNNER_SCOPE_REQUIRED",
        ),
        (&runner, Uuid::now_v7(), "unreadable_scan", "JOB_NOT_ACTIVE"),
        (
            &runner,
            job,
            "The scan is unreadable.",
            "INVALID_FAILURE_REASON",
        ),
        (&runner, job, "", "INVALID_FAILURE_REASON"),
    ] {
        assert_eq!(
            error_code(&report_failure(&world, who, file_id, job_id, code, None).await),
            expected,
            "{code:?}"
        );
    }
    files.expect_silence(Duration::from_millis(600)).await;
    jobs.expect_no_command(Duration::from_millis(300)).await;
    assert_eq!(
        world.file(&owner, file_id).await["processingState"],
        "PROCESSING"
    );

    // When: the runner declares its job failed
    ok(&report_failure(
        &world,
        &runner,
        file_id,
        job,
        "unreadable_scan",
        Some("page 3 is blank"),
    )
    .await);

    // Then: the file is FAILED with the runner's own code, open to a reprocess
    let failed = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "ProcessingFailed"
    })
    .await;
    assert_eq!(failed["cause"]["reason"], "unreadable_scan");
    assert_eq!(failed["view"]["processingState"], "FAILED");
    assert_eq!(failed["view"]["processingError"], "unreadable_scan");
    assert!(failed["view"]["progress"].is_null());
    assert_eq!(failed["view"]["affordances"]["process"]["allowed"], true);
    // And: Jobs is told, with the code and the message
    let fail = jobs.await_fail(job).await;
    assert_eq!(
        fail.note.as_deref(),
        Some("unreadable_scan: page 3 is blank")
    );
    // And: what the run reported so far stays readable; the job is over
    let pages = world.file_pages(&owner, file_id).await;
    assert_eq!(pages.len(), 1);
    assert_eq!(pages[0]["markdown"], "page one, read");
    assert_eq!(
        error_code(&context(&world, &runner, file_id, job).await),
        "JOB_NOT_ACTIVE"
    );
    assert_eq!(
        error_code(&report_failure(&world, &runner, file_id, job, "again", None).await),
        "JOB_NOT_ACTIVE",
        "a job ends once"
    );

    // When: Jobs' own `failed` follows — with its cause, not the runner's code
    jobs.fail(job, "DECLARED_BY_OWNER", None).await;

    // Then: it is logged and changes nothing
    crate::poll_until!(Duration::from_secs(15), {
        (world.job_events(job).await.last().map(String::as_str) == Some("failed")).then_some(())
    });
    refute_delta(
        &mut files,
        "workspaceDriveChanged",
        Duration::from_millis(800),
        |node| node["view"]["processingError"] != "unreadable_scan",
    )
    .await;
    assert_eq!(
        world.file(&owner, file_id).await["processingError"],
        "unreadable_scan"
    );
    jobs.expect_no_command(Duration::from_millis(500)).await;

    world.cleanup().await;
}

#[tokio::test]
async fn a_jobs_failure_after_the_runners_final_report_changes_nothing() {
    // Given: a two-step rule, and a file whose first step's runner sent its
    // final report — the second step runs
    let world = World::start("pod-runner-done-then-failed").await;
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
    let file_id = upload_processed(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "two-steps.txt", BYTES),
    )
    .await;
    let first = jobs.await_create(file_id).await.job_id;
    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id: first,
            pages: vec![(1, "rendered")],
            origin: None,
            indexer: None,
            done: true,
        },
    )
    .await);
    jobs.await_finish(first).await;
    let second = jobs.await_create(file_id).await.job_id;
    let running = world.await_state(&owner, file_id, "PROCESSING").await;
    assert_eq!(running["progress"]["stepIndex"], 1);
    let mut files = drive_subscription(&world, &owner, drive).await;
    quiet(&mut files).await;

    // When: Jobs says the first job failed (a finish it refused, a late
    // backstop) — and cancelled, and completed
    jobs.fail(first, "RUN_TIMED_OUT", Some("too_slow")).await;
    jobs.cancel(first).await;
    jobs.complete(first).await;

    // Then: the facts are logged on the first job and the file does not move:
    // still PROCESSING its second step, which still opens the file
    let mut expected = vec![
        "cancelled",
        "completed",
        "failed",
        "queued",
        "reported_done",
    ];
    expected.sort_unstable();
    crate::poll_until!(Duration::from_secs(15), {
        let mut events = world.job_events(first).await;
        events.sort_unstable();
        (events == expected).then_some(())
    });
    refute_delta(
        &mut files,
        "workspaceDriveChanged",
        Duration::from_millis(800),
        |node| {
            node["view"]["processingState"] != "PROCESSING"
                || node["view"]["progress"]["stepIndex"] != 1
        },
    )
    .await;
    let file = world.file(&owner, file_id).await;
    assert_eq!(file["processingState"], "PROCESSING");
    assert!(file["processingError"].is_null());
    assert_eq!(file["updatedAt"], running["updatedAt"]);
    world.await_source_promoted(file_id).await;
    assert!(context(&world, &runner, file_id, second).await["errors"].is_null());
    jobs.expect_no_command(Duration::from_millis(500)).await;

    world.cleanup().await;
}
