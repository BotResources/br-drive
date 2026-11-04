//! The hand-off between two steps of a chain. Jobs holds one live job per
//! file and consumes `job.finish` and `job.create` on separate durables, in no
//! order: a next step's `job.create` staged beside the previous step's
//! `job.finish` could be judged first and refused `duplicate_active_entity`.
//! So the final report of a step that has a next one only decides that step —
//! its job id minted and held (`NextStepHeld`) — and the `job.create` is sent
//! once Jobs itself ended the previous job (since 0.8.1).
//!
//! The Jobs stand-in settles the commands it reads in order, so it never
//! refuses that create: these scenarios prove instead that no `job.create`
//! is sent before Jobs' end of the previous job, rather than replaying the
//! race itself.

use std::time::Duration;

use uuid::Uuid;

use crate::harness::runner::{
    INDEX, RENDER, RUNNER_SCOPE, Report, RuleSpec, context, create_ruleset, report,
};
use crate::harness::upload::{UploadRequest, process, upload_processed};
use crate::harness::{
    Fact, JobsStandIn, World, drive_subscription, error_code, manager_passport, next_drive_delta,
    ok, passport, refute_delta, service_passport,
};

const BYTES: &[u8] = b"a document read in two steps";
const CANCEL: &str = "mutation($f:UUID!){workspaceCancelProcessing(fileId:$f){success}}";

async fn two_step_rule(world: &World, manager: &str) {
    ok(&create_ruleset(
        world,
        manager,
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
}

async fn report_done(world: &World, runner: &str, file_id: Uuid, job_id: Uuid) {
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
}

/// The `NextStepHeld` facts of a file's processing.
fn held(facts: &[Fact]) -> Vec<&Fact> {
    facts
        .iter()
        .filter(|fact| fact.event_type == "NextStepHeld")
        .collect()
}

struct Chain {
    world: World,
    jobs: JobsStandIn,
    owner: String,
    runner: String,
    drive: Uuid,
    file_id: Uuid,
    first: Uuid,
}

/// A two-step chain whose first step's job runs.
async fn a_running_two_step_chain(pod: &str) -> Chain {
    let world = World::start(pod).await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    two_step_rule(&world, &manager).await;
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = upload_processed(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "two-steps.txt", BYTES),
    )
    .await;
    let first = jobs.await_create(file_id).await.job_id;
    world.await_source_promoted(file_id).await;
    Chain {
        world,
        jobs,
        owner,
        runner,
        drive,
        file_id,
        first,
    }
}

#[tokio::test]
async fn the_next_step_is_created_only_once_jobs_completed_the_previous_one_and_the_chain_lands_ready()
 {
    // Given: a two-step chain whose first step runs
    let Chain {
        world,
        jobs,
        owner,
        runner,
        drive,
        file_id,
        first,
    } = a_running_two_step_chain("pod-handoff").await;
    let mut files = drive_subscription(&world, &owner, drive).await;

    // When: the runner's final report of the first step lands
    report_done(&world, &runner, file_id, first).await;

    // Then: Jobs is told the first job is done, and nothing else — the next
    // step's job is not asked for while Jobs has not ended the first one
    jobs.await_finish(first).await;
    jobs.expect_no_command(Duration::from_secs(1)).await;
    let waiting = world.file(&owner, file_id).await;
    assert_eq!(
        waiting["processingState"], "PROCESSING",
        "the chain still runs: {waiting}"
    );
    assert_eq!(
        waiting["progress"]["stepIndex"], 1,
        "the chain is at its next step, nothing run yet: {waiting}"
    );
    assert_eq!(waiting["progress"]["runnerType"], INDEX);
    assert_eq!(waiting["progress"]["plan"], serde_json::json!([]));
    assert_eq!(
        world.job_of(file_id).await,
        Some(first),
        "no job runs the next step yet"
    );
    assert!(
        world.held_passport(file_id).await.is_some(),
        "the chain keeps its passport for the next step"
    );
    let facts = world.processing_facts(file_id).await;
    let held_steps = held(&facts);
    assert_eq!(held_steps.len(), 1, "{facts:?}");
    let next = Uuid::parse_str(held_steps[0].payload["job_id"].as_str().unwrap()).unwrap();
    assert_eq!(
        held_steps[0].payload,
        serde_json::json!({
            "kind": "NextStepHeld",
            "step_index": 1,
            "runner_type": INDEX,
            "job_id": next,
            "after_job_id": first,
        })
    );
    assert_eq!(
        facts
            .iter()
            .rev()
            .take(2)
            .map(|fact| fact.event_type.as_str())
            .collect::<Vec<_>>(),
        vec!["NextStepHeld", "JobReportedDone"],
        "the report ends the job and holds the next step, nothing more"
    );
    assert_eq!(
        error_code(&context(&world, &runner, file_id, first).await),
        "JOB_NOT_ACTIVE",
        "the job reported done no longer opens the file"
    );
    assert_eq!(
        error_code(
            &report(
                &world,
                &runner,
                file_id,
                Report {
                    job_id: first,
                    pages: vec![(1, "again")],
                    origin: None,
                    indexer: None,
                    done: true,
                },
            )
            .await
        ),
        "JOB_NOT_ACTIVE",
        "a replayed final report moves nothing"
    );

    // When: Jobs completes the first job
    jobs.complete(first).await;

    // Then: the next step's job is created, with the id held at the report
    let started = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "ProcessingStarted"
    })
    .await;
    assert_eq!(started["cause"]["step"], 1);
    assert_eq!(started["cause"]["job_id"], next.to_string());
    assert_eq!(started["view"]["progress"]["runnerType"], INDEX);
    let create = jobs.await_create(file_id).await;
    assert_eq!(create.job_id, next, "the id minted at the report");
    assert_eq!(create.runner_type, INDEX);
    assert_eq!(create.config.as_ref().unwrap()["step"], 1);
    assert!(
        !jobs.rejected(next),
        "Jobs accepts it: the first job had ended"
    );

    // And: the second step runs to the end of the chain
    report_done(&world, &runner, file_id, next).await;
    jobs.await_finish(next).await;
    jobs.complete(next).await;
    let ready = world.await_state(&owner, file_id, "READY").await;
    assert!(ready["processingError"].is_null());
    assert_eq!(
        world.job_log(file_id).await,
        vec![
            (first, 0, Some("reported_done".to_string())),
            (next, 1, Some("reported_done".to_string())),
        ]
    );
    jobs.expect_no_command(Duration::from_millis(500)).await;

    world.cleanup().await;
}

#[tokio::test]
async fn a_replayed_completed_creates_the_next_step_once() {
    // Given: a two-step chain whose first step was reported done
    let Chain {
        world,
        jobs,
        owner,
        runner,
        file_id,
        first,
        ..
    } = a_running_two_step_chain("pod-handoff-replay").await;
    report_done(&world, &runner, file_id, first).await;
    jobs.await_finish(first).await;

    // When: Jobs' `completed` of the first job arrives, then arrives again
    jobs.complete(first).await;
    let create = jobs.await_create(file_id).await;
    let again = jobs.complete(first).await;
    world.await_consumed(again).await;

    // Then: the next step's job was asked for once, and runs
    jobs.expect_no_command(Duration::from_secs(1)).await;
    assert_eq!(world.job_of(file_id).await, Some(create.job_id));
    let facts = world.processing_facts(file_id).await;
    assert_eq!(
        facts
            .iter()
            .filter(|fact| fact.event_type == "JobCreated")
            .count(),
        2,
        "one job per step: {facts:?}"
    );
    let replayed: Vec<_> = world
        .job_facts(first)
        .await
        .into_iter()
        .filter(|fact| fact.event_type == "JobFactIgnored")
        .collect();
    assert_eq!(replayed.len(), 1, "{facts:?}");
    assert_eq!(replayed[0].payload["why"], "not_the_current_job");
    assert_eq!(replayed[0].payload["received"]["kind"], "JobCompleted");
    let file = world.file(&owner, file_id).await;
    assert_eq!(file["processingState"], "PROCESSING");

    world.cleanup().await;
}

#[tokio::test]
async fn a_cancel_between_the_final_report_and_jobs_completed_skips_the_next_step_at_once() {
    // Given: a two-step chain whose first step was reported done, its next
    // step held until Jobs completes the first job
    let Chain {
        world,
        jobs,
        owner,
        runner,
        drive,
        file_id,
        first,
    } = a_running_two_step_chain("pod-handoff-cancel").await;
    report_done(&world, &runner, file_id, first).await;
    jobs.await_finish(first).await;
    let mut files = drive_subscription(&world, &owner, drive).await;

    // When: the owner cancels before Jobs' `completed` arrives
    ok(&world
        .gql(&owner, CANCEL, serde_json::json!({ "f": file_id }))
        .await);

    // Then: nothing runs, so the held step is skipped at once — not on a
    // Jobs end that may never come: the file is FAILED `cancelled`, the
    // harmless cancel of the settled job is the only command
    let failed = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "ProcessingFailed"
    })
    .await;
    assert_eq!(failed["cause"]["reason"], "cancelled");
    assert_eq!(failed["view"]["processingError"], "cancelled");
    assert_eq!(failed["view"]["affordances"]["process"]["allowed"], true);
    jobs.await_cancel(first).await;
    jobs.expect_no_command(Duration::from_secs(1)).await;
    assert_eq!(
        world.job_log(file_id).await,
        vec![(first, 0, Some("reported_done".to_string()))],
        "no job exists for the step never launched"
    );
    let facts = world.processing_facts(file_id).await;
    assert_eq!(
        facts.last().map(|fact| fact.event_type.as_str()),
        Some("StepSkipped"),
        "{facts:?}"
    );
    assert_eq!(
        facts.last().unwrap().payload,
        serde_json::json!({
            "kind": "StepSkipped",
            "step_index": 1,
            "runner_type": INDEX,
            "after_job_id": first,
        })
    );
    assert_eq!(world.held_passport(file_id).await, None);

    // And: Jobs' `completed` of the first job, arriving after, changes
    // nothing — no step is created
    let again = jobs.complete(first).await;
    world.await_consumed(again).await;
    refute_delta(
        &mut files,
        "workspaceDriveChanged",
        Duration::from_millis(500),
        |node| node["view"]["processingState"] != "FAILED",
    )
    .await;
    jobs.expect_no_command(Duration::from_millis(500)).await;

    world.cleanup().await;
}

#[tokio::test]
async fn a_jobs_failure_of_the_job_reported_done_still_hands_off_to_the_next_step() {
    // Given: a two-step chain whose first step was reported done — and Jobs,
    // which had failed the job before the `job.finish` reached it
    let Chain {
        world,
        jobs,
        owner,
        runner,
        file_id,
        first,
        ..
    } = a_running_two_step_chain("pod-handoff-failed").await;
    report_done(&world, &runner, file_id, first).await;
    jobs.await_finish(first).await;

    // When: Jobs' `failed` of the first job arrives instead of `completed`
    jobs.fail(first, "lease_expired", None).await;

    // Then: the report was the job's end — the failure is recorded as
    // ignored — and the file, free in Jobs, moves on to the next step
    let create = jobs.await_create(file_id).await;
    assert_eq!(create.runner_type, INDEX);
    assert!(!jobs.rejected(create.job_id));
    assert_eq!(world.job_of(file_id).await, Some(create.job_id));
    let ignored: Vec<_> = world
        .job_facts(first)
        .await
        .into_iter()
        .filter(|fact| fact.event_type == "JobFactIgnored")
        .collect();
    assert_eq!(ignored.len(), 1);
    assert_eq!(ignored[0].payload["why"], "job_already_ended");
    assert_eq!(ignored[0].payload["received"]["kind"], "JobFailed");
    assert_eq!(
        world.job_end(first).await,
        Some(("reported_done".to_string(), None))
    );
    let file = world.file(&owner, file_id).await;
    assert_eq!(file["processingState"], "PROCESSING");
    assert!(file["processingError"].is_null());

    world.cleanup().await;
}

#[tokio::test]
async fn a_failed_after_the_completed_that_released_the_step_creates_nothing_more() {
    // Given: a two-step chain handed on to its second step by Jobs' `completed`
    let Chain {
        world,
        jobs,
        owner,
        runner,
        file_id,
        first,
        ..
    } = a_running_two_step_chain("pod-handoff-late-failed").await;
    report_done(&world, &runner, file_id, first).await;
    jobs.await_finish(first).await;
    jobs.complete(first).await;
    let second = jobs.await_create(file_id).await.job_id;

    // When: a `failed` of the first job arrives after
    let late = jobs.fail(first, "lease_expired", None).await;
    world.await_consumed(late).await;

    // Then: no second create, and the second step still runs
    jobs.expect_no_command(Duration::from_secs(1)).await;
    assert_eq!(world.job_of(file_id).await, Some(second));
    let file = world.file(&owner, file_id).await;
    assert_eq!(file["processingState"], "PROCESSING");
    assert_eq!(file["progress"]["stepIndex"], 1);

    world.cleanup().await;
}

#[tokio::test]
async fn a_freeze_while_the_next_step_is_held_skips_it_and_a_later_completed_creates_nothing() {
    // Given: a two-step chain whose next step is held
    let Chain {
        world,
        jobs,
        owner,
        runner,
        drive,
        file_id,
        first,
    } = a_running_two_step_chain("pod-handoff-freeze").await;
    report_done(&world, &runner, file_id, first).await;
    jobs.await_finish(first).await;

    // When: the owner freezes the drive
    ok(&world
        .gql(
            &owner,
            "mutation($id:UUID!){workspaceFreeze(id:$id){success}}",
            serde_json::json!({ "id": drive }),
        )
        .await);

    // Then: nothing ran, so no cancel is sent; the held step is skipped and
    // the file is FAILED `cancelled`
    let failed = world.await_state(&owner, file_id, "FAILED").await;
    assert_eq!(failed["processingError"], "cancelled");
    jobs.expect_no_command(Duration::from_millis(800)).await;
    let facts = world.processing_facts(file_id).await;
    assert_eq!(
        facts.last().map(|fact| fact.event_type.as_str()),
        Some("StepSkipped"),
        "{facts:?}"
    );

    // And: Jobs' `completed` of the first job, arriving after, creates nothing
    let late = jobs.complete(first).await;
    world.await_consumed(late).await;
    jobs.expect_no_command(Duration::from_secs(1)).await;
    assert_eq!(
        world.file(&owner, file_id).await["processingState"],
        "FAILED"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn a_file_deleted_while_its_next_step_is_held_gets_no_step_from_a_later_completed() {
    // Given: a two-step chain whose next step is held
    let Chain {
        world,
        jobs,
        owner,
        runner,
        file_id,
        first,
        ..
    } = a_running_two_step_chain("pod-handoff-delete").await;
    report_done(&world, &runner, file_id, first).await;
    jobs.await_finish(first).await;

    // When: the owner deletes the file, then Jobs completes the first job
    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!){workspaceDeleteFile(fileId:$f){success}}",
            serde_json::json!({ "f": file_id }),
        )
        .await);
    let late = jobs.complete(first).await;
    world.await_consumed(late).await;

    // Then: no job is asked for a file that is gone
    jobs.expect_no_command(Duration::from_secs(1)).await;
    assert!(world.file(&owner, file_id).await.is_null());

    world.cleanup().await;
}

#[tokio::test]
async fn a_process_gesture_while_the_next_step_is_held_is_refused_file_processing() {
    // Given: a two-step chain whose next step is held
    let Chain {
        world,
        jobs,
        owner,
        runner,
        file_id,
        first,
        ..
    } = a_running_two_step_chain("pod-handoff-process").await;
    report_done(&world, &runner, file_id, first).await;
    jobs.await_finish(first).await;

    // When: the owner asks to process the file again
    let refused = process(&world, &owner, file_id).await;

    // Then: the file still processes: refused, nothing sent
    assert_eq!(error_code(&refused), "FILE_PROCESSING");
    jobs.expect_no_command(Duration::from_millis(800)).await;

    world.cleanup().await;
}
