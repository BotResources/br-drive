//! The chain's priority (since 0.9.0). The gesture that starts a chain — the
//! upload request (pinned for its commit), `ProcessFile` or `RegeneratePage`
//! — may name a priority from 1 (lowest) to 5 (highest); out of range is
//! `INVALID_PRIORITY`, refused before anything is recorded. It is kept with
//! the file's processing like the Passport, every step's `job.create` asks
//! Jobs for it — the first step, a held next step, a first step that awaited
//! the source — and it is gone at the chain's end. None named asks for
//! nothing: Jobs' default applies.

use std::time::Duration;

use uuid::Uuid;

use crate::harness::runner::{
    INDEX, RENDER, RUNNER_SCOPE, Report, RuleSpec, create_ruleset, finish_job,
    install_regenerate_rule, install_render_rule, report,
};
use crate::harness::upload::{
    PROCESS_FILE, UploadRequest, commit, post_bytes, sha256_hex, ticket, upload,
};
use crate::harness::{
    JobsStandIn, World, WorldOptions, error_code, manager_passport, ok, service_passport,
};

const BYTES: &[u8] = b"a document some person needs before the others";
const REQUEST: &str = "mutation($f:UUID!,$d:UUID!,$p:String!,$n:String!,$m:String!,$s:ByteCount!,$h:String!,$q:Int){\
    workspaceRequestUpload(fileId:$f,driveId:$d,path:$p,name:$n,mediaType:$m,size:$s,sha256:$h,priority:$q){fileId url fields}}";
const PROCESS: &str =
    "mutation($f:UUID!,$q:Int){workspaceProcessFile(fileId:$f,priority:$q){success}}";
const REGENERATE: &str = "mutation($f:UUID!,$n:Int!,$q:Int){\
    workspaceRegeneratePage(fileId:$f,number:$n,priority:$q){success}}";

/// `RequestUpload` of `name` in `drive`, naming `priority` (or none).
async fn request(
    world: &World,
    owner: &str,
    file_id: Uuid,
    drive: Uuid,
    name: &str,
    priority: Option<i64>,
) -> serde_json::Value {
    world
        .gql(
            owner,
            REQUEST,
            serde_json::json!({
                "f": file_id,
                "d": drive,
                "p": "",
                "n": name,
                "m": "text/plain",
                "s": BYTES.len(),
                "h": sha256_hex(BYTES),
                "q": priority,
            }),
        )
        .await
}

/// The default `upload` rule `text/plain → [render, index]`.
async fn install_two_step_rule(world: &World, manager: &str) {
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

/// The runner's final report of `job_id`, page 1 included.
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

#[tokio::test]
async fn a_priority_pinned_at_upload_is_asked_by_every_step_of_the_commits_chain() {
    // Given: a host processing on commit with a two-step default upload rule,
    // and an upload naming priority 4 whose source the reaper is held off
    let world = World::start_with(
        "pod-priority-upload",
        WorldOptions {
            process_on_commit: true,
            ..WorldOptions::default()
        },
    )
    .await;
    let jobs = JobsStandIn::attach(&world).await;
    let owner = manager_passport(Uuid::now_v7(), "Ada");
    let runner = service_passport(&[RUNNER_SCOPE]);
    install_two_step_rule(&world, &owner).await;
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = Uuid::now_v7();
    let upload_ticket =
        ticket(&request(&world, &owner, file_id, drive, "urgent.txt", Some(4)).await);
    let pinned = world.facts("drive_file", serde_json::json!(file_id)).await;
    assert_eq!(
        pinned
            .iter()
            .map(|fact| fact.event_type.as_str())
            .collect::<Vec<_>>(),
        vec!["UploadTicketIssued", "UploadPriorityChosen"]
    );
    assert_eq!(
        pinned[1].payload,
        serde_json::json!({ "kind": "UploadPriorityChosen", "priority": 4 })
    );
    let hold = world.hold_promotion(file_id).await;
    assert!((200..300).contains(&post_bytes(&world, &upload_ticket, BYTES, "urgent.txt").await));
    world.await_lock_waiters(1).await;

    // When: the owner commits it, and the engine promotes the source
    ok(&commit(&world, &owner, file_id).await);

    // Then: the chain holds the priority while its first step awaits the
    // source, and that step's job asks for it once the source is promoted
    assert_eq!(world.held_priority(file_id).await, Some(4));
    let started = world.processing_facts(file_id).await;
    assert_eq!(started[0].event_type, "ChainStarted");
    assert_eq!(started[0].payload["priority"], 4);
    jobs.expect_no_command(Duration::from_millis(300)).await;
    hold.release().await;
    let first = jobs.await_create(file_id).await;
    assert_eq!(first.runner_type, RENDER);
    assert_eq!(
        first.priority,
        Some(4),
        "the awaited first step asks for it"
    );

    // When: the first step ends, its next step held until Jobs ends it
    report_done(&world, &runner, file_id, first.job_id).await;
    assert_eq!(world.held_priority(file_id).await, Some(4));
    finish_job(&world, &jobs, first.job_id).await;

    // Then: the next step's job asks for it too
    let second = jobs.await_create(file_id).await;
    assert_eq!(second.runner_type, INDEX);
    assert_eq!(second.priority, Some(4), "the held next step asks for it");

    // When: the chain ends READY
    report_done(&world, &runner, file_id, second.job_id).await;
    world.await_state(&owner, file_id, "READY").await;

    // Then: the priority is gone with the chain
    assert_eq!(world.held_priority(file_id).await, None);

    world.cleanup().await;
}

#[tokio::test]
async fn process_file_and_regenerate_page_ask_for_the_priority_they_name_or_for_nothing() {
    // Given: a stored file, a default upload rule and a regenerate rule
    let world = World::start("pod-priority-gestures").await;
    let jobs = JobsStandIn::attach(&world).await;
    let owner = manager_passport(Uuid::now_v7(), "Ada");
    let runner = service_passport(&[RUNNER_SCOPE]);
    install_render_rule(&world, &owner).await;
    install_regenerate_rule(&world, &owner).await;
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "report.txt", BYTES),
    )
    .await;
    world.await_source_promoted(file_id).await;

    // When: the owner processes it at priority 2
    ok(&world
        .gql(&owner, PROCESS, serde_json::json!({ "f": file_id, "q": 2 }))
        .await);

    // Then: its job asks for priority 2
    let processed = jobs.await_create(file_id).await;
    assert_eq!(processed.priority, Some(2));
    assert_eq!(world.held_priority(file_id).await, Some(2));
    report_done(&world, &runner, file_id, processed.job_id).await;
    world.await_state(&owner, file_id, "READY").await;
    assert_eq!(world.held_priority(file_id).await, None);
    finish_job(&world, &jobs, processed.job_id).await;

    // When: the owner regenerates page 1 at priority 5
    ok(&world
        .gql(
            &owner,
            REGENERATE,
            serde_json::json!({ "f": file_id, "n": 1, "q": 5 }),
        )
        .await);

    // Then: its job asks for priority 5
    let regenerated = jobs.await_create(file_id).await;
    assert_eq!(regenerated.priority, Some(5));
    report_done(&world, &runner, file_id, regenerated.job_id).await;
    world.await_state(&owner, file_id, "READY").await;
    finish_job(&world, &jobs, regenerated.job_id).await;

    // When: the owner reprocesses it, naming no priority
    ok(&world
        .gql(
            &owner,
            PROCESS_FILE,
            serde_json::json!({ "f": file_id, "r": null }),
        )
        .await);

    // Then: its job asks for nothing, and the chain holds none
    let reprocessed = jobs.await_create(file_id).await;
    assert_eq!(reprocessed.priority, None, "Jobs' default applies");
    assert_eq!(world.held_priority(file_id).await, None);
    let started = world.processing_facts(file_id).await;
    let last_start = started
        .iter()
        .rev()
        .find(|fact| fact.event_type == "ChainStarted")
        .expect("the reprocess started a chain");
    assert!(
        last_start.payload.get("priority").is_none(),
        "{}",
        last_start.payload
    );

    world.cleanup().await;
}

#[tokio::test]
async fn a_priority_out_of_range_is_refused_before_anything_is_recorded() {
    // Given: a stored file with a page, a default upload rule and a
    // regenerate rule
    let world = World::start("pod-priority-range").await;
    let jobs = JobsStandIn::attach(&world).await;
    let owner = manager_passport(Uuid::now_v7(), "Ada");
    let runner = service_passport(&[RUNNER_SCOPE]);
    install_render_rule(&world, &owner).await;
    install_regenerate_rule(&world, &owner).await;
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "report.txt", BYTES),
    )
    .await;
    world.await_source_promoted(file_id).await;
    ok(&world
        .gql(&owner, PROCESS, serde_json::json!({ "f": file_id }))
        .await);
    let job = jobs.await_create(file_id).await;
    report_done(&world, &runner, file_id, job.job_id).await;
    world.await_state(&owner, file_id, "READY").await;
    finish_job(&world, &jobs, job.job_id).await;

    for priority in [0, 6] {
        // When: an upload names a priority out of range
        let refused_upload = Uuid::now_v7();
        let response = request(
            &world,
            &owner,
            refused_upload,
            drive,
            "refused.txt",
            Some(priority),
        )
        .await;

        // Then: it is refused, and no file is recorded
        assert_eq!(error_code(&response), "INVALID_PRIORITY", "{priority}");
        assert!(
            world
                .facts("drive_file", serde_json::json!(refused_upload))
                .await
                .is_empty(),
            "{priority}: no upload ticket"
        );
        assert_eq!(
            world.file(&owner, refused_upload).await,
            serde_json::Value::Null
        );

        // When: the owner processes, then regenerates, naming it
        let processed = world
            .gql(
                &owner,
                PROCESS,
                serde_json::json!({ "f": file_id, "q": priority }),
            )
            .await;
        let regenerated = world
            .gql(
                &owner,
                REGENERATE,
                serde_json::json!({ "f": file_id, "n": 1, "q": priority }),
            )
            .await;

        // Then: both are refused, and no chain starts
        assert_eq!(error_code(&processed), "INVALID_PRIORITY", "{priority}");
        assert_eq!(error_code(&regenerated), "INVALID_PRIORITY", "{priority}");
    }
    jobs.expect_no_command(Duration::from_millis(500)).await;
    let chains = world
        .processing_facts(file_id)
        .await
        .into_iter()
        .filter(|fact| fact.event_type == "ChainStarted")
        .count();
    assert_eq!(chains, 1, "no chain recorded on the file's processing");
    assert_eq!(
        world.file(&owner, file_id).await["processingState"],
        "READY"
    );

    world.cleanup().await;
}
