//! A chain's first step is asked of Jobs only once the engine promoted the
//! file's source (since 0.8.2). The engine serves no download URL for a
//! verified source before its reaper verified the landed object, so a step
//! created at the commit met a runner context with no source to read. Now a
//! chain started on a source not yet promoted records its plan, mints its
//! first job's id and awaits the promotion (`SourceAwaited`), the file
//! PROCESSING; the source's post-upload policy releases it (`JobCreated`),
//! and the runner context serves the source on its first call.
//!
//! The suite holds the reaper off a source with a row lock on its blob row
//! ([`World::hold_promotion`]) and anchors every "not yet" on the reaper
//! actually waiting on that lock ([`World::await_lock_waiters`]).

use std::time::Duration;

use uuid::Uuid;

use crate::harness::runner::{RENDER, RUNNER_SCOPE, context, install_render_rule};
use crate::harness::upload::{UploadRequest, commit, post_bytes, process, request, ticket};
use crate::harness::{
    Fact, JobsStandIn, PromotionHold, World, WorldOptions, error_code, manager_passport, ok,
    service_passport,
};

const BYTES: &[u8] = b"a document whose source the engine still verifies";
const CANCEL: &str = "mutation($f:UUID!){workspaceCancelProcessing(fileId:$f){success}}";
const DELETE: &str = "mutation($f:UUID!){workspaceDeleteFile(fileId:$f){success}}";
const DELETE_RULE: &str = "mutation($id:UUID!){workspaceDeleteRuleset(id:$id){success}}";
const FREEZE: &str = "mutation($id:UUID!){workspaceFreeze(id:$id){success}}";

fn processing_on_commit() -> WorldOptions {
    WorldOptions {
        process_on_commit: true,
        ..WorldOptions::default()
    }
}

struct Awaiting {
    world: World,
    jobs: JobsStandIn,
    owner_id: Uuid,
    owner: String,
    drive: Uuid,
    rule: Uuid,
    file_id: Uuid,
    hold: PromotionHold,
}

/// A text upload whose source the reaper is held off, its bytes landed: the
/// file is PENDING and the reaper waits on the hold.
async fn landed_and_held(world: World) -> Awaiting {
    let jobs = JobsStandIn::attach(&world).await;
    let owner_id = Uuid::now_v7();
    let owner = manager_passport(owner_id, "Ada");
    let rule = install_render_rule(&world, &owner).await;
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = Uuid::now_v7();
    let upload_ticket = ticket(
        &request(
            &world,
            &owner,
            file_id,
            &UploadRequest::text(drive, "", "awaited.txt", BYTES),
        )
        .await,
    );
    let hold = world.hold_promotion(file_id).await;
    assert!((200..300).contains(&post_bytes(&world, &upload_ticket, BYTES, "awaited.txt").await));
    world.await_lock_waiters(1).await;
    Awaiting {
        world,
        jobs,
        owner_id,
        owner,
        drive,
        rule,
        file_id,
        hold,
    }
}

/// The kinds of the library's own decisions on the file's processing — what
/// Jobs reports of a created job (`JobQueued`, …) left out.
fn kinds(facts: &[Fact]) -> Vec<&str> {
    facts
        .iter()
        .map(|fact| fact.event_type.as_str())
        .filter(|kind| !matches!(*kind, "JobQueued" | "JobStarted"))
        .collect()
}

fn awaited_job(facts: &[Fact]) -> Uuid {
    let awaited = facts
        .iter()
        .find(|fact| fact.event_type == "SourceAwaited")
        .unwrap_or_else(|| panic!("the step awaits the source: {facts:?}"));
    Uuid::parse_str(awaited.payload["job_id"].as_str().unwrap()).unwrap()
}

#[tokio::test]
async fn the_first_job_is_asked_of_jobs_only_once_the_source_is_promoted() {
    // Given: a default upload rule, and a landed upload the reaper has not
    // promoted — it waits on the hold
    let world = World::start_with("pod-promotion-commit", processing_on_commit()).await;
    let Awaiting {
        world,
        jobs,
        owner_id,
        owner,
        file_id,
        hold,
        ..
    } = landed_and_held(world).await;
    let runner = service_passport(&[RUNNER_SCOPE]);

    // When: the owner commits it
    ok(&commit(&world, &owner, file_id).await);

    // Then: the file is PROCESSING at its first step, nothing run yet, and
    // the commit recorded the chain and its awaited step in one transaction
    let file = world.file(&owner, file_id).await;
    assert_eq!(file["processingState"], "PROCESSING", "{file}");
    assert_eq!(file["progress"]["stepIndex"], 0);
    assert_eq!(file["progress"]["runnerType"], RENDER);
    assert_eq!(file["progress"]["plan"], serde_json::json!([]));
    let committed = world
        .facts("drive_file", serde_json::json!(file_id))
        .await
        .pop()
        .expect("the commit");
    assert_eq!(committed.event_type, "UploadCommitted");
    let facts = world.processing_facts(file_id).await;
    assert_eq!(kinds(&facts), vec!["ChainStarted", "SourceAwaited"]);
    let minted = awaited_job(&facts);
    assert_eq!(
        facts[1].payload,
        serde_json::json!({
            "kind": "SourceAwaited",
            "step_index": 0,
            "runner_type": RENDER,
            "job_id": minted,
        })
    );
    for fact in &facts {
        assert_eq!(fact.correlation_id, committed.correlation_id, "one gesture");
        assert_eq!(fact.actor_id, owner_id);
    }
    assert!(
        world.held_passport(file_id).await.is_some(),
        "the chain keeps the committer's passport for its first step"
    );
    // And: while the reaper still waits on the source, no job is asked of
    // Jobs, the minted job opens nothing, and a reprocess is refused
    world.await_lock_waiters(1).await;
    jobs.expect_no_command(Duration::from_secs(1)).await;
    assert_eq!(
        error_code(&context(&world, &runner, file_id, minted).await),
        "JOB_NOT_ACTIVE",
        "no runner is offered the file before its source is readable"
    );
    assert_eq!(
        error_code(&process(&world, &owner, file_id).await),
        "FILE_PROCESSING"
    );
    assert_eq!(
        kinds(&world.processing_facts(file_id).await),
        vec!["ChainStarted", "SourceAwaited"],
        "still nothing created"
    );

    // When: the reaper promotes the source
    hold.release().await;

    // Then: the awaited step is asked of Jobs with the id minted at the commit
    let create = jobs.await_create(file_id).await;
    assert_eq!(create.job_id, minted);
    assert_eq!(create.runner_type, RENDER);
    let carried: br_core_auth::Passport =
        serde_json::from_value(create.passport.clone().expect("the job carries a passport"))
            .expect("the carried passport decodes");
    assert_eq!(
        carried,
        <br_core_auth::Passport as br_core_auth::PassportHeader>::from_header(&owner).unwrap(),
        "the committer's passport, captured at the commit"
    );
    assert_eq!(
        world
            .blob_state(world.source_of(file_id).await)
            .await
            .as_deref(),
        Some("uploaded")
    );
    // And: the runner's very first call reads the source
    let first = context(&world, &runner, file_id, minted).await;
    assert!(
        first.get("errors").is_none(),
        "the first call is served: {first}"
    );
    let source_url = first["data"]["workspaceRunnerContext"]["sourceUrl"]
        .as_str()
        .expect("a source url");
    let fetched = world.http.get(source_url).send().await.unwrap();
    assert!(fetched.status().is_success(), "{}", fetched.status());
    assert_eq!(fetched.bytes().await.unwrap().as_ref(), BYTES);
    let facts = world.processing_facts(file_id).await;
    assert_eq!(
        kinds(&facts),
        vec!["ChainStarted", "SourceAwaited", "JobCreated"]
    );
    assert_eq!(facts[2].payload["job_id"], minted.to_string());
    assert_eq!(facts[2].payload["step_index"], 0);
    assert_eq!(world.job_of(file_id).await, Some(minted));

    // When: the promotion is delivered again
    let replayed = world.send_source_promoted(file_id).await;
    world.await_consumed(replayed).await;

    // Then: the step is created once
    jobs.expect_no_command(Duration::from_millis(500)).await;
    assert_eq!(
        kinds(&world.processing_facts(file_id).await),
        vec!["ChainStarted", "SourceAwaited", "JobCreated"]
    );

    world.cleanup().await;
}

#[tokio::test]
async fn a_commit_on_a_source_already_promoted_starts_the_chain_in_its_own_transaction() {
    // Given: a landed upload the reaper already promoted (a late commit)
    let world = World::start_with("pod-promotion-late-commit", processing_on_commit()).await;
    let jobs = JobsStandIn::attach(&world).await;
    let owner = manager_passport(Uuid::now_v7(), "Ada");
    install_render_rule(&world, &owner).await;
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = Uuid::now_v7();
    let upload_ticket = ticket(
        &request(
            &world,
            &owner,
            file_id,
            &UploadRequest::text(drive, "", "late.txt", BYTES),
        )
        .await,
    );
    assert!((200..300).contains(&post_bytes(&world, &upload_ticket, BYTES, "late.txt").await));
    world.await_source_promoted(file_id).await;

    // When: the owner commits it
    ok(&commit(&world, &owner, file_id).await);

    // Then: the first job is asked of Jobs by the commit itself — nothing
    // awaits a promotion that already happened
    let create = jobs.await_create(file_id).await;
    let facts = world.processing_facts(file_id).await;
    assert_eq!(kinds(&facts), vec!["ChainStarted", "JobCreated"]);
    assert_eq!(facts[1].payload["job_id"], create.job_id.to_string());
    assert_eq!(
        facts[0].correlation_id, facts[1].correlation_id,
        "one gesture"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn a_process_file_right_after_the_commit_awaits_the_source_too() {
    // Given: a host with the two-gesture flow, and a landed upload the reaper
    // has not promoted, committed: READY, stored only
    let world = World::start("pod-promotion-process-file").await;
    let Awaiting {
        world,
        jobs,
        owner,
        file_id,
        hold,
        ..
    } = landed_and_held(world).await;
    ok(&commit(&world, &owner, file_id).await);
    assert_eq!(
        world.file(&owner, file_id).await["processingState"],
        "READY"
    );

    // When: the owner processes it at once
    ok(&process(&world, &owner, file_id).await);

    // Then: the file is PROCESSING and its first step awaits the source
    assert_eq!(
        world.file(&owner, file_id).await["processingState"],
        "PROCESSING"
    );
    let minted = awaited_job(&world.processing_facts(file_id).await);
    world.await_lock_waiters(1).await;
    jobs.expect_no_command(Duration::from_secs(1)).await;

    // When: the reaper promotes the source
    hold.release().await;

    // Then: the step is asked of Jobs with its minted id
    assert_eq!(jobs.await_create(file_id).await.job_id, minted);

    world.cleanup().await;
}

#[tokio::test]
async fn a_cancel_while_the_source_is_awaited_drops_the_step_and_the_promotion_starts_nothing() {
    // Given: a committed upload whose first step awaits the source
    let world = World::start_with("pod-promotion-cancel", processing_on_commit()).await;
    let Awaiting {
        world,
        jobs,
        owner,
        file_id,
        hold,
        ..
    } = landed_and_held(world).await;
    ok(&commit(&world, &owner, file_id).await);
    let minted = awaited_job(&world.processing_facts(file_id).await);

    // When: the owner cancels the processing
    ok(&world
        .gql(&owner, CANCEL, serde_json::json!({ "f": file_id }))
        .await);

    // Then: the step is dropped at once — the file FAILED `cancelled` — and
    // Jobs, which never heard of the step, is sent nothing
    let file = world.file(&owner, file_id).await;
    assert_eq!(file["processingState"], "FAILED");
    assert_eq!(file["processingError"], "cancelled");
    assert!(file["progress"].is_null());
    let facts = world.processing_facts(file_id).await;
    assert_eq!(
        kinds(&facts),
        vec!["ChainStarted", "SourceAwaited", "AwaitedStepDropped"]
    );
    assert_eq!(
        facts[2].payload,
        serde_json::json!({
            "kind": "AwaitedStepDropped",
            "step_index": 0,
            "runner_type": RENDER,
            "job_id": minted,
            "reason_code": "cancelled",
        })
    );
    assert!(world.held_passport(file_id).await.is_none());
    jobs.expect_no_command(Duration::from_secs(1)).await;

    // When: the reaper promotes the source, and the promotion is delivered
    // again on top
    hold.release().await;
    world.await_source_promoted(file_id).await;
    let replayed = world.send_source_promoted(file_id).await;
    world.await_consumed(replayed).await;

    // Then: nothing starts
    jobs.expect_no_command(Duration::from_millis(500)).await;
    assert_eq!(world.processing_facts(file_id).await.len(), 3);
    assert_eq!(
        world.file(&owner, file_id).await["processingState"],
        "FAILED"
    );

    // And: a reprocess on the promoted source starts its job in its own
    // gesture
    ok(&process(&world, &owner, file_id).await);
    let create = jobs.await_create(file_id).await;
    assert_ne!(create.job_id, minted, "a new chain, a new job");
    let facts = world.processing_facts(file_id).await;
    assert_eq!(
        kinds(&facts[3..]),
        vec!["ChainStarted", "JobCreated"],
        "no wait on a promoted source"
    );
    assert_eq!(facts[3].payload["trigger"], "REPROCESS");

    world.cleanup().await;
}

#[tokio::test]
async fn a_freeze_while_the_source_is_awaited_drops_the_step_and_sends_no_cancel() {
    // Given: a committed upload whose first step awaits the source
    let world = World::start_with("pod-promotion-freeze", processing_on_commit()).await;
    let Awaiting {
        world,
        jobs,
        owner,
        drive,
        file_id,
        hold,
        ..
    } = landed_and_held(world).await;
    ok(&commit(&world, &owner, file_id).await);

    // When: the host freezes the drive
    ok(&world
        .gql(&owner, FREEZE, serde_json::json!({ "id": drive }))
        .await);

    // Then: the step is dropped, the file FAILED `cancelled`, and Jobs is
    // sent nothing
    let file = world.file(&owner, file_id).await;
    assert_eq!(file["processingState"], "FAILED");
    assert_eq!(file["processingError"], "cancelled");
    assert_eq!(
        kinds(&world.processing_facts(file_id).await),
        vec!["ChainStarted", "SourceAwaited", "AwaitedStepDropped"]
    );
    jobs.expect_no_command(Duration::from_secs(1)).await;

    // When: the reaper promotes the source
    hold.release().await;
    world.await_source_promoted(file_id).await;
    let replayed = world.send_source_promoted(file_id).await;
    world.await_consumed(replayed).await;

    // Then: nothing starts
    jobs.expect_no_command(Duration::from_millis(500)).await;
    assert_eq!(world.processing_facts(file_id).await.len(), 3);

    world.cleanup().await;
}

#[tokio::test]
async fn a_delete_while_the_source_is_awaited_leaves_nothing_to_start() {
    // Given: a committed upload whose first step awaits the source
    let world = World::start_with("pod-promotion-delete", processing_on_commit()).await;
    let Awaiting {
        world,
        jobs,
        owner,
        file_id,
        hold,
        ..
    } = landed_and_held(world).await;
    ok(&commit(&world, &owner, file_id).await);
    let source = world.source_of(file_id).await;

    // When: the owner deletes the file — its release of the source queues
    // behind the reaper's promotion, both held — and the hold is lifted
    let deleting = world.gql(&owner, DELETE, serde_json::json!({ "f": file_id }));
    let lifting = async {
        world.await_lock_waiters(2).await;
        hold.release().await;
    };
    let (deleted, ()) = tokio::join!(deleting, lifting);
    ok(&deleted);

    // Then: the file is gone, its source released, and no job was ever
    // asked of Jobs — nor sent a cancel for a step it never heard of
    assert!(world.file(&owner, file_id).await.is_null());
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while world.blob_state(source).await.as_deref() == Some("uploaded")
        || world.blob_state(source).await.as_deref() == Some("pending")
    {
        assert!(
            std::time::Instant::now() < deadline,
            "the source is never released"
        );
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    let replayed = world.send_source_promoted(file_id).await;
    world.await_consumed(replayed).await;
    jobs.expect_no_command(Duration::from_secs(1)).await;

    world.cleanup().await;
}

#[tokio::test]
async fn an_initiator_erased_while_the_source_is_awaited_drops_the_step_passport_missing() {
    // Given: a committed upload whose first step awaits the source
    let world = World::start_with("pod-promotion-erase", processing_on_commit()).await;
    let Awaiting {
        world,
        jobs,
        owner_id,
        owner,
        file_id,
        hold,
        ..
    } = landed_and_held(world).await;
    ok(&commit(&world, &owner, file_id).await);
    let minted = awaited_job(&world.processing_facts(file_id).await);

    // When: the committer is erased, then the reaper promotes the source
    world.erase(owner_id).await;
    assert!(world.held_passport(file_id).await.is_none());
    hold.release().await;

    // Then: the step is never sent without a passport — it is dropped and
    // the file FAILED `passport_missing`
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let facts = loop {
        let facts = world.processing_facts(file_id).await;
        if facts.len() > 2 {
            break facts;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the promotion never landed"
        );
        tokio::time::sleep(Duration::from_millis(40)).await;
    };
    assert_eq!(
        kinds(&facts),
        vec!["ChainStarted", "SourceAwaited", "AwaitedStepDropped"]
    );
    assert_eq!(facts[2].payload["job_id"], minted.to_string());
    assert_eq!(facts[2].payload["reason_code"], "passport_missing");
    let (state, error): (String, Option<String>) =
        sqlx::query_as("SELECT state, error_code FROM drive.file_processing WHERE file_id = $1")
            .bind(file_id)
            .fetch_one(&world.db.app)
            .await
            .expect("read the file's processing");
    assert_eq!(
        (state.as_str(), error.as_deref()),
        ("failed", Some("passport_missing"))
    );
    jobs.expect_no_command(Duration::from_millis(500)).await;

    world.cleanup().await;
}

#[tokio::test]
async fn the_rule_is_decided_at_the_commit_and_runs_even_if_deleted_before_the_promotion() {
    // Given: a committed upload whose first step awaits the source
    let world = World::start_with("pod-promotion-rule-gone", processing_on_commit()).await;
    let Awaiting {
        world,
        jobs,
        owner,
        rule,
        file_id,
        hold,
        ..
    } = landed_and_held(world).await;
    ok(&commit(&world, &owner, file_id).await);
    let minted = awaited_job(&world.processing_facts(file_id).await);

    // When: the rule is deleted, then the reaper promotes the source
    ok(&world
        .gql(&owner, DELETE_RULE, serde_json::json!({ "id": rule }))
        .await);
    hold.release().await;

    // Then: the chain the commit decided runs — its snapshot, its job id
    let create = jobs.await_create(file_id).await;
    assert_eq!(create.job_id, minted);
    assert_eq!(create.runner_type, RENDER);
    let file = world.file(&owner, file_id).await;
    assert_eq!(file["processingState"], "PROCESSING");
    assert_eq!(file["rulesetId"], rule.to_string());
    assert_eq!(file["steps"][0]["runnerType"], RENDER);

    world.cleanup().await;
}
