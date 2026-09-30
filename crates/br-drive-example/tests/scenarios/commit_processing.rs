//! The commit starts the upload rule's chain (`DriveHost::process_on_commit`):
//! a file a default `upload` rule matches goes from PENDING to PROCESSING in
//! the commit's transaction, never READY in between; a file no rule matches
//! is stored, READY, and the commit is not refused; with the switch off the
//! commit only stores, as the two-gesture flow does.

use std::time::Duration;

use uuid::Uuid;

use crate::harness::runner::{RuleSpec, create_ruleset, install_render_rule};
use crate::harness::upload::{UploadRequest, commit, post_bytes, process, request, ticket, upload};
use crate::harness::{
    JobsStandIn, World, WorldOptions, drive_subscription, error_code, manager_passport, ok, quiet,
};

const BYTES: &[u8] = b"a document processed from its commit";

fn processing_on_commit() -> WorldOptions {
    WorldOptions {
        process_on_commit: true,
        ..WorldOptions::default()
    }
}

/// Requests an upload and posts its bytes: the file is PENDING, its object
/// landed.
async fn landed(world: &World, passport: &str, drive: Uuid, name: &str) -> Uuid {
    let file_id = Uuid::now_v7();
    let upload_ticket = ticket(
        &request(
            world,
            passport,
            file_id,
            &UploadRequest::text(drive, "", name, BYTES),
        )
        .await,
    );
    let posted = post_bytes(world, &upload_ticket, BYTES, name).await;
    assert!((200..300).contains(&posted), "the object lands: {posted}");
    file_id
}

#[tokio::test]
async fn a_commit_starts_the_matching_upload_rule_in_its_own_transaction() {
    // Given: a default upload rule for text, and a landed text upload, watched
    let world = World::start_with("pod-commit-processes", processing_on_commit()).await;
    let jobs = JobsStandIn::attach(&world).await;
    let owner_id = Uuid::now_v7();
    let owner = manager_passport(owner_id, "Ada");
    install_render_rule(&world, &owner).await;
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = landed(&world, &owner, drive, "processed.txt").await;
    let mut files = drive_subscription(&world, &owner, drive).await;
    quiet(&mut files).await;

    // When: the owner commits it
    ok(&commit(&world, &owner, file_id).await);

    // Then: the file is PROCESSING, its first job asked of Jobs — never READY
    let create = jobs.await_create(file_id).await;
    let file = world.file(&owner, file_id).await;
    assert_eq!(file["processingState"], "PROCESSING");
    assert_eq!(file["progress"]["stepIndex"], 0);
    let mut shown = Vec::new();
    while let Some(delta) = files.try_next_payload(Duration::from_millis(800)).await {
        let node = &delta["workspaceDriveChanged"];
        if node["view"]["id"] == file_id.to_string() {
            shown.push(node["view"]["processingState"].clone());
        }
    }
    assert!(!shown.is_empty(), "the commit is published");
    assert!(
        shown.iter().all(|state| state == "PROCESSING"),
        "never READY first: {shown:?}"
    );
    // And: the facts of the one transaction — the commit, then the chain and
    // its job — share its correlation and its hand
    let file_facts = world.facts("drive_file", serde_json::json!(file_id)).await;
    let committed = file_facts.last().expect("the commit");
    assert_eq!(committed.event_type, "UploadCommitted");
    let processing = world.processing_facts(file_id).await;
    let chain: Vec<_> = processing
        .iter()
        .filter(|fact| matches!(fact.event_type.as_str(), "ChainStarted" | "JobCreated"))
        .collect();
    assert_eq!(
        chain
            .iter()
            .map(|fact| fact.event_type.as_str())
            .collect::<Vec<_>>(),
        vec!["ChainStarted", "JobCreated"]
    );
    assert_eq!(chain[0].seq, 1);
    assert_eq!(chain[0].payload["trigger"], "UPLOAD");
    assert_eq!(chain[1].payload["job_id"], create.job_id.to_string());
    for fact in &chain {
        assert_eq!(fact.correlation_id, committed.correlation_id, "one gesture");
        assert_eq!(fact.actor_id, owner_id);
        assert_eq!(fact.occurred_at, committed.occurred_at, "one transaction");
    }
    // And: a later ProcessFile is a reprocess, refused while the chain runs
    assert_eq!(
        error_code(&process(&world, &owner, file_id).await),
        "FILE_PROCESSING"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn a_commit_whose_chain_cannot_start_rolls_back_whole() {
    // Given: a default upload rule whose job the host's fact table refuses
    let world = World::start_with("pod-commit-rollback", processing_on_commit()).await;
    let jobs = JobsStandIn::attach(&world).await;
    let owner = manager_passport(Uuid::now_v7(), "Ada");
    ok(&create_ruleset(
        &world,
        &owner,
        RuleSpec {
            name: "unrecordable",
            trigger: "UPLOAD",
            media_types: &["text/plain"],
            steps: &[(
                br_drive_example::kernel::drive::UNRECORDABLE_RUNNER_TYPE,
                serde_json::json!({}),
            )],
            is_default: true,
        },
    )
    .await);
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = landed(&world, &owner, drive, "refused.txt").await;

    // When: the owner commits it
    let refused = commit(&world, &owner, file_id).await;

    // Then: the commit fails with the host's code, and nothing of it stays —
    // the file is still PENDING, no fact was kept, no job was asked for
    assert_eq!(error_code(&refused), "FACT_REFUSED");
    let file = world.file(&owner, file_id).await;
    assert_eq!(file["processingState"], "PENDING");
    let kinds: Vec<String> = world
        .facts("drive_file", serde_json::json!(file_id))
        .await
        .into_iter()
        .map(|fact| fact.event_type)
        .collect();
    assert_eq!(kinds, vec!["UploadTicketIssued"]);
    assert!(world.processing_facts(file_id).await.is_empty());
    jobs.expect_no_command(Duration::from_secs(1)).await;

    world.cleanup().await;
}

#[tokio::test]
async fn a_commit_no_rule_matches_stores_the_file_ready() {
    // Given: a default upload rule for text only, and a landed binary upload
    let world = World::start_with("pod-commit-stores", processing_on_commit()).await;
    let jobs = JobsStandIn::attach(&world).await;
    let owner = manager_passport(Uuid::now_v7(), "Ada");
    install_render_rule(&world, &owner).await;
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = Uuid::now_v7();
    let binary = UploadRequest {
        media_type: "application/octet-stream",
        ..UploadRequest::text(drive, "", "stored.bin", BYTES)
    };
    let upload_ticket = ticket(&request(&world, &owner, file_id, &binary).await);
    assert!((200..300).contains(&post_bytes(&world, &upload_ticket, BYTES, "stored.bin").await));

    // When: the owner commits it
    ok(&commit(&world, &owner, file_id).await);

    // Then: it is READY — stored only — with no job, no refusal, no chain
    let file = world.file(&owner, file_id).await;
    assert_eq!(file["processingState"], "READY");
    assert!(file["progress"].is_null());
    assert!(world.processing_facts(file_id).await.is_empty());
    jobs.expect_no_command(Duration::from_secs(1)).await;

    world.cleanup().await;
}

#[tokio::test]
async fn with_the_switch_off_the_commit_only_stores_and_process_file_starts_the_chain() {
    // Given: a host that keeps the two-gesture flow, and a default upload rule
    let world = World::start("pod-commit-two-steps").await;
    let jobs = JobsStandIn::attach(&world).await;
    let owner = manager_passport(Uuid::now_v7(), "Ada");
    let rule = install_render_rule(&world, &owner).await;
    let drive = world.create_workspace(&owner, "library").await;

    // When: the owner uploads and commits a text file
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "two-steps.txt", BYTES),
    )
    .await;

    // Then: it is READY, no job asked for
    assert_eq!(
        world.file(&owner, file_id).await["processingState"],
        "READY"
    );
    jobs.expect_no_command(Duration::from_secs(1)).await;
    assert!(world.processing_facts(file_id).await.is_empty());

    // When: the owner processes it
    ok(&process(&world, &owner, file_id).await);

    // Then: the upload rule's chain starts
    jobs.await_create(file_id).await;
    let file = world.file(&owner, file_id).await;
    assert_eq!(file["processingState"], "PROCESSING");
    assert_eq!(file["rulesetId"], rule.to_string());

    world.cleanup().await;
}
