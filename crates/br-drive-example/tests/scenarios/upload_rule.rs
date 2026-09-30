//! The uploader chooses the `upload` rule at `RequestUpload` (`rulesetId`):
//! an `upload` rule matching the file's media type, validated as
//! `ProcessFile` validates a named rule, pinned on the pending file and run
//! by the commit's chain in place of the default. Without one, the default
//! `upload` rule runs, as in 0.5.0.

use std::time::Duration;

use uuid::Uuid;

use crate::harness::runner::{RENDER, RuleSpec, create_ruleset, install_render_rule, ruleset_id};
use crate::harness::upload::{
    UploadRequest, commit, post_bytes, request, request_with_ruleset, ticket,
};
use crate::harness::{JobsStandIn, World, WorldOptions, error_code, manager_passport, ok};

const BYTES: &[u8] = b"a document whose processing its uploader chose";
const PRIVATE: &str = "render-on-premises";

fn processing_on_commit() -> WorldOptions {
    WorldOptions {
        process_on_commit: true,
        ..WorldOptions::default()
    }
}

/// A second `upload` rule for text, not the default: the variant an uploader
/// picks per upload.
async fn install_private_rule(world: &World, manager: &str) -> Uuid {
    ruleset_id(
        &create_ruleset(
            world,
            manager,
            RuleSpec {
                name: "render text on premises",
                trigger: "UPLOAD",
                media_types: &["text/plain"],
                steps: &[(PRIVATE, serde_json::json!({ "where": "on-premises" }))],
                is_default: false,
            },
        )
        .await,
    )
}

/// Requests `name` naming `ruleset` and lands its bytes: PENDING, landed.
async fn landed_with(
    world: &World,
    passport: &str,
    drive: Uuid,
    name: &str,
    ruleset: Option<Uuid>,
) -> Uuid {
    let file_id = Uuid::now_v7();
    let upload = UploadRequest::text(drive, "", name, BYTES);
    let response = match ruleset {
        Some(ruleset) => request_with_ruleset(world, passport, file_id, &upload, ruleset).await,
        None => request(world, passport, file_id, &upload).await,
    };
    let upload_ticket = ticket(&response);
    let posted = post_bytes(world, &upload_ticket, BYTES, name).await;
    assert!((200..300).contains(&posted), "the object lands: {posted}");
    file_id
}

#[tokio::test]
async fn a_named_upload_variant_runs_at_commit_instead_of_the_default() {
    // Given: a default upload rule for text, and a second upload rule for
    // text the uploader may pick instead
    let world = World::start_with("pod-upload-rule-chosen", processing_on_commit()).await;
    let jobs = JobsStandIn::attach(&world).await;
    let owner_id = Uuid::now_v7();
    let owner = manager_passport(owner_id, "Ada");
    let default_rule = install_render_rule(&world, &owner).await;
    let private_rule = install_private_rule(&world, &owner).await;
    let drive = world.create_workspace(&owner, "library").await;

    // When: one upload names the variant, another names nothing, and both
    // are committed
    let chosen = landed_with(&world, &owner, drive, "chosen.txt", Some(private_rule)).await;
    let plain = landed_with(&world, &owner, drive, "plain.txt", None).await;
    ok(&commit(&world, &owner, chosen).await);
    ok(&commit(&world, &owner, plain).await);

    // Then: the chosen file's chain runs the variant, with its options
    let create = jobs.await_create(chosen).await;
    assert_eq!(create.runner_type, PRIVATE);
    let config = create.config.expect("the job carries its config");
    assert_eq!(config["options"]["where"], "on-premises");
    let file = world.file(&owner, chosen).await;
    assert_eq!(file["processingState"], "PROCESSING");
    assert_eq!(file["rulesetId"], private_rule.to_string());
    assert_eq!(file["steps"][0]["runnerType"], PRIVATE);
    // And: the other one runs the default, as in 0.5.0
    assert_eq!(jobs.await_create(plain).await.runner_type, RENDER);
    assert_eq!(
        world.file(&owner, plain).await["rulesetId"],
        default_rule.to_string()
    );
    // And: the choice is a fact of the upload ticket's gesture, and the chain
    // names the chosen rule
    let facts = world.facts("drive_file", serde_json::json!(chosen)).await;
    let kinds: Vec<&str> = facts.iter().map(|fact| fact.event_type.as_str()).collect();
    assert_eq!(
        kinds,
        vec![
            "UploadTicketIssued",
            "UploadRulesetChosen",
            "UploadCommitted"
        ]
    );
    assert_eq!(facts[1].seq, 2);
    assert_eq!(facts[1].payload["ruleset_id"], private_rule.to_string());
    assert_eq!(facts[1].correlation_id, facts[0].correlation_id);
    assert_eq!(facts[1].actor_id, owner_id);
    let chain = world.processing_facts(chosen).await;
    assert_eq!(chain[0].event_type, "ChainStarted");
    assert_eq!(chain[0].payload["ruleset_id"], private_rule.to_string());
    let plain_kinds: Vec<String> = world
        .facts("drive_file", serde_json::json!(plain))
        .await
        .into_iter()
        .map(|fact| fact.event_type)
        .collect();
    assert_eq!(plain_kinds, vec!["UploadTicketIssued", "UploadCommitted"]);

    world.cleanup().await;
}

#[tokio::test]
async fn a_rule_that_cannot_run_the_upload_is_refused_at_request_upload() {
    // Given: a reprocess rule for text, an upload rule for PDFs only
    let world = World::start("pod-upload-rule-refused").await;
    let owner = manager_passport(Uuid::now_v7(), "Ada");
    let reprocess_rule = ruleset_id(
        &create_ruleset(
            &world,
            &owner,
            RuleSpec {
                name: "reprocess text",
                trigger: "REPROCESS",
                media_types: &["text/plain"],
                steps: &[(RENDER, serde_json::json!({}))],
                is_default: false,
            },
        )
        .await,
    );
    let pdf_rule = ruleset_id(
        &create_ruleset(
            &world,
            &owner,
            RuleSpec {
                name: "render pdf",
                trigger: "UPLOAD",
                media_types: &["application/pdf"],
                steps: &[(RENDER, serde_json::json!({}))],
                is_default: false,
            },
        )
        .await,
    );
    let drive = world.create_workspace(&owner, "library").await;
    let upload = UploadRequest::text(drive, "", "refused.txt", BYTES);

    for (ruleset, code) in [
        (reprocess_rule, "RULESET_MISMATCH"),
        (pdf_rule, "RULESET_MISMATCH"),
        (Uuid::now_v7(), "RULESET_NOT_FOUND"),
    ] {
        // When: a text upload names a rule that cannot run it
        let file_id = Uuid::now_v7();
        let refused = request_with_ruleset(&world, &owner, file_id, &upload, ruleset).await;

        // Then: the request is refused, and nothing of it stays
        assert_eq!(error_code(&refused), code);
        assert!(world.file(&owner, file_id).await.is_null());
        assert!(
            world
                .facts("drive_file", serde_json::json!(file_id))
                .await
                .is_empty()
        );
    }
    assert!(world.drive_files(&owner, drive).await.is_empty());

    world.cleanup().await;
}

#[tokio::test]
async fn a_chosen_rule_deleted_before_the_commit_stores_the_file_never_the_default() {
    // Given: a default upload rule and a variant, an upload naming the
    // variant, and the variant deleted before the commit
    let world = World::start_with("pod-upload-rule-gone", processing_on_commit()).await;
    let jobs = JobsStandIn::attach(&world).await;
    let owner = manager_passport(Uuid::now_v7(), "Ada");
    install_render_rule(&world, &owner).await;
    let private_rule = install_private_rule(&world, &owner).await;
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = landed_with(&world, &owner, drive, "orphan.txt", Some(private_rule)).await;
    ok(&world
        .gql(
            &owner,
            "mutation($id:UUID!){workspaceDeleteRuleset(id:$id){success}}",
            serde_json::json!({ "id": private_rule }),
        )
        .await);

    // When: the owner commits it
    ok(&commit(&world, &owner, file_id).await);

    // Then: the file is stored, READY — the default never runs in the chosen
    // rule's place — with no job and no chain
    let file = world.file(&owner, file_id).await;
    assert_eq!(file["processingState"], "READY");
    assert!(file["rulesetId"].is_null());
    assert!(world.processing_facts(file_id).await.is_empty());
    jobs.expect_no_command(Duration::from_secs(1)).await;

    world.cleanup().await;
}
