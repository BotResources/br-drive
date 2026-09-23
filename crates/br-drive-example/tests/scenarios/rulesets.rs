use std::time::Duration;

use contract_jobs::catalog::RunnerTypeLifecycle;
use uuid::Uuid;

use crate::harness::runner::{INDEX, RENDER, RuleSpec, create_ruleset, ruleset_id};
use crate::harness::upload::{UploadRequest, upload};
use crate::harness::{
    JobsStandIn, World, error_code, manager_passport, ok, passport, service_passport,
};

const BYTES: &[u8] = b"ruled bytes";

#[tokio::test]
async fn with_no_rule_declared_an_upload_is_ready_on_commit_and_no_job_is_created() {
    let world = World::start("pod-rules-empty").await;
    let jobs = JobsStandIn::attach(&world).await;
    let owner = passport(Uuid::now_v7());
    let drive = world.create_workspace(&owner, "library").await;

    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "plain.txt", BYTES),
    )
    .await;
    let file = world.file(&owner, file_id).await;
    assert_eq!(file["processingState"], "READY");
    assert!(file["rulesetId"].is_null());
    assert!(file["progress"].is_null());
    jobs.expect_no_command(Duration::from_secs(1)).await;
    assert!(
        world.rulesets(&owner).await.is_empty(),
        "the rule table is empty at boot"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn rulesets_are_managed_by_the_hosts_managers_and_validated_at_save() {
    let world = World::start("pod-rules-crud").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let reader = passport(Uuid::now_v7());
    let runner = service_passport(&["workspace:runner"]);
    jobs.declare_runner_type(RENDER, RunnerTypeLifecycle::Active)
        .await;
    world.await_known_runner_type(RENDER, Some("active")).await;

    let refused = create_ruleset(
        &world,
        &reader,
        RuleSpec {
            name: "not mine to write",
            trigger: "UPLOAD",
            media_types: &["text/plain"],
            steps: &[(RENDER, serde_json::json!({}))],
            is_default: true,
        },
    )
    .await;
    assert_eq!(error_code(&refused), "NOT_A_WORKSPACE_MANAGER");

    let saved = create_ruleset(
        &world,
        &manager,
        RuleSpec {
            name: "Text default",
            trigger: "UPLOAD",
            media_types: &["text/plain", "Text/Markdown"],
            steps: &[
                (RENDER, serde_json::json!({ "dpi": 120 })),
                ("summarize", serde_json::json!({})),
            ],
            is_default: true,
        },
    )
    .await;
    let default_id = ruleset_id(&saved);
    assert_eq!(
        ok(&saved)["workspaceCreateRuleset"]["unknownRunnerTypes"],
        serde_json::json!(["summarize"]),
        "a step whose runner type is not ACTIVE in the catalogue is warned at save"
    );

    let listed = world.rulesets(&reader).await;
    assert_eq!(listed.len(), 1, "any human of the host reads the rules");
    assert_eq!(listed[0]["name"], "Text default");
    assert_eq!(listed[0]["trigger"], "UPLOAD");
    assert_eq!(
        listed[0]["mediaTypes"],
        serde_json::json!(["text/plain", "text/markdown"])
    );
    assert_eq!(
        listed[0]["steps"][0]["options"],
        serde_json::json!({ "dpi": 120 })
    );
    assert!(
        world.rulesets(&runner).await.is_empty(),
        "a service sees no rules"
    );

    let taken = create_ruleset(
        &world,
        &manager,
        RuleSpec {
            name: "text DEFAULT",
            trigger: "REPROCESS",
            media_types: &["*"],
            steps: &[(RENDER, serde_json::json!({}))],
            is_default: false,
        },
    )
    .await;
    assert_eq!(error_code(&taken), "RULESET_NAME_TAKEN");
    let second_default = create_ruleset(
        &world,
        &manager,
        RuleSpec {
            name: "Another text default",
            trigger: "UPLOAD",
            media_types: &["text/plain"],
            steps: &[(RENDER, serde_json::json!({}))],
            is_default: true,
        },
    )
    .await;
    assert_eq!(error_code(&second_default), "DEFAULT_ALREADY_SET");
    let star = create_ruleset(
        &world,
        &manager,
        RuleSpec {
            name: "Catch-all",
            trigger: "UPLOAD",
            media_types: &["*"],
            steps: &[(RENDER, serde_json::json!({}))],
            is_default: true,
        },
    )
    .await;
    ok(&star);
    let variant = ruleset_id(
        &create_ruleset(
            &world,
            &manager,
            RuleSpec {
                name: "Text on-premises",
                trigger: "UPLOAD",
                media_types: &["text/plain"],
                steps: &[(RENDER, serde_json::json!({ "where": "here" }))],
                is_default: false,
            },
        )
        .await,
    );
    for (name, media_types, steps, code) in [
        ("", vec!["text/plain"], vec![RENDER], "INVALID_RULESET"),
        ("No media", vec![], vec![RENDER], "INVALID_RULESET"),
        (
            "Bad media",
            vec!["plain"],
            vec![RENDER],
            "INVALID_MEDIA_TYPE",
        ),
        ("No steps", vec!["text/plain"], vec![], "INVALID_RULESET"),
        (
            "Bad step",
            vec!["text/plain"],
            vec!["has space"],
            "INVALID_RULESET",
        ),
    ] {
        let steps: Vec<(&str, serde_json::Value)> = steps
            .into_iter()
            .map(|runner_type| (runner_type, serde_json::json!({})))
            .collect();
        let refused = create_ruleset(
            &world,
            &manager,
            RuleSpec {
                name,
                trigger: "UPLOAD",
                media_types: &media_types,
                steps: &steps,
                is_default: false,
            },
        )
        .await;
        assert_eq!(error_code(&refused), code, "{name:?}");
    }
    let bad_trigger = world
        .gql(
            &manager,
            "mutation($id:UUID!){workspaceCreateRuleset(id:$id,name:\"x\",trigger:SOMETIMES,mediaTypes:[\"*\"],steps:[]){id}}",
            serde_json::json!({ "id": Uuid::now_v7() }),
        )
        .await;
    assert!(
        bad_trigger.get("errors").is_some(),
        "an unknown trigger is refused by the schema"
    );

    let updated = world
        .gql(
            &manager,
            "mutation($id:UUID!,$s:[RulesetStepInput!]){workspaceUpdateRuleset(id:$id,steps:$s){id unknownRunnerTypes}}",
            serde_json::json!({ "id": variant, "s": [{ "runnerType": RENDER }] }),
        )
        .await;
    assert_eq!(
        ok(&updated)["workspaceUpdateRuleset"]["unknownRunnerTypes"],
        serde_json::json!([])
    );
    let unchanged = world
        .gql(
            &manager,
            "mutation($id:UUID!,$s:[RulesetStepInput!]){workspaceUpdateRuleset(id:$id,steps:$s){id unknownRunnerTypes}}",
            serde_json::json!({ "id": variant, "s": [{ "runnerType": RENDER }] }),
        )
        .await;
    assert_eq!(error_code(&unchanged), "NOTHING_TO_CHANGE");
    let promoted = world
        .gql(
            &manager,
            "mutation($id:UUID!){workspaceUpdateRuleset(id:$id,isDefault:true){id unknownRunnerTypes}}",
            serde_json::json!({ "id": variant }),
        )
        .await;
    assert_eq!(
        error_code(&promoted),
        "DEFAULT_ALREADY_SET",
        "promoting a variant while a default covers text/plain is refused"
    );
    let unknown = world
        .gql(
            &manager,
            "mutation($id:UUID!){workspaceDeleteRuleset(id:$id){success}}",
            serde_json::json!({ "id": Uuid::now_v7() }),
        )
        .await;
    assert_eq!(error_code(&unknown), "RULESET_NOT_FOUND");
    ok(&world
        .gql(
            &manager,
            "mutation($id:UUID!){workspaceDeleteRuleset(id:$id){success}}",
            serde_json::json!({ "id": default_id }),
        )
        .await);
    let names: Vec<String> = world
        .rulesets(&manager)
        .await
        .iter()
        .map(|rule| rule["name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(names, vec!["Catch-all", "Text on-premises"]);
    let _ = INDEX;

    world.cleanup().await;
}
