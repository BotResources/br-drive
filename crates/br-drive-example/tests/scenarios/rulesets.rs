use std::time::Duration;

use contract_jobs::catalog::RunnerTypeLifecycle;
use uuid::Uuid;

use crate::harness::runner::{INDEX, RENDER, RuleSpec, create_ruleset, ruleset_id};
use crate::harness::upload::{UploadRequest, upload};
use crate::harness::{
    JobsStandIn, RULESET_DELTAS, World, WorldOptions, catalogue_subscription, error_code,
    manager_passport, next_delta, ok, passport, service_passport,
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
    world
        .await_runner_type(&manager, RENDER, Some("ACTIVE"))
        .await;

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
        "a step whose runner type is not known ACTIVE is warned at save, and the rule is saved"
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
        ok(&updated)["workspaceUpdateRuleset"],
        serde_json::json!({ "id": variant.to_string(), "unknownRunnerTypes": [] })
    );
    let unchanged = world
        .gql(
            &manager,
            "mutation($id:UUID!,$s:[RulesetStepInput!]){workspaceUpdateRuleset(id:$id,steps:$s){id}}",
            serde_json::json!({ "id": variant, "s": [{ "runnerType": RENDER }] }),
        )
        .await;
    assert_eq!(error_code(&unchanged), "NOTHING_TO_CHANGE");
    let promoted = world
        .gql(
            &manager,
            "mutation($id:UUID!){workspaceUpdateRuleset(id:$id,isDefault:true){id}}",
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

#[tokio::test]
async fn two_concurrent_saves_of_one_name_answer_exactly_one_name_taken() {
    let world = World::start("pod-rules-concurrent").await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");

    let world_ref = &world;
    let manager_ref = &manager;
    let save = |media: &'static str| async move {
        let steps = [(RENDER, serde_json::json!({}))];
        let media_types = [media];
        create_ruleset(
            world_ref,
            manager_ref,
            RuleSpec {
                name: "Same Name",
                trigger: "UPLOAD",
                media_types: &media_types,
                steps: &steps,
                is_default: true,
            },
        )
        .await
    };
    let (left, right) = tokio::join!(save("text/plain"), save("text/markdown"));
    let mut codes: Vec<String> = [&left, &right]
        .into_iter()
        .map(|response| {
            if response.get("errors").is_none() {
                ok(response);
                "OK".to_string()
            } else {
                error_code(response)
            }
        })
        .collect();
    codes.sort();
    assert_eq!(
        codes,
        vec!["OK".to_string(), "RULESET_NAME_TAKEN".to_string()],
        "the advisory lock serializes the two saves: {left} / {right}"
    );
    assert_eq!(world.rulesets(&manager).await.len(), 1);

    world.cleanup().await;
}

#[tokio::test]
async fn the_rule_table_is_read_live_and_a_save_carries_its_warning_as_the_cause() {
    let world = World::start("pod-rules-live").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    jobs.declare_runner_type(RENDER, RunnerTypeLifecycle::Active)
        .await;
    world
        .await_runner_type(&manager, RENDER, Some("ACTIVE"))
        .await;
    let mut live =
        catalogue_subscription(&world, &manager, RULESET_DELTAS, "workspaceRulesetsChanged").await;

    let saved = create_ruleset(
        &world,
        &manager,
        RuleSpec {
            name: "Live",
            trigger: "UPLOAD",
            media_types: &["*"],
            steps: &[
                (RENDER, serde_json::json!({})),
                ("ghost", serde_json::json!({})),
            ],
            is_default: true,
        },
    )
    .await;
    let id = ruleset_id(&saved);
    let upsert = next_delta(&mut live, "workspaceRulesetsChanged", |node| {
        node["__typename"] == "DriveUpsert" && node["view"]["id"] == id.to_string()
    })
    .await;
    assert!(
        upsert["cause"].is_null()
            || upsert["cause"]["unknown_runner_types"] == serde_json::json!(["ghost"]),
        "a rule entering the window arrives by repopulation, a later save carries its warning: {upsert}"
    );
    assert_eq!(
        ok(&saved)["workspaceCreateRuleset"]["unknownRunnerTypes"],
        serde_json::json!(["ghost"])
    );
    // When: an update introduces a type the copy does not know
    let introduced = world
        .gql(
            &manager,
            "mutation($id:UUID!,$s:[RulesetStepInput!]){workspaceUpdateRuleset(id:$id,steps:$s){id unknownRunnerTypes}}",
            serde_json::json!({ "id": id, "s": [{ "runnerType": RENDER }, { "runnerType": "phantom" }] }),
        )
        .await;
    assert_eq!(
        ok(&introduced)["workspaceUpdateRuleset"]["unknownRunnerTypes"],
        serde_json::json!(["phantom"])
    );
    // Then: the live upsert carries exactly that warning as its cause
    let warned = next_delta(&mut live, "workspaceRulesetsChanged", |node| {
        node["__typename"] == "DriveUpsert"
            && node["cause"]["kind"] == "Saved"
            && node["view"]["steps"]
                .as_array()
                .is_some_and(|s| s.len() == 2)
    })
    .await;
    assert_eq!(
        warned["cause"],
        serde_json::json!({ "kind": "Saved", "unknown_runner_types": ["phantom"] }),
        "the warning list is the save's cause"
    );

    // When: an update leaves only the known type
    ok(&world
        .gql(
            &manager,
            "mutation($id:UUID!,$s:[RulesetStepInput!]){workspaceUpdateRuleset(id:$id,steps:$s){id}}",
            serde_json::json!({ "id": id, "s": [{ "runnerType": RENDER }] }),
        )
        .await);
    let updated = next_delta(&mut live, "workspaceRulesetsChanged", |node| {
        node["__typename"] == "DriveUpsert"
            && node["cause"]["kind"] == "Saved"
            && node["view"]["steps"]
                .as_array()
                .is_some_and(|s| s.len() == 1)
    })
    .await;
    assert_eq!(
        updated["cause"],
        serde_json::json!({ "kind": "Saved", "unknown_runner_types": [] }),
        "a save warning about nothing says so"
    );
    ok(&world
        .gql(
            &manager,
            "mutation($id:UUID!){workspaceDeleteRuleset(id:$id){success}}",
            serde_json::json!({ "id": id }),
        )
        .await);
    let removed = next_delta(&mut live, "workspaceRulesetsChanged", |node| {
        node["__typename"] == "DriveRemove"
    })
    .await;
    assert_eq!(removed["projector"], "drive_rulesets");
    assert_eq!(removed["key"], id.to_string());

    world.cleanup().await;
}

#[tokio::test]
async fn the_known_runner_types_follow_jobs_catalogue_for_the_people_who_read_the_rules() {
    // Given: Jobs publishes four entries the library must not trust, then one
    // active type and one deprecated type — the copy follows the bucket in
    // order, so once both types are listed the untrusted entries were seen
    let world = World::start("pod-rules-known-types").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let reader = passport(Uuid::now_v7());
    let runner = service_passport(&["workspace:runner"]);
    let oversized = "r".repeat(129);
    jobs.declare_runner_type(&oversized, RunnerTypeLifecycle::Active)
        .await;
    jobs.publish_catalogue_entry("garbled", serde_json::json!("not an entry"))
        .await;
    jobs.publish_catalogue_entry(
        "alias",
        serde_json::json!({ "runner_type": "other", "lifecycle": "ACTIVE" }),
    )
    .await;
    jobs.publish_catalogue_entry(
        "future",
        serde_json::json!({
            "runner_type": "future",
            "lifecycle": "ACTIVE",
            "version": contract_jobs::runner::WIRE_VERSION + 1
        }),
    )
    .await;
    jobs.declare_runner_type(RENDER, RunnerTypeLifecycle::Active)
        .await;
    jobs.declare_runner_type(INDEX, RunnerTypeLifecycle::Deprecated)
        .await;

    // When: a reader of the rules lists the known runner types
    let listed = world
        .await_runner_types(&reader, "render and index", |listed| listed.len() == 2)
        .await;

    // Then: the two published types, in name order, with what Jobs declared;
    // nothing of the untrusted entries
    let names: Vec<(&str, &str)> = listed
        .iter()
        .map(|entry| {
            (
                entry["runnerType"].as_str().unwrap(),
                entry["lifecycle"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(names, vec![(INDEX, "DEPRECATED"), (RENDER, "ACTIVE")]);
    assert!(
        listed
            .iter()
            .all(|entry| entry["seenAt"].as_str().is_some_and(|at| !at.is_empty()))
    );
    // And: the host's ReadRulesets gate keeps a runner out, as for the rules
    assert!(
        world.runner_types(&runner).await.is_empty(),
        "a principal refused the rules reads no runner types"
    );

    // When: a manager saves a rule naming the active, the deprecated and an
    // unpublished type, the active and the unpublished ones twice
    let saved = create_ruleset(
        &world,
        &manager,
        RuleSpec {
            name: "mixed",
            trigger: "UPLOAD",
            media_types: &["*"],
            steps: &[
                (RENDER, serde_json::json!({})),
                (INDEX, serde_json::json!({})),
                ("ghost", serde_json::json!({})),
                (RENDER, serde_json::json!({})),
                ("ghost", serde_json::json!({ "second": true })),
            ],
            is_default: true,
        },
    )
    .await;

    // Then: the rule is saved and warns about the deprecated and the
    // unpublished types, once each, in name order
    assert_eq!(
        ok(&saved)["workspaceCreateRuleset"]["unknownRunnerTypes"],
        serde_json::json!(["ghost", INDEX])
    );
    assert_eq!(world.rulesets(&manager).await.len(), 1);

    // When: Jobs deprecates render, retires index, and fixes the garbled entry
    jobs.declare_runner_type(RENDER, RunnerTypeLifecycle::Deprecated)
        .await;
    jobs.retire_runner_type(INDEX).await;
    jobs.declare_runner_type("garbled", RunnerTypeLifecycle::Active)
        .await;

    // Then: the list follows
    world
        .await_runner_types(&reader, "the catalogue's changes", |listed| {
            let pairs: Vec<(&str, &str)> = listed
                .iter()
                .filter_map(|entry| {
                    Some((entry["runnerType"].as_str()?, entry["lifecycle"].as_str()?))
                })
                .collect();
            pairs == vec![("garbled", "ACTIVE"), (RENDER, "DEPRECATED")]
        })
        .await;

    // And: a garbled entry replacing a good one takes the type out of the list
    jobs.publish_catalogue_entry("garbled", serde_json::json!({ "lifecycle": 3 }))
        .await;
    world.await_runner_type(&reader, "garbled", None).await;

    // And: a value that is not JSON at all takes its type out too, and the
    // copy keeps following the catalogue after it
    jobs.declare_runner_type("garbled", RunnerTypeLifecycle::Active)
        .await;
    world
        .await_runner_type(&reader, "garbled", Some("ACTIVE"))
        .await;
    jobs.publish_catalogue_bytes("garbled", b"\x00 not json")
        .await;
    world.await_runner_type(&reader, "garbled", None).await;
    jobs.declare_runner_type("later", RunnerTypeLifecycle::Active)
        .await;
    world
        .await_runner_type(&reader, "later", Some("ACTIVE"))
        .await;

    world.cleanup().await;
}

#[tokio::test]
async fn a_host_without_the_catalogue_watch_warns_every_step_and_still_launches() {
    // Given: a host that does not start the watch, while Jobs publishes render
    let world = World::start_with(
        "pod-rules-no-watch",
        WorldOptions {
            watch_catalogue: false,
            ..WorldOptions::default()
        },
    )
    .await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    jobs.declare_runner_type(RENDER, RunnerTypeLifecycle::Active)
        .await;

    // When: a manager saves an upload rule naming render
    let saved = create_ruleset(
        &world,
        &manager,
        RuleSpec {
            name: "blind",
            trigger: "UPLOAD",
            media_types: &["text/plain"],
            steps: &[(RENDER, serde_json::json!({}))],
            is_default: true,
        },
    )
    .await;

    // Then: the host vouches for no type — a warning, the rule is saved — and
    // lists none
    assert_eq!(
        ok(&saved)["workspaceCreateRuleset"]["unknownRunnerTypes"],
        serde_json::json!([RENDER])
    );
    assert!(world.runner_types(&manager).await.is_empty());

    // When: a file is uploaded
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "blind.txt", BYTES),
    )
    .await;

    // Then: the step's job is created all the same: the copy is never a
    // condition on a launch
    assert_eq!(jobs.await_create(file_id).await.runner_type, RENDER);
    world.await_state(&owner, file_id, "PROCESSING").await;

    world.cleanup().await;
}
