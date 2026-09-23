use std::time::Duration;

use contract_jobs::catalog::RunnerTypeLifecycle;
use contract_jobs::command::TriggeredBy;
use uuid::Uuid;

use crate::harness::runner::{
    INDEX, RENDER, RUNNER_SCOPE, Report, RuleSpec, context, create_ruleset, report, ruleset_id,
};
use crate::harness::upload::{UploadRequest, request, ticket, upload};
use crate::harness::{
    JobsStandIn, World, drive_subscription, error_code, manager_passport, next_drive_delta, ok,
    passport, service_passport,
};

const BYTES: &[u8] = b"the source to process";

async fn catalogue(world: &World, jobs: &JobsStandIn) {
    jobs.declare_runner_type(RENDER, RunnerTypeLifecycle::Active)
        .await;
    jobs.declare_runner_type(INDEX, RunnerTypeLifecycle::Active)
        .await;
    world.await_known_runner_type(RENDER, Some("active")).await;
    world.await_known_runner_type(INDEX, Some("active")).await;
}

async fn two_step_rule(world: &World, manager: &str) -> Uuid {
    ruleset_id(
        &create_ruleset(
            world,
            manager,
            RuleSpec {
                name: "render then index",
                trigger: "UPLOAD",
                media_types: &["text/plain"],
                steps: &[
                    (RENDER, serde_json::json!({ "dpi": 150 })),
                    (INDEX, serde_json::json!({})),
                ],
                is_default: true,
            },
        )
        .await,
    )
}

async fn reprocess_rule(world: &World, manager: &str) -> Uuid {
    ruleset_id(
        &create_ruleset(
            world,
            manager,
            RuleSpec {
                name: "reprocess text",
                trigger: "REPROCESS",
                media_types: &["text/*"],
                steps: &[(RENDER, serde_json::json!({}))],
                is_default: true,
            },
        )
        .await,
    )
}

#[tokio::test]
async fn the_ruleset_chain_runs_step_by_step_over_jobs_facts_and_lands_ready() {
    let world = World::start("pod-chain").await;
    let jobs = JobsStandIn::attach(&world).await;
    let owner_id = Uuid::now_v7();
    let owner = manager_passport(owner_id, "Ada Lovelace");
    let runner = service_passport(&[RUNNER_SCOPE]);
    catalogue(&world, &jobs).await;
    let rule = two_step_rule(&world, &owner).await;
    let drive = world.create_workspace(&owner, "library").await;
    let mut files = drive_subscription(&world, &owner, drive).await;

    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "report.txt", BYTES),
    )
    .await;
    let started = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["view"]["processingState"] == "PROCESSING"
    })
    .await;
    assert_eq!(
        started["view"]["progress"]["stepIndex"], 0,
        "the file enters the window already processing (the engine delivers an entering key by \
         repopulation, without the ProcessingStarted cause)"
    );
    assert_eq!(started["view"]["rulesetId"], rule.to_string());
    assert_eq!(started["view"]["progress"]["stepIndex"], 0);
    assert_eq!(started["view"]["progress"]["stepCount"], 2);
    assert_eq!(started["view"]["progress"]["runnerType"], RENDER);
    assert_eq!(started["view"]["progress"]["plan"], serde_json::json!([]));
    assert_eq!(
        started["view"]["affordances"]["editPage"]["reason"],
        "FILE_PROCESSING"
    );
    assert_eq!(
        started["view"]["affordances"]["process"]["reason"],
        "FILE_PROCESSING"
    );

    let create = jobs.await_create(file_id).await;
    let job_a = create.job_id;
    assert_eq!(create.runner_type, RENDER);
    assert_eq!(create.producer, "workspace");
    assert_eq!(create.source_bc.as_deref(), Some("workspace"));
    assert_eq!(create.source_entity_id, Some(file_id));
    assert!(create.parent_job_id.is_none());
    assert_eq!(
        create.triggered_by,
        Some(TriggeredBy::Identified {
            id: owner_id,
            display_name: "Ada Lovelace".to_string(),
        })
    );
    let config = create.config.clone().expect("the job config");
    assert_eq!(config["host"], "workspace");
    assert_eq!(config["file_id"], file_id.to_string());
    assert_eq!(config["job_id"], job_a.to_string());
    assert_eq!(config["context_root"], "workspaceRunnerContext");
    assert_eq!(
        config["image_upload_root"],
        "workspaceRunnerRequestImageUpload"
    );
    assert_eq!(config["report_root"], "workspaceRunnerReport");
    assert_eq!(config["step"], 0);
    assert_eq!(config["options"], serde_json::json!({ "dpi": 150 }));
    assert!(
        !config.to_string().contains("http"),
        "the config never carries a presigned URL: {config}"
    );

    let run_a = Uuid::now_v7();
    jobs.queue(job_a, RENDER).await;
    jobs.start(job_a, run_a).await;
    jobs.declare_plan(job_a, run_a, &["load", "render"]).await;
    let planned = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "ProgressChanged"
    })
    .await;
    assert_eq!(
        planned["view"]["progress"]["plan"],
        serde_json::json!(["load", "render"])
    );
    assert!(planned["view"]["progress"]["currentIndex"].is_null());
    jobs.start_step(job_a, run_a, 1, "render").await;
    let stepped = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert"
            && node["cause"]["kind"] == "ProgressChanged"
            && node["view"]["progress"]["currentIndex"] == 1
    })
    .await;
    assert_eq!(stepped["view"]["progress"]["currentLabel"], "render");

    world.await_source_promoted(file_id).await;
    let ctx = ok(&context(&world, &runner, file_id, job_a).await)["workspaceRunnerContext"].clone();
    assert_eq!(ctx["fileId"], file_id.to_string());
    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id: job_a,
            pages: vec![(1, "rendered page one")],
            origin: None,
            indexer: None,
            done: true,
        },
    )
    .await);
    jobs.await_finish(job_a).await;
    jobs.complete(job_a).await;

    let next = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "ProcessingStarted"
    })
    .await;
    assert_eq!(next["cause"]["step"], 1);
    assert_eq!(next["view"]["progress"]["stepIndex"], 1);
    assert_eq!(next["view"]["progress"]["runnerType"], INDEX);
    assert_eq!(next["view"]["progress"]["plan"], serde_json::json!([]));
    let create_b = jobs.await_create(file_id).await;
    let job_b = create_b.job_id;
    assert_ne!(job_b, job_a);
    assert_eq!(create_b.runner_type, INDEX);
    assert_eq!(create_b.parent_job_id, Some(job_a));
    assert_eq!(create_b.config.as_ref().unwrap()["step"], 1);
    assert_eq!(
        error_code(&context(&world, &runner, file_id, job_a).await),
        "JOB_NOT_ACTIVE",
        "the finished step's job no longer opens the file"
    );

    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id: job_b,
            pages: vec![],
            origin: None,
            indexer: Some(("One page about nothing.", 1, 12)),
            done: true,
        },
    )
    .await);
    jobs.await_finish(job_b).await;
    jobs.complete(job_b).await;
    let finished = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "ProcessingFinished"
    })
    .await;
    assert_eq!(finished["view"]["processingState"], "READY");
    assert!(finished["view"]["progress"].is_null());
    assert_eq!(finished["view"]["summary"], "One page about nothing.");
    assert_eq!(finished["view"]["rulesetId"], rule.to_string());
    assert_eq!(finished["view"]["affordances"]["process"]["allowed"], true);
    let file = world.file(&owner, file_id).await;
    assert_eq!(
        file["steps"].as_array().unwrap().len(),
        2,
        "the snapshot stays for replay"
    );
    assert!(file["processingError"].is_null());
    assert_eq!(world.file_pages(&owner, file_id).await.len(), 1);
    jobs.expect_no_command(Duration::from_secs(1)).await;

    world.cleanup().await;
}

#[tokio::test]
async fn a_variant_is_picked_by_id_the_catch_all_serves_other_media_types_and_mismatches_are_refused()
 {
    let world = World::start("pod-chain-variant").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    catalogue(&world, &jobs).await;
    two_step_rule(&world, &manager).await;
    let variant = ruleset_id(
        &create_ruleset(
            &world,
            &manager,
            RuleSpec {
                name: "render on-premises",
                trigger: "UPLOAD",
                media_types: &["text/plain"],
                steps: &[(RENDER, serde_json::json!({ "where": "here" }))],
                is_default: false,
            },
        )
        .await,
    );
    let catch_all = ruleset_id(
        &create_ruleset(
            &world,
            &manager,
            RuleSpec {
                name: "everything else",
                trigger: "UPLOAD",
                media_types: &["*"],
                steps: &[(INDEX, serde_json::json!({}))],
                is_default: true,
            },
        )
        .await,
    );
    let reprocess = reprocess_rule(&world, &manager).await;
    let drive = world.create_workspace(&owner, "library").await;

    let picked = Uuid::now_v7();
    let picked_ticket = ticket(
        &request(
            &world,
            &owner,
            picked,
            &UploadRequest::text(drive, "", "picked.txt", BYTES),
        )
        .await,
    );
    let status =
        crate::harness::upload::post_bytes(&world, &picked_ticket, BYTES, "picked.txt").await;
    assert!((200..300).contains(&status));
    let mismatch = world
        .gql(
            &owner,
            "mutation($f:UUID!,$r:UUID){workspaceCommitUpload(fileId:$f,rulesetId:$r){success}}",
            serde_json::json!({ "f": picked, "r": reprocess }),
        )
        .await;
    assert_eq!(
        error_code(&mismatch),
        "RULESET_MISMATCH",
        "a reprocess rule cannot serve an upload"
    );
    let unknown = world
        .gql(
            &owner,
            "mutation($f:UUID!,$r:UUID){workspaceCommitUpload(fileId:$f,rulesetId:$r){success}}",
            serde_json::json!({ "f": picked, "r": Uuid::now_v7() }),
        )
        .await;
    assert_eq!(error_code(&unknown), "RULESET_NOT_FOUND");
    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!,$r:UUID){workspaceCommitUpload(fileId:$f,rulesetId:$r){success}}",
            serde_json::json!({ "f": picked, "r": variant }),
        )
        .await);
    let create = jobs.await_create(picked).await;
    assert_eq!(create.runner_type, RENDER);
    assert_eq!(
        create.config.as_ref().unwrap()["options"],
        serde_json::json!({ "where": "here" })
    );
    assert_eq!(
        world.file(&owner, picked).await["rulesetId"],
        variant.to_string()
    );

    let other = Uuid::now_v7();
    let other_ticket = ticket(
        &request(
            &world,
            &owner,
            other,
            &UploadRequest {
                drive,
                path: "",
                name: "data.csv",
                media_type: "text/csv",
                bytes: BYTES,
            },
        )
        .await,
    );
    let status = crate::harness::upload::post_bytes(&world, &other_ticket, BYTES, "data.csv").await;
    assert!((200..300).contains(&status));
    ok(&crate::harness::upload::commit(&world, &owner, other).await);
    let create = jobs.await_create(other).await;
    assert_eq!(
        create.runner_type, INDEX,
        "text/csv has no exact rule; the catch-all serves it"
    );
    assert_eq!(
        world.file(&owner, other).await["rulesetId"],
        catch_all.to_string()
    );

    world.cleanup().await;
}

#[tokio::test]
async fn an_unknown_or_deprecated_runner_type_fails_the_file_before_any_job_is_created() {
    let world = World::start("pod-chain-unavailable").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    jobs.declare_runner_type("legacy", RunnerTypeLifecycle::Deprecated)
        .await;
    jobs.publish_catalogue_noise("garbled", serde_json::json!({ "not": "an entry" }))
        .await;
    world
        .await_known_runner_type("legacy", Some("deprecated"))
        .await;
    let saved = create_ruleset(
        &world,
        &manager,
        RuleSpec {
            name: "ghost first",
            trigger: "UPLOAD",
            media_types: &["text/plain"],
            steps: &[
                ("ghost", serde_json::json!({})),
                ("legacy", serde_json::json!({})),
            ],
            is_default: true,
        },
    )
    .await;
    assert_eq!(
        ok(&saved)["workspaceCreateRuleset"]["unknownRunnerTypes"],
        serde_json::json!(["ghost", "legacy"]),
        "a deprecated type is warned like an unknown one"
    );
    let drive = world.create_workspace(&owner, "library").await;

    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "doomed.txt", BYTES),
    )
    .await;
    let file = world.await_state(&owner, file_id, "FAILED").await;
    assert_eq!(file["processingError"], "runner_type_unavailable");
    assert!(file["progress"].is_null());
    assert_eq!(file["affordances"]["process"]["allowed"], true);
    jobs.expect_no_command(Duration::from_secs(1)).await;

    let reprocess = reprocess_rule(&world, &manager).await;
    let retried = world
        .gql(
            &owner,
            "mutation($f:UUID!){workspaceProcess(fileId:$f){success}}",
            serde_json::json!({ "f": file_id }),
        )
        .await;
    ok(&retried);
    let _ = reprocess;
    let file = world.await_state(&owner, file_id, "FAILED").await;
    assert_eq!(
        file["processingError"], "runner_type_unavailable",
        "the reprocess rule names a type the catalogue does not carry yet"
    );
    jobs.declare_runner_type(RENDER, RunnerTypeLifecycle::Active)
        .await;
    world.await_known_runner_type(RENDER, Some("active")).await;
    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!){workspaceProcess(fileId:$f){success}}",
            serde_json::json!({ "f": file_id }),
        )
        .await);
    let create = jobs.await_create(file_id).await;
    assert_eq!(create.runner_type, RENDER);
    assert_eq!(
        world.file(&owner, file_id).await["processingState"],
        "PROCESSING"
    );
    jobs.retire_runner_type(RENDER).await;
    world.await_known_runner_type(RENDER, None).await;

    world.cleanup().await;
}

#[tokio::test]
async fn a_failed_or_rejected_run_carries_its_reason_and_a_reprocess_starts_over() {
    let world = World::start("pod-chain-failure").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    catalogue(&world, &jobs).await;
    two_step_rule(&world, &manager).await;
    let drive = world.create_workspace(&owner, "library").await;
    let mut files = drive_subscription(&world, &owner, drive).await;

    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "flaky.txt", BYTES),
    )
    .await;
    let job_a = jobs.await_create(file_id).await.job_id;
    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id: job_a,
            pages: vec![(1, "half done")],
            origin: None,
            indexer: None,
            done: false,
        },
    )
    .await);
    let no_rule = world
        .gql(
            &owner,
            "mutation($f:UUID!){workspaceProcess(fileId:$f){success}}",
            serde_json::json!({ "f": file_id }),
        )
        .await;
    assert_eq!(error_code(&no_rule), "FILE_PROCESSING");
    jobs.fail(job_a, "runner_error", Some("ocr_timeout")).await;
    let failed = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "ProcessingFailed"
    })
    .await;
    assert_eq!(failed["cause"]["reason"], "ocr_timeout");
    assert_eq!(failed["view"]["processingState"], "FAILED");
    assert_eq!(failed["view"]["processingError"], "ocr_timeout");
    assert!(failed["view"]["progress"].is_null());
    assert_eq!(
        error_code(&context(&world, &runner, file_id, job_a).await),
        "JOB_NOT_ACTIVE"
    );
    assert_eq!(world.file_pages(&owner, file_id).await.len(), 1);

    let no_rule = world
        .gql(
            &owner,
            "mutation($f:UUID!){workspaceProcess(fileId:$f){success}}",
            serde_json::json!({ "f": file_id }),
        )
        .await;
    assert_eq!(error_code(&no_rule), "NO_RULESET_MATCHES");
    reprocess_rule(&world, &manager).await;
    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!){workspaceProcess(fileId:$f){success}}",
            serde_json::json!({ "f": file_id }),
        )
        .await);
    let restarted = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "ProcessingStarted"
    })
    .await;
    assert_eq!(restarted["view"]["processingState"], "PROCESSING");
    assert!(restarted["view"]["processingError"].is_null());
    assert_eq!(restarted["view"]["progress"]["stepCount"], 1);
    assert!(
        world.file_pages(&owner, file_id).await.is_empty(),
        "a reprocess wipes the rendition at chain start"
    );
    let job_b = jobs.await_create(file_id).await.job_id;
    assert_ne!(job_b, job_a);
    jobs.reject_creation(job_b, "duplicate_active_entity").await;
    let rejected = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "ProcessingFailed"
    })
    .await;
    assert_eq!(
        rejected["view"]["processingError"],
        "duplicate_active_entity"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn a_foreign_cancel_fails_the_file_while_the_cancel_we_asked_for_is_absorbed() {
    let world = World::start("pod-chain-cancel").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    catalogue(&world, &jobs).await;
    two_step_rule(&world, &manager).await;
    let drive = world.create_workspace(&owner, "library").await;

    let foreign = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "foreign.txt", BYTES),
    )
    .await;
    let job_foreign = jobs.await_create(foreign).await.job_id;
    jobs.cancel(job_foreign).await;
    let file = world.await_state(&owner, foreign, "FAILED").await;
    assert_eq!(file["processingError"], "cancelled");

    let ours = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "ours.txt", BYTES),
    )
    .await;
    let job_ours = jobs.await_create(ours).await.job_id;
    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!){workspaceDeleteFile(fileId:$f){success}}",
            serde_json::json!({ "f": ours }),
        )
        .await);
    assert_eq!(jobs.await_cancel(job_ours).await.job_id, job_ours);
    jobs.cancel(job_ours).await;
    jobs.complete(job_ours).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(world.file(&owner, ours).await.is_null());
    assert_eq!(
        world.file(&owner, foreign).await["processingError"],
        "cancelled",
        "the other file is untouched by a cancel that was ours"
    );
    jobs.expect_no_command(Duration::from_secs(1)).await;

    world.cleanup().await;
}

#[tokio::test]
async fn a_rule_edited_or_deleted_mid_chain_never_reaches_a_running_or_a_processed_file() {
    let world = World::start("pod-chain-snapshot").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    catalogue(&world, &jobs).await;
    let drive = world.create_workspace(&owner, "library").await;

    let early = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "early.txt", BYTES),
    )
    .await;
    assert_eq!(world.file(&owner, early).await["processingState"], "READY");
    let rule = two_step_rule(&world, &manager).await;
    jobs.expect_no_command(Duration::from_secs(1)).await;
    assert!(
        world.file(&owner, early).await["rulesetId"].is_null(),
        "a rule never applies retroactively"
    );

    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "running.txt", BYTES),
    )
    .await;
    let job_a = jobs.await_create(file_id).await.job_id;
    ok(&world
        .gql(
            &manager,
            "mutation($id:UUID!,$s:[RulesetStepInput!]){workspaceUpdateRuleset(id:$id,steps:$s){id}}",
            serde_json::json!({ "id": rule, "s": [{ "runnerType": RENDER }] }),
        )
        .await);
    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id: job_a,
            pages: vec![(1, "one")],
            origin: None,
            indexer: None,
            done: true,
        },
    )
    .await);
    jobs.await_finish(job_a).await;
    jobs.complete(job_a).await;
    let create_b = jobs.await_create(file_id).await;
    assert_eq!(
        create_b.runner_type, INDEX,
        "the running file follows its snapshot, not the edited rule"
    );
    ok(&world
        .gql(
            &manager,
            "mutation($id:UUID!){workspaceDeleteRuleset(id:$id){success}}",
            serde_json::json!({ "id": rule }),
        )
        .await);
    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id: create_b.job_id,
            pages: vec![],
            origin: None,
            indexer: Some(("done", 1, 3)),
            done: true,
        },
    )
    .await);
    jobs.await_finish(create_b.job_id).await;
    jobs.complete(create_b.job_id).await;
    let file = world.await_state(&owner, file_id, "READY").await;
    assert_eq!(
        file["rulesetId"],
        rule.to_string(),
        "a processed file keeps the id of a rule that no longer exists"
    );
    assert_eq!(file["steps"].as_array().unwrap().len(), 2);

    let late = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "late.txt", BYTES),
    )
    .await;
    assert_eq!(world.file(&owner, late).await["processingState"], "READY");
    jobs.expect_no_command(Duration::from_secs(1)).await;

    world.cleanup().await;
}

#[tokio::test]
async fn every_jobs_fact_replayed_changes_nothing() {
    let world = World::start("pod-chain-replay").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    catalogue(&world, &jobs).await;
    two_step_rule(&world, &manager).await;
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "replayed.txt", BYTES),
    )
    .await;
    world.await_source_promoted(file_id).await;
    let job_a = jobs.await_create(file_id).await.job_id;
    let run_a = Uuid::now_v7();
    let mut files = drive_subscription(&world, &owner, drive).await;

    for _ in 0..2 {
        jobs.queue(job_a, RENDER).await;
        jobs.start(job_a, run_a).await;
        jobs.declare_plan(job_a, run_a, &["render"]).await;
        jobs.start_step(job_a, run_a, 0, "render").await;
    }
    let mut progress_deltas = 0;
    let mut last = serde_json::Value::Null;
    while let Some(delta) = files.try_next_payload(Duration::from_secs(2)).await {
        let node = &delta["workspaceDriveChanged"];
        if node["cause"]["kind"] == "ProgressChanged" {
            progress_deltas += 1;
            last = node["view"]["progress"].clone();
        }
    }
    assert_eq!(
        progress_deltas, 2,
        "one delta per fact that changed the row (the plan, the step), none for the replays"
    );
    assert_eq!(last["plan"], serde_json::json!(["render"]));
    assert_eq!(last["currentIndex"], 0);
    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id: job_a,
            pages: vec![(1, "one")],
            origin: None,
            indexer: None,
            done: true,
        },
    )
    .await);
    jobs.await_finish(job_a).await;
    jobs.complete(job_a).await;
    jobs.complete(job_a).await;
    let job_b = jobs.await_create(file_id).await.job_id;
    next_drive_delta(&mut files, |node| {
        node["cause"]["kind"] == "ProcessingStarted" && node["cause"]["step"] == 1
    })
    .await;
    jobs.expect_no_command(Duration::from_secs(1)).await;
    files.expect_silence(Duration::from_secs(1)).await;

    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id: job_b,
            pages: vec![],
            origin: None,
            indexer: Some(("s", 1, 1)),
            done: true,
        },
    )
    .await);
    jobs.await_finish(job_b).await;
    jobs.complete(job_b).await;
    let ready = world.await_state(&owner, file_id, "READY").await;
    let settled = ready["updatedAt"].clone();
    for job in [job_a, job_b] {
        jobs.queue(job, RENDER).await;
        jobs.start(job, run_a).await;
        jobs.declare_plan(job, run_a, &["again"]).await;
        jobs.start_step(job, run_a, 0, "again").await;
        jobs.complete(job).await;
        jobs.fail(job, "late", Some("late_failure")).await;
        jobs.cancel(job).await;
        jobs.reject_creation(job, "id_reuse").await;
    }
    jobs.cancel(Uuid::now_v7()).await;
    tokio::time::sleep(Duration::from_millis(700)).await;
    let after = world.file(&owner, file_id).await;
    assert_eq!(after["processingState"], "READY");
    assert!(after["processingError"].is_null());
    assert_eq!(
        after["updatedAt"], settled,
        "no replayed fact touched the row"
    );
    jobs.expect_no_command(Duration::from_secs(1)).await;

    world.cleanup().await;
}

#[tokio::test]
async fn a_page_edit_during_processing_and_a_report_on_a_pending_file_are_refused() {
    let world = World::start("pod-chain-guards").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    catalogue(&world, &jobs).await;
    two_step_rule(&world, &manager).await;
    let drive = world.create_workspace(&owner, "library").await;

    let pending = Uuid::now_v7();
    ticket(
        &request(
            &world,
            &owner,
            pending,
            &UploadRequest::text(drive, "", "pending.txt", BYTES),
        )
        .await,
    );
    let on_pending = report(
        &world,
        &runner,
        pending,
        Report {
            job_id: Uuid::now_v7(),
            pages: vec![(1, "too early")],
            origin: None,
            indexer: None,
            done: false,
        },
    )
    .await;
    assert_eq!(
        error_code(&on_pending),
        "JOB_NOT_ACTIVE",
        "a file that never landed has no job"
    );

    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "busy.txt", BYTES),
    )
    .await;
    let job_a = jobs.await_create(file_id).await.job_id;
    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id: job_a,
            pages: vec![(1, "runner's page")],
            origin: None,
            indexer: None,
            done: false,
        },
    )
    .await);
    let edit = world
        .gql(
            &owner,
            "mutation($f:UUID!,$n:Int!,$m:String!){workspaceEditPage(fileId:$f,number:$n,markdown:$m){success}}",
            serde_json::json!({ "f": file_id, "n": 1, "m": "hands off" }),
        )
        .await;
    assert_eq!(error_code(&edit), "FILE_PROCESSING");
    let regenerate = world
        .gql(
            &owner,
            "mutation($f:UUID!,$n:Int!){workspaceRegeneratePage(fileId:$f,number:$n){success}}",
            serde_json::json!({ "f": file_id, "n": 1 }),
        )
        .await;
    assert_eq!(error_code(&regenerate), "FILE_PROCESSING");
    let page = &world.file_pages(&owner, file_id).await[0];
    assert_eq!(page["affordances"]["editPage"]["reason"], "FILE_PROCESSING");
    assert_eq!(
        page["affordances"]["regeneratePage"]["reason"],
        "FILE_PROCESSING"
    );

    world.cleanup().await;
}
