//! The facts: every change of a file, its processing, a page, a label or a
//! rule reaches the host's fact table through `DriveHost::record_facts`, in the
//! gesture's transaction — numbered 1, 2, 3… per object, naming who acted (a
//! person and the admin behind an impersonated session, a runner, a service
//! account) and the gesture they belong to. The last change the views show
//! (`updatedAt`, a page's `updatedBy`) is state, written with them.

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::harness::runner::{
    RUNNER_SCOPE, Report, RuleSpec, create_ruleset, install_render_rule, report, ruleset_id,
};
use crate::harness::upload::{UploadRequest, process, upload_processed};
use crate::harness::{
    Fact, JobsStandIn, World, error_code, impersonated_passport, manager_passport, ok,
    service_passport_as,
};

const BYTES: &[u8] = b"a document whose every change is on record";

fn instant(value: &serde_json::Value) -> DateTime<Utc> {
    serde_json::from_value(value.clone()).expect("an instant")
}

/// The labels and the rules with their creation and last change.
async fn catalogue(world: &World, passport: &str) -> serde_json::Value {
    let read = world
        .gql(
            passport,
            "query{workspaceLabels{id createdAt updatedAt} workspaceRulesets{id createdAt updatedAt}}",
            serde_json::json!({}),
        )
        .await;
    ok(&read).clone()
}

fn kinds(facts: &[Fact]) -> Vec<&str> {
    facts.iter().map(|fact| fact.event_type.as_str()).collect()
}

/// Jobs' information about a job (queued, started, completed) rides its own
/// durables: its place among the other facts is the broker's.
fn moving(facts: &[Fact]) -> Vec<&Fact> {
    facts
        .iter()
        .filter(|fact| {
            !matches!(
                fact.event_type.as_str(),
                "JobQueued" | "JobStarted" | "JobCompleted"
            )
        })
        .collect()
}

/// Every object's facts are numbered from 1, without a gap.
fn assert_gap_free(facts: &[Fact]) {
    let seqs: Vec<i64> = facts.iter().map(|fact| fact.seq).collect();
    let expected: Vec<i64> = (1..=facts.len() as i64).collect();
    assert_eq!(seqs, expected, "{facts:?}");
}

fn last(facts: &[Fact]) -> &Fact {
    facts.last().expect("at least one fact")
}

fn page_key(file_id: Uuid, number: i32) -> serde_json::Value {
    serde_json::json!({ "file_id": file_id, "number": number })
}

#[tokio::test]
async fn every_change_is_a_fact_in_the_host_table_naming_its_hand_and_its_gesture() {
    // Given: an owner, an admin who may borrow the owner's session, a runner,
    // a manager, and a render rule
    let world = World::start("pod-audit").await;
    let jobs = JobsStandIn::attach(&world).await;
    let owner_id = Uuid::now_v7();
    let owner = manager_passport(owner_id, "Owner");
    let admin_id = Uuid::now_v7();
    let borrowed = impersonated_passport(owner_id, admin_id, &["workspace:manage"]);
    let runner_id = Uuid::now_v7();
    let runner = service_passport_as(runner_id, &[RUNNER_SCOPE]);
    install_render_rule(&world, &owner).await;
    let drive = world.create_workspace(&owner, "library").await;

    // When: the owner uploads and processes a file, and the runner reports it
    let file_id = upload_processed(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "audited.txt", BYTES),
    )
    .await;
    let job = jobs.await_create(file_id).await.job_id;
    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id: job,
            pages: vec![(1, "page one"), (2, "page two")],
            origin: None,
            indexer: Some(("Two pages.", 2, 9)),
            done: true,
        },
    )
    .await);
    world.await_state(&owner, file_id, "READY").await;

    // Then: the file's facts are the owner's gestures and the runner's report,
    // in order, gap-free; the processing's too
    let file_facts = world.facts("drive_file", serde_json::json!(file_id)).await;
    assert_gap_free(&file_facts);
    let hands: Vec<(&str, Uuid, &str, bool)> = file_facts
        .iter()
        .map(|fact| {
            (
                fact.event_type.as_str(),
                fact.actor_id,
                fact.actor_kind.as_str(),
                fact.is_runner,
            )
        })
        .collect();
    assert_eq!(
        hands,
        vec![
            ("UploadTicketIssued", owner_id, "human", false),
            ("UploadCommitted", owner_id, "human", false),
            ("ReportStored", runner_id, "service", true),
        ]
    );
    assert!(
        file_facts
            .iter()
            .all(|fact| fact.impersonator_id.is_none() && fact.causation_id.is_none())
    );
    assert_eq!(
        file_facts[0].payload["name"], "audited.txt",
        "a fact carries its data"
    );
    let all = world.processing_facts(file_id).await;
    assert_gap_free(&all);
    let processing = moving(&all);
    assert_eq!(
        processing
            .iter()
            .map(|fact| fact.event_type.as_str())
            .collect::<Vec<_>>(),
        vec!["ChainStarted", "JobCreated", "JobReportedDone"]
    );
    assert_eq!(processing[1].payload["job_id"], job.to_string());
    // And: one gesture, one correlation — the process gesture's two facts,
    // the report's file, page and processing facts
    assert_eq!(processing[0].correlation_id, processing[1].correlation_id);
    assert_eq!(processing[0].actor_id, owner_id);
    let reported = processing[2].correlation_id;
    assert_ne!(reported, processing[0].correlation_id);
    assert_eq!(last(&file_facts).correlation_id, reported);
    let page_one = world.facts("drive_page", page_key(file_id, 1)).await;
    assert_eq!(kinds(&page_one), vec!["Reported"]);
    assert_eq!(page_one[0].correlation_id, reported);
    assert_eq!(page_one[0].actor_id, runner_id);
    assert_eq!(page_one[0].payload["job_id"], job.to_string());
    // And: the views show the last change, as state
    let shown = world.file(&owner, file_id).await;
    assert_eq!(instant(&shown["updatedAt"]), last(&file_facts).occurred_at);
    let pages = world.file_pages(&owner, file_id).await;
    assert!(
        pages
            .iter()
            .all(|page| page["updatedBy"] == runner_id.to_string()),
        "the runner wrote every page: {pages:?}"
    );

    // When: the admin, in the owner's borrowed session, retitles the file and
    // corrects page one
    ok(&world
        .gql(
            &borrowed,
            "mutation($f:UUID!,$t:String!){workspaceRetitleFile(fileId:$f,title:$t){success}}",
            serde_json::json!({ "f": file_id, "t": "Audited" }),
        )
        .await);
    ok(&world
        .gql(
            &borrowed,
            "mutation($f:UUID!,$n:Int!,$m:String!){workspaceEditPage(fileId:$f,number:$n,markdown:$m){success}}",
            serde_json::json!({ "f": file_id, "n": 1, "m": "page one, corrected" }),
        )
        .await);

    // Then: both changes are the owner's — the effective identity — and name
    // the admin behind the session; the views show them as the last changes
    let file_facts = world.facts("drive_file", serde_json::json!(file_id)).await;
    assert_gap_free(&file_facts);
    let retitled = last(&file_facts);
    assert_eq!(
        (
            retitled.event_type.as_str(),
            retitled.actor_id,
            retitled.impersonator_id
        ),
        ("Retitled", owner_id, Some(admin_id))
    );
    assert_eq!(
        retitled.payload,
        serde_json::json!({ "kind": "Retitled", "from": "audited", "to": "Audited" })
    );
    let shown = world.file(&owner, file_id).await;
    assert_eq!(instant(&shown["updatedAt"]), retitled.occurred_at);
    let page_one = world.facts("drive_page", page_key(file_id, 1)).await;
    assert_gap_free(&page_one);
    let edited = last(&page_one);
    assert_eq!(
        (
            edited.event_type.as_str(),
            edited.actor_id,
            edited.impersonator_id
        ),
        ("Edited", owner_id, Some(admin_id))
    );
    let pages = world.file_pages(&owner, file_id).await;
    assert_eq!(pages[0]["updatedBy"], owner_id.to_string());
    assert_eq!(pages[0]["origin"], "EDITED");
    assert_eq!(
        pages[1]["updatedBy"],
        runner_id.to_string(),
        "page two's last change is still the runner's"
    );
    let file_before_jobs = instant(&shown["updatedAt"]);

    // When: the owner reprocesses it and Jobs fails the run
    ok(&process(&world, &owner, file_id).await);
    let rerun = jobs.await_create(file_id).await.job_id;
    let failure = jobs.fail(rerun, "RUNNER_LOST", None).await;
    world.await_state(&owner, file_id, "FAILED").await;

    // Then: the failure is a fact of Jobs' service account, caused by Jobs'
    // message, and the file's last change moves with it
    let all = world.processing_facts(file_id).await;
    assert_gap_free(&all);
    let failed = *moving(&all).last().expect("the failure");
    assert_eq!(
        (
            failed.event_type.as_str(),
            failed.actor_kind.as_str(),
            failed.impersonator_id,
            failed.causation_id
        ),
        ("JobFailed", "service", None, Some(failure))
    );
    assert_ne!(failed.actor_id, owner_id);
    let shown = world.file(&owner, file_id).await;
    assert!(instant(&shown["updatedAt"]) > file_before_jobs);
    assert_eq!(instant(&shown["updatedAt"]), failed.occurred_at);

    // When: the owner, as manager, creates a label and a rule, then the admin
    // edits the label and the owner the rule
    let label = Uuid::now_v7();
    ok(&world
        .gql(
            &owner,
            "mutation($id:UUID!){workspaceCreateLabel(id:$id,name:\"Audit\",color:\"#112233\"){success}}",
            serde_json::json!({ "id": label }),
        )
        .await);
    let rule = ruleset_id(
        &create_ruleset(
            &world,
            &owner,
            RuleSpec {
                name: "audited rule",
                trigger: "REPROCESS",
                media_types: &["text/plain"],
                steps: &[("render", serde_json::json!({}))],
                is_default: false,
            },
        )
        .await,
    );
    let before = catalogue(&world, &owner).await;
    assert_eq!(
        before["workspaceLabels"][0]["updatedAt"], before["workspaceLabels"][0]["createdAt"],
        "a label never changed was last changed when created"
    );
    ok(&world
        .gql(
            &borrowed,
            "mutation($id:UUID!){workspaceUpdateLabel(id:$id,color:\"#445566\"){success}}",
            serde_json::json!({ "id": label }),
        )
        .await);
    ok(&world
        .gql(
            &owner,
            "mutation($id:UUID!){workspaceUpdateRuleset(id:$id,name:\"audited rule, renamed\"){id}}",
            serde_json::json!({ "id": rule }),
        )
        .await);

    // Then: each is a fact of its own noun, and the catalogue's last change is
    // the latest one
    let label_facts = world.facts("drive_label", serde_json::json!(label)).await;
    assert_gap_free(&label_facts);
    assert_eq!(kinds(&label_facts), vec!["Created", "Updated"]);
    assert_eq!(
        (label_facts[1].actor_id, label_facts[1].impersonator_id),
        (owner_id, Some(admin_id))
    );
    assert_eq!(label_facts[1].payload["color"], "#445566");
    let rule_facts = world.facts("drive_ruleset", serde_json::json!(rule)).await;
    assert_gap_free(&rule_facts);
    assert_eq!(kinds(&rule_facts), vec!["Created", "Saved"]);
    assert_eq!(rule_facts[1].actor_id, owner_id);
    let after = catalogue(&world, &owner).await;
    let label_view = after["workspaceLabels"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == label.to_string())
        .unwrap()
        .clone();
    assert_eq!(
        instant(&label_view["updatedAt"]),
        label_facts[1].occurred_at
    );
    let rule_view = after["workspaceRulesets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == rule.to_string())
        .unwrap()
        .clone();
    assert_eq!(instant(&rule_view["updatedAt"]), rule_facts[1].occurred_at);

    // And: every fact the gestures handed states what happened — none is
    // named as a request
    let types: Vec<String> =
        sqlx::query_scalar("SELECT DISTINCT event_type FROM workspace_fact ORDER BY event_type")
            .fetch_all(&world.db.app)
            .await
            .unwrap();
    assert!(types.len() >= 10, "{types:?}");
    for kind in &types {
        for word in ["Requested", "Wanted", "Needed"] {
            assert!(!kind.ends_with(word), "{kind} is not a fact: {types:?}");
        }
    }

    world.cleanup().await;
}

#[tokio::test]
async fn a_fact_the_host_cannot_record_rolls_the_whole_gesture_back() {
    // Given: a stored file, and a host whose fact table refuses one title
    let world = World::start("pod-audit-refused").await;
    let owner_id = Uuid::now_v7();
    let owner = manager_passport(owner_id, "Owner");
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = crate::harness::upload::upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "kept.txt", BYTES),
    )
    .await;
    let before = world.file(&owner, file_id).await;
    let facts_before = world.facts("drive_file", serde_json::json!(file_id)).await;

    // When: the owner retitles it to the title the host's table refuses
    let refused = world
        .gql(
            &owner,
            "mutation($f:UUID!,$t:String!){workspaceRetitleFile(fileId:$f,title:$t){success}}",
            serde_json::json!({
                "f": file_id,
                "t": br_drive_example::kernel::drive::UNRECORDABLE_TITLE,
            }),
        )
        .await;

    // Then: the gesture fails with the host's code, and neither the state nor
    // any fact moved
    assert_eq!(error_code(&refused), "FACT_REFUSED");
    let after = world.file(&owner, file_id).await;
    assert_eq!(after["title"], before["title"]);
    assert_eq!(after["updatedAt"], before["updatedAt"]);
    let facts_after = world.facts("drive_file", serde_json::json!(file_id)).await;
    assert_eq!(facts_after.len(), facts_before.len());
    let version: i64 = sqlx::query_scalar("SELECT version FROM drive.file WHERE id = $1")
        .bind(file_id)
        .fetch_one(&world.db.app)
        .await
        .unwrap();
    assert_eq!(
        version,
        facts_before.len() as i64,
        "the version did not move"
    );

    // And: the next gesture numbers its fact right after the last one kept
    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!,$t:String!){workspaceRetitleFile(fileId:$f,title:$t){success}}",
            serde_json::json!({ "f": file_id, "t": "Kept" }),
        )
        .await);
    let facts = world.facts("drive_file", serde_json::json!(file_id)).await;
    assert_gap_free(&facts);
    assert_eq!(last(&facts).event_type, "Retitled");

    world.cleanup().await;
}
