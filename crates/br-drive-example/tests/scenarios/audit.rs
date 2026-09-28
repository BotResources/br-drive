//! The audit trail: every change of a file, a page, a label or a rule is an
//! append-only fact naming who acted — a person (and the admin behind an
//! impersonated session), a runner, a service account — and the last change
//! the views show (`updatedAt`, a page's `updatedBy`) is read from those facts.

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::harness::runner::{
    RUNNER_SCOPE, Report, RuleSpec, create_ruleset, install_render_rule, report, ruleset_id,
};
use crate::harness::upload::{UploadRequest, process, upload_processed};
use crate::harness::{
    JobsStandIn, World, impersonated_passport, manager_passport, ok, service_passport_as,
};

const BYTES: &[u8] = b"a document whose every change is on record";
const REDACTED: Uuid = Uuid::nil();

/// A recorded fact: its type, actor, actor kind, impersonator, instant.
type FactRow = (String, Option<Uuid>, String, Option<Uuid>, DateTime<Utc>);

async fn facts(world: &World, aggregate_type: &str, id: Uuid, page: Option<i32>) -> Vec<FactRow> {
    sqlx::query_as(
        "SELECT fact_type, actor_id, actor_kind, impersonator_id, occurred_at FROM drive.fact \
         WHERE aggregate_type = $1 AND aggregate_id = $2 AND page_number IS NOT DISTINCT FROM $3 \
         ORDER BY occurred_at, id",
    )
    .bind(aggregate_type)
    .bind(id)
    .bind(page)
    .fetch_all(&world.db.app)
    .await
    .expect("read the facts")
}

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

fn last(rows: &[FactRow]) -> &FactRow {
    rows.last().expect("at least one fact")
}

#[tokio::test]
async fn every_change_is_a_fact_naming_its_hand_and_the_last_change_is_read_from_them() {
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

    // Then: the owner's gestures are the owner's facts, the runner's report
    // the runner's — and the file's last change is its latest fact
    let file_facts = facts(&world, "file", file_id, None).await;
    let hands: Vec<(&str, Option<Uuid>, &str)> = file_facts
        .iter()
        .map(|row| (row.0.as_str(), row.1, row.2.as_str()))
        .collect();
    assert_eq!(
        hands,
        vec![
            ("UploadCommitted", Some(owner_id), "human"),
            ("ProcessingStarted", Some(owner_id), "human"),
            ("ReportStored", Some(runner_id), "runner"),
            ("ProcessingFinished", Some(runner_id), "runner"),
        ]
    );
    let shown = world.file(&owner, file_id).await;
    assert_eq!(instant(&shown["updatedAt"]), last(&file_facts).4);
    let pages = world.file_pages(&owner, file_id).await;
    assert!(
        pages
            .iter()
            .all(|page| page["updatedBy"] == runner_id.to_string()),
        "the runner wrote every page: {pages:?}"
    );
    let page_one = facts(&world, "page", file_id, Some(1)).await;
    assert_eq!(page_one.len(), 1);
    assert_eq!(
        (page_one[0].0.as_str(), page_one[0].2.as_str()),
        ("Reported", "runner")
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
    let file_facts = facts(&world, "file", file_id, None).await;
    let retitled = last(&file_facts);
    assert_eq!(
        (
            retitled.0.as_str(),
            retitled.1,
            retitled.2.as_str(),
            retitled.3
        ),
        ("Retitled", Some(owner_id), "human", Some(admin_id))
    );
    let shown = world.file(&owner, file_id).await;
    assert_eq!(instant(&shown["updatedAt"]), retitled.4);
    let edited = facts(&world, "page", file_id, Some(1)).await;
    let edited = last(&edited);
    assert_eq!(
        (edited.0.as_str(), edited.1, edited.3),
        ("Edited", Some(owner_id), Some(admin_id))
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
    jobs.fail(rerun, "RUNNER_LOST", None).await;
    world.await_state(&owner, file_id, "FAILED").await;

    // Then: the failure is a fact of Jobs' service account
    let file_facts = facts(&world, "file", file_id, None).await;
    let failed = last(&file_facts);
    assert_eq!(
        (failed.0.as_str(), failed.2.as_str(), failed.3),
        ("ProcessingFailed", "service", None)
    );
    assert_ne!(failed.1, Some(owner_id));
    let shown = world.file(&owner, file_id).await;
    assert!(instant(&shown["updatedAt"]) > file_before_jobs);
    assert_eq!(instant(&shown["updatedAt"]), failed.4);

    // When: the owner, as manager, edits a label and a rule
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

    // Then: each is a fact of its own kind, and the catalogue's last change
    // is read from it
    let label_facts = facts(&world, "label", label, None).await;
    assert_eq!(label_facts.len(), 1);
    assert_eq!(
        (
            label_facts[0].0.as_str(),
            label_facts[0].1,
            label_facts[0].3
        ),
        ("Updated", Some(owner_id), Some(admin_id))
    );
    let rule_facts = facts(&world, "ruleset", rule, None).await;
    assert_eq!(rule_facts.len(), 1);
    assert_eq!(
        (rule_facts[0].0.as_str(), rule_facts[0].1),
        ("Saved", Some(owner_id))
    );
    let after = catalogue(&world, &owner).await;
    let label_view = after["workspaceLabels"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == label.to_string())
        .unwrap()
        .clone();
    assert_eq!(instant(&label_view["updatedAt"]), label_facts[0].4);
    let rule_view = after["workspaceRulesets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == rule.to_string())
        .unwrap()
        .clone();
    assert_eq!(instant(&rule_view["updatedAt"]), rule_facts[0].4);

    // When: the admin is erased
    world.erase(admin_id).await;

    // Then: every fact keeps its change and its effective hand, and no longer
    // names the admin behind the session
    let named: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM drive.fact WHERE actor_id = $1 OR impersonator_id = $1",
    )
    .bind(admin_id)
    .fetch_one(&world.db.app)
    .await
    .unwrap();
    assert_eq!(named, 0);
    let file_facts = facts(&world, "file", file_id, None).await;
    let retitled = file_facts
        .iter()
        .find(|row| row.0 == "Retitled")
        .expect("the retitle stays on record");
    assert_eq!((retitled.1, retitled.3), (Some(owner_id), Some(REDACTED)));

    world.cleanup().await;
}
