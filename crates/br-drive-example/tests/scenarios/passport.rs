//! The Passport a chain acts under: the one its initiator presented, carried
//! by every step's `job.create` (v3) — Jobs refuses a root job without one —
//! held with the file's processing while it runs, and gone at every end of
//! it. It is never recorded anywhere else.

use std::time::Duration;

use br_core_auth::{AuthMethod, Passport, PassportClaims, PassportHeader};
use uuid::Uuid;

use crate::harness::runner::{
    INDEX, RENDER, RUNNER_SCOPE, Report, RuleSpec, create_ruleset, finish_job, install_render_rule,
    report,
};
use crate::harness::upload::{UploadRequest, upload_processed};
use crate::harness::{
    JobsStandIn, World, impersonated_passport, manager_passport, ok, service_passport,
    service_passport_as,
};

const BYTES: &[u8] = b"a document processed under its owner's passport";
const CANCEL: &str = "mutation($f:UUID!){workspaceCancelProcessing(fileId:$f){success}}";
const FREEZE: &str = "mutation($id:UUID!){workspaceFreeze(id:$id){success}}";
const REPORT_FAILURE: &str = "mutation($f:UUID!,$j:UUID!,$c:String!){\
     workspaceRunnerReportFailure(fileId:$f,jobId:$j,reasonCode:$c){success}}";

/// A file of `drive` whose first job Jobs accepted, the passport that job
/// carries checked against the owner's: answers the file and its job.
async fn running(
    world: &World,
    jobs: &JobsStandIn,
    owner: &str,
    drive: Uuid,
    name: &str,
) -> (Uuid, Uuid) {
    let file_id =
        upload_processed(world, owner, &UploadRequest::text(drive, "", name, BYTES)).await;
    let create = jobs.await_create(file_id).await;
    let presented = Passport::from_header(owner).unwrap();
    let carried: Passport =
        serde_json::from_value(create.passport.clone().expect("the job carries a passport"))
            .expect("the carried passport decodes as a Passport");
    assert_eq!(carried, presented);
    assert_eq!(
        create.triggered_by.as_ref().map(|user| user.id()),
        presented.user_id(),
        "the job names the passport's own user"
    );
    assert!(create.parent_job_id.is_none());
    assert_eq!(
        world.held_passport(file_id).await,
        Some(serde_json::to_value(&presented).unwrap()),
        "{name}: held while PROCESSING"
    );
    (file_id, create.job_id)
}

/// A manager's Passport carrying `marker` as a claim of its own: a value
/// found nowhere but in that Passport, so any record of it is a leak.
fn marked_manager_passport(user: Uuid, display_name: &str, marker: &str) -> String {
    let mut claims = serde_json::Map::new();
    claims.insert("scopes".into(), serde_json::json!(["workspace:manage"]));
    claims.insert("name".into(), serde_json::json!(display_name));
    claims.insert("session_marker".into(), serde_json::json!(marker));
    Passport::human(
        user,
        false,
        true,
        AuthMethod::Jwt,
        None,
        PassportClaims::from_map(claims),
    )
    .to_header()
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

/// The runner's final report of `job_id`, and Jobs completing it.
async fn step_done(world: &World, jobs: &JobsStandIn, runner: &str, file_id: Uuid, job_id: Uuid) {
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
    finish_job(world, jobs, job_id).await;
}

#[tokio::test]
async fn every_end_of_a_processing_drops_the_passport_it_held() {
    // Given: a one-step render rule, and an owner's drive
    let world = World::start("pod-passport-ends").await;
    let jobs = JobsStandIn::attach(&world).await;
    let owner_id = Uuid::now_v7();
    let marker = format!("session-marker-{}", Uuid::now_v7().simple());
    let owner = marked_manager_passport(owner_id, "Ada", &marker);
    let runner = service_passport(&[RUNNER_SCOPE]);
    install_render_rule(&world, &owner).await;
    let drive = world.create_workspace(&owner, "library").await;

    // When: a run ends READY on the runner's final report
    let (ready, job) = running(&world, &jobs, &owner, drive, "ready.txt").await;
    ok(&report(
        &world,
        &runner,
        ready,
        Report {
            job_id: job,
            pages: vec![(1, "done")],
            origin: None,
            indexer: None,
            done: true,
        },
    )
    .await);
    world.await_state(&owner, ready, "READY").await;
    // Then: the passport is gone
    assert_eq!(world.held_passport(ready).await, None, "READY");

    // When: the runner declares its job failed
    let (declared, job) = running(&world, &jobs, &owner, drive, "declared.txt").await;
    ok(&world
        .gql(
            &runner,
            REPORT_FAILURE,
            serde_json::json!({ "f": declared, "j": job, "c": "unreadable_scan" }),
        )
        .await);
    world.await_state(&owner, declared, "FAILED").await;
    assert_eq!(world.held_passport(declared).await, None, "runner failure");

    // When: Jobs fails a job
    let (failed, job) = running(&world, &jobs, &owner, drive, "failed.txt").await;
    jobs.fail(job, "attempts_exhausted", Some("broken")).await;
    world.await_state(&owner, failed, "FAILED").await;
    assert_eq!(world.held_passport(failed).await, None, "Jobs failed");

    // When: Jobs refuses to create a job
    let (rejected, job) = running(&world, &jobs, &owner, drive, "rejected.txt").await;
    jobs.reject_creation(job, "runner_type_retired").await;
    world.await_state(&owner, rejected, "FAILED").await;
    assert_eq!(
        world.held_passport(rejected).await,
        None,
        "creation rejected"
    );

    // When: the owner cancels a job, and Jobs confirms
    let (cancelled, _) = running(&world, &jobs, &owner, drive, "cancelled.txt").await;
    ok(&world
        .gql(&owner, CANCEL, serde_json::json!({ "f": cancelled }))
        .await);
    world.await_state(&owner, cancelled, "FAILED").await;
    assert_eq!(world.held_passport(cancelled).await, None, "cancelled");

    // When: the owner cancels, and the runner's final report lands before
    // Jobs acts on the cancel
    let (crossed, job) = running(&world, &jobs, &owner, drive, "crossed.txt").await;
    jobs.drop_cancels(true);
    ok(&world
        .gql(&owner, CANCEL, serde_json::json!({ "f": crossed }))
        .await);
    jobs.await_cancel(job).await;
    ok(&report(
        &world,
        &runner,
        crossed,
        Report {
            job_id: job,
            pages: vec![(1, "done before the cancel")],
            origin: None,
            indexer: None,
            done: true,
        },
    )
    .await);
    world.await_state(&owner, crossed, "READY").await;
    jobs.await_finish(job).await;
    jobs.drop_cancels(false);
    assert_eq!(
        world.held_passport(crossed).await,
        None,
        "a cancel crossing a final report"
    );

    // When: the owner freezes a drive whose file processes
    let closing = world.create_workspace(&owner, "closing").await;
    let (frozen, _) = running(&world, &jobs, &owner, closing, "frozen.txt").await;
    ok(&world
        .gql(&owner, FREEZE, serde_json::json!({ "id": closing }))
        .await);
    world.await_state(&owner, frozen, "FAILED").await;
    assert_eq!(world.held_passport(frozen).await, None, "drive frozen");

    // And: no fact ever recorded the passport — not even a claim of it
    assert!(
        !world.facts_mention(&marker).await,
        "the passport is never a fact"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn a_chain_started_under_a_borrowed_session_carries_it_whole_and_stops_when_its_impersonator_is_erased()
 {
    // Given: a two-step rule, an owner's drive, and an admin who may borrow
    // the owner's session
    let world = World::start("pod-passport-impersonated").await;
    let jobs = JobsStandIn::attach(&world).await;
    let owner_id = Uuid::now_v7();
    let owner = manager_passport(owner_id, "Ada");
    let admin_id = Uuid::now_v7();
    let borrowed = impersonated_passport(owner_id, admin_id, &["workspace:manage"]);
    let runner = service_passport(&[RUNNER_SCOPE]);
    install_two_step_rule(&world, &owner).await;
    let drive = world.create_workspace(&owner, "library").await;

    // When: the admin, in the owner's borrowed session, uploads a file
    let (file_id, first) = running(&world, &jobs, &borrowed, drive, "borrowed.txt").await;

    // Then: Jobs accepted a job carrying the Passport presented, impersonator
    // included, naming the owner — the person whose session it is
    let carried: Passport = serde_json::from_value(world.held_passport(file_id).await.unwrap())
        .expect("the held passport decodes");
    assert_eq!(carried.user_id(), Some(owner_id));
    assert_eq!(carried.impersonator_id(), Some(admin_id));
    assert!(jobs.is_live(first));

    // And: the owner, in their own session, starts another chain
    let (own, own_first) = running(&world, &jobs, &owner, drive, "own.txt").await;

    // When: the admin is erased while both chains run
    world.erase(admin_id).await;

    // Then: the borrowed session's Passport is off its chain; the owner's own
    // is untouched
    assert_eq!(
        world.held_passport(file_id).await,
        None,
        "the erase takes the impersonator's borrowed passport off the chain"
    );
    assert_eq!(
        world.held_passport(own).await,
        Some(serde_json::to_value(Passport::from_header(&owner).unwrap()).unwrap()),
        "a passport the erased admin never held stays"
    );

    // When: the borrowed chain's first step ends
    step_done(&world, &jobs, &runner, file_id, first).await;

    // Then: its next step is never asked of Jobs, and the file is FAILED
    // `passport_missing`, visibly
    let file = world.await_state(&owner, file_id, "FAILED").await;
    assert_eq!(file["processingError"], br_drive::PASSPORT_MISSING);
    jobs.expect_no_command(Duration::from_secs(1)).await;
    let abandoned = world
        .processing_facts(file_id)
        .await
        .into_iter()
        .rfind(|fact| fact.event_type == "StepAbandoned")
        .expect("the chain stopped as StepAbandoned");
    assert_eq!(abandoned.payload["step_index"], 1);
    assert_eq!(abandoned.payload["reason_code"], br_drive::PASSPORT_MISSING);

    // And: the owner's own chain goes on under the owner's Passport
    step_done(&world, &jobs, &runner, own, own_first).await;
    let next = jobs.await_create(own).await;
    assert_eq!(next.runner_type, INDEX);
    let carried: Passport = serde_json::from_value(next.passport.unwrap()).unwrap();
    assert_eq!(carried, Passport::from_header(&owner).unwrap());

    world.cleanup().await;
}

#[tokio::test]
async fn a_service_account_starts_a_chain_under_its_own_passport_naming_nobody() {
    // Given: a render rule, and a service account's own drive
    let world = World::start("pod-passport-service").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let service = service_passport_as(Uuid::now_v7(), &[]);
    install_render_rule(&world, &manager).await;
    let drive = world.create_workspace(&service, "automation").await;

    // When: the service account uploads a file
    let (file_id, job) = running(&world, &jobs, &service, drive, "batch.txt").await;

    // Then: Jobs accepted a job carrying the service Passport, naming nobody
    let create_passport: Passport =
        serde_json::from_value(world.held_passport(file_id).await.unwrap()).unwrap();
    assert!(create_passport.user_id().is_none(), "a service passport");
    assert!(jobs.is_live(job));

    world.cleanup().await;
}
