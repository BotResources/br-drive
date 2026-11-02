//! The Passport a chain acts under: the one its initiator presented, carried
//! by every step's `job.create` (v3) — Jobs refuses a root job without one —
//! held with the file's processing while it runs, and gone at every end of
//! it. It is never recorded anywhere else.

use br_core_auth::{Passport, PassportHeader};
use uuid::Uuid;

use crate::harness::runner::{RUNNER_SCOPE, Report, install_render_rule, report};
use crate::harness::upload::{UploadRequest, upload_processed};
use crate::harness::{JobsStandIn, World, manager_passport, ok, service_passport};

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

#[tokio::test]
async fn every_end_of_a_processing_drops_the_passport_it_held() {
    // Given: a one-step render rule, and an owner's drive
    let world = World::start("pod-passport-ends").await;
    let jobs = JobsStandIn::attach(&world).await;
    let owner_id = Uuid::now_v7();
    let owner = manager_passport(owner_id, "Ada");
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

    // When: the owner freezes a drive whose file processes
    let closing = world.create_workspace(&owner, "closing").await;
    let (frozen, _) = running(&world, &jobs, &owner, closing, "frozen.txt").await;
    ok(&world
        .gql(&owner, FREEZE, serde_json::json!({ "id": closing }))
        .await);
    world.await_state(&owner, frozen, "FAILED").await;
    assert_eq!(world.held_passport(frozen).await, None, "drive frozen");

    // And: no fact ever recorded a passport
    assert!(
        !world.facts_mention("auth_method").await,
        "the passport is never a fact"
    );

    world.cleanup().await;
}
