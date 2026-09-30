//! `br_drive::freeze_drive`, in the host's own gesture (the example host's
//! `workspaceFreeze`): every running processing of the drive ends, as a
//! cancel — `job.cancel` sent, the job ended at once — and every pending
//! upload is abandoned as its deadline would, each with its facts, so no work
//! lands on the drive afterwards.

use std::time::Duration;

use uuid::Uuid;

use crate::harness::runner::{RUNNER_SCOPE, Report, install_render_rule, report};
use crate::harness::upload::{
    UploadRequest, commit, post_bytes, request, ticket, upload, upload_processed,
};
use crate::harness::{
    JobsStandIn, World, drive_subscription, error_code, manager_passport, next_drive_delta, ok,
    passport, service_passport,
};

const BYTES: &[u8] = b"a document on a drive being closed";
const FREEZE: &str = "mutation($id:UUID!){workspaceFreeze(id:$id){success}}";

async fn freeze(world: &World, passport: &str, workspace: Uuid) -> serde_json::Value {
    world
        .gql(passport, FREEZE, serde_json::json!({ "id": workspace }))
        .await
}

#[tokio::test]
async fn a_freeze_cancels_running_jobs_and_reaps_pending_uploads_with_their_facts() {
    // Given: a drive holding a running chain, a landed upload never committed
    // and a stored file — and another drive whose chain runs
    let world = World::start("pod-freeze").await;
    let jobs = JobsStandIn::attach(&world).await;
    let owner_id = Uuid::now_v7();
    let owner = manager_passport(owner_id, "Ada");
    let runner = service_passport(&[RUNNER_SCOPE]);
    install_render_rule(&world, &owner).await;
    let drive = world.create_workspace(&owner, "closing").await;
    let other = world.create_workspace(&owner, "open").await;
    let running = upload_processed(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "running.txt", BYTES),
    )
    .await;
    let running_job = jobs.await_create(running).await.job_id;
    let pending = Uuid::now_v7();
    let pending_ticket = ticket(
        &request(
            &world,
            &owner,
            pending,
            &UploadRequest::text(drive, "", "pending.txt", BYTES),
        )
        .await,
    );
    assert!((200..300).contains(&post_bytes(&world, &pending_ticket, BYTES, "pending.txt").await));
    let pending_source = world.source_of(pending).await;
    let stored = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "stored.txt", BYTES),
    )
    .await;
    let elsewhere = upload_processed(
        &world,
        &owner,
        &UploadRequest::text(other, "", "elsewhere.txt", BYTES),
    )
    .await;
    let elsewhere_job = jobs.await_create(elsewhere).await.job_id;
    let mut files = drive_subscription(&world, &owner, drive).await;

    // When: the owner freezes the drive
    ok(&freeze(&world, &owner, drive).await);

    // Then: Jobs is asked to cancel the running job, and the file is FAILED
    // `cancelled` at once — its live window says so
    assert_eq!(jobs.await_cancel(running_job).await.job_id, running_job);
    next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert"
            && node["view"]["id"] == running.to_string()
            && node["view"]["processingState"] == "FAILED"
    })
    .await;
    let file = world.file(&owner, running).await;
    assert_eq!(file["processingState"], "FAILED");
    assert_eq!(file["processingError"], "cancelled");
    assert!(file["progress"].is_null());
    // And: the cancel and the end are facts of the freeze's one gesture
    let processing = world.processing_facts(running).await;
    let ended: Vec<_> = processing
        .iter()
        .filter(|fact| {
            matches!(
                fact.event_type.as_str(),
                "JobCancelSent" | "JobCancelledOnFreeze"
            )
        })
        .collect();
    assert_eq!(
        ended
            .iter()
            .map(|fact| fact.event_type.as_str())
            .collect::<Vec<_>>(),
        vec!["JobCancelSent", "JobCancelledOnFreeze"]
    );
    for fact in &ended {
        assert_eq!(fact.job_id(), Some(running_job));
        assert_eq!(fact.actor_id, owner_id);
    }
    assert_eq!(ended[0].correlation_id, ended[1].correlation_id);
    // And: the pending upload is gone, its object released, with its last fact
    // in the same gesture
    assert!(world.file(&owner, pending).await.is_null());
    assert_eq!(
        world.blob_state(pending_source).await.as_deref(),
        Some("orphaned")
    );
    let pending_facts = world.facts("drive_file", serde_json::json!(pending)).await;
    let abandoned = pending_facts.last().expect("the pending file's facts");
    assert_eq!(abandoned.event_type, "UploadAbandoned");
    assert_eq!(abandoned.correlation_id, ended[0].correlation_id);
    assert_eq!(abandoned.actor_id, owner_id);
    // And: the stored file and the other drive are untouched
    assert_eq!(world.file(&owner, stored).await["processingState"], "READY");
    assert_eq!(
        world.file(&owner, elsewhere).await["processingState"],
        "PROCESSING"
    );
    assert_eq!(world.job_of(elsewhere).await, Some(elsewhere_job));

    // And: no work lands afterwards — Jobs' own `cancelled` is recorded as
    // ignored, the runner's report and a late commit are refused
    let ignored = crate::poll_until!(Duration::from_secs(15), {
        world
            .processing_facts(running)
            .await
            .into_iter()
            .find(|fact| fact.event_type == "JobFactIgnored")
    });
    assert_eq!(ignored.payload["why"], "job_already_ended");
    assert_eq!(ignored.payload["received"]["kind"], "JobCancelled");
    assert_eq!(
        error_code(
            &report(
                &world,
                &runner,
                running,
                Report {
                    job_id: running_job,
                    pages: vec![(1, "too late")],
                    origin: None,
                    indexer: None,
                    done: true,
                },
            )
            .await
        ),
        "JOB_NOT_ACTIVE"
    );
    assert!(world.file_pages(&owner, running).await.is_empty());
    assert_eq!(
        error_code(&commit(&world, &owner, pending).await),
        "FILE_NOT_FOUND"
    );
    assert_eq!(
        world.file(&owner, running).await["processingState"],
        "FAILED"
    );

    // And: a second freeze ends nothing more, and only the owner may freeze
    ok(&freeze(&world, &owner, drive).await);
    jobs.expect_no_command(Duration::from_secs(1)).await;
    assert_eq!(
        error_code(&freeze(&world, &passport(Uuid::now_v7()), drive).await),
        "NOT_THE_WORKSPACE_OWNER"
    );
    let cancels = world
        .processing_facts(running)
        .await
        .into_iter()
        .filter(|fact| {
            matches!(
                fact.event_type.as_str(),
                "JobCancelSent" | "JobCancelledOnFreeze"
            )
        })
        .count();
    assert_eq!(cancels, 2, "the second freeze found nothing in flight");

    world.cleanup().await;
}
