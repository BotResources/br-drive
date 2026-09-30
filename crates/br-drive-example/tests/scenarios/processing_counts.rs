//! `br_drive::processing_counts`: a host reads how many files of its drives
//! are pending, processing, ready or failed without reading the library's
//! processing table — the example host shows them on its workspace.

use std::time::Duration;

use uuid::Uuid;

use crate::harness::runner::install_render_rule;
use crate::harness::upload::{UploadRequest, request, ticket, upload, upload_processed};
use crate::harness::{JobsStandIn, Subscription, World, manager_passport, next_delta};

const BYTES: &[u8] = b"a document counted by its state";

const WORKSPACE_DELTAS: &str = "subscription{workspaceDeltas{__typename \
    ... on WorkspaceReset{views{... on WorkspaceView{id}}} \
    ... on WorkspaceUpsert{view{... on WorkspaceView{id fileCount readyFileCount \
      pendingFileCount processingFileCount failedFileCount}}}}}";

#[tokio::test]
async fn the_host_reads_its_drives_files_by_processing_state() {
    // Given: a drive holding a pending upload, a stored file, a file whose
    // chain runs and a file whose job failed — and an empty drive beside it
    let world = World::start("pod-processing-counts").await;
    let jobs = JobsStandIn::attach(&world).await;
    let owner = manager_passport(Uuid::now_v7(), "Ada");
    install_render_rule(&world, &owner).await;
    let drive = world.create_workspace(&owner, "library").await;
    let empty = world.create_workspace(&owner, "empty").await;
    ticket(
        &request(
            &world,
            &owner,
            Uuid::now_v7(),
            &UploadRequest::text(drive, "", "pending.txt", BYTES),
        )
        .await,
    );
    upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "stored.txt", BYTES),
    )
    .await;
    let running = upload_processed(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "running.txt", BYTES),
    )
    .await;
    jobs.await_create(running).await;
    let failing = upload_processed(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "failing.txt", BYTES),
    )
    .await;
    let failing_job = jobs.await_create(failing).await.job_id;
    let mut workspaces = Subscription::open_with(
        &world.subscription_url(),
        &owner,
        WORKSPACE_DELTAS,
        serde_json::json!({}),
    )
    .await;
    workspaces.next_payload(Duration::from_secs(10)).await;

    // When: Jobs fails the job of one of them
    jobs.fail(failing_job, "runner_crashed", None).await;

    // Then: the workspace's view republishes with one file in each state
    let counted = |node: &serde_json::Value| {
        let view = &node["view"];
        (
            view["fileCount"].as_i64(),
            view["pendingFileCount"].as_i64(),
            view["processingFileCount"].as_i64(),
            view["readyFileCount"].as_i64(),
            view["failedFileCount"].as_i64(),
        )
    };
    next_delta(&mut workspaces, "workspaceDeltas", |node| {
        node["__typename"] == "WorkspaceUpsert"
            && node["view"]["id"] == drive.to_string()
            && counted(node) == (Some(4), Some(1), Some(1), Some(1), Some(1))
    })
    .await;
    // And: a read answers the same, and an empty drive counts nothing
    let counts = world.workspace_counts(&owner, drive).await;
    assert_eq!(
        counts,
        serde_json::json!({
            "fileCount": 4,
            "pendingFileCount": 1,
            "processingFileCount": 1,
            "readyFileCount": 1,
            "failedFileCount": 1,
        })
    );
    let nothing = world.workspace_counts(&owner, empty).await;
    assert_eq!(
        nothing,
        serde_json::json!({
            "fileCount": 0,
            "pendingFileCount": 0,
            "processingFileCount": 0,
            "readyFileCount": 0,
            "failedFileCount": 0,
        })
    );

    world.cleanup().await;
}
