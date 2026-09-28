//! The host's own objects refresh as the files of their drive change: the
//! workspace view carrying its drive's file counts republishes.

use std::time::Duration;

use uuid::Uuid;

use crate::harness::runner::{RUNNER_SCOPE, Report, finish_job, install_render_rule, report};
use crate::harness::upload::{
    UploadRequest, commit, post_bytes, request, ticket, upload, upload_processed,
};
use crate::harness::{
    JobsStandIn, Subscription, World, manager_passport, next_delta, ok, passport, refute_delta,
    service_passport,
};

const BYTES: &[u8] = b"a document whose drive counts move";

const WORKSPACE_DELTAS: &str = "subscription{workspaceDeltas{__typename \
    ... on WorkspaceReset{views{... on WorkspaceView{id fileCount readyFileCount}}} \
    ... on WorkspaceUpsert{cause view{... on WorkspaceView{id fileCount readyFileCount}}}}}";

async fn workspace_subscription(world: &World, passport: &str) -> Subscription {
    let mut sub = Subscription::open_with(
        &world.subscription_url(),
        passport,
        WORKSPACE_DELTAS,
        serde_json::json!({}),
    )
    .await;
    let reset = sub.next_payload(Duration::from_secs(10)).await;
    assert_eq!(reset["workspaceDeltas"]["__typename"], "WorkspaceReset");
    sub
}

async fn counts_reach(sub: &mut Subscription, workspace: Uuid, files: i64, ready: i64) {
    next_delta(sub, "workspaceDeltas", |node| {
        node["__typename"] == "WorkspaceUpsert"
            && node["view"]["id"] == workspace.to_string()
            && node["view"]["fileCount"] == files
            && node["view"]["readyFileCount"] == ready
    })
    .await;
}

#[tokio::test]
async fn the_hosts_own_object_republishes_as_the_files_of_its_drive_land_fail_move_and_go() {
    // Given: two workspaces, whose view shows their drive's file counts, watched live
    let world = World::start("pod-host-refresh").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    let library = world.create_workspace(&owner, "library").await;
    let archive = world.create_workspace(&owner, "archive").await;
    let mut workspaces = workspace_subscription(&world, &owner).await;

    // When: a file is requested, then committed
    let upload_request = UploadRequest {
        media_type: "application/octet-stream",
        ..UploadRequest::text(library, "", "plain.bin", BYTES)
    };
    let plain = Uuid::now_v7();
    let upload_ticket = ticket(&request(&world, &owner, plain, &upload_request).await);
    // Then: the pending file counts, not yet READY
    counts_reach(&mut workspaces, library, 1, 0).await;
    let posted = post_bytes(&world, &upload_ticket, BYTES, "plain.bin").await;
    assert!((200..300).contains(&posted));
    ok(&commit(&world, &owner, plain).await);
    // Then: it lands READY
    counts_reach(&mut workspaces, library, 1, 1).await;

    // When: a second file enters a chain that fails
    install_render_rule(&world, &manager).await;
    let doomed = upload_processed(
        &world,
        &owner,
        &UploadRequest::text(library, "", "doomed.txt", BYTES),
    )
    .await;
    counts_reach(&mut workspaces, library, 2, 1).await;
    let job = jobs.await_create(doomed).await.job_id;
    jobs.fail(job, "runner_error", Some("unreadable")).await;
    world.await_state(&owner, doomed, "FAILED").await;

    // When: the READY file moves to the other workspace
    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!,$d:UUID!){workspaceUpdateFile(fileId:$f,driveId:$d){success}}",
            serde_json::json!({ "f": plain, "d": archive }),
        )
        .await);
    // Then: both workspaces republish
    counts_reach(&mut workspaces, library, 1, 0).await;
    counts_reach(&mut workspaces, archive, 1, 1).await;

    // When: the failed file is deleted
    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!){workspaceDeleteFile(fileId:$f){success}}",
            serde_json::json!({ "f": doomed }),
        )
        .await);
    // Then: its workspace is empty again
    counts_reach(&mut workspaces, library, 0, 0).await;

    // When: a file's chain finishes
    let finished = upload_processed(
        &world,
        &owner,
        &UploadRequest::text(library, "", "finished.txt", BYTES),
    )
    .await;
    counts_reach(&mut workspaces, library, 1, 0).await;
    let runner = service_passport(&[RUNNER_SCOPE]);
    let job = jobs.await_create(finished).await.job_id;
    ok(&report(
        &world,
        &runner,
        finished,
        Report {
            job_id: job,
            pages: vec![(1, "done")],
            origin: None,
            indexer: None,
            done: true,
        },
    )
    .await);
    finish_job(&world, &jobs, job).await;
    // Then: the READY count rises when the chain lands
    counts_reach(&mut workspaces, library, 1, 1).await;

    // When: a change that moves no count happens (a retitle)
    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!){workspaceRetitleFile(fileId:$f,title:\"Done\"){success}}",
            serde_json::json!({ "f": finished }),
        )
        .await);
    // Then: the host object is recomputed but unchanged, so nothing is sent
    refute_delta(
        &mut workspaces,
        "workspaceDeltas",
        Duration::from_millis(800),
        |node| node["__typename"] == "WorkspaceUpsert",
    )
    .await;

    // When: a folder of more files than the bulk threshold is deleted
    for name in ["a.bin", "b.bin", "c.bin", "d.bin"] {
        upload(
            &world,
            &owner,
            &UploadRequest {
                media_type: "application/octet-stream",
                ..UploadRequest::text(archive, "bulk", name, BYTES)
            },
        )
        .await;
    }
    counts_reach(&mut workspaces, archive, 5, 5).await;
    ok(&world
        .gql(
            &owner,
            "mutation($d:UUID!,$p:String!){workspaceDeleteFolder(driveId:$d,prefix:$p){success}}",
            serde_json::json!({ "d": archive, "p": "bulk" }),
        )
        .await);
    // Then: the bulk path refreshes the host object too
    counts_reach(&mut workspaces, archive, 1, 1).await;

    // And: a fresh session's snapshot carries the current counts
    let mut fresh = Subscription::open_with(
        &world.subscription_url(),
        &owner,
        WORKSPACE_DELTAS,
        serde_json::json!({}),
    )
    .await;
    let reset = fresh.next_payload(Duration::from_secs(10)).await;
    let views = reset["workspaceDeltas"]["views"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let counts = |id: Uuid| {
        views
            .iter()
            .find(|view| view["id"] == id.to_string())
            .map(|view| (view["fileCount"].clone(), view["readyFileCount"].clone()))
    };
    assert_eq!(
        counts(library),
        Some((serde_json::json!(1), serde_json::json!(1)))
    );
    assert_eq!(
        counts(archive),
        Some((serde_json::json!(1), serde_json::json!(1)))
    );

    world.cleanup().await;
}
