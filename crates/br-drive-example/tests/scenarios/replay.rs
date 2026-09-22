use std::time::Duration;

use uuid::Uuid;

use crate::harness::upload::{UploadRequest, request, ticket, upload};
use crate::harness::{
    DRIVE_DELTAS, Subscription, World, WorldOptions, drive_subscription, next_drive_delta, ok,
    passport,
};

const BYTES: &[u8] = b"replay bytes";

#[tokio::test]
async fn a_redelivered_upload_deadline_changes_nothing_for_a_ready_or_an_already_removed_file() {
    let world = World::start_with(
        "pod-replay-deadline",
        WorldOptions {
            upload_window: Duration::from_secs(1),
            ..WorldOptions::default()
        },
    )
    .await;
    let owner = passport(Uuid::now_v7());
    let drive = world.create_workspace(&owner, "library").await;
    let ready = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "ready.txt", BYTES),
    )
    .await;
    let mut sub = drive_subscription(&world, &owner, drive).await;

    let abandoned = Uuid::now_v7();
    ticket(
        &request(
            &world,
            &owner,
            abandoned,
            &UploadRequest::text(drive, "", "abandoned.txt", BYTES),
        )
        .await,
    );
    next_drive_delta(&mut sub, |node| {
        node["__typename"] == "DriveRemove" && node["key"] == abandoned.to_string()
    })
    .await;

    world.send_upload_deadline(abandoned).await;
    world.send_upload_deadline(ready).await;
    world.send_upload_deadline(Uuid::now_v7()).await;
    sub.expect_silence(Duration::from_secs(3)).await;

    let file = world.file(&owner, ready).await;
    assert_eq!(
        file["processingState"], "READY",
        "a deadline on a committed file is a no-op"
    );
    assert!(world.file(&owner, abandoned).await.is_null());
    let dead_letters: i64 = sqlx::query_scalar("SELECT count(*) FROM service_engine.dead_letter")
        .fetch_one(&world.db.app)
        .await
        .unwrap();
    assert_eq!(
        dead_letters, 0,
        "a replayed deadline is absorbed, never parked"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn a_fresh_subscription_after_mutations_receives_the_committed_state_as_its_reset() {
    let world = World::start("pod-replay-reconnect").await;
    let owner = passport(Uuid::now_v7());
    let drive = world.create_workspace(&owner, "library").await;
    let kept = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "docs", "kept.txt", BYTES),
    )
    .await;
    let gone = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "docs", "gone.txt", BYTES),
    )
    .await;
    let mut first = drive_subscription(&world, &owner, drive).await;

    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!,$n:String){workspaceUpdateFile(fileId:$f,name:$n){success}}",
            serde_json::json!({ "f": kept, "n": "renamed.txt" }),
        )
        .await);
    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!){workspaceDeleteFile(fileId:$f){success}}",
            serde_json::json!({ "f": gone }),
        )
        .await);
    next_drive_delta(&mut first, |node| {
        node["__typename"] == "DriveRemove" && node["key"] == gone.to_string()
    })
    .await;
    drop(first);

    let mut second = Subscription::open_with(
        &world.subscription_url(),
        &owner,
        DRIVE_DELTAS,
        serde_json::json!({ "d": drive }),
    )
    .await;
    let reset = second.next_payload(Duration::from_secs(10)).await;
    let node = &reset["workspaceDriveChanged"];
    assert_eq!(node["__typename"], "DriveReset");
    let views = node["views"].as_array().unwrap();
    assert_eq!(
        views.len(),
        1,
        "the reset carries only what survived: {reset}"
    );
    assert_eq!(views[0]["id"], kept.to_string());
    assert_eq!(views[0]["name"], "renamed.txt");

    world.cleanup().await;
}
