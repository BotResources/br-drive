//! The engine's `window_capacity` bounds a drive listing (engine 0.4.0): a
//! window over the capacity is refused `WINDOW_TOO_LARGE` at attach and on a
//! one-shot listing, while a file of that drive still reads, and downloads,
//! by its one row; a session already live is never ended for its size.

use std::time::Duration;

use uuid::Uuid;

use crate::harness::upload::{UploadRequest, upload};
use crate::harness::{
    DRIVE_DELTAS, Subscription, World, WorldOptions, drive_subscription, error_code,
    next_drive_delta, ok, passport,
};
use crate::poll_until;

const CAPACITY: usize = 2;

async fn drive_files_response(world: &World, owner: &str, drive: Uuid) -> serde_json::Value {
    world
        .gql(
            owner,
            "query($d:UUID!){workspaceDriveFiles(driveId:$d){id}}",
            serde_json::json!({ "d": drive }),
        )
        .await
}

#[tokio::test]
async fn a_drive_over_the_window_capacity_is_refused_as_a_window_and_read_by_its_rows() {
    let world = World::start_with(
        "pod-capacity",
        WorldOptions {
            window_capacity: Some(CAPACITY),
            ..WorldOptions::default()
        },
    )
    .await;
    let owner = passport(Uuid::now_v7());
    let drive = world.create_workspace(&owner, "capacity").await;
    let first = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "one.txt", b"one"),
    )
    .await;
    upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "two.txt", b"two"),
    )
    .await;

    // At the capacity, the listing and the session attach.
    assert_eq!(world.drive_files(&owner, drive).await.len(), CAPACITY);
    let mut live = drive_subscription(&world, &owner, drive).await;

    let third = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "three.txt", b"three"),
    )
    .await;

    // The session attached before the drive grew stays live and sees the
    // file that took it past the capacity.
    let grown = next_drive_delta(&mut live, |node| {
        node["__typename"] == "DriveUpsert" && node["view"]["id"] == third.to_string()
    })
    .await;
    assert_eq!(grown["view"]["name"], "three.txt");

    // Past the capacity, a one-shot listing and a new session are refused.
    let refused = drive_files_response(&world, &owner, drive).await;
    assert_eq!(error_code(&refused), "WINDOW_TOO_LARGE", "{refused}");
    let mut late = Subscription::open_with(
        &world.subscription_url(),
        &owner,
        DRIVE_DELTAS,
        serde_json::json!({ "d": drive }),
    )
    .await;
    let errors = late.next_error(Duration::from_secs(10)).await;
    assert_eq!(
        errors[0]["extensions"]["code"], "WINDOW_TOO_LARGE",
        "{errors}"
    );

    // One file still reads, and downloads, by its one row.
    assert_eq!(world.file(&owner, first).await["name"], "one.txt");
    let url = poll_until!(Duration::from_secs(15), {
        ok(&world.file_access(&owner, first).await)["workspaceFileAccess"]
            .as_str()
            .map(str::to_string)
    });
    assert!(
        url.contains("response-content-disposition=attachment"),
        "the owner downloads the file: {url}"
    );

    world.cleanup().await;
}
