use std::time::Duration;

use uuid::Uuid;

use crate::harness::upload::{UploadRequest, request, upload};
use crate::harness::{
    DRIVE_DELTAS, Subscription, World, drive_subscription, error_code, next_drive_delta, ok,
    passport,
};

const BYTES: &[u8] = b"visibility bytes";

#[tokio::test]
async fn losing_the_workspace_removes_its_files_from_the_old_owners_live_session() {
    let world = World::start("pod-visibility-transfer").await;
    let old_owner_id = Uuid::now_v7();
    let new_owner_id = Uuid::now_v7();
    let old_owner = passport(old_owner_id);
    let new_owner = passport(new_owner_id);
    let drive = world.create_workspace(&old_owner, "handover").await;
    let file_id = upload(
        &world,
        &old_owner,
        &UploadRequest::text(drive, "", "handover.txt", BYTES),
    )
    .await;
    let mut old_session = drive_subscription(&world, &old_owner, drive).await;

    let mut stranger_session = Subscription::open_with(
        &world.subscription_url(),
        &new_owner,
        DRIVE_DELTAS,
        serde_json::json!({ "d": drive }),
    )
    .await;
    let empty = stranger_session.next_payload(Duration::from_secs(10)).await;
    assert_eq!(
        empty["workspaceDriveChanged"]["views"]
            .as_array()
            .unwrap()
            .len(),
        0,
        "the future owner sees nothing before the transfer"
    );

    ok(&world
        .gql(
            &old_owner,
            "mutation($id:UUID!,$to:UUID!){workspaceTransfer(id:$id,to:$to){success}}",
            serde_json::json!({ "id": drive, "to": new_owner_id }),
        )
        .await);

    let removed = next_drive_delta(&mut old_session, |node| {
        node["__typename"] == "DriveRemove" && node["key"] == file_id.to_string()
    })
    .await;
    assert_eq!(removed["projector"], "drive_files");
    assert!(
        world.file(&old_owner, file_id).await.is_null(),
        "the old owner no longer reads the file"
    );
    assert!(world.drive_files(&old_owner, drive).await.is_empty());
    assert!(
        ok(&world.file_access(&old_owner, file_id).await)["workspaceFileAccess"].is_null(),
        "holding the id buys the old owner no download"
    );

    let granted = next_drive_delta(&mut stranger_session, |node| {
        node["__typename"] == "DriveUpsert" && node["view"]["id"] == file_id.to_string()
    })
    .await;
    assert_eq!(granted["view"]["name"], "handover.txt");
    assert_eq!(
        world.file(&new_owner, file_id).await["affordances"]["delete"]["allowed"],
        true
    );

    ok(&world
        .gql(
            &new_owner,
            "mutation($f:UUID!,$n:String){workspaceUpdateFile(fileId:$f,name:$n){success}}",
            serde_json::json!({ "f": file_id, "n": "renamed-by-the-new-owner.txt" }),
        )
        .await);
    let renamed = next_drive_delta(&mut stranger_session, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "Renamed"
    })
    .await;
    assert_eq!(renamed["view"]["name"], "renamed-by-the-new-owner.txt");
    old_session.expect_silence(Duration::from_secs(2)).await;

    world.cleanup().await;
}

#[tokio::test]
async fn an_outsider_sees_nothing_and_every_gesture_is_refused_by_the_host_gate() {
    let world = World::start("pod-visibility-outsider").await;
    let owner = passport(Uuid::now_v7());
    let outsider = passport(Uuid::now_v7());
    let drive = world.create_workspace(&owner, "private").await;
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "mine.txt", BYTES),
    )
    .await;

    assert!(world.file(&outsider, file_id).await.is_null());
    assert!(world.drive_files(&outsider, drive).await.is_empty());
    assert!(ok(&world.file_access(&outsider, file_id).await)["workspaceFileAccess"].is_null());

    let uploading = request(
        &world,
        &outsider,
        Uuid::now_v7(),
        &UploadRequest::text(drive, "", "intruder.txt", BYTES),
    )
    .await;
    assert_eq!(
        error_code(&uploading),
        "NOT_THE_WORKSPACE_OWNER",
        "a gesture addressed to a drive answers the host's code"
    );
    // A gesture addressed to a file the outsider cannot see answers exactly
    // what an unknown id answers.
    let deleting = world
        .gql(
            &outsider,
            "mutation($f:UUID!){workspaceDeleteFile(fileId:$f){success}}",
            serde_json::json!({ "f": file_id }),
        )
        .await;
    assert_eq!(error_code(&deleting), "FILE_NOT_FOUND");
    let renaming = world
        .gql(
            &outsider,
            "mutation($f:UUID!,$n:String){workspaceUpdateFile(fileId:$f,name:$n){success}}",
            serde_json::json!({ "f": file_id, "n": "stolen.txt" }),
        )
        .await;
    assert_eq!(error_code(&renaming), "FILE_NOT_FOUND");
    let moving = world
        .gql(
            &outsider,
            "mutation($d:UUID!,$o:String!,$n:String!){workspaceMoveFolder(driveId:$d,oldPrefix:$o,newPrefix:$n){success}}",
            serde_json::json!({ "d": drive, "o": "a", "n": "b" }),
        )
        .await;
    assert_eq!(error_code(&moving), "NOT_THE_WORKSPACE_OWNER");
    let committing = world
        .gql(
            &outsider,
            "mutation($f:UUID!){workspaceCommitUpload(fileId:$f){success}}",
            serde_json::json!({ "f": file_id }),
        )
        .await;
    assert_eq!(error_code(&committing), "FILE_NOT_FOUND");
    let unknown_file = world
        .gql(
            &outsider,
            "mutation($f:UUID!){workspaceDeleteFile(fileId:$f){success}}",
            serde_json::json!({ "f": Uuid::now_v7() }),
        )
        .await;
    assert_eq!(error_code(&unknown_file), error_code(&deleting));
    let deleting_folder = world
        .gql(
            &outsider,
            "mutation($d:UUID!,$p:String!){workspaceDeleteFolder(driveId:$d,prefix:$p){success}}",
            serde_json::json!({ "d": drive, "p": "a" }),
        )
        .await;
    assert_eq!(error_code(&deleting_folder), "NOT_THE_WORKSPACE_OWNER");
    let unknown_drive = request(
        &world,
        &owner,
        Uuid::now_v7(),
        &UploadRequest::text(Uuid::now_v7(), "", "nowhere.txt", BYTES),
    )
    .await;
    assert_eq!(
        error_code(&unknown_drive),
        "NOT_THE_WORKSPACE_OWNER",
        "the gate answers first, so an unknown drive id is indistinguishable from a foreign one"
    );

    assert!(
        !world.file(&owner, file_id).await.is_null(),
        "nothing the outsider tried changed the file"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn a_file_moves_between_two_drives_of_one_owner_and_never_toward_a_foreign_one() {
    let world = World::start("pod-visibility-cross-drive").await;
    let owner = passport(Uuid::now_v7());
    let stranger = passport(Uuid::now_v7());
    let source_drive = world.create_workspace(&owner, "from").await;
    let target_drive = world.create_workspace(&owner, "to").await;
    let foreign_drive = world.create_workspace(&stranger, "theirs").await;
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(source_drive, "box", "moving.txt", BYTES),
    )
    .await;
    let mut from_session = drive_subscription(&world, &owner, source_drive).await;
    let mut to_session = drive_subscription(&world, &owner, target_drive).await;

    let foreign = world
        .gql(
            &owner,
            "mutation($f:UUID!,$d:UUID){workspaceUpdateFile(fileId:$f,driveId:$d){success}}",
            serde_json::json!({ "f": file_id, "d": foreign_drive }),
        )
        .await;
    assert_eq!(error_code(&foreign), "NOT_THE_WORKSPACE_OWNER");

    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!,$d:UUID){workspaceUpdateFile(fileId:$f,driveId:$d){success}}",
            serde_json::json!({ "f": file_id, "d": target_drive }),
        )
        .await);

    next_drive_delta(&mut from_session, |node| {
        node["__typename"] == "DriveRemove" && node["key"] == file_id.to_string()
    })
    .await;
    let arrived = next_drive_delta(&mut to_session, |node| {
        node["__typename"] == "DriveUpsert" && node["view"]["id"] == file_id.to_string()
    })
    .await;
    assert_eq!(
        arrived["view"]["path"], "box",
        "the path travels with the file"
    );
    assert!(world.drive_files(&owner, source_drive).await.is_empty());
    assert_eq!(world.drive_files(&owner, target_drive).await.len(), 1);
    assert_eq!(
        world.file(&owner, file_id).await["driveId"],
        target_drive.to_string()
    );

    world.cleanup().await;
}
