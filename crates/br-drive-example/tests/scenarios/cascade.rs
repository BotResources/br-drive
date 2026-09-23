use uuid::Uuid;

use crate::harness::upload::{UploadRequest, upload};
use crate::harness::{World, drive_subscription, error_code, next_drive_delta, ok, passport};

const BYTES: &[u8] = b"cascade bytes";

#[tokio::test]
async fn deleting_a_file_releases_its_blob_in_the_same_transaction() {
    let world = World::start("pod-cascade-file").await;
    let owner = passport(Uuid::now_v7());
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "gone.txt", BYTES),
    )
    .await;
    let source = world.source_of(file_id).await;
    let mut sub = drive_subscription(&world, &owner, drive).await;

    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!){workspaceDeleteFile(fileId:$f){success}}",
            serde_json::json!({ "f": file_id }),
        )
        .await);

    next_drive_delta(&mut sub, |node| {
        node["__typename"] == "DriveRemove" && node["key"] == file_id.to_string()
    })
    .await;
    assert!(world.file(&owner, file_id).await.is_null());
    assert_eq!(
        world.blob_state(source).await.as_deref(),
        Some("orphaned"),
        "the delete released the source in the same transaction"
    );
    let again = world
        .gql(
            &owner,
            "mutation($f:UUID!){workspaceDeleteFile(fileId:$f){success}}",
            serde_json::json!({ "f": file_id }),
        )
        .await;
    assert_eq!(error_code(&again), "FILE_NOT_FOUND");

    world.cleanup().await;
}

#[tokio::test]
async fn deleting_the_workspace_cascades_to_its_files_and_releases_their_blobs() {
    let world = World::start("pod-cascade-drive").await;
    let owner = passport(Uuid::now_v7());
    let drive = world.create_workspace(&owner, "library").await;
    let mut sources = Vec::new();
    let mut files = Vec::new();
    for name in ["one.txt", "two.txt", "three.txt"] {
        let id = upload(
            &world,
            &owner,
            &UploadRequest::text(drive, "deep/er", name, BYTES),
        )
        .await;
        sources.push(world.source_of(id).await);
        files.push(id);
    }
    let mut sub = drive_subscription(&world, &owner, drive).await;

    ok(&world
        .gql(
            &owner,
            "mutation($id:UUID!){workspaceDelete(id:$id){success}}",
            serde_json::json!({ "id": drive }),
        )
        .await);

    next_drive_delta(&mut sub, |node| {
        node["__typename"] == "DriveRemove" && node["key"] == files[0].to_string()
    })
    .await;
    for file in &files {
        assert!(world.file(&owner, *file).await.is_null());
    }
    for source in &sources {
        assert_eq!(world.blob_state(*source).await.as_deref(), Some("orphaned"));
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM drive.file WHERE drive_id = $1")
        .bind(drive)
        .fetch_one(&world.db.app)
        .await
        .unwrap();
    assert_eq!(count, 0);
    let drives: i64 = sqlx::query_scalar("SELECT count(*) FROM drive.drive WHERE id = $1")
        .bind(drive)
        .fetch_one(&world.db.app)
        .await
        .unwrap();
    assert_eq!(drives, 0, "the drive row goes with its host object");

    world.cleanup().await;
}
