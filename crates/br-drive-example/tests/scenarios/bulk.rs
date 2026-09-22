use std::collections::BTreeSet;
use std::time::Duration;

use uuid::Uuid;

use crate::harness::upload::{UploadRequest, upload};
use crate::harness::{World, drive_subscription, next_drive_delta, ok, passport};

const BYTES: &[u8] = b"bulk bytes";
const OVER_THE_THRESHOLD: usize = 5;

async fn seed_many(world: &World, owner: &str, drive: Uuid, path: &str) -> Vec<Uuid> {
    let mut ids = Vec::new();
    for index in 0..OVER_THE_THRESHOLD {
        let name = format!("file-{index}.txt");
        ids.push(
            upload(
                world,
                owner,
                &UploadRequest::text(drive, path, &name, BYTES),
            )
            .await,
        );
    }
    ids
}

fn reset_views(node: &serde_json::Value) -> Vec<serde_json::Value> {
    node["views"].as_array().cloned().unwrap_or_default()
}

#[tokio::test]
async fn moving_a_folder_past_the_reset_threshold_rewrites_every_path_and_resets_the_session() {
    let world = World::start("pod-bulk-move").await;
    let owner = passport(Uuid::now_v7());
    let drive = world.create_workspace(&owner, "library").await;
    let ids = seed_many(&world, &owner, drive, "big").await;
    let mut sub = drive_subscription(&world, &owner, drive).await;

    ok(&world
        .gql(
            &owner,
            "mutation($d:UUID!,$o:String!,$n:String!){workspaceMoveFolder(driveId:$d,oldPrefix:$o,newPrefix:$n){success}}",
            serde_json::json!({ "d": drive, "o": "big", "n": "moved/big" }),
        )
        .await);

    let reset = next_drive_delta(&mut sub, |node| {
        node["__typename"] == "DriveReset"
            && reset_views(node)
                .iter()
                .all(|view| view["path"] == "moved/big")
            && reset_views(node).len() == OVER_THE_THRESHOLD
    })
    .await;
    let seen: BTreeSet<String> = reset_views(&reset)
        .iter()
        .map(|view| view["id"].as_str().unwrap().to_string())
        .collect();
    let expected: BTreeSet<String> = ids.iter().map(Uuid::to_string).collect();
    assert_eq!(
        seen, expected,
        "past the threshold the session is reset from the committed state instead of one delta per file"
    );
    let files = world.drive_files(&owner, drive).await;
    assert!(files.iter().all(|file| file["path"] == "moved/big"));
    assert_eq!(
        world.folder_gestures(drive).await.len(),
        1,
        "the hook still runs once, in the bulk transaction"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn deleting_a_folder_past_the_reset_threshold_removes_every_file_and_releases_every_blob() {
    let world = World::start("pod-bulk-delete-folder").await;
    let owner = passport(Uuid::now_v7());
    let drive = world.create_workspace(&owner, "library").await;
    let ids = seed_many(&world, &owner, drive, "big").await;
    let keeper = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "keep.txt", BYTES),
    )
    .await;
    let mut sources = Vec::new();
    for id in &ids {
        sources.push(world.source_of(*id).await);
    }
    let mut sub = drive_subscription(&world, &owner, drive).await;

    ok(&world
        .gql(
            &owner,
            "mutation($d:UUID!,$p:String!){workspaceDeleteFolder(driveId:$d,prefix:$p){success}}",
            serde_json::json!({ "d": drive, "p": "big" }),
        )
        .await);

    let reset = next_drive_delta(&mut sub, |node| {
        node["__typename"] == "DriveReset" && reset_views(node).len() == 1
    })
    .await;
    assert_eq!(reset_views(&reset)[0]["id"], keeper.to_string());
    for id in &ids {
        assert!(world.file(&owner, *id).await.is_null());
    }
    for source in &sources {
        assert_eq!(world.blob_state(*source).await.as_deref(), Some("orphaned"));
    }
    assert_ne!(
        world
            .blob_state(world.source_of(keeper).await)
            .await
            .as_deref(),
        Some("orphaned")
    );
    assert_eq!(world.folder_gestures(drive).await.len(), 1);

    world.cleanup().await;
}

#[tokio::test]
async fn deleting_a_workspace_past_the_reset_threshold_cascades_through_the_bulk_path() {
    let world = World::start("pod-bulk-delete-drive").await;
    let owner = passport(Uuid::now_v7());
    let drive = world.create_workspace(&owner, "library").await;
    let ids = seed_many(&world, &owner, drive, "deep/tree").await;
    let mut sources = Vec::new();
    for id in &ids {
        sources.push(world.source_of(*id).await);
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
        node["__typename"] == "DriveReset" && reset_views(node).is_empty()
    })
    .await;
    sub.expect_silence(Duration::from_secs(1)).await;
    for id in &ids {
        assert!(world.file(&owner, *id).await.is_null());
    }
    for source in &sources {
        assert_eq!(world.blob_state(*source).await.as_deref(), Some("orphaned"));
    }
    let drives: i64 = sqlx::query_scalar("SELECT count(*) FROM drive.drive WHERE id = $1")
        .bind(drive)
        .fetch_one(&world.db.app)
        .await
        .unwrap();
    assert_eq!(drives, 0);

    world.cleanup().await;
}
