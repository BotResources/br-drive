use std::collections::BTreeMap;

use uuid::Uuid;

use crate::harness::upload::{UploadRequest, upload};
use crate::harness::{World, drive_subscription, error_code, next_drive_delta, ok, passport};

const BYTES: &[u8] = b"folder bytes";

async fn seed_tree(world: &World, owner: &str, drive: Uuid) -> BTreeMap<&'static str, Uuid> {
    let mut ids = BTreeMap::new();
    for (path, name) in [("docs", "a.txt"), ("docs/sub", "b.txt"), ("other", "c.txt")] {
        let id = upload(world, owner, &UploadRequest::text(drive, path, name, BYTES)).await;
        ids.insert(name, id);
    }
    ids
}

fn paths(files: &[serde_json::Value]) -> BTreeMap<String, String> {
    files
        .iter()
        .map(|file| {
            (
                file["name"].as_str().unwrap().to_string(),
                file["path"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

async fn move_folder(
    world: &World,
    owner: &str,
    drive: Uuid,
    old: &str,
    new: &str,
) -> serde_json::Value {
    world
        .gql(
            owner,
            "mutation($d:UUID!,$o:String!,$n:String!){workspaceMoveFolder(driveId:$d,oldPrefix:$o,newPrefix:$n){success}}",
            serde_json::json!({ "d": drive, "o": old, "n": new }),
        )
        .await
}

async fn delete_folder(world: &World, owner: &str, drive: Uuid, prefix: &str) -> serde_json::Value {
    world
        .gql(
            owner,
            "mutation($d:UUID!,$p:String!){workspaceDeleteFolder(driveId:$d,prefix:$p){success}}",
            serde_json::json!({ "d": drive, "p": prefix }),
        )
        .await
}

async fn update_file(
    world: &World,
    owner: &str,
    file: Uuid,
    variables: serde_json::Value,
) -> serde_json::Value {
    let mut variables = variables;
    variables["f"] = serde_json::json!(file);
    world
        .gql(
            owner,
            "mutation($f:UUID!,$n:String,$p:String,$d:UUID){workspaceUpdateFile(fileId:$f,name:$n,path:$p,driveId:$d){success}}",
            variables,
        )
        .await
}

#[tokio::test]
async fn moving_a_folder_rewrites_every_path_underneath_and_runs_the_host_hook_in_the_transaction()
{
    let world = World::start_blobs("pod-folder-move").await;
    let owner = passport(Uuid::now_v7());
    let drive = world.create_workspace(&owner, "library").await;
    let ids = seed_tree(&world, &owner, drive).await;
    let mut sub = drive_subscription(&world, &owner, drive).await;

    ok(&move_folder(&world, &owner, drive, "docs", "archive/2025").await);

    let moved = next_drive_delta(&mut sub, |node| {
        node["__typename"] == "DriveUpsert"
            && node["view"]["id"] == ids["b.txt"].to_string()
            && node["cause"]["kind"] == "FolderMoved"
    })
    .await;
    assert_eq!(moved["view"]["path"], "archive/2025/sub");

    let after = paths(&world.drive_files(&owner, drive).await);
    assert_eq!(after["a.txt"], "archive/2025");
    assert_eq!(after["b.txt"], "archive/2025/sub");
    assert_eq!(after["c.txt"], "other", "a sibling folder is untouched");

    assert_eq!(
        world.folder_gestures(drive).await,
        vec![(
            "moved".to_string(),
            "docs".to_string(),
            Some("archive/2025".to_string())
        )],
        "the host hook ran and committed with the move"
    );

    let refused = move_folder(&world, &owner, drive, "archive", "forbidden").await;
    assert_eq!(
        error_code(&refused),
        "FORBIDDEN_FOLDER",
        "a hook refusal surfaces as the host's own code"
    );
    assert_eq!(
        paths(&world.drive_files(&owner, drive).await),
        after,
        "a refused hook rolls the bulk path rewrite back"
    );
    assert_eq!(world.folder_gestures(drive).await.len(), 1);

    let missing = move_folder(&world, &owner, drive, "nowhere", "somewhere").await;
    assert_eq!(error_code(&missing), "FOLDER_NOT_FOUND");
    let into_itself = move_folder(&world, &owner, drive, "archive", "archive/2025/deeper").await;
    assert_eq!(error_code(&into_itself), "FOLDER_INTO_ITSELF");
    let root = move_folder(&world, &owner, drive, "/", "elsewhere").await;
    assert_eq!(error_code(&root), "INVALID_PATH");

    upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "other/sub", "b.txt", BYTES),
    )
    .await;
    let taken = move_folder(&world, &owner, drive, "archive/2025", "other").await;
    assert_eq!(
        error_code(&taken),
        "NAME_TAKEN",
        "a move that would land on an occupied (path, name) is refused as a whole"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn deleting_a_folder_removes_its_files_releases_their_blobs_and_runs_the_host_hook() {
    let world = World::start_blobs("pod-folder-delete").await;
    let owner = passport(Uuid::now_v7());
    let drive = world.create_workspace(&owner, "library").await;
    let ids = seed_tree(&world, &owner, drive).await;
    let sources: BTreeMap<&str, Uuid> = {
        let mut sources = BTreeMap::new();
        for (name, id) in &ids {
            sources.insert(*name, world.source_of(*id).await);
        }
        sources
    };
    let mut sub = drive_subscription(&world, &owner, drive).await;

    ok(&delete_folder(&world, &owner, drive, "docs").await);

    next_drive_delta(&mut sub, |node| {
        node["__typename"] == "DriveRemove" && node["key"] == ids["a.txt"].to_string()
    })
    .await;

    let after = paths(&world.drive_files(&owner, drive).await);
    assert_eq!(after.keys().collect::<Vec<_>>(), vec!["c.txt"]);
    assert_eq!(
        world.blob_state(sources["a.txt"]).await.as_deref(),
        Some("orphaned")
    );
    assert_eq!(
        world.blob_state(sources["b.txt"]).await.as_deref(),
        Some("orphaned")
    );
    assert_ne!(
        world.blob_state(sources["c.txt"]).await.as_deref(),
        Some("orphaned"),
        "the untouched file keeps its source"
    );
    assert_eq!(
        world.folder_gestures(drive).await,
        vec![("deleted".to_string(), "docs".to_string(), None)]
    );

    let banned = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "forbidden", "keep.txt", BYTES),
    )
    .await;
    let refused = delete_folder(&world, &owner, drive, "forbidden").await;
    assert_eq!(error_code(&refused), "FORBIDDEN_FOLDER");
    assert!(
        !world.file(&owner, banned).await.is_null(),
        "a refused hook rolls the bulk delete back"
    );
    assert_ne!(
        world
            .blob_state(world.source_of(banned).await)
            .await
            .as_deref(),
        Some("orphaned")
    );
    assert_eq!(world.folder_gestures(drive).await.len(), 1);

    let root = delete_folder(&world, &owner, drive, "").await;
    assert_eq!(error_code(&root), "INVALID_PATH");
    let missing = delete_folder(&world, &owner, drive, "docs").await;
    assert_eq!(error_code(&missing), "FOLDER_NOT_FOUND");

    world.cleanup().await;
}

#[tokio::test]
async fn renaming_and_moving_a_file_updates_its_path_and_a_taken_name_is_refused() {
    let world = World::start_blobs("pod-file-update").await;
    let owner = passport(Uuid::now_v7());
    let drive = world.create_workspace(&owner, "library").await;
    let ids = seed_tree(&world, &owner, drive).await;
    let mut sub = drive_subscription(&world, &owner, drive).await;

    ok(&update_file(
        &world,
        &owner,
        ids["a.txt"],
        serde_json::json!({ "n": "renamed.txt" }),
    )
    .await);
    let renamed = next_drive_delta(&mut sub, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "Renamed"
    })
    .await;
    assert_eq!(renamed["view"]["name"], "renamed.txt");
    assert_eq!(renamed["view"]["path"], "docs");

    ok(&update_file(
        &world,
        &owner,
        ids["a.txt"],
        serde_json::json!({ "p": "/other/" }),
    )
    .await);
    assert_eq!(world.file(&owner, ids["a.txt"]).await["path"], "other");

    let taken = update_file(
        &world,
        &owner,
        ids["a.txt"],
        serde_json::json!({ "n": "c.txt" }),
    )
    .await;
    assert_eq!(error_code(&taken), "NAME_TAKEN");

    let unchanged = update_file(&world, &owner, ids["a.txt"], serde_json::json!({})).await;
    assert_eq!(error_code(&unchanged), "NOTHING_TO_CHANGE");

    let bad_name = update_file(
        &world,
        &owner,
        ids["a.txt"],
        serde_json::json!({ "n": "x/y" }),
    )
    .await;
    assert_eq!(error_code(&bad_name), "INVALID_NAME");

    let missing = update_file(
        &world,
        &owner,
        Uuid::now_v7(),
        serde_json::json!({ "n": "ghost.txt" }),
    )
    .await;
    assert_eq!(error_code(&missing), "FILE_NOT_FOUND");

    world.cleanup().await;
}

#[tokio::test]
async fn a_protected_file_refuses_user_rename_move_and_delete_and_says_so_in_its_affordances() {
    let world = World::start_blobs("pod-protected").await;
    let owner = passport(Uuid::now_v7());
    let drive = world.create_workspace(&owner, "library").await;
    let ids = seed_tree(&world, &owner, drive).await;
    let mut sub = drive_subscription(&world, &owner, drive).await;

    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!,$p:Boolean!){workspaceProtectFile(fileId:$f,protected:$p){success}}",
            serde_json::json!({ "f": ids["a.txt"], "p": true }),
        )
        .await);
    let protected = next_drive_delta(&mut sub, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "ProtectionChanged"
    })
    .await;
    for action in ["delete", "rename", "move"] {
        assert_eq!(
            protected["view"]["affordances"][action]["allowed"], false,
            "{action} is not afforded on a protected file"
        );
        assert_eq!(
            protected["view"]["affordances"][action]["reason"],
            "FILE_PROTECTED"
        );
    }
    assert_eq!(
        protected["view"]["affordances"]["download"]["allowed"], true,
        "protection restricts curation, not reading"
    );

    let renamed = update_file(
        &world,
        &owner,
        ids["a.txt"],
        serde_json::json!({ "n": "nope.txt" }),
    )
    .await;
    assert_eq!(error_code(&renamed), "FILE_PROTECTED");
    let deleted = world
        .gql(
            &owner,
            "mutation($f:UUID!){workspaceDeleteFile(fileId:$f){success}}",
            serde_json::json!({ "f": ids["a.txt"] }),
        )
        .await;
    assert_eq!(error_code(&deleted), "FILE_PROTECTED");
    let moved = move_folder(&world, &owner, drive, "docs", "elsewhere").await;
    assert_eq!(error_code(&moved), "FILE_PROTECTED");
    let removed = delete_folder(&world, &owner, drive, "docs").await;
    assert_eq!(error_code(&removed), "FILE_PROTECTED");
    assert_eq!(
        paths(&world.drive_files(&owner, drive).await).len(),
        3,
        "nothing moved or vanished"
    );

    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!,$p:Boolean!){workspaceProtectFile(fileId:$f,protected:$p){success}}",
            serde_json::json!({ "f": ids["a.txt"], "p": false }),
        )
        .await);
    assert_eq!(
        world.file(&owner, ids["a.txt"]).await["affordances"]["delete"]["allowed"],
        true
    );
    ok(&delete_folder(&world, &owner, drive, "docs").await);

    world.cleanup().await;
}
