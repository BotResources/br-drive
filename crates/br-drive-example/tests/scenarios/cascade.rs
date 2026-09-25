use uuid::Uuid;

use crate::harness::runner::{RUNNER_SCOPE, Report, install_render_rule, report, upload_image};
use crate::harness::upload::{UploadRequest, upload};
use crate::harness::{
    JobsStandIn, World, drive_subscription, error_code, manager_passport, next_drive_delta, ok,
    passport, service_passport,
};

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
async fn deleting_one_file_releases_its_image_blobs_with_its_source_and_closes_its_page_window() {
    let world = World::start("pod-cascade-file-images").await;
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    let jobs = JobsStandIn::attach(&world).await;
    install_render_rule(&world, &manager_passport(Uuid::now_v7(), "Ada")).await;
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "illustrated.txt", BYTES),
    )
    .await;
    let job_id = world.await_job(file_id).await;
    upload_image(&world, &runner, file_id, job_id, "p001-img01.png", b"one").await;
    upload_image(&world, &runner, file_id, job_id, "p002-img01.png", b"two").await;
    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id,
            pages: vec![(1, "![a](p001-img01.png)"), (2, "![b](p002-img01.png)")],
            origin: None,
            indexer: None,
            done: true,
        },
    )
    .await);
    let images = [
        world.image_row(file_id, "p001-img01.png").await.unwrap().0,
        world.image_row(file_id, "p002-img01.png").await.unwrap().0,
    ];
    let source = world.source_of(file_id).await;
    let mut pages = crate::harness::pages_subscription(&world, &owner, file_id).await;

    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!){workspaceDeleteFile(fileId:$f){success}}",
            serde_json::json!({ "f": file_id }),
        )
        .await);

    for reference in images {
        assert_eq!(
            world.blob_state(reference).await.as_deref(),
            Some("orphaned"),
            "the per-row delete releases every image blob in its transaction"
        );
    }
    jobs.await_cancel(job_id).await;
    jobs.cancel(job_id).await;
    assert_eq!(world.blob_state(source).await.as_deref(), Some("orphaned"));
    crate::harness::next_page_delta(&mut pages, |node| {
        node["__typename"] == "DriveRemove" && node["key"]["number"] == 1
    })
    .await;
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM drive.file_image WHERE file_id = $1")
        .bind(file_id)
        .fetch_one(&world.db.app)
        .await
        .unwrap();
    assert_eq!(rows, 0);

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
