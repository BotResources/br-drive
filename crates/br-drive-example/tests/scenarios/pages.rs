use std::time::Duration;

use uuid::Uuid;

use crate::harness::runner::{
    RUNNER_SCOPE, Report, assign_job, image_ticket, report, request_image,
};
use crate::harness::upload::{UploadRequest, post_bytes, upload};
use crate::harness::{
    World, drive_subscription, error_code, next_drive_delta, ok, passport, service_passport,
};
use crate::poll_until;

const SOURCE: &[u8] = b"pages source";
const IMAGE_A: &[u8] = b"image a";
const IMAGE_B: &[u8] = b"image b";
const IMAGE_C: &[u8] = b"image c";

async fn upload_image(
    world: &World,
    runner: &str,
    file_id: Uuid,
    job_id: Uuid,
    name: &str,
    bytes: &[u8],
) -> String {
    let ticket = image_ticket(&request_image(world, runner, file_id, job_id, name, bytes).await);
    let status = post_bytes(world, &ticket, bytes, name).await;
    assert!((200..300).contains(&status), "{name} lands: {status}");
    ticket.fields["key"].as_str().unwrap().to_string()
}

async fn image_source(world: &World, file_id: Uuid, name: &str) -> Uuid {
    sqlx::query_scalar("SELECT blob_ref FROM drive.file_image WHERE file_id = $1 AND name = $2")
        .bind(file_id)
        .bind(name)
        .fetch_one(&world.db.app)
        .await
        .expect("the image row carries its blob reference")
}

#[tokio::test]
async fn a_user_edits_a_page_of_a_ready_file_and_the_rendition_travels_in_the_deltas() {
    let world = World::start("pod-page-edit").await;
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "doc.txt", SOURCE),
    )
    .await;
    let job_id = assign_job(&world, &owner, file_id).await;
    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id,
            pages: vec![(1, "as rendered")],
            origin: None,
            indexer: None,
            done: true,
        },
    )
    .await);
    let mut sub = drive_subscription(&world, &owner, drive).await;

    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!,$n:Int!,$m:String!){workspaceEditPage(fileId:$f,number:$n,markdown:$m){success}}",
            serde_json::json!({ "f": file_id, "n": 1, "m": "as corrected by a human" }),
        )
        .await);
    let edited = next_drive_delta(&mut sub, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "PageEdited"
    })
    .await;
    assert_eq!(edited["cause"]["number"], 1);
    assert_eq!(
        edited["view"]["pages"][0]["markdown"],
        "as corrected by a human"
    );
    assert_eq!(edited["view"]["pages"][0]["origin"], "EDITED");

    let missing = world
        .gql(
            &owner,
            "mutation($f:UUID!,$n:Int!,$m:String!){workspaceEditPage(fileId:$f,number:$n,markdown:$m){success}}",
            serde_json::json!({ "f": file_id, "n": 7, "m": "no such page" }),
        )
        .await;
    assert_eq!(error_code(&missing), "PAGE_NOT_FOUND");

    let outsider = passport(Uuid::now_v7());
    let foreign = world
        .gql(
            &outsider,
            "mutation($f:UUID!,$n:Int!,$m:String!){workspaceEditPage(fileId:$f,number:$n,markdown:$m){success}}",
            serde_json::json!({ "f": file_id, "n": 1, "m": "vandalism" }),
        )
        .await;
    assert_eq!(error_code(&foreign), "NOT_THE_WORKSPACE_OWNER");

    let pending = Uuid::now_v7();
    let ticket = crate::harness::upload::ticket(
        &crate::harness::upload::request(
            &world,
            &owner,
            pending,
            &UploadRequest::text(drive, "", "pending.txt", SOURCE),
        )
        .await,
    );
    assert_eq!(ticket.file_id, pending);
    let not_ready = world
        .gql(
            &owner,
            "mutation($f:UUID!,$n:Int!,$m:String!){workspaceEditPage(fileId:$f,number:$n,markdown:$m){success}}",
            serde_json::json!({ "f": pending, "n": 1, "m": "too early" }),
        )
        .await;
    assert_eq!(error_code(&not_ready), "FILE_NOT_READY");
    assert_eq!(
        world.file(&owner, pending).await["affordances"]["editPage"]["reason"],
        "FILE_NOT_READY"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn regenerating_a_page_replaces_that_pages_images_by_name_and_releases_the_rest() {
    let world = World::start("pod-page-regenerate").await;
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "doc.txt", SOURCE),
    )
    .await;
    let job_id = assign_job(&world, &owner, file_id).await;

    let key_a = upload_image(&world, &runner, file_id, job_id, "p001-img01.png", IMAGE_A).await;
    upload_image(&world, &runner, file_id, job_id, "p001-img02.png", IMAGE_B).await;
    upload_image(&world, &runner, file_id, job_id, "p002-img01.png", IMAGE_C).await;
    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id,
            pages: vec![
                (1, "![a](p001-img01.png) ![b](p001-img02.png)"),
                (2, "![c](p002-img01.png)"),
            ],
            origin: None,
            indexer: None,
            done: true,
        },
    )
    .await);
    let old_a = image_source(&world, file_id, "p001-img01.png").await;
    let old_b = image_source(&world, file_id, "p001-img02.png").await;
    let untouched_c = image_source(&world, file_id, "p002-img01.png").await;
    poll_until!(Duration::from_secs(15), {
        (world.blob_state(old_a).await.as_deref() == Some("uploaded")).then_some(())
    });

    let regeneration = assign_job(&world, &owner, file_id).await;
    let key_a2 = upload_image(
        &world,
        &runner,
        file_id,
        regeneration,
        "p001-img01.png",
        b"image a, regenerated",
    )
    .await;
    assert_ne!(key_a, key_a2, "a replaced image gets a new object key");
    assert_eq!(
        world.blob_state(old_a).await.as_deref(),
        Some("orphaned"),
        "replacing an image by name releases the old blob"
    );
    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id: regeneration,
            pages: vec![(1, "![a](p001-img01.png) only")],
            origin: Some("REGENERATED"),
            indexer: None,
            done: true,
        },
    )
    .await);

    let file = world.file(&owner, file_id).await;
    let names: Vec<&str> = file["images"]
        .as_array()
        .unwrap()
        .iter()
        .map(|image| image["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        vec!["p001-img01.png", "p002-img01.png"],
        "the regenerated page keeps only the images it references; page 2 is untouched"
    );
    assert_eq!(file["pages"][0]["origin"], "REGENERATED");
    assert_eq!(file["pages"][1]["origin"], "RUNNER");
    assert_eq!(world.blob_state(old_b).await.as_deref(), Some("orphaned"));
    assert_ne!(
        world.blob_state(untouched_c).await.as_deref(),
        Some("orphaned")
    );

    world.cleanup().await;
}

#[tokio::test]
async fn deleting_a_file_releases_its_image_blobs_and_drops_its_pages() {
    let world = World::start("pod-page-cascade").await;
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "sub", "doc.txt", SOURCE),
    )
    .await;
    let job_id = assign_job(&world, &owner, file_id).await;
    upload_image(&world, &runner, file_id, job_id, "p001-img01.png", IMAGE_A).await;
    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id,
            pages: vec![(1, "![a](p001-img01.png)")],
            origin: None,
            indexer: None,
            done: true,
        },
    )
    .await);
    let image = image_source(&world, file_id, "p001-img01.png").await;
    let source = world.source_of(file_id).await;

    ok(&world
        .gql(
            &owner,
            "mutation($d:UUID!,$p:String!){workspaceDeleteFolder(driveId:$d,prefix:$p){success}}",
            serde_json::json!({ "d": drive, "p": "sub" }),
        )
        .await);

    assert_eq!(world.blob_state(image).await.as_deref(), Some("orphaned"));
    assert_eq!(world.blob_state(source).await.as_deref(), Some("orphaned"));
    let pages: i64 = sqlx::query_scalar("SELECT count(*) FROM drive.file_page WHERE file_id = $1")
        .bind(file_id)
        .fetch_one(&world.db.app)
        .await
        .unwrap();
    let images: i64 =
        sqlx::query_scalar("SELECT count(*) FROM drive.file_image WHERE file_id = $1")
            .bind(file_id)
            .fetch_one(&world.db.app)
            .await
            .unwrap();
    assert_eq!((pages, images), (0, 0));

    world.cleanup().await;
}
