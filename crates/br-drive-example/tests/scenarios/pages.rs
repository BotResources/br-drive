use std::time::Duration;

use uuid::Uuid;

use crate::harness::runner::{
    RUNNER_SCOPE, Report, finish_job, install_regenerate_rule, install_render_rule, report,
    upload_image,
};
use crate::harness::upload::{UploadRequest, upload};
use crate::harness::{
    JobsStandIn, PAGE_DELTAS, Subscription, World, drain_with_a_rename, drive_subscription,
    error_code, manager_passport, next_drive_delta, next_page_delta, ok, pages_reset,
    pages_subscription, passport, service_passport,
};
use crate::poll_until;

const SOURCE: &[u8] = b"pages source";
const IMAGE_A: &[u8] = b"image a";
const IMAGE_B: &[u8] = b"image b";
const IMAGE_C: &[u8] = b"image c";

async fn image_source(world: &World, file_id: Uuid, name: &str) -> Uuid {
    world
        .image_row(file_id, name)
        .await
        .expect("the image row")
        .0
}

async fn rules(world: &World) -> (JobsStandIn, String) {
    let jobs = JobsStandIn::attach(world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    install_render_rule(world, &jobs, &manager).await;
    (jobs, manager)
}

async fn rendered_file(
    world: &World,
    jobs: &JobsStandIn,
    owner: &str,
    runner: &str,
    drive: Uuid,
    name: &str,
) -> Uuid {
    let file_id = upload(world, owner, &UploadRequest::text(drive, "", name, SOURCE)).await;
    world.await_source_promoted(file_id).await;
    let job_id = world.await_job(file_id).await;
    ok(&report(
        world,
        runner,
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
    finish_job(world, jobs, job_id).await;
    world.await_state(owner, file_id, "READY").await;
    file_id
}

#[tokio::test]
async fn a_user_edits_a_page_of_a_ready_file_and_only_that_page_travels() {
    let world = World::start("pod-page-edit").await;
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    let (jobs, _) = rules(&world).await;
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = rendered_file(&world, &jobs, &owner, &runner, drive, "doc.txt").await;
    let mut files = drive_subscription(&world, &owner, drive).await;
    let mut pages = pages_subscription(&world, &owner, file_id).await;
    drain_with_a_rename(&world, &owner, &mut files, file_id, "doc-settled.txt").await;
    let before = world.file(&owner, file_id).await["updatedAt"].clone();

    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!,$n:Int!,$m:String!){workspaceEditPage(fileId:$f,number:$n,markdown:$m){success}}",
            serde_json::json!({ "f": file_id, "n": 1, "m": "as corrected by a human" }),
        )
        .await);
    let edited = next_page_delta(&mut pages, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "Edited"
    })
    .await;
    assert_eq!(edited["view"]["fileId"], file_id.to_string());
    assert_eq!(edited["view"]["number"], 1);
    assert_eq!(edited["view"]["markdown"], "as corrected by a human");
    assert_eq!(edited["view"]["origin"], "EDITED");
    files.expect_silence(Duration::from_secs(1)).await;
    assert_eq!(
        world.file(&owner, file_id).await["updatedAt"],
        before,
        "a page edit never rewrites the file row"
    );
    let stored = world.file_pages(&owner, file_id).await;
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0]["origin"], "EDITED");
    assert_eq!(stored[0]["affordances"]["editPage"]["allowed"], true);

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
    assert!(
        pages_reset(&world, &outsider, file_id).await.is_empty(),
        "an outsider's FilePages window is empty"
    );

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

    world.cleanup().await;
}

#[tokio::test]
async fn a_long_report_reaches_the_page_window_page_by_page_and_never_rewrites_the_file_row() {
    let world = World::start("pod-page-long-report").await;
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    let _rules = rules(&world).await;
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "long.txt", SOURCE),
    )
    .await;
    world.await_source_promoted(file_id).await;
    let job_id = world.await_job(file_id).await;
    let mut files = drive_subscription(&world, &owner, drive).await;
    let mut pages = pages_subscription(&world, &owner, file_id).await;
    drain_with_a_rename(&world, &owner, &mut files, file_id, "long-settled.txt").await;
    let before = world.file(&owner, file_id).await["updatedAt"].clone();

    const PAGES: i32 = 300;
    let markdowns: Vec<String> = (1..=PAGES).map(|n| format!("page {n}")).collect();
    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id,
            pages: markdowns
                .iter()
                .enumerate()
                .map(|(index, markdown)| (index as i32 + 1, markdown.as_str()))
                .collect(),
            origin: None,
            indexer: None,
            done: false,
        },
    )
    .await);
    let mut seen = std::collections::BTreeSet::new();
    while seen.len() < PAGES as usize {
        let delta = next_page_delta(&mut pages, |node| {
            node["__typename"] == "DriveUpsert" || node["__typename"] == "DriveReset"
        })
        .await;
        if delta["__typename"] == "DriveReset" {
            for view in delta["views"].as_array().unwrap() {
                seen.insert(view["number"].as_i64().unwrap());
            }
        } else {
            seen.insert(delta["view"]["number"].as_i64().unwrap());
        }
    }
    assert_eq!(
        seen.len(),
        PAGES as usize,
        "every page reaches the page window, as upserts or as one reset past the engine threshold"
    );
    files.expect_silence(Duration::from_secs(1)).await;
    assert_eq!(
        world.file(&owner, file_id).await["updatedAt"],
        before,
        "the file row is untouched by a page batch"
    );
    assert_eq!(
        world.file_pages(&owner, file_id).await.len(),
        PAGES as usize
    );

    let too_many: Vec<(i32, &str)> = (1..=(br_drive::MAX_REPORT_PAGES as i32 + 1))
        .map(|n| (n, "x"))
        .collect();
    let refused = report(
        &world,
        &runner,
        file_id,
        Report {
            job_id,
            pages: too_many,
            origin: None,
            indexer: None,
            done: false,
        },
    )
    .await;
    assert_eq!(error_code(&refused), "BATCH_TOO_LARGE");

    world.cleanup().await;
}

#[tokio::test]
async fn a_page_window_follows_one_file_and_closes_when_the_file_leaves_the_owners_sight() {
    let world = World::start("pod-page-window").await;
    let owner_id = Uuid::now_v7();
    let owner = passport(owner_id);
    let runner = service_passport(&[RUNNER_SCOPE]);
    let (jobs, manager) = rules(&world).await;
    install_regenerate_rule(&world, &manager).await;
    let drive = world.create_workspace(&owner, "library").await;
    let file_a = rendered_file(&world, &jobs, &owner, &runner, drive, "a.txt").await;
    let file_b = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "b.txt", SOURCE),
    )
    .await;
    let job_b = world.await_job(file_b).await;

    let mut window_a = Subscription::open_with(
        &world.subscription_url(),
        &owner,
        PAGE_DELTAS,
        serde_json::json!({ "f": file_a }),
    )
    .await;
    let reset = window_a.next_payload(Duration::from_secs(10)).await;
    let views = reset["workspaceFilePages"]["views"].as_array().unwrap();
    assert_eq!(views.len(), 1, "the Reset carries every page of file A");
    assert_eq!(views[0]["fileId"], file_a.to_string());
    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!,$n:Int!,$m:String!){workspaceEditPage(fileId:$f,number:$n,markdown:$m){success}}",
            serde_json::json!({ "f": file_a, "n": 1, "m": "settled" }),
        )
        .await);
    next_page_delta(&mut window_a, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "Edited"
    })
    .await;

    ok(&report(
        &world,
        &runner,
        file_b,
        Report {
            job_id: job_b,
            pages: vec![(1, "b one"), (2, "b two")],
            origin: None,
            indexer: None,
            done: true,
        },
    )
    .await);
    window_a.expect_silence(Duration::from_secs(1)).await;
    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!,$n:Int!){workspaceRegeneratePage(fileId:$f,number:$n){success}}",
            serde_json::json!({ "f": file_a, "n": 1 }),
        )
        .await);
    let job_a = world.await_job(file_a).await;
    ok(&report(
        &world,
        &runner,
        file_a,
        Report {
            job_id: job_a,
            pages: vec![(2, "a two")],
            origin: None,
            indexer: None,
            done: true,
        },
    )
    .await);
    let arrived = next_page_delta(&mut window_a, |node| node["__typename"] == "DriveUpsert").await;
    assert_eq!(arrived["view"]["fileId"], file_a.to_string());
    assert_eq!(arrived["view"]["number"], 2);

    let new_owner_id = Uuid::now_v7();
    ok(&world
        .gql(
            &owner,
            "mutation($id:UUID!,$to:UUID!){workspaceTransfer(id:$id,to:$to){success}}",
            serde_json::json!({ "id": drive, "to": new_owner_id }),
        )
        .await);
    let mut removed = std::collections::BTreeSet::new();
    while removed.len() < 2 {
        let delta =
            next_page_delta(&mut window_a, |node| node["__typename"] == "DriveRemove").await;
        assert_eq!(delta["projector"], "drive_pages");
        assert_eq!(delta["key"]["fileId"], file_a.to_string());
        removed.insert(delta["key"]["number"].as_i64().unwrap());
    }
    assert_eq!(removed, std::collections::BTreeSet::from([1, 2]));
    assert!(world.file_pages(&owner, file_a).await.is_empty());
    assert_eq!(
        world
            .file_pages(&passport(new_owner_id), file_a)
            .await
            .len(),
        2
    );

    world.cleanup().await;
}

#[tokio::test]
async fn regenerating_a_page_replaces_that_pages_images_by_name_and_releases_the_rest() {
    let world = World::start("pod-page-regenerate").await;
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    let (jobs, manager) = rules(&world).await;
    install_regenerate_rule(&world, &manager).await;
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "doc.txt", SOURCE),
    )
    .await;
    let job_id = world.await_job(file_id).await;
    assert_eq!(jobs.await_create(file_id).await.job_id, job_id);

    upload_image(&world, &runner, file_id, job_id, "p001-img01.png", IMAGE_A).await;
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
    finish_job(&world, &jobs, job_id).await;
    world.await_state(&owner, file_id, "READY").await;
    let mut files = drive_subscription(&world, &owner, drive).await;
    let mut pages = pages_subscription(&world, &owner, file_id).await;

    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!,$n:Int!,$c:String){workspaceRegeneratePage(fileId:$f,number:$n,comment:$c){success}}",
            serde_json::json!({ "f": file_id, "n": 1, "c": "the figure is cut" }),
        )
        .await);
    let regeneration = world.await_job(file_id).await;
    let started = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "ProcessingStarted"
    })
    .await;
    assert_eq!(started["view"]["progress"]["stepIndex"], 0);
    let create = jobs.await_create(file_id).await;
    assert_eq!(create.job_id, regeneration);
    assert_eq!(
        create.config.as_ref().unwrap()["options"],
        serde_json::json!({ "page": 1, "comment": "the figure is cut" }),
        "the page and the comment travel in the first step's options"
    );
    upload_image(
        &world,
        &runner,
        file_id,
        regeneration,
        "p001-img01.png",
        b"image a, regenerated",
    )
    .await;
    poll_until!(Duration::from_secs(15), {
        (world.blob_state(old_a).await.as_deref() == Some("orphaned")).then_some(())
    });
    let new_a = image_source(&world, file_id, "p001-img01.png").await;
    assert_ne!(new_a, old_a, "the replacement is current once it landed");
    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id: regeneration,
            pages: vec![(
                1,
                "![a](p001-img01.png) only, and p001-img02.png.bak is not a reference",
            )],
            origin: Some("REGENERATED"),
            indexer: None,
            done: true,
        },
    )
    .await);
    let regenerated = next_page_delta(&mut pages, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["origin"] == "REGENERATED"
    })
    .await;
    assert_eq!(regenerated["view"]["number"], 1);
    let dropped = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "ImagesDropped"
    })
    .await;
    assert_eq!(
        dropped["cause"]["names"],
        serde_json::json!(["p001-img02.png"])
    );

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
    let stored = world.file_pages(&owner, file_id).await;
    assert_eq!(stored[0]["origin"], "REGENERATED");
    assert_eq!(stored[1]["origin"], "RUNNER");
    assert_eq!(world.blob_state(old_b).await.as_deref(), Some("orphaned"));
    assert_ne!(
        world.blob_state(untouched_c).await.as_deref(),
        Some("orphaned")
    );

    world.cleanup().await;
}

#[tokio::test]
async fn deleting_a_folder_releases_its_image_blobs_and_drops_its_pages() {
    let world = World::start("pod-page-cascade").await;
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    let (jobs, _) = rules(&world).await;
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "sub", "doc.txt", SOURCE),
    )
    .await;
    let job_id = world.await_job(file_id).await;
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
    jobs.await_cancel(job_id).await;
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
