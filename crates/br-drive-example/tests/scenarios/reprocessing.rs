//! A reprocess rewrites the results in place: nothing is wiped when it starts,
//! the old results stay readable while the new run reports over them, a page
//! a person edited is kept, and the end of the chain trims what the new run
//! no longer has. A page regeneration is asked for its page: it overwrites it,
//! edited or not.

use std::time::Duration;

use uuid::Uuid;

use crate::harness::runner::{
    RUNNER_SCOPE, Report, install_regenerate_rule, install_render_rule, report, upload_image,
};
use crate::harness::upload::{UploadRequest, process, upload_processed};
use crate::harness::{
    JobsStandIn, World, drive_subscription, error_code, manager_passport, next_drive_delta,
    next_page_delta, ok, pages_subscription, passport, service_passport,
};

const SOURCE: &[u8] = b"a document processed more than once";
const EDIT_PAGE: &str = "mutation($f:UUID!,$n:Int!,$m:String!){\
     workspaceEditPage(fileId:$f,number:$n,markdown:$m){success}}";

async fn edit(world: &World, owner: &str, file_id: Uuid, number: i32, markdown: &str) {
    ok(&world
        .gql(
            owner,
            EDIT_PAGE,
            serde_json::json!({ "f": file_id, "n": number, "m": markdown }),
        )
        .await);
}

fn markdown_of(pages: &[serde_json::Value]) -> Vec<(i64, String, String)> {
    pages
        .iter()
        .map(|page| {
            (
                page["number"].as_i64().unwrap_or_default(),
                page["markdown"].as_str().unwrap_or_default().to_string(),
                page["origin"].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect()
}

/// A file processed once: four pages, two images (on pages 1 and 4), an
/// indexing — READY.
async fn processed_file(
    world: &World,
    jobs: &JobsStandIn,
    owner: &str,
    runner: &str,
    drive: Uuid,
) -> Uuid {
    let file_id = upload_processed(
        world,
        owner,
        &UploadRequest::text(drive, "", "doc.txt", SOURCE),
    )
    .await;
    world.await_source_promoted(file_id).await;
    let job = jobs.await_create(file_id).await.job_id;
    upload_image(
        world,
        runner,
        file_id,
        job,
        "p001-img01.png",
        b"first image",
    )
    .await;
    upload_image(
        world,
        runner,
        file_id,
        job,
        "p004-img01.png",
        b"fourth image",
    )
    .await;
    ok(&report(
        world,
        runner,
        file_id,
        Report {
            job_id: job,
            pages: vec![
                (1, "v1 one ![a](p001-img01.png)"),
                (2, "v1 two"),
                (3, "v1 three"),
                (4, "v1 four ![d](p004-img01.png)"),
            ],
            origin: None,
            indexer: Some(("Version one.", 4, 40)),
            done: true,
        },
    )
    .await);
    jobs.await_finish(job).await;
    world.await_state(owner, file_id, "READY").await;
    file_id
}

#[tokio::test]
async fn a_reprocess_rewrites_in_place_keeps_edited_pages_and_trims_at_the_end() {
    // Given: a file processed once (four pages, two images), whose pages 2 and
    // 4 a person corrected
    let world = World::start("pod-reprocess-in-place").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    install_render_rule(&world, &manager).await;
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = processed_file(&world, &jobs, &owner, &runner, drive).await;
    edit(&world, &owner, file_id, 2, "two, as a person corrected it").await;
    edit(&world, &owner, file_id, 4, "four, as a person corrected it").await;
    let fourth_image = world.image_row(file_id, "p004-img01.png").await.unwrap().0;
    let first_image = world.image_row(file_id, "p001-img01.png").await.unwrap().0;

    // When: the owner reprocesses it
    ok(&process(&world, &owner, file_id).await);
    let job = jobs.await_create(file_id).await.job_id;

    // Then: nothing is wiped — every page, the images and the indexing stay
    // readable while the new run works
    let running = world.await_state(&owner, file_id, "PROCESSING").await;
    assert_eq!(running["summary"], "Version one.");
    assert_eq!(running["pageCount"], 4);
    assert_eq!(running["estimatedTokens"], 40);
    assert_eq!(running["images"].as_array().map(Vec::len), Some(2));
    assert_eq!(world.file_pages(&owner, file_id).await.len(), 4);
    let mut pages = pages_subscription(&world, &owner, file_id).await;
    let mut files = drive_subscription(&world, &owner, drive).await;

    // When: the new run reports two pages — one of them the corrected page 2 —
    // and a smaller indexing
    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id: job,
            pages: vec![(1, "v2 one ![a](p001-img01.png)"), (2, "v2 two")],
            origin: None,
            indexer: Some(("Version two.", 2, 20)),
            done: false,
        },
    )
    .await);

    // Then: page 1 is replaced, the corrected page 2 is kept, pages 3 and 4
    // are still there — the chain has not ended
    let replaced = next_page_delta(&mut pages, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "Reported"
    })
    .await;
    assert_eq!(replaced["view"]["number"], 1);
    assert_eq!(replaced["view"]["markdown"], "v2 one ![a](p001-img01.png)");
    let reported = world.file(&owner, file_id).await;
    assert_eq!(reported["summary"], "Version two.");
    assert_eq!(reported["pageCount"], 2);
    assert_eq!(
        markdown_of(&world.file_pages(&owner, file_id).await),
        vec![
            (1, "v2 one ![a](p001-img01.png)".into(), "RUNNER".into()),
            (2, "two, as a person corrected it".into(), "EDITED".into()),
            (3, "v1 three".into(), "RUNNER".into()),
            (4, "four, as a person corrected it".into(), "EDITED".into()),
        ],
        "a runner never overwrites an edited page on a reprocess"
    );
    assert_eq!(reported["images"].as_array().map(Vec::len), Some(2));

    // When: the run sends its final report
    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id: job,
            pages: vec![],
            origin: None,
            indexer: None,
            done: true,
        },
    )
    .await);

    // Then: the pages above the new count go — the corrected page 4 too — and
    // so does the image no page references any more
    let mut trimmed = Vec::new();
    while trimmed.len() < 2 {
        let removed = next_page_delta(&mut pages, |node| node["__typename"] == "DriveRemove").await;
        assert!(
            removed["cause"].is_null(),
            "a page leaving the window is delivered by repopulation, without a cause"
        );
        trimmed.push(removed["key"]["number"].as_i64().unwrap_or_default());
    }
    trimmed.sort_unstable();
    assert_eq!(trimmed, vec![3, 4]);
    let dropped = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "ImagesDropped"
    })
    .await;
    assert_eq!(
        dropped["cause"]["names"],
        serde_json::json!(["p004-img01.png"])
    );
    let ready = world.await_state(&owner, file_id, "READY").await;
    assert_eq!(
        ready["images"],
        serde_json::json!([{ "name": "p001-img01.png", "mediaType": "image/png", "sizeBytes": 11, "page": 1 }])
    );
    assert_eq!(
        markdown_of(&world.file_pages(&owner, file_id).await),
        vec![
            (1, "v2 one ![a](p001-img01.png)".into(), "RUNNER".into()),
            (2, "two, as a person corrected it".into(), "EDITED".into()),
        ]
    );
    assert_eq!(
        world.blob_state(fourth_image).await.as_deref(),
        Some("orphaned"),
        "the unreferenced image's object is released"
    );
    assert_ne!(
        world.blob_state(first_image).await.as_deref(),
        Some("orphaned")
    );
    jobs.await_finish(job).await;
    jobs.expect_no_command(Duration::from_millis(500)).await;

    world.cleanup().await;
}

#[tokio::test]
async fn a_page_regeneration_overwrites_the_page_even_when_a_person_edited_it() {
    // Given: a processed file whose page 2 a person corrected, and a
    // regeneration rule
    let world = World::start("pod-regenerate-edited").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    install_render_rule(&world, &manager).await;
    install_regenerate_rule(&world, &manager).await;
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = processed_file(&world, &jobs, &owner, &runner, drive).await;
    edit(&world, &owner, file_id, 2, "two, as a person corrected it").await;

    // When: the owner asks for page 2 again, and its runner reports it
    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!,$n:Int!){workspaceRegeneratePage(fileId:$f,number:$n){success}}",
            serde_json::json!({ "f": file_id, "n": 2 }),
        )
        .await);
    let create = jobs.await_create(file_id).await;
    assert_eq!(create.config.as_ref().unwrap()["options"]["page"], 2);
    let mut pages = pages_subscription(&world, &owner, file_id).await;
    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id: create.job_id,
            pages: vec![(2, "two, regenerated")],
            origin: Some("REGENERATED"),
            indexer: None,
            done: true,
        },
    )
    .await);

    // Then: the regenerated page replaces the corrected one; the others stay
    let regenerated = next_page_delta(&mut pages, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "Reported"
    })
    .await;
    assert_eq!(regenerated["view"]["number"], 2);
    assert_eq!(regenerated["view"]["markdown"], "two, regenerated");
    assert_eq!(regenerated["view"]["origin"], "REGENERATED");
    world.await_state(&owner, file_id, "READY").await;
    let stored = markdown_of(&world.file_pages(&owner, file_id).await);
    assert_eq!(
        stored.len(),
        4,
        "the page count did not change: nothing is trimmed"
    );
    assert_eq!(
        stored[1],
        (2, "two, regenerated".into(), "REGENERATED".into())
    );
    assert_eq!(stored[3].1, "v1 four ![d](p004-img01.png)");

    world.cleanup().await;
}

#[tokio::test]
async fn a_failed_reprocess_leaves_the_previous_results_readable() {
    // Given: a file processed once
    let world = World::start("pod-reprocess-failed").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    install_render_rule(&world, &manager).await;
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = processed_file(&world, &jobs, &owner, &runner, drive).await;
    let before = world.file(&owner, file_id).await;
    let pages_before = markdown_of(&world.file_pages(&owner, file_id).await);
    let mut files = drive_subscription(&world, &owner, drive).await;

    // When: a reprocess starts, and its runner dies before reporting anything
    ok(&process(&world, &owner, file_id).await);
    let job = jobs.await_create(file_id).await.job_id;
    jobs.fail(job, "RUNNER_LOST", None).await;

    // Then: the file is FAILED with Jobs' cause, and everything the first run
    // produced is still there to read
    let failed = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "ProcessingFailed"
    })
    .await;
    assert_eq!(failed["view"]["processingState"], "FAILED");
    assert_eq!(failed["view"]["processingError"], "RUNNER_LOST");
    assert_eq!(failed["view"]["summary"], before["summary"]);
    assert_eq!(failed["view"]["pageCount"], before["pageCount"]);
    assert_eq!(failed["view"]["estimatedTokens"], before["estimatedTokens"]);
    assert_eq!(failed["view"]["images"].as_array().map(Vec::len), Some(2));
    assert_eq!(
        markdown_of(&world.file_pages(&owner, file_id).await),
        pages_before
    );
    // And: the page gestures wait for a READY file, the reprocess is offered
    let pages = world.file_pages(&owner, file_id).await;
    assert_eq!(
        pages[0]["affordances"]["editPage"]["reason"],
        "FILE_NOT_READY"
    );
    assert_eq!(failed["view"]["affordances"]["process"]["allowed"], true);
    assert_eq!(
        error_code(
            &world
                .gql(
                    &owner,
                    EDIT_PAGE,
                    serde_json::json!({ "f": file_id, "n": 1, "m": "not now" }),
                )
                .await
        ),
        "FILE_NOT_READY"
    );

    world.cleanup().await;
}
