use std::time::Duration;

use uuid::Uuid;

use crate::harness::runner::{
    RUNNER_SCOPE, Report, context, image_ticket, install_render_rule, report, request_image,
    upload_image,
};
use crate::harness::upload::{UploadRequest, post_bytes, upload};
use crate::harness::{
    JobsStandIn, World, WorldOptions, drain_with_a_rename, drive_subscription, error_code,
    manager_passport, next_drive_delta, next_page_delta, ok, pages_subscription, passport,
    service_passport,
};
use crate::poll_until;

const SOURCE: &[u8] = b"the source document";
const IMAGE: &[u8] = b"\x89PNG fake image bytes";

async fn file_with_job(world: &World, jobs: &JobsStandIn, owner: &str) -> (Uuid, Uuid, Uuid) {
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    install_render_rule(world, jobs, &manager).await;
    let drive = world.create_workspace(owner, "library").await;
    let file_id = upload(
        world,
        owner,
        &UploadRequest::text(drive, "docs", "source.txt", SOURCE),
    )
    .await;
    let job_id = world.await_job(file_id).await;
    (drive, file_id, job_id)
}

#[tokio::test]
async fn the_runner_context_carries_a_fresh_presigned_get_on_the_source_and_the_rendition() {
    let world = World::start("pod-runner-context").await;
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    let jobs = JobsStandIn::attach(&world).await;
    let (_, file_id, job_id) = file_with_job(&world, &jobs, &owner).await;

    let context_json = poll_until!(Duration::from_secs(15), {
        let response = context(&world, &runner, file_id, job_id).await;
        (response.get("errors").is_none())
            .then(|| response["data"]["workspaceRunnerContext"].clone())
    });
    assert_eq!(context_json["fileId"], file_id.to_string());
    assert_eq!(context_json["mediaType"], "text/plain");
    assert_eq!(context_json["name"], "source.txt");
    assert!(context_json["pageCount"].is_null());
    assert_eq!(context_json["pages"].as_array().unwrap().len(), 0);
    let url = context_json["sourceUrl"].as_str().expect("a presigned GET");
    assert!(
        url.contains("response-content-disposition=inline"),
        "the runner reads the source inline: {url}"
    );
    let got = world
        .http
        .get(url)
        .send()
        .await
        .expect("GET the source")
        .bytes()
        .await
        .expect("read the source");
    assert_eq!(got.as_ref(), SOURCE);

    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id,
            pages: vec![(1, "# Page one"), (2, "Page two")],
            origin: None,
            indexer: None,
            done: false,
        },
    )
    .await);
    let again =
        ok(&context(&world, &runner, file_id, job_id).await)["workspaceRunnerContext"].clone();
    let pages = again["pages"].as_array().unwrap();
    assert_eq!(
        pages.len(),
        2,
        "the indexer reads the rendition, not the source"
    );
    assert_eq!(pages[0]["origin"], "RUNNER");
    let second = again["sourceUrl"]
        .as_str()
        .expect("a presigned GET on every call");
    let status = world
        .http
        .get(second)
        .send()
        .await
        .expect("GET the source again")
        .status();
    assert!(
        status.is_success(),
        "the second call's GET is live: {status}"
    );

    let missing = context(&world, &runner, Uuid::now_v7(), job_id).await;
    assert_eq!(error_code(&missing), "FILE_NOT_FOUND");

    world.cleanup().await;
}

#[tokio::test]
async fn the_runner_context_says_source_not_available_until_the_reaper_promoted_the_source() {
    let world = World::start_with(
        "pod-runner-source-pending",
        WorldOptions {
            reaper_interval: Duration::from_secs(600),
            ..WorldOptions::default()
        },
    )
    .await;
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    let jobs = JobsStandIn::attach(&world).await;
    let (_, file_id, job_id) = file_with_job(&world, &jobs, &owner).await;

    let pending = context(&world, &runner, file_id, job_id).await;
    assert_eq!(
        error_code(&pending),
        "SOURCE_NOT_AVAILABLE",
        "a verified source is not downloadable before the engine reaper promotes it"
    );
    assert_eq!(
        world.file(&owner, file_id).await["processingState"],
        "PROCESSING",
        "the commit itself went through and the chain started"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn the_runner_roots_refuse_a_human_and_a_service_without_the_runner_scope() {
    let world = World::start("pod-runner-scope").await;
    let owner = passport(Uuid::now_v7());
    let jobs = JobsStandIn::attach(&world).await;
    let (drive, file_id, job_id) = file_with_job(&world, &jobs, &owner).await;
    let unscoped = service_passport(&["workspace:other"]);

    for principal in [&owner, &unscoped] {
        assert_eq!(
            error_code(&context(&world, principal, file_id, job_id).await),
            "RUNNER_SCOPE_REQUIRED"
        );
        assert_eq!(
            error_code(
                &request_image(&world, principal, file_id, job_id, "p001-img01.png", IMAGE).await
            ),
            "RUNNER_SCOPE_REQUIRED"
        );
        assert_eq!(
            error_code(
                &report(
                    &world,
                    principal,
                    file_id,
                    Report {
                        job_id,
                        pages: vec![(1, "sneaky")],
                        origin: None,
                        indexer: None,
                        done: true,
                    },
                )
                .await
            ),
            "RUNNER_SCOPE_REQUIRED"
        );
    }
    assert!(
        world.file_pages(&owner, file_id).await.is_empty(),
        "nothing was stored"
    );

    let runner = service_passport(&[RUNNER_SCOPE]);
    assert!(
        world.drive_files(&runner, drive).await.is_empty(),
        "a runner sees nothing through the drive views"
    );
    assert!(world.file(&runner, file_id).await.is_null());
    assert!(world.file_pages(&runner, file_id).await.is_empty());

    world.cleanup().await;
}

#[tokio::test]
async fn a_runner_holding_the_scope_but_not_the_files_job_is_refused_every_root() {
    let world = World::start("pod-runner-wrong-job").await;
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    let jobs = JobsStandIn::attach(&world).await;
    let (_, file_id, _) = file_with_job(&world, &jobs, &owner).await;
    let stale = Uuid::now_v7();

    let refused = context(&world, &runner, file_id, stale).await;
    assert_eq!(
        error_code(&refused),
        "JOB_NOT_ACTIVE",
        "the context root is gated on the file's active job like the two write roots"
    );
    let refused = report(
        &world,
        &runner,
        file_id,
        Report {
            job_id: stale,
            pages: vec![(1, "late")],
            origin: None,
            indexer: None,
            done: true,
        },
    )
    .await;
    assert_eq!(error_code(&refused), "JOB_NOT_ACTIVE");
    let refused = request_image(&world, &runner, file_id, stale, "p001-img01.png", IMAGE).await;
    assert_eq!(error_code(&refused), "JOB_NOT_ACTIVE");
    assert!(world.file_pages(&owner, file_id).await.is_empty());
    assert_eq!(
        world.file(&owner, file_id).await["images"]
            .as_array()
            .unwrap()
            .len(),
        0
    );

    world.cleanup().await;
}

#[tokio::test]
async fn an_image_round_trips_through_a_verified_upload_and_a_wrong_checksum_is_refused_by_storage()
{
    let world = World::start("pod-runner-image").await;
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    let jobs = JobsStandIn::attach(&world).await;
    let (drive, file_id, job_id) = file_with_job(&world, &jobs, &owner).await;
    let mut sub = drive_subscription(&world, &owner, drive).await;

    let ticket = image_ticket(
        &request_image(&world, &runner, file_id, job_id, "p002-img01.png", IMAGE).await,
    );
    let status = post_bytes(&world, &ticket, IMAGE, "p002-img01.png").await;
    assert!((200..300).contains(&status), "the image lands: {status}");
    let requested = next_drive_delta(&mut sub, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "ImageRequested"
    })
    .await;
    assert_eq!(requested["view"]["images"][0]["name"], "p002-img01.png");
    assert_eq!(requested["view"]["images"][0]["page"], 2);

    let url = poll_until!(Duration::from_secs(15), {
        ok(&world.image_access(&owner, file_id, "p002-img01.png").await)["workspaceFileAccess"]
            .as_str()
            .map(str::to_string)
    });
    assert!(url.contains("response-content-disposition=inline"));
    let got = world
        .http
        .get(url)
        .send()
        .await
        .expect("GET the image")
        .bytes()
        .await
        .expect("read the image");
    assert_eq!(got.as_ref(), IMAGE);
    next_drive_delta(&mut sub, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "ImageAvailable"
    })
    .await;
    assert!(
        ok(&world.image_access(&owner, file_id, "p009-img09.png").await)["workspaceFileAccess"]
            .is_null(),
        "an unknown image name resolves to nothing"
    );
    let outsider = passport(Uuid::now_v7());
    assert!(
        ok(&world
            .image_access(&outsider, file_id, "p002-img01.png")
            .await)["workspaceFileAccess"]
            .is_null(),
        "a non-owner cannot mint a GET on the image"
    );
    assert!(
        ok(&world.image_access(&runner, file_id, "p002-img01.png").await)["workspaceFileAccess"]
            .is_null(),
        "the runner reads images through its own context, never through the drive view"
    );

    let wrong = image_ticket(
        &world
            .gql(
                &runner,
                "mutation($f:UUID!,$j:UUID!,$n:String!,$m:String!,$s:ByteCount!,$h:String!){\
                 workspaceRunnerRequestImageUpload(fileId:$f,jobId:$j,name:$n,mediaType:$m,size:$s,sha256:$h)\
                 {fileId url fields}}",
                serde_json::json!({
                    "f": file_id, "j": job_id, "n": "p002-img02.png", "m": "image/png",
                    "s": IMAGE.len(),
                    "h": "0000000000000000000000000000000000000000000000000000000000000000",
                }),
            )
            .await,
    );
    let status = post_bytes(&world, &wrong, IMAGE, "p002-img02.png").await;
    assert!(
        !(200..300).contains(&status),
        "object storage refuses bytes that are not the pinned ones: {status}"
    );
    assert!(
        !world
            .object_exists(wrong.fields["key"].as_str().unwrap())
            .await
    );

    for (name, code) in [
        ("img01.png", "INVALID_IMAGE_NAME"),
        ("p2-img01.png", "INVALID_IMAGE_NAME"),
        ("p002-img01.PNG", "INVALID_IMAGE_NAME"),
        ("p0002-img01.png", "INVALID_IMAGE_NAME"),
        ("p1000-img000.png", "INVALID_IMAGE_NAME"),
    ] {
        let refused = request_image(&world, &runner, file_id, job_id, name, IMAGE).await;
        assert_eq!(error_code(&refused), code, "{name}");
    }
    for name in ["p1000-img01.png", "p002-img100.png"] {
        let ticket =
            image_ticket(&request_image(&world, &runner, file_id, job_id, name, IMAGE).await);
        assert_eq!(
            ticket.file_id, file_id,
            "a page past 999 and an image past 99 have their name: {name}"
        );
    }
    let oversized = world
        .gql(
            &runner,
            "mutation($f:UUID!,$j:UUID!,$n:String!,$m:String!,$s:ByteCount!,$h:String!){\
             workspaceRunnerRequestImageUpload(fileId:$f,jobId:$j,name:$n,mediaType:$m,size:$s,sha256:$h)\
             {fileId url fields}}",
            serde_json::json!({
                "f": file_id, "j": job_id, "n": "p002-img03.png", "m": "image/png",
                "s": 2u64 << 20,
                "h": crate::harness::upload::sha256_hex(IMAGE),
            }),
        )
        .await;
    assert_eq!(
        error_code(&oversized),
        "FILE_TOO_LARGE",
        "the host's IMAGE_MAX_BYTES is enforced at request time"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn replacing_an_image_keeps_the_old_object_readable_until_the_new_one_lands() {
    let world = World::start("pod-runner-image-replace").await;
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    let jobs = JobsStandIn::attach(&world).await;
    let (_, file_id, job_id) = file_with_job(&world, &jobs, &owner).await;
    let name = "p001-img01.png";

    upload_image(&world, &runner, file_id, job_id, name, IMAGE).await;
    let (first, _, _) = world.image_row(file_id, name).await.unwrap();
    poll_until!(Duration::from_secs(15), {
        world
            .image_row(file_id, name)
            .await
            .filter(|(_, _, landed)| *landed)
    });

    let replacement = image_ticket(
        &request_image(&world, &runner, file_id, job_id, name, b"a different image").await,
    );
    let (current, pending, _) = world.image_row(file_id, name).await.unwrap();
    assert_eq!(current, first, "the landed blob stays current");
    let second = pending.expect("the replacement is pending on the row");
    assert_ne!(
        world.blob_state(first).await.as_deref(),
        Some("orphaned"),
        "requesting a replacement releases nothing"
    );
    let retried = request_image(&world, &runner, file_id, job_id, name, b"a different image").await;
    assert_eq!(
        error_code(&retried),
        "IMAGE_UPLOAD_PENDING",
        "a re-request while the replacement is in flight is refused; the first ticket stands"
    );
    assert_eq!(
        world.image_row(file_id, name).await.unwrap().1,
        Some(second),
        "one pending blob, not two"
    );

    let status = post_bytes(&world, &replacement, b"not the pinned bytes", name).await;
    assert!(
        !(200..300).contains(&status),
        "the replacement upload fails: {status}"
    );
    let url = ok(&world.image_access(&owner, file_id, name).await)["workspaceFileAccess"]
        .as_str()
        .expect("the old image is still readable")
        .to_string();
    let got = world
        .http
        .get(url)
        .send()
        .await
        .expect("GET the old image")
        .bytes()
        .await
        .expect("read the old image");
    assert_eq!(got.as_ref(), IMAGE);

    let status = post_bytes(&world, &replacement, b"a different image", name).await;
    assert!(
        (200..300).contains(&status),
        "the replacement lands: {status}"
    );
    poll_until!(Duration::from_secs(15), {
        (world.image_row(file_id, name).await.unwrap().0 == second).then_some(())
    });
    assert_eq!(
        world.blob_state(first).await.as_deref(),
        Some("orphaned"),
        "the old blob is released when the replacement lands"
    );
    assert!(world.image_row(file_id, name).await.unwrap().1.is_none());
    let url = poll_until!(Duration::from_secs(15), {
        let url = ok(&world.image_access(&owner, file_id, name).await)["workspaceFileAccess"]
            .as_str()
            .map(str::to_string);
        match url {
            Some(url) => {
                let got = world
                    .http
                    .get(&url)
                    .send()
                    .await
                    .expect("GET the replacement")
                    .bytes()
                    .await
                    .expect("read the replacement");
                (got.as_ref() == b"a different image").then_some(url)
            }
            None => None,
        }
    });
    assert!(url.contains("response-content-disposition=inline"));

    world.cleanup().await;
}

#[tokio::test]
async fn report_batches_upsert_by_number_and_a_replayed_batch_changes_nothing() {
    let world = World::start("pod-runner-report").await;
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    let jobs = JobsStandIn::attach(&world).await;
    let (drive, file_id, job_id) = file_with_job(&world, &jobs, &owner).await;
    world.await_source_promoted(file_id).await;
    let mut files = drive_subscription(&world, &owner, drive).await;
    let mut pages = pages_subscription(&world, &owner, file_id).await;
    drain_with_a_rename(&world, &owner, &mut files, file_id, "source-settled.txt").await;

    let first = Report {
        job_id,
        pages: vec![(1, "one"), (2, "two")],
        origin: None,
        indexer: None,
        done: false,
    };
    ok(&report(&world, &runner, file_id, first).await);
    let entered = next_page_delta(&mut pages, |node| {
        node["__typename"] == "DriveUpsert" && node["view"]["number"] == 2
    })
    .await;
    assert_eq!(entered["view"]["markdown"], "two");
    assert_eq!(
        entered["view"]["affordances"]["editPage"]["reason"], "FILE_PROCESSING",
        "no page edit while the chain runs"
    );
    assert!(
        entered["cause"].is_null(),
        "a page entering the window is delivered by repopulation, without a cause"
    );

    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id,
            pages: vec![(2, "two, revised"), (3, "three")],
            origin: None,
            indexer: None,
            done: false,
        },
    )
    .await);
    let revised = next_page_delta(&mut pages, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "Reported"
    })
    .await;
    assert_eq!(revised["view"]["number"], 2);
    assert_eq!(revised["view"]["markdown"], "two, revised");
    assert_eq!(revised["cause"]["job_id"], job_id.to_string());
    assert_eq!(revised["cause"]["origin"], "RUNNER");
    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id,
            pages: vec![(2, "two, revised"), (3, "three")],
            origin: None,
            indexer: None,
            done: false,
        },
    )
    .await);
    files.expect_silence(Duration::from_secs(1)).await;
    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id,
            pages: vec![],
            origin: None,
            indexer: Some(("A three-page document.", 3, 420)),
            done: true,
        },
    )
    .await);
    let done = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "ReportStored"
    })
    .await;
    assert_eq!(done["cause"]["done"], true);
    assert_eq!(done["view"]["summary"], "A three-page document.");
    assert_eq!(done["view"]["pageCount"], 3);
    assert_eq!(done["view"]["estimatedTokens"], 420);

    let stored = world.file_pages(&owner, file_id).await;
    assert_eq!(
        stored.len(),
        3,
        "a replayed batch upserts, never duplicates"
    );
    assert_eq!(stored[1]["markdown"], "two, revised");
    assert_eq!(stored[2]["number"], 3);
    assert!(stored.iter().all(|page| page["origin"] == "RUNNER"));

    let empty = report(
        &world,
        &runner,
        file_id,
        Report {
            job_id,
            pages: vec![],
            origin: None,
            indexer: None,
            done: false,
        },
    )
    .await;
    assert_eq!(error_code(&empty), "NOTHING_TO_CHANGE");
    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id,
            pages: vec![],
            origin: None,
            indexer: None,
            done: true,
        },
    )
    .await);
    let partial = world
        .gql(
            &runner,
            "mutation($f:UUID!,$j:UUID!){workspaceRunnerReport(fileId:$f,jobId:$j,summary:\"alone\"){success}}",
            serde_json::json!({ "f": file_id, "j": job_id }),
        )
        .await;
    assert_eq!(error_code(&partial), "INDEXER_FIELDS_TOGETHER");
    let estimate_alone = world
        .gql(
            &runner,
            "mutation($f:UUID!,$j:UUID!){workspaceRunnerReport(fileId:$f,jobId:$j,estimatedTokens:12){success}}",
            serde_json::json!({ "f": file_id, "j": job_id }),
        )
        .await;
    assert_eq!(
        error_code(&estimate_alone),
        "INDEXER_FIELDS_TOGETHER",
        "a token estimate only rides along with a summary and a page count"
    );
    ok(&world
        .gql(
            &runner,
            "mutation($f:UUID!,$j:UUID!){workspaceRunnerReport(fileId:$f,jobId:$j,summary:\"Re-indexed.\",pageCount:3){success}}",
            serde_json::json!({ "f": file_id, "j": job_id }),
        )
        .await);
    let reindexed = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "ReportStored"
    })
    .await;
    assert_eq!(reindexed["view"]["summary"], "Re-indexed.");
    assert_eq!(reindexed["view"]["pageCount"], 3);
    assert!(
        reindexed["view"]["estimatedTokens"].is_null(),
        "an indexing without an estimate is accepted and clears the previous one"
    );
    // The same summary and page count with an estimate, then without it: the
    // estimate alone decides whether the indexing changed.
    for (estimate, expected) in [
        (serde_json::json!(99), serde_json::json!(99)),
        (serde_json::Value::Null, serde_json::Value::Null),
    ] {
        ok(&world
            .gql(
                &runner,
                "mutation($f:UUID!,$j:UUID!,$t:Int){workspaceRunnerReport(fileId:$f,jobId:$j,summary:\"Re-indexed.\",pageCount:3,estimatedTokens:$t){success}}",
                serde_json::json!({ "f": file_id, "j": job_id, "t": estimate }),
            )
            .await);
        let stored = next_drive_delta(&mut files, |node| {
            node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "ReportStored"
        })
        .await;
        assert_eq!(stored["view"]["estimatedTokens"], expected);
    }
    let negative = report(
        &world,
        &runner,
        file_id,
        Report {
            job_id,
            pages: vec![],
            origin: None,
            indexer: Some(("negative", -1, 10)),
            done: false,
        },
    )
    .await;
    assert_eq!(error_code(&negative), "INVALID_INDEXER_VALUE");
    let zero = report(
        &world,
        &runner,
        file_id,
        Report {
            job_id,
            pages: vec![(0, "no page zero")],
            origin: None,
            indexer: None,
            done: false,
        },
    )
    .await;
    assert_eq!(error_code(&zero), "INVALID_PAGE");
    let twice = report(
        &world,
        &runner,
        file_id,
        Report {
            job_id,
            pages: vec![(4, "four"), (4, "four again")],
            origin: None,
            indexer: None,
            done: false,
        },
    )
    .await;
    assert_eq!(error_code(&twice), "INVALID_PAGE");
    let edited = report(
        &world,
        &runner,
        file_id,
        Report {
            job_id,
            pages: vec![(1, "runners do not edit")],
            origin: Some("EDITED"),
            indexer: None,
            done: false,
        },
    )
    .await;
    assert_eq!(error_code(&edited), "INVALID_PAGE_ORIGIN");

    world.cleanup().await;
}

#[tokio::test]
async fn two_concurrent_report_batches_on_one_file_both_land() {
    let world = World::start("pod-runner-concurrent-report").await;
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    let jobs = JobsStandIn::attach(&world).await;
    let (_, file_id, job_id) = file_with_job(&world, &jobs, &owner).await;

    let batch = |pages: Vec<(i32, &'static str)>| {
        report(
            &world,
            &runner,
            file_id,
            Report {
                job_id,
                pages,
                origin: None,
                indexer: None,
                done: false,
            },
        )
    };
    let (left, right) = tokio::join!(
        batch(vec![(1, "one"), (2, "two"), (3, "three")]),
        batch(vec![(3, "three"), (4, "four"), (5, "five")]),
    );
    ok(&left);
    ok(&right);

    let numbers: Vec<i64> = world
        .file_pages(&owner, file_id)
        .await
        .iter()
        .map(|page| page["number"].as_i64().unwrap())
        .collect();
    assert_eq!(numbers, vec![1, 2, 3, 4, 5]);

    world.cleanup().await;
}
