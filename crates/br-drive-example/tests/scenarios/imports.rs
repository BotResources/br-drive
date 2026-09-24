//! The host-privileged import of an existing rendition, and the host's own
//! objects refreshing as the files of their drive change.

use std::time::Duration;

use uuid::Uuid;

use crate::harness::runner::{RUNNER_SCOPE, install_render_rule};
use crate::harness::upload::{
    UploadRequest, commit, post_bytes, request, sha256_hex, ticket, upload,
};
use crate::harness::{
    JobsStandIn, Subscription, World, drive_subscription, error_code, manager_passport, next_delta,
    next_drive_delta, next_page_delta, ok, pages_subscription, passport, service_passport,
};

const BYTES: &[u8] = b"a document converted long ago";
const IMAGE: &[u8] = b"\x89PNG an imported figure";
const IMPORT_SCOPE: &str = "workspace:import";

const IMPORT_PAGES: &str = "mutation($f:UUID!,$p:[ImportedPageInput!]!,$s:String,$c:Int,$t:Int){\
    workspaceImportPages(fileId:$f,pages:$p,summary:$s,pageCount:$c,estimatedTokens:$t){success}}";
const IMPORT_IMAGE: &str = "mutation($f:UUID!,$n:String!,$m:String!,$s:ByteCount!,$h:String!){\
    workspaceImportImage(fileId:$f,name:$n,mediaType:$m,size:$s,sha256:$h){fileId url fields}}";

async fn import_pages(
    world: &World,
    passport: &str,
    file_id: Uuid,
    pages: serde_json::Value,
    indexing: Option<(&str, i32)>,
) -> serde_json::Value {
    let mut variables = serde_json::json!({ "f": file_id, "p": pages });
    if let Some((summary, count)) = indexing {
        variables["s"] = serde_json::json!(summary);
        variables["c"] = serde_json::json!(count);
    }
    world.gql(passport, IMPORT_PAGES, variables).await
}

async fn import_image(
    world: &World,
    passport: &str,
    file_id: Uuid,
    name: &str,
) -> serde_json::Value {
    world
        .gql(
            passport,
            IMPORT_IMAGE,
            serde_json::json!({
                "f": file_id, "n": name, "m": "image/png",
                "s": IMAGE.len(), "h": sha256_hex(IMAGE),
            }),
        )
        .await
}

#[tokio::test]
async fn an_importer_writes_a_rendition_and_its_images_into_a_ready_file_without_any_job() {
    // Given: a READY file (no rule declared) watched by its owner, and the
    // host's migration account holding the import scope
    let world = World::start("pod-import").await;
    let jobs = JobsStandIn::attach(&world).await;
    let owner = passport(Uuid::now_v7());
    let importer = service_passport(&[IMPORT_SCOPE]);
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "legacy.txt", BYTES),
    )
    .await;
    world.await_source_promoted(file_id).await;
    let mut files = drive_subscription(&world, &owner, drive).await;
    let mut pages = pages_subscription(&world, &owner, file_id).await;

    // When: the importer writes three pages — one a person had corrected — and the indexing
    ok(&import_pages(
        &world,
        &importer,
        file_id,
        serde_json::json!([
            { "number": 1, "markdown": "# Chapter one" },
            { "number": 2, "markdown": "Corrected by hand", "origin": "EDITED" },
            { "number": 3, "markdown": "![figure](p003-img01.png)", "origin": "RUNNER" },
        ]),
        Some(("An old three-page document.", 3)),
    )
    .await);

    // Then: every page reaches the live window with its origin kept
    let mut seen = Vec::new();
    while seen.len() < 3 {
        let delta = next_page_delta(&mut pages, |node| {
            node["__typename"] == "DriveUpsert" || node["__typename"] == "DriveReset"
        })
        .await;
        let views = match delta["__typename"].as_str() {
            Some("DriveReset") => delta["views"].as_array().cloned().unwrap_or_default(),
            _ => vec![delta["view"].clone()],
        };
        for view in views {
            seen.push((view["number"].as_i64().unwrap(), view["origin"].clone()));
        }
    }
    seen.sort_by_key(|(number, _)| *number);
    seen.dedup_by_key(|(number, _)| *number);
    assert_eq!(
        seen,
        vec![
            (1, serde_json::json!("RUNNER")),
            (2, serde_json::json!("EDITED")),
            (3, serde_json::json!("RUNNER")),
        ]
    );
    // And: the file carries the indexing, still READY, with no estimate
    let indexed = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "RenditionImported"
    })
    .await;
    assert_eq!(indexed["view"]["summary"], "An old three-page document.");
    assert_eq!(indexed["view"]["pageCount"], 3);
    assert!(indexed["view"]["estimatedTokens"].is_null());
    assert_eq!(indexed["view"]["processingState"], "READY");

    // When: the importer uploads the figure page 3 references
    let image_ticket = crate::harness::runner::image_ticket_of(
        &import_image(&world, &importer, file_id, "p003-img01.png").await,
        "workspaceImportImage",
    );
    let posted = post_bytes(&world, &image_ticket, IMAGE, "p003-img01.png").await;
    assert!((200..300).contains(&posted), "the figure lands: {posted}");

    // Then: it lands on the file and the owner can read it by name
    let landed = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "ImageAvailable"
    })
    .await;
    assert_eq!(landed["view"]["images"][0]["name"], "p003-img01.png");
    assert_eq!(landed["view"]["images"][0]["page"], 3);
    let access = world.image_access(&owner, file_id, "p003-img01.png").await;
    assert!(
        ok(&access)["workspaceFileAccess"].is_string(),
        "the owner reads the imported figure: {access}"
    );

    // And: an imported page is a page like any other — the owner edits it
    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!){workspaceEditPage(fileId:$f,number:1,markdown:\"# Chapter 1\"){success}}",
            serde_json::json!({ "f": file_id }),
        )
        .await);
    // And: Jobs never heard of any of it
    jobs.expect_no_command(Duration::from_millis(800)).await;

    world.cleanup().await;
}

#[tokio::test]
async fn an_import_is_the_hosts_privilege_on_a_ready_file_and_obeys_every_rendition_rule() {
    // Given: a READY file, a file being processed, a pending upload, and three principals
    let world = World::start("pod-import-refused").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    let importer = service_passport(&[IMPORT_SCOPE]);
    let runner = service_passport(&[RUNNER_SCOPE]);
    let drive = world.create_workspace(&owner, "library").await;
    let ready = upload(
        &world,
        &owner,
        &UploadRequest {
            media_type: "application/octet-stream",
            ..UploadRequest::text(drive, "", "ready.bin", BYTES)
        },
    )
    .await;
    install_render_rule(&world, &jobs, &manager).await;
    let processing = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "busy.txt", BYTES),
    )
    .await;
    jobs.await_create(processing).await;
    let pending = Uuid::now_v7();
    ticket(
        &request(
            &world,
            &owner,
            pending,
            &UploadRequest::text(drive, "", "pending.txt", BYTES),
        )
        .await,
    );
    let mut files = drive_subscription(&world, &owner, drive).await;
    // Let the deltas of the Given reach the fresh session first.
    while files
        .try_next_payload(Duration::from_millis(500))
        .await
        .is_some()
    {}
    let one = serde_json::json!([{ "number": 1, "markdown": "x" }]);

    // When / Then: the owner and a runner are not importers — the host's code, first
    for principal in [&owner, &runner] {
        let refused = import_pages(&world, principal, ready, one.clone(), None).await;
        assert_eq!(error_code(&refused), "NOT_AN_IMPORTER");
        let image = import_image(&world, principal, ready, "p001-img01.png").await;
        assert_eq!(error_code(&image), "NOT_AN_IMPORTER");
    }
    // And: the importer meets the state: never during a chain, never before the commit
    assert_eq!(
        error_code(&import_pages(&world, &importer, processing, one.clone(), None).await),
        "FILE_PROCESSING"
    );
    assert_eq!(
        error_code(&import_pages(&world, &importer, pending, one.clone(), None).await),
        "FILE_NOT_READY"
    );
    assert_eq!(
        error_code(&import_pages(&world, &importer, Uuid::now_v7(), one.clone(), None).await),
        "FILE_NOT_FOUND"
    );
    // And: the rendition rules of a runner report hold for an import
    for (pages, indexing, code) in [
        (
            serde_json::json!([{ "number": 0, "markdown": "x" }]),
            None,
            "INVALID_PAGE",
        ),
        (
            serde_json::json!([{ "number": 2, "markdown": "a" }, { "number": 2, "markdown": "b" }]),
            None,
            "INVALID_PAGE",
        ),
        (serde_json::json!([]), None, "NOTHING_TO_CHANGE"),
        (
            serde_json::json!([]),
            Some(("negative", -1)),
            "INVALID_INDEXER_VALUE",
        ),
    ] {
        let refused = import_pages(&world, &importer, ready, pages, indexing).await;
        assert_eq!(error_code(&refused), code);
    }
    let alone = world
        .gql(
            &importer,
            IMPORT_PAGES,
            serde_json::json!({ "f": ready, "p": [], "t": 12 }),
        )
        .await;
    assert_eq!(error_code(&alone), "INDEXER_FIELDS_TOGETHER");
    let misnamed = import_image(&world, &importer, ready, "figure.png").await;
    assert_eq!(error_code(&misnamed), "INVALID_IMAGE_NAME");

    // And: nothing moved for anyone
    files.expect_silence(Duration::from_millis(600)).await;
    assert!(world.file_pages(&owner, ready).await.is_empty());

    world.cleanup().await;
}

const WORKSPACE_DELTAS: &str = "subscription{workspaceDeltas{__typename \
    ... on WorkspaceReset{views{... on WorkspaceView{id fileCount readyFileCount}}} \
    ... on WorkspaceUpsert{cause view{... on WorkspaceView{id fileCount readyFileCount}}}}}";

async fn workspace_subscription(world: &World, passport: &str) -> Subscription {
    let mut sub = Subscription::open_with(
        &world.subscription_url(),
        passport,
        WORKSPACE_DELTAS,
        serde_json::json!({}),
    )
    .await;
    let reset = sub.next_payload(Duration::from_secs(10)).await;
    assert_eq!(reset["workspaceDeltas"]["__typename"], "WorkspaceReset");
    sub
}

async fn counts_reach(sub: &mut Subscription, workspace: Uuid, files: i64, ready: i64) {
    next_delta(sub, "workspaceDeltas", |node| {
        node["__typename"] == "WorkspaceUpsert"
            && node["view"]["id"] == workspace.to_string()
            && node["view"]["fileCount"] == files
            && node["view"]["readyFileCount"] == ready
    })
    .await;
}

#[tokio::test]
async fn the_hosts_own_object_republishes_as_the_files_of_its_drive_land_fail_move_and_go() {
    // Given: two workspaces, whose view shows their drive's file counts, watched live
    let world = World::start("pod-host-refresh").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    let library = world.create_workspace(&owner, "library").await;
    let archive = world.create_workspace(&owner, "archive").await;
    let mut workspaces = workspace_subscription(&world, &owner).await;

    // When: a file is requested, then committed
    let upload_request = UploadRequest {
        media_type: "application/octet-stream",
        ..UploadRequest::text(library, "", "plain.bin", BYTES)
    };
    let plain = Uuid::now_v7();
    let upload_ticket = ticket(&request(&world, &owner, plain, &upload_request).await);
    // Then: the pending file counts, not yet READY
    counts_reach(&mut workspaces, library, 1, 0).await;
    let posted = post_bytes(&world, &upload_ticket, BYTES, "plain.bin").await;
    assert!((200..300).contains(&posted));
    ok(&commit(&world, &owner, plain).await);
    // Then: it lands READY
    counts_reach(&mut workspaces, library, 1, 1).await;

    // When: a second file enters a chain that fails
    install_render_rule(&world, &jobs, &manager).await;
    let doomed = upload(
        &world,
        &owner,
        &UploadRequest::text(library, "", "doomed.txt", BYTES),
    )
    .await;
    counts_reach(&mut workspaces, library, 2, 1).await;
    let job = jobs.await_create(doomed).await.job_id;
    jobs.fail(job, "runner_error", Some("unreadable")).await;
    world.await_state(&owner, doomed, "FAILED").await;

    // When: the READY file moves to the other workspace
    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!,$d:UUID!){workspaceUpdateFile(fileId:$f,driveId:$d){success}}",
            serde_json::json!({ "f": plain, "d": archive }),
        )
        .await);
    // Then: both workspaces republish
    counts_reach(&mut workspaces, library, 1, 0).await;
    counts_reach(&mut workspaces, archive, 1, 1).await;

    // When: the failed file is deleted
    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!){workspaceDeleteFile(fileId:$f){success}}",
            serde_json::json!({ "f": doomed }),
        )
        .await);
    // Then: its workspace is empty again
    counts_reach(&mut workspaces, library, 0, 0).await;

    world.cleanup().await;
}
