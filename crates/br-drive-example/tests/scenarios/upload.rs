use std::time::Duration;

use service_engine::BlobRef;
use service_engine::blobs::BlobState;
use uuid::Uuid;

use crate::harness::upload::{
    UploadRequest, commit, post_bytes, request, request_with_hash, sha256_hex, ticket, upload,
};
use crate::harness::{
    World, WorldOptions, drive_subscription, error_code, next_drive_delta, ok, passport,
};
use crate::poll_until;

const PAYLOAD: &[u8] = b"the quick brown fox";
const WRONG_SHA256: &str = "0000000000000000000000000000000000000000000000000000000000000000";

#[tokio::test]
async fn a_verified_upload_round_trips_and_the_file_becomes_ready() {
    let world = World::start("pod-upload-roundtrip").await;
    let owner = passport(Uuid::now_v7());
    let drive = world.create_workspace(&owner, "library").await;
    let mut sub = drive_subscription(&world, &owner, drive).await;

    let file_id = Uuid::now_v7();
    let ticket = ticket(
        &request(
            &world,
            &owner,
            file_id,
            &UploadRequest::text(drive, "/docs/2026/", "fox.txt", PAYLOAD),
        )
        .await,
    );
    assert_eq!(ticket.file_id, file_id, "the ticket echoes the client id");

    let requested = next_drive_delta(&mut sub, |node| {
        node["__typename"] == "DriveUpsert" && node["view"]["id"] == file_id.to_string()
    })
    .await;
    assert_eq!(requested["view"]["processingState"], "PENDING");
    assert_eq!(
        requested["view"]["path"], "docs/2026",
        "the path is normalized: no leading or trailing slash"
    );
    assert_eq!(
        requested["view"]["affordances"]["download"]["allowed"], false,
        "nothing to download before the upload is committed"
    );
    assert_eq!(
        requested["view"]["affordances"]["download"]["reason"],
        "FILE_NOT_READY"
    );

    let status = post_bytes(&world, &ticket, PAYLOAD, "fox.txt").await;
    assert!(
        (200..300).contains(&status),
        "object storage accepts the exact bytes: {status}"
    );

    ok(&commit(&world, &owner, file_id).await);
    let mut causes = std::collections::BTreeSet::new();
    while causes.len() < 2 {
        let delta = next_drive_delta(&mut sub, |node| {
            node["__typename"] == "DriveUpsert"
                && matches!(
                    node["cause"]["kind"].as_str(),
                    Some("UploadCommitted" | "SourceAvailable")
                )
        })
        .await;
        let kind = delta["cause"]["kind"].as_str().unwrap().to_string();
        if kind == "UploadCommitted" {
            assert_eq!(delta["view"]["processingState"], "READY");
            assert_eq!(delta["view"]["affordances"]["delete"]["allowed"], true);
            assert_eq!(delta["view"]["affordances"]["download"]["allowed"], true);
        }
        causes.insert(kind);
    }

    let file = world.file(&owner, file_id).await;
    assert_eq!(file["sizeBytes"], PAYLOAD.len());
    assert_eq!(file["sha256"], sha256_hex(PAYLOAD));
    assert_eq!(file["mediaType"], "text/plain");

    let url = poll_until!(Duration::from_secs(15), {
        ok(&world.file_access(&owner, file_id).await)["workspaceFileAccess"]
            .as_str()
            .map(str::to_string)
    });
    assert!(
        url.contains("response-content-disposition=attachment"),
        "the source is served as an attachment: {url}"
    );
    let got = world
        .http
        .get(url)
        .send()
        .await
        .expect("GET the presigned download URL")
        .bytes()
        .await
        .expect("read the downloaded bytes");
    assert_eq!(
        got.as_ref(),
        PAYLOAD,
        "the presigned GET round-trips the bytes"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn bytes_whose_checksum_does_not_match_are_refused_by_storage_and_the_commit_is_refused() {
    let world = World::start("pod-upload-wrong-sha").await;
    let owner = passport(Uuid::now_v7());
    let drive = world.create_workspace(&owner, "library").await;

    let file_id = Uuid::now_v7();
    let ticket = ticket(
        &request_with_hash(
            &world,
            &owner,
            file_id,
            &UploadRequest::text(drive, "", "wrong.txt", PAYLOAD),
            WRONG_SHA256,
        )
        .await,
    );
    let status = post_bytes(&world, &ticket, PAYLOAD, "wrong.txt").await;
    assert!(
        !(200..300).contains(&status),
        "object storage refuses bytes whose checksum is not the pinned one: {status}"
    );
    let object_key = ticket.fields["key"].as_str().expect("the object key");
    assert!(
        !world.object_exists(object_key).await,
        "no object landed under the presigned key"
    );

    let refused = commit(&world, &owner, file_id).await;
    assert_eq!(error_code(&refused), "UPLOAD_NOT_LANDED");
    assert_eq!(
        world.file(&owner, file_id).await["processingState"],
        "PENDING"
    );
    assert_eq!(
        error_code(&world.file_access(&owner, file_id).await),
        "FILE_NOT_READY",
        "a pending file affords no download, and the query says why"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn a_commit_in_the_pending_window_reads_the_storage_head_before_the_reaper_sweeps() {
    let world = World::start_with(
        "pod-upload-pending-window",
        WorldOptions {
            reaper_interval: Duration::from_secs(3600),
            ..WorldOptions::default()
        },
    )
    .await;
    let owner = passport(Uuid::now_v7());
    let drive = world.create_workspace(&owner, "library").await;

    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "early.txt", PAYLOAD),
    )
    .await;
    assert_eq!(
        world.file(&owner, file_id).await["processingState"],
        "READY"
    );

    let head = world
        .service
        .blob_reader()
        .head(BlobRef(world.source_of(file_id).await))
        .await
        .expect("head does not error")
        .expect("the object landed");
    assert_eq!(head.state, BlobState::Pending, "no sweep has run yet");
    assert_eq!(head.verified(), Some(true));

    let refused = commit(&world, &owner, file_id).await;
    assert_eq!(
        error_code(&refused),
        "FILE_NOT_PENDING",
        "a second commit is refused, never silently acked"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn an_abandoned_upload_is_reaped_and_the_file_removed() {
    let world = World::start_with(
        "pod-upload-abandoned",
        WorldOptions {
            upload_window: Duration::from_secs(1),
            ..WorldOptions::default()
        },
    )
    .await;
    let owner = passport(Uuid::now_v7());
    let drive = world.create_workspace(&owner, "library").await;
    let mut sub = drive_subscription(&world, &owner, drive).await;

    let file_id = Uuid::now_v7();
    ticket(
        &request(
            &world,
            &owner,
            file_id,
            &UploadRequest::text(drive, "", "never.txt", PAYLOAD),
        )
        .await,
    );
    let source = world.source_of(file_id).await;
    assert_eq!(world.blob_state(source).await.as_deref(), Some("pending"));

    next_drive_delta(&mut sub, |node| {
        node["__typename"] == "DriveRemove" && node["key"] == file_id.to_string()
    })
    .await;
    assert!(world.file(&owner, file_id).await.is_null());
    assert_eq!(
        world.blob_state(source).await.as_deref(),
        Some("orphaned"),
        "the deadline released the blob in the same transaction"
    );
    poll_until!(Duration::from_secs(30), {
        world.blob_state(source).await.is_none().then_some(())
    });

    world.cleanup().await;
}

#[tokio::test]
async fn an_upload_that_landed_but_was_never_committed_is_reaped_with_its_object() {
    let world = World::start_with(
        "pod-upload-landed-abandoned",
        WorldOptions {
            upload_window: Duration::from_secs(1),
            ..WorldOptions::default()
        },
    )
    .await;
    let owner = passport(Uuid::now_v7());
    let drive = world.create_workspace(&owner, "library").await;
    let mut sub = drive_subscription(&world, &owner, drive).await;

    let file_id = Uuid::now_v7();
    let ticket = ticket(
        &request(
            &world,
            &owner,
            file_id,
            &UploadRequest::text(drive, "", "landed.txt", PAYLOAD),
        )
        .await,
    );
    let status = post_bytes(&world, &ticket, PAYLOAD, "landed.txt").await;
    assert!((200..300).contains(&status), "the bytes land: {status}");
    let source = world.source_of(file_id).await;
    let object_key = world
        .object_key_of(source)
        .await
        .expect("the blob row names its object");
    assert!(world.object_exists(&object_key).await);

    next_drive_delta(&mut sub, |node| {
        node["__typename"] == "DriveRemove" && node["key"] == file_id.to_string()
    })
    .await;
    assert!(world.file(&owner, file_id).await.is_null());
    assert_eq!(world.blob_state(source).await.as_deref(), Some("orphaned"));
    let refused = commit(&world, &owner, file_id).await;
    assert_eq!(error_code(&refused), "FILE_NOT_FOUND");
    poll_until!(Duration::from_secs(30), {
        (!world.object_exists(&object_key).await).then_some(())
    });
    poll_until!(Duration::from_secs(10), {
        world.blob_state(source).await.is_none().then_some(())
    });

    world.cleanup().await;
}

#[tokio::test]
async fn a_sibling_collision_derives_a_counter_before_the_extension() {
    let world = World::start("pod-upload-collision").await;
    let owner = passport(Uuid::now_v7());
    let drive = world.create_workspace(&owner, "library").await;

    let mut names = Vec::new();
    for _ in 0..3 {
        let file_id = upload(
            &world,
            &owner,
            &UploadRequest::text(drive, "reports", "report.pdf", PAYLOAD),
        )
        .await;
        names.push(
            world.file(&owner, file_id).await["name"]
                .as_str()
                .unwrap()
                .to_string(),
        );
    }
    assert_eq!(names, ["report.pdf", "report (1).pdf", "report (2).pdf"]);

    let elsewhere = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "reports/old", "report.pdf", PAYLOAD),
    )
    .await;
    assert_eq!(
        world.file(&owner, elsewhere).await["name"],
        "report.pdf",
        "uniqueness is per folder"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn a_path_or_name_that_does_not_normalize_is_refused() {
    let world = World::start("pod-upload-normalization").await;
    let owner = passport(Uuid::now_v7());
    let drive = world.create_workspace(&owner, "library").await;

    for (path, name, code) in [
        ("../escape", "a.txt", "INVALID_PATH"),
        ("docs//x", "a.txt", "INVALID_PATH"),
        ("docs/./x", "a.txt", "INVALID_PATH"),
        ("docs", "sub/a.txt", "INVALID_NAME"),
        ("docs", "", "INVALID_NAME"),
        ("docs", "..", "INVALID_NAME"),
    ] {
        let refused = request(
            &world,
            &owner,
            Uuid::now_v7(),
            &UploadRequest::text(drive, path, name, PAYLOAD),
        )
        .await;
        assert_eq!(error_code(&refused), code, "{path:?} / {name:?}");
    }
    let refused = request_with_hash(
        &world,
        &owner,
        Uuid::now_v7(),
        &UploadRequest::text(drive, "", "a.txt", PAYLOAD),
        "not-a-digest",
    )
    .await;
    assert_eq!(error_code(&refused), "INVALID_SHA256");
    for media_type in ["text", "text/plain; charset=utf-8", "text plain"] {
        let refused = request(
            &world,
            &owner,
            Uuid::now_v7(),
            &UploadRequest {
                drive,
                path: "",
                name: "a.txt",
                media_type,
                bytes: PAYLOAD,
            },
        )
        .await;
        assert_eq!(error_code(&refused), "INVALID_MEDIA_TYPE", "{media_type:?}");
    }
    let padded = request(
        &world,
        &owner,
        Uuid::now_v7(),
        &UploadRequest::text(drive, "docs /x", " a.txt", PAYLOAD),
    )
    .await;
    assert_eq!(error_code(&padded), "INVALID_PATH");
    let deep = "s".repeat(200);
    let too_long = [deep.as_str(); 6].join("/");
    let over = request(
        &world,
        &owner,
        Uuid::now_v7(),
        &UploadRequest::text(drive, &too_long, "a.txt", PAYLOAD),
    )
    .await;
    assert_eq!(error_code(&over), "INVALID_PATH");

    let unrenderable = request(
        &world,
        &owner,
        Uuid::now_v7(),
        &UploadRequest {
            drive,
            path: "",
            name: "blob.bin",
            media_type: "application/x-unrenderable",
            bytes: PAYLOAD,
        },
    )
    .await;
    assert_eq!(
        error_code(&unrenderable),
        "UNRENDERABLE_MEDIA_TYPE",
        "the host gate sees the requested media type and answers with its own code"
    );

    let file_id = Uuid::now_v7();
    ticket(
        &request(
            &world,
            &owner,
            file_id,
            &UploadRequest::text(drive, "", "twice.txt", PAYLOAD),
        )
        .await,
    );
    let reused = request(
        &world,
        &owner,
        file_id,
        &UploadRequest::text(drive, "", "twice.txt", PAYLOAD),
    )
    .await;
    assert_eq!(error_code(&reused), "KEY_REUSED");

    world.cleanup().await;
}
