//! A file's title: set at upload or defaulted from the name, changed on its own
//! gesture, independent of the name, the path and the drive.

use uuid::Uuid;

use crate::harness::upload::{UploadRequest, request, upload};
use crate::harness::{
    DRIVE_DELTAS, Subscription, World, drive_subscription, error_code, next_drive_delta, ok,
    passport, quiet,
};

const BYTES: &[u8] = b"a titled document";
const RETITLE: &str =
    "mutation($f:UUID!,$t:String!){workspaceRetitleFile(fileId:$f,title:$t){success}}";

#[tokio::test]
async fn an_upload_takes_its_title_or_the_name_without_its_extension() {
    // Given: an owner's workspace
    let world = World::start("pod-title-upload").await;
    let owner = passport(Uuid::now_v7());
    let drive = world.create_workspace(&owner, "library").await;

    // When: one file is uploaded without a title, one with a padded title
    let defaulted = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "quarterly report.txt", BYTES),
    )
    .await;
    let titled = upload(
        &world,
        &owner,
        &UploadRequest {
            title: Some("  Board minutes, March "),
            ..UploadRequest::text(drive, "", "minutes.txt", BYTES)
        },
    )
    .await;

    // Then: the first carries its name without the extension, the second its trimmed title
    let file = world.file(&owner, defaulted).await;
    assert_eq!(file["title"], "quarterly report");
    assert_eq!(file["name"], "quarterly report.txt");
    assert_eq!(
        world.file(&owner, titled).await["title"],
        "Board minutes, March"
    );

    // And: a colliding name is renamed, while the default title stays the uploaded name's
    let again = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "quarterly report.txt", BYTES),
    )
    .await;
    let collided = world.file(&owner, again).await;
    assert_eq!(collided["name"], "quarterly report (1).txt");
    assert_eq!(collided["title"], "quarterly report");

    // And: a title past 255 characters, blank, or with a control character is refused
    let long = "é".repeat(256);
    for title in [long.as_str(), "   ", "line\nbreak"] {
        let refused = request(
            &world,
            &owner,
            Uuid::now_v7(),
            &UploadRequest {
                title: Some(title),
                ..UploadRequest::text(drive, "", "refused.txt", BYTES)
            },
        )
        .await;
        assert_eq!(error_code(&refused), "INVALID_TITLE", "{title:?}");
    }
    assert!(
        !world
            .drive_files(&owner, drive)
            .await
            .iter()
            .any(|file| file["name"] == "refused.txt"),
        "a refused title leaves no file behind"
    );
    let exact = "é".repeat(255);
    ok(&request(
        &world,
        &owner,
        Uuid::now_v7(),
        &UploadRequest {
            title: Some(exact.as_str()),
            ..UploadRequest::text(drive, "", "exact.txt", BYTES)
        },
    )
    .await);

    world.cleanup().await;
}

#[tokio::test]
async fn a_retitle_changes_the_title_only_and_a_rename_or_move_never_touches_it() {
    // Given: a titled file in an owner's workspace, watched live
    let world = World::start("pod-title-retitle").await;
    let owner = passport(Uuid::now_v7());
    let stranger = passport(Uuid::now_v7());
    let drive = world.create_workspace(&owner, "library").await;
    let other = world.create_workspace(&owner, "archive").await;
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "inbox", "scan-0042.txt", BYTES),
    )
    .await;
    world.await_source_promoted(file_id).await;
    let mut files = drive_subscription(&world, &owner, drive).await;
    quiet(&mut files).await;
    let before = world.file(&owner, file_id).await;
    assert_eq!(before["title"], "scan-0042");
    assert_eq!(before["affordances"]["retitle"]["allowed"], true);

    // When: the owner retitles it
    ok(&world
        .gql(
            &owner,
            RETITLE,
            serde_json::json!({ "f": file_id, "t": "Lease agreement" }),
        )
        .await);

    // Then: the delta carries the new title, the name, path and state untouched
    let retitled = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "Retitled"
    })
    .await;
    assert_eq!(retitled["view"]["title"], "Lease agreement");
    assert_eq!(retitled["view"]["name"], "scan-0042.txt");
    assert_eq!(retitled["view"]["path"], "inbox");
    assert_eq!(retitled["view"]["processingState"], "READY");

    // And: the same title again is NOTHING_TO_CHANGE, a bad one INVALID_TITLE,
    // a stranger gets the host's refusal, and nothing moves
    let same = world
        .gql(
            &owner,
            RETITLE,
            serde_json::json!({ "f": file_id, "t": " Lease agreement " }),
        )
        .await;
    assert_eq!(error_code(&same), "NOTHING_TO_CHANGE");
    let blank = world
        .gql(
            &owner,
            RETITLE,
            serde_json::json!({ "f": file_id, "t": "" }),
        )
        .await;
    assert_eq!(error_code(&blank), "INVALID_TITLE");
    let foreign = world
        .gql(
            &stranger,
            RETITLE,
            serde_json::json!({ "f": file_id, "t": "Mine now" }),
        )
        .await;
    assert_eq!(
        error_code(&foreign),
        "FILE_NOT_FOUND",
        "to a stranger the file is not found, as an unknown id is"
    );
    let unknown = world
        .gql(
            &owner,
            RETITLE,
            serde_json::json!({ "f": Uuid::now_v7(), "t": "Nobody" }),
        )
        .await;
    assert_eq!(error_code(&unknown), "FILE_NOT_FOUND");
    files
        .expect_silence(std::time::Duration::from_millis(600))
        .await;

    // When: the file is renamed, then moved to another drive
    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!,$n:String!){workspaceUpdateFile(fileId:$f,name:$n){success}}",
            serde_json::json!({ "f": file_id, "n": "lease-2026.txt" }),
        )
        .await);
    let renamed = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "Renamed"
    })
    .await;
    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!,$d:UUID!){workspaceUpdateFile(fileId:$f,driveId:$d){success}}",
            serde_json::json!({ "f": file_id, "d": other }),
        )
        .await);

    // Then: the title follows the file, unchanged
    assert_eq!(renamed["view"]["name"], "lease-2026.txt");
    assert_eq!(renamed["view"]["title"], "Lease agreement");
    let moved = world.file(&owner, file_id).await;
    assert_eq!(moved["driveId"], other.to_string());
    assert_eq!(moved["title"], "Lease agreement");

    world.cleanup().await;
}

#[tokio::test]
async fn a_protected_file_keeps_its_name_and_place_but_can_be_retitled() {
    // Given: a file its host protected, watched by its owner
    let world = World::start("pod-title-protected").await;
    let owner = passport(Uuid::now_v7());
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "contract.txt", BYTES),
    )
    .await;
    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!,$p:Boolean!){workspaceProtectFile(fileId:$f,protected:$p){success}}",
            serde_json::json!({ "f": file_id, "p": true }),
        )
        .await);
    world.await_source_promoted(file_id).await;
    let mut files = drive_subscription(&world, &owner, drive).await;
    quiet(&mut files).await;

    // Then: rename and move are blocked by the protection, retitle is not
    let file = world.file(&owner, file_id).await;
    assert_eq!(file["affordances"]["rename"]["reason"], "FILE_PROTECTED");
    assert_eq!(file["affordances"]["move"]["reason"], "FILE_PROTECTED");
    assert_eq!(file["affordances"]["retitle"]["allowed"], true);

    // When: the owner retitles it
    ok(&world
        .gql(
            &owner,
            RETITLE,
            serde_json::json!({ "f": file_id, "t": "Signed contract" }),
        )
        .await);
    // Then: the title changes, live
    let retitled = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "Retitled"
    })
    .await;
    assert_eq!(retitled["view"]["title"], "Signed contract");
    assert_eq!(retitled["view"]["name"], "contract.txt");

    // When: the owner tries to rename it
    let renamed = world
        .gql(
            &owner,
            "mutation($f:UUID!){workspaceUpdateFile(fileId:$f,name:\"other.txt\"){success}}",
            serde_json::json!({ "f": file_id }),
        )
        .await;
    // Then: the protection holds, and nothing moves
    assert_eq!(error_code(&renamed), "FILE_PROTECTED");
    files
        .expect_silence(std::time::Duration::from_millis(600))
        .await;

    // And: a fresh session's snapshot carries the new title
    let mut fresh = Subscription::open_with(
        &world.subscription_url(),
        &owner,
        DRIVE_DELTAS,
        serde_json::json!({ "d": drive }),
    )
    .await;
    let reset = fresh.next_payload(std::time::Duration::from_secs(10)).await;
    let views = reset["workspaceDriveChanged"]["views"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(
        views
            .iter()
            .any(|view| view["id"] == file_id.to_string() && view["title"] == "Signed contract"),
        "{reset}"
    );

    world.cleanup().await;
}
