//! The host's gate comes first: every gesture carries what the host needs to
//! decide, a principal who cannot see a file learns exactly what an unknown id
//! answers, and one who sees it learns the host's reason before the file's state.

use std::time::Duration;

use uuid::Uuid;

use crate::harness::runner::{RUNNER_SCOPE, install_render_rule};
use crate::harness::upload::{UploadRequest, commit, post_bytes, request, ticket, upload};
use crate::harness::{
    JobsStandIn, LABEL_DELTAS, Subscription, World, catalogue_subscription, drive_subscription,
    error_code, manager_passport, next_delta, next_drive_delta, ok, passport, quiet, refute_delta,
    service_passport,
};

const BYTES: &[u8] = b"a gated document";
const DRIVE: &str = "workspaceDriveChanged";

#[tokio::test]
async fn a_pending_upload_is_committed_by_its_uploader_only_and_by_nobody_once_the_drive_changed_hands()
 {
    // Given: an upload requested by the owner of a workspace, then the workspace transferred
    let world = World::start("pod-gate-commit").await;
    let uploader_id = Uuid::now_v7();
    let uploader = passport(uploader_id);
    let heir_id = Uuid::now_v7();
    let heir = passport(heir_id);
    let drive = world.create_workspace(&uploader, "library").await;
    let upload_request = UploadRequest::text(drive, "", "draft.txt", BYTES);
    let file_id = Uuid::now_v7();
    let upload_ticket = ticket(&request(&world, &uploader, file_id, &upload_request).await);
    let posted = post_bytes(&world, &upload_ticket, BYTES, "draft.txt").await;
    assert!((200..300).contains(&posted), "the bytes land: {posted}");
    ok(&world
        .gql(
            &uploader,
            "mutation($id:UUID!,$to:UUID!){workspaceTransfer(id:$id,to:$to){success}}",
            serde_json::json!({ "id": drive, "to": heir_id }),
        )
        .await);
    let mut files = drive_subscription(&world, &heir, drive).await;
    quiet(&mut files).await;

    // Then: the heir sees the pending file, and that its commit is not theirs
    let pending = world.file(&heir, file_id).await;
    assert_eq!(pending["processingState"], "PENDING");
    assert_eq!(
        pending["affordances"]["commit"]["reason"],
        "NOT_THE_UPLOADER"
    );

    // When: the heir, who may create files in the drive, commits the uploader's file
    let refused = commit(&world, &heir, file_id).await;
    // Then: the host refuses it on the uploader
    assert_eq!(error_code(&refused), "NOT_THE_UPLOADER");

    // When: the uploader, who lost the drive, commits it
    let lost = commit(&world, &uploader, file_id).await;
    // Then: to them the file is not found — they no longer see the drive
    assert_eq!(error_code(&lost), "FILE_NOT_FOUND");

    // And: the file stays PENDING, nothing reached the heir's session
    refute_delta(&mut files, DRIVE, Duration::from_millis(800), |node| {
        node["cause"]["kind"] == "UploadCommitted" || node["view"]["processingState"] == "READY"
    })
    .await;
    assert_eq!(
        world.file(&heir, file_id).await["processingState"],
        "PENDING"
    );

    // And: the heir's own upload commits as before, its commit theirs
    let own = upload(
        &world,
        &heir,
        &UploadRequest::text(drive, "", "mine.txt", BYTES),
    )
    .await;
    assert_eq!(world.file(&heir, own).await["processingState"], "READY");

    world.cleanup().await;
}

async fn code(
    world: &World,
    passport: &str,
    mutation: &str,
    variables: serde_json::Value,
) -> String {
    error_code(&world.gql(passport, mutation, variables).await)
}

const PROCESS: &str = "mutation($f:UUID!){workspaceProcess(fileId:$f){success}}";
const EDIT_PAGE: &str =
    "mutation($f:UUID!){workspaceEditPage(fileId:$f,number:1,markdown:\"x\"){success}}";
const REGENERATE: &str = "mutation($f:UUID!){workspaceRegeneratePage(fileId:$f,number:1){success}}";
const DELETE: &str = "mutation($f:UUID!){workspaceDeleteFile(fileId:$f){success}}";
const RENAME: &str =
    "mutation($f:UUID!){workspaceUpdateFile(fileId:$f,name:\"taken.bin\"){success}}";
const MOVE: &str = "mutation($f:UUID!,$d:UUID){workspaceUpdateFile(fileId:$f,driveId:$d){success}}";
const COMMIT: &str = "mutation($f:UUID!){workspaceCommitUpload(fileId:$f){success}}";
const ANNOTATE: &str =
    "mutation($f:UUID!){workspaceAnnotateFile(fileId:$f,metadata:{by:\"stranger\"}){success}}";
const SET_LABELS: &str =
    "mutation($f:UUID!){workspaceSetFileLabels(fileId:$f,labelIds:[]){success}}";
const PROTECT: &str =
    "mutation($f:UUID!){workspaceProtectFile(fileId:$f,protected:false){success}}";

fn snapshot(file: &serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "state": file["processingState"], "protected": file["protected"],
        "name": file["name"], "metadata": file["metadata"], "updatedAt": file["updatedAt"],
    })
}

#[tokio::test]
async fn a_stranger_learns_nothing_of_a_file_it_cannot_see_and_changes_nothing() {
    // Given: in an owner's workspace, a file being processed, a protected file
    // and a pending upload; and a stranger with a workspace of its own
    let world = World::start("pod-gate-order").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    let stranger = passport(Uuid::now_v7());
    install_render_rule(&world, &jobs, &manager).await;
    let drive = world.create_workspace(&owner, "library").await;
    let elsewhere = world.create_workspace(&stranger, "elsewhere").await;
    let processing = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "busy.txt", BYTES),
    )
    .await;
    jobs.await_create(processing).await;
    let kept = upload(
        &world,
        &owner,
        &UploadRequest {
            media_type: "application/octet-stream",
            ..UploadRequest::text(drive, "", "kept.bin", BYTES)
        },
    )
    .await;
    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!,$p:Boolean!){workspaceProtectFile(fileId:$f,protected:$p){success}}",
            serde_json::json!({ "f": kept, "p": true }),
        )
        .await);
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
    for file in [processing, kept] {
        world.await_source_promoted(file).await;
    }
    let mut files = drive_subscription(&world, &owner, drive).await;
    quiet(&mut files).await;
    let mut snapshots = Vec::new();
    for file in [processing, kept, pending] {
        snapshots.push(snapshot(&world.file(&owner, file).await));
    }

    // When / Then: every gesture addressed to one of these files answers the
    // stranger exactly what an unknown id answers — the state never shows
    let unknown = Uuid::now_v7();
    for (mutation, file) in [
        (PROCESS, processing),
        (EDIT_PAGE, processing),
        (REGENERATE, processing),
        (DELETE, kept),
        (RENAME, kept),
        (COMMIT, pending),
        (ANNOTATE, kept),
        (SET_LABELS, kept),
    ] {
        let on_file = code(
            &world,
            &stranger,
            mutation,
            serde_json::json!({ "f": file }),
        )
        .await;
        let on_unknown = code(
            &world,
            &stranger,
            mutation,
            serde_json::json!({ "f": unknown }),
        )
        .await;
        assert_eq!(on_file, "FILE_NOT_FOUND", "{mutation}");
        assert_eq!(on_file, on_unknown, "{mutation}");
    }
    // And: a move of the protected file into the stranger's own drive is not found either
    assert_eq!(
        code(
            &world,
            &stranger,
            MOVE,
            serde_json::json!({ "f": kept, "d": elsewhere })
        )
        .await,
        "FILE_NOT_FOUND"
    );
    // And: the host's own protect gesture refuses the stranger too
    assert_eq!(
        code(&world, &stranger, PROTECT, serde_json::json!({ "f": kept })).await,
        "NOT_THE_WORKSPACE_OWNER"
    );

    // And: nothing moved — no delta, no job asked for, every file as it was
    refute_delta(&mut files, DRIVE, Duration::from_millis(800), |node| {
        node["__typename"] != "DriveReset"
    })
    .await;
    jobs.expect_no_command(Duration::from_millis(500)).await;
    let mut after = Vec::new();
    for file in [processing, kept, pending] {
        after.push(snapshot(&world.file(&owner, file).await));
    }
    assert_eq!(after, snapshots);

    // And: the owner, whom the host allows, meets the state refusals
    for (mutation, file, state) in [
        (PROCESS, processing, "FILE_PROCESSING"),
        (EDIT_PAGE, processing, "FILE_PROCESSING"),
        (REGENERATE, processing, "FILE_PROCESSING"),
        (DELETE, kept, "FILE_PROTECTED"),
        (RENAME, kept, "FILE_PROTECTED"),
    ] {
        assert_eq!(
            code(&world, &owner, mutation, serde_json::json!({ "f": file })).await,
            state,
            "{mutation}"
        );
    }
    let file = world.file(&owner, processing).await;
    assert_eq!(file["affordances"]["process"]["reason"], "FILE_PROCESSING");
    jobs.expect_no_command(Duration::from_millis(500)).await;

    world.cleanup().await;
}

#[tokio::test]
async fn a_move_to_an_unknown_drive_is_refused_like_a_move_to_a_foreign_one() {
    // Given: a stranger's own file, and an owner's drive the stranger cannot use
    let world = World::start("pod-gate-move-target").await;
    let owner = passport(Uuid::now_v7());
    let stranger = passport(Uuid::now_v7());
    let foreign = world.create_workspace(&owner, "library").await;
    let own_drive = world.create_workspace(&stranger, "mine").await;
    let own = upload(
        &world,
        &stranger,
        &UploadRequest::text(own_drive, "", "mine.txt", BYTES),
    )
    .await;

    // When: the stranger moves it into the foreign drive, then into an unknown one
    let to_foreign = code(
        &world,
        &stranger,
        MOVE,
        serde_json::json!({ "f": own, "d": foreign }),
    )
    .await;
    let to_unknown = code(
        &world,
        &stranger,
        MOVE,
        serde_json::json!({ "f": own, "d": Uuid::now_v7() }),
    )
    .await;

    // Then: both answer the host's refusal: an unknown drive id is no oracle
    assert_eq!(to_foreign, "NOT_THE_WORKSPACE_OWNER");
    assert_eq!(to_unknown, to_foreign);
    assert_eq!(
        world.file(&stranger, own).await["driveId"],
        own_drive.to_string()
    );

    world.cleanup().await;
}

async fn create_label(world: &World, manager: &str, name: &str) {
    ok(&world
        .gql(
            manager,
            "mutation($id:UUID!,$n:String!,$c:String!){workspaceCreateLabel(id:$id,name:$n,color:$c){success}}",
            serde_json::json!({ "id": Uuid::now_v7(), "n": name, "c": "#336699" }),
        )
        .await);
}

async fn label_reset(world: &World, passport: &str) -> (Subscription, usize) {
    let mut sub = Subscription::open_with(
        &world.subscription_url(),
        passport,
        LABEL_DELTAS,
        serde_json::json!({}),
    )
    .await;
    let reset = sub.next_payload(Duration::from_secs(10)).await;
    let node = &reset["workspaceLabelsChanged"];
    assert_eq!(node["__typename"], "DriveReset", "{reset}");
    (sub, node["views"].as_array().map_or(0, Vec::len))
}

#[tokio::test]
async fn the_label_catalogue_is_read_through_the_hosts_gate_which_can_keep_a_runner_out() {
    // Given: a label catalogue and a runner service account
    let world = World::start("pod-gate-labels").await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let reader = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    create_label(&world, &manager, "Urgent").await;

    // When: the reader and the runner read the catalogue, and watch it
    let mut live =
        catalogue_subscription(&world, &reader, LABEL_DELTAS, "workspaceLabelsChanged").await;
    let (mut runner_live, runner_seen) = label_reset(&world, &runner).await;

    // Then: the reader sees it, the runner — refused by the host — sees nothing
    assert_eq!(world.labels(&reader).await.len(), 1);
    assert!(world.labels(&runner).await.is_empty());
    assert_eq!(runner_seen, 0, "the runner's window opens empty");

    // And: a new label reaches the reader live and never the runner
    create_label(&world, &manager, "Reviewed").await;
    next_delta(&mut live, "workspaceLabelsChanged", |node| {
        node["__typename"] == "DriveUpsert" && node["view"]["name"] == "Reviewed"
    })
    .await;
    runner_live.expect_silence(Duration::from_millis(800)).await;

    world.cleanup().await;
}

const ANNOTATE_WITH: &str =
    "mutation($f:UUID!,$m:JSON!){workspaceAnnotateFile(fileId:$f,metadata:$m){success}}";

#[tokio::test]
async fn a_metadata_write_asks_the_hosts_gate_for_the_principal_it_is_written_for() {
    // Given: a file in an owner's workspace, watched by the owner
    let world = World::start("pod-gate-metadata").await;
    let owner = passport(Uuid::now_v7());
    let stranger = passport(Uuid::now_v7());
    let drive = world.create_workspace(&owner, "library").await;
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "notes.txt", BYTES),
    )
    .await;
    world.await_source_promoted(file_id).await;
    let mut files = drive_subscription(&world, &owner, drive).await;
    quiet(&mut files).await;
    assert_eq!(
        world.file(&owner, file_id).await["affordances"]["setMetadata"]["allowed"],
        true
    );

    // When: a stranger writes the file's metadata through the host's mutation,
    // which delegates the decision to the library's gate — once with a new
    // value, once with the value the file already holds
    let refused = code(
        &world,
        &stranger,
        ANNOTATE_WITH,
        serde_json::json!({ "f": file_id, "m": { "by": "stranger" } }),
    )
    .await;
    let unchanged = code(
        &world,
        &stranger,
        ANNOTATE_WITH,
        serde_json::json!({ "f": file_id, "m": {} }),
    )
    .await;

    // Then: both are the not-found answer, never NOTHING_TO_CHANGE, and nothing moved
    assert_eq!(refused, "FILE_NOT_FOUND");
    assert_eq!(unchanged, "FILE_NOT_FOUND");
    refute_delta(&mut files, DRIVE, Duration::from_millis(800), |node| {
        node["cause"]["kind"] == "MetadataChanged"
    })
    .await;
    assert_eq!(
        world.file(&owner, file_id).await["metadata"],
        serde_json::json!({})
    );

    // When: the owner writes it
    ok(&world
        .gql(
            &owner,
            ANNOTATE_WITH,
            serde_json::json!({ "f": file_id, "m": { "by": "owner" } }),
        )
        .await);
    // Then: the delta and the query carry the owner's value
    let written = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "MetadataChanged"
    })
    .await;
    assert_eq!(
        written["view"]["metadata"],
        serde_json::json!({ "by": "owner" })
    );
    assert_eq!(
        world.file(&owner, file_id).await["metadata"],
        serde_json::json!({ "by": "owner" })
    );

    world.cleanup().await;
}
