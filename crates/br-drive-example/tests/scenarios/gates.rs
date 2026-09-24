//! The host's gate comes first: every gesture carries what the host needs to
//! decide, and a refused principal learns the host's code, never the file's
//! state.

use std::time::Duration;

use uuid::Uuid;

use crate::harness::runner::{RUNNER_SCOPE, install_render_rule};
use crate::harness::upload::{UploadRequest, commit, post_bytes, request, ticket, upload};
use crate::harness::{
    JobsStandIn, LABEL_DELTAS, Subscription, World, catalogue_subscription, drive_subscription,
    error_code, manager_passport, next_delta, next_drive_delta, ok, passport, service_passport,
};

const BYTES: &[u8] = b"a gated document";

/// Lets the deltas of the gestures that built the Given reach a fresh session,
/// so the silence asserted next is about the gesture under test.
async fn quiet(sub: &mut Subscription) {
    while sub
        .try_next_payload(Duration::from_millis(500))
        .await
        .is_some()
    {}
}

#[tokio::test]
async fn a_pending_upload_is_committed_by_its_uploader_only_even_after_the_drive_changed_hands() {
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

    // When: the new owner, who may create files in the drive, commits the uploader's file
    let refused = commit(&world, &heir, file_id).await;

    // Then: the host refuses it on the file's uploader, and the file stays PENDING
    assert_eq!(error_code(&refused), "NOT_THE_UPLOADER");
    files.expect_silence(Duration::from_millis(600)).await;
    assert_eq!(
        world.file(&heir, file_id).await["processingState"],
        "PENDING"
    );

    // And: the heir's own upload commits as before
    let own = upload(
        &world,
        &heir,
        &UploadRequest::text(drive, "", "mine.txt", BYTES),
    )
    .await;
    assert_eq!(world.file(&heir, own).await["processingState"], "READY");

    world.cleanup().await;
}

async fn refused(world: &World, passport: &str, mutation: &str, file_id: Uuid) -> String {
    error_code(
        &world
            .gql(passport, mutation, serde_json::json!({ "f": file_id }))
            .await,
    )
}

const PROCESS: &str = "mutation($f:UUID!){workspaceProcess(fileId:$f){success}}";
const EDIT_PAGE: &str =
    "mutation($f:UUID!){workspaceEditPage(fileId:$f,number:1,markdown:\"x\"){success}}";
const REGENERATE: &str = "mutation($f:UUID!){workspaceRegeneratePage(fileId:$f,number:1){success}}";
const DELETE: &str = "mutation($f:UUID!){workspaceDeleteFile(fileId:$f){success}}";

#[tokio::test]
async fn a_stranger_learns_the_hosts_refusal_never_the_state_of_a_file_it_may_not_touch() {
    // Given: a file being processed and a protected file, both in an owner's workspace
    let world = World::start("pod-gate-order").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    let stranger = passport(Uuid::now_v7());
    install_render_rule(&world, &jobs, &manager).await;
    let drive = world.create_workspace(&owner, "library").await;
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

    // When / Then: every state-bound gesture answers the stranger with the host's code
    for (mutation, file) in [
        (PROCESS, processing),
        (EDIT_PAGE, processing),
        (REGENERATE, processing),
        (DELETE, kept),
    ] {
        assert_eq!(
            refused(&world, &stranger, mutation, file).await,
            "NOT_THE_WORKSPACE_OWNER",
            "{mutation}"
        );
    }
    // And: an unknown id is not found, for the stranger as for anyone
    assert_eq!(
        refused(&world, &stranger, PROCESS, Uuid::now_v7()).await,
        "FILE_NOT_FOUND"
    );

    // And: the owner, whom the host allows, still learns the state
    assert_eq!(
        refused(&world, &owner, PROCESS, processing).await,
        "FILE_PROCESSING"
    );
    assert_eq!(
        refused(&world, &owner, EDIT_PAGE, processing).await,
        "FILE_PROCESSING"
    );
    assert_eq!(
        refused(&world, &owner, DELETE, kept).await,
        "FILE_PROTECTED"
    );
    let file = world.file(&owner, processing).await;
    assert_eq!(file["affordances"]["process"]["reason"], "FILE_PROCESSING");

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
    let mut files = drive_subscription(&world, &owner, drive).await;
    quiet(&mut files).await;
    let annotate =
        "mutation($f:UUID!,$m:JSON!){workspaceAnnotateFile(fileId:$f,metadata:$m){success}}";

    // When: a stranger writes the file's metadata through the host's mutation,
    // which delegates the decision to the library's gate
    let refused = world
        .gql(
            &stranger,
            annotate,
            serde_json::json!({ "f": file_id, "m": { "by": "stranger" } }),
        )
        .await;

    // Then: the host's gate refuses it, before any state check, and nothing moves
    assert_eq!(error_code(&refused), "NOT_THE_WORKSPACE_OWNER");
    files.expect_silence(Duration::from_millis(600)).await;
    let unchanged = world
        .gql(
            &stranger,
            annotate,
            serde_json::json!({ "f": file_id, "m": {} }),
        )
        .await;
    assert_eq!(
        error_code(&unchanged),
        "NOT_THE_WORKSPACE_OWNER",
        "an unchanged value is not disclosed as NOTHING_TO_CHANGE to a refused principal"
    );

    // And: the owner writes it
    ok(&world
        .gql(
            &owner,
            annotate,
            serde_json::json!({ "f": file_id, "m": { "by": "owner" } }),
        )
        .await);
    let written = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "MetadataChanged"
    })
    .await;
    assert_eq!(written["view"]["id"], file_id.to_string());

    world.cleanup().await;
}
