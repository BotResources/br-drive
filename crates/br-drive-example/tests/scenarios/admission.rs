//! Admission at subscription open: the host's `DriveHost::admit_subscription`
//! answers for every subscription of the drive slice, before any stream is
//! attached. The example host refuses a deactivated account with
//! `ACTIVE_USER_REQUIRED`.

use std::time::Duration;

use uuid::Uuid;

use crate::harness::upload::{UploadRequest, upload};
use crate::harness::{
    DRIVE_DELTAS, LABEL_DELTAS, PAGE_DELTAS, RULESET_DELTAS, SseSubscription, Subscription, World,
    deactivated_passport, passport,
};

const BYTES: &[u8] = b"admission bytes";
const REFUSED: &str = "ACTIVE_USER_REQUIRED";
const WAIT: Duration = Duration::from_secs(10);

/// The four subscriptions of the drive slice, each as (root, query, variables).
fn subscriptions(
    drive: Uuid,
    file_id: Uuid,
) -> [(&'static str, &'static str, serde_json::Value); 4] {
    [
        (
            "workspaceDriveChanged",
            DRIVE_DELTAS,
            serde_json::json!({ "d": drive }),
        ),
        (
            "workspaceFilePages",
            PAGE_DELTAS,
            serde_json::json!({ "f": file_id }),
        ),
        (
            "workspaceLabelsChanged",
            LABEL_DELTAS,
            serde_json::json!({}),
        ),
        (
            "workspaceRulesetsChanged",
            RULESET_DELTAS,
            serde_json::json!({}),
        ),
    ]
}

/// A workspace owned by a person, holding one file.
async fn owned_drive(world: &World, owner: &str) -> (Uuid, Uuid) {
    let drive = world.create_workspace(owner, "admission").await;
    let file_id = upload(
        world,
        owner,
        &UploadRequest::text(drive, "", "admitted.txt", BYTES),
    )
    .await;
    (drive, file_id)
}

#[tokio::test]
async fn a_deactivated_person_is_refused_every_drive_subscription_over_websocket() {
    let world = World::start("pod-admission-ws-refused").await;
    let person = Uuid::now_v7();
    let (drive, file_id) = owned_drive(&world, &passport(person)).await;
    let deactivated = deactivated_passport(person);

    for (root, query, variables) in subscriptions(drive, file_id) {
        let mut sub =
            Subscription::open_with(&world.subscription_url(), &deactivated, query, variables)
                .await;
        let errors = sub.next_error(WAIT).await;
        assert_eq!(
            errors[0]["extensions"]["code"], REFUSED,
            "{root} is refused at open with the host's reason: {errors}"
        );
        sub.expect_silence(Duration::from_millis(600)).await;
    }

    world.cleanup().await;
}

#[tokio::test]
async fn a_deactivated_person_is_refused_every_drive_subscription_over_sse() {
    let world = World::start("pod-admission-sse-refused").await;
    let person = Uuid::now_v7();
    let (drive, file_id) = owned_drive(&world, &passport(person)).await;
    let deactivated = deactivated_passport(person);

    for (root, query, variables) in subscriptions(drive, file_id) {
        let sub = SseSubscription::open(
            &world.http,
            &world.service.http("/graphql"),
            &deactivated,
            query,
            variables,
        )
        .await;
        let errors = sub.refusal(WAIT).await;
        assert_eq!(
            errors[0]["extensions"]["code"], REFUSED,
            "{root} is refused at open with the host's reason: {errors}"
        );
    }

    world.cleanup().await;
}

#[tokio::test]
async fn an_admitted_person_opens_every_drive_subscription_over_websocket() {
    let world = World::start("pod-admission-ws-admitted").await;
    let owner = passport(Uuid::now_v7());
    let (drive, file_id) = owned_drive(&world, &owner).await;

    for (root, query, variables) in subscriptions(drive, file_id) {
        let mut sub =
            Subscription::open_with(&world.subscription_url(), &owner, query, variables).await;
        let reset = sub.next_payload(WAIT).await;
        assert_eq!(
            reset[root]["__typename"], "DriveReset",
            "{root} opens on its Reset: {reset}"
        );
        if root == "workspaceDriveChanged" {
            assert_eq!(
                reset[root]["views"][0]["id"],
                file_id.to_string(),
                "the owner's drive Reset holds the file: {reset}"
            );
        }
    }

    world.cleanup().await;
}

#[tokio::test]
async fn an_admitted_person_opens_every_drive_subscription_over_sse() {
    let world = World::start("pod-admission-sse-admitted").await;
    let owner = passport(Uuid::now_v7());
    let (drive, file_id) = owned_drive(&world, &owner).await;

    for (root, query, variables) in subscriptions(drive, file_id) {
        let mut sub = SseSubscription::open(
            &world.http,
            &world.service.http("/graphql"),
            &owner,
            query,
            variables,
        )
        .await;
        let reset = sub.next_payload(WAIT).await;
        assert_eq!(
            reset[root]["__typename"], "DriveReset",
            "{root} opens on its Reset: {reset}"
        );
        if root == "workspaceDriveChanged" {
            assert_eq!(
                reset[root]["views"][0]["id"],
                file_id.to_string(),
                "the owner's drive Reset holds the file: {reset}"
            );
        }
    }

    world.cleanup().await;
}
