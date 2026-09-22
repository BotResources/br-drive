use std::time::Duration;

use uuid::Uuid;

use crate::harness::{Subscription, World, error_code, ok, passport};

const RECV: Duration = Duration::from_secs(10);

#[tokio::test]
async fn the_host_boots_with_the_drive_slice_and_serves_both_slices_under_its_prefix() {
    let world = World::start("pod-smoke").await;
    let owner_id = Uuid::now_v7();
    let owner = passport(owner_id);

    let version = world
        .gql(
            &owner,
            "query{workspaceDriveVersion}",
            serde_json::json!({}),
        )
        .await;
    assert_eq!(
        ok(&version)["workspaceDriveVersion"],
        br_drive::VERSION,
        "the library slice answers under the host prefix"
    );

    let query = "subscription{workspaceDeltas{\
        __typename \
        ... on WorkspaceReset{revision views{... on WorkspaceView{id name}}} \
        ... on WorkspaceUpsert{revision cause view{... on WorkspaceView{id name ownerId affordances}}} \
        ... on WorkspaceRemove{revision projector key}}}";
    let mut sub = Subscription::open(&world.subscription_url(), &owner, query).await;
    let reset = sub.next_payload(RECV).await;
    assert_eq!(reset["workspaceDeltas"]["__typename"], "WorkspaceReset");
    assert_eq!(
        reset["workspaceDeltas"]["views"].as_array().unwrap().len(),
        0,
        "a fresh owner sees no workspace"
    );

    let workspace = Uuid::now_v7();
    ok(&world
        .gql(
            &owner,
            "mutation($id:UUID!,$n:String!){workspaceCreate(id:$id,name:$n){success}}",
            serde_json::json!({ "id": workspace, "n": "library" }),
        )
        .await);

    let upsert = loop {
        let delta = sub.next_payload(RECV).await;
        if delta["workspaceDeltas"]["__typename"] == "WorkspaceUpsert"
            && delta["workspaceDeltas"]["view"]["id"] == workspace.to_string()
        {
            break delta;
        }
    };
    let node = &upsert["workspaceDeltas"];
    assert_eq!(node["view"]["id"], workspace.to_string());
    assert_eq!(node["view"]["ownerId"], owner_id.to_string());
    assert_eq!(node["view"]["affordances"]["delete"]["allowed"], true);

    let reused = world
        .gql(
            &owner,
            "mutation($id:UUID!,$n:String!){workspaceCreate(id:$id,name:$n){success}}",
            serde_json::json!({ "id": workspace, "n": "again" }),
        )
        .await;
    assert_eq!(
        error_code(&reused),
        "KEY_REUSED",
        "a second create on the same id is refused, never silently acked"
    );

    let outsider = passport(Uuid::now_v7());
    let hidden = world
        .gql(
            &outsider,
            "query($id:UUID!){workspaceWorkspace(id:$id){id}}",
            serde_json::json!({ "id": workspace }),
        )
        .await;
    assert!(
        ok(&hidden)["workspaceWorkspace"].is_null(),
        "a principal who does not own the workspace does not see it"
    );

    world.cleanup().await;
}
