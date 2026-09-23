use std::time::Duration;

use uuid::Uuid;

use crate::harness::upload::{UploadRequest, upload};
use crate::harness::{
    LABEL_DELTAS, World, catalogue_subscription, drive_subscription, error_code, manager_passport,
    next_delta, next_drive_delta, ok, passport, service_passport,
};

const BYTES: &[u8] = b"labelled bytes";

async fn create_label(world: &World, passport: &str, name: &str, color: &str) -> serde_json::Value {
    world
        .gql(
            passport,
            "mutation($id:UUID!,$n:String!,$c:String!){workspaceCreateLabel(id:$id,name:$n,color:$c){success}}",
            serde_json::json!({ "id": Uuid::now_v7(), "n": name, "c": color }),
        )
        .await
}

async fn set_labels(
    world: &World,
    passport: &str,
    file_id: Uuid,
    labels: &[Uuid],
) -> serde_json::Value {
    world
        .gql(
            passport,
            "mutation($f:UUID!,$l:[UUID!]!){workspaceSetFileLabels(fileId:$f,labelIds:$l){success}}",
            serde_json::json!({ "f": file_id, "l": labels }),
        )
        .await
}

fn id_of(label: &serde_json::Value) -> Uuid {
    Uuid::parse_str(label["id"].as_str().unwrap()).unwrap()
}

#[tokio::test]
async fn labels_are_a_host_catalogue_managed_by_its_managers_and_read_live_by_everyone() {
    let world = World::start("pod-labels-crud").await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let reader = passport(Uuid::now_v7());
    let runner = service_passport(&["workspace:runner"]);
    let mut live =
        catalogue_subscription(&world, &reader, LABEL_DELTAS, "workspaceLabelsChanged").await;

    let refused = create_label(&world, &reader, "Not mine", "#123456").await;
    assert_eq!(error_code(&refused), "NOT_A_WORKSPACE_MANAGER");
    ok(&create_label(&world, &manager, "  Urgent ", "#FF0000").await);
    let created = next_delta(&mut live, "workspaceLabelsChanged", |node| {
        node["__typename"] == "DriveUpsert" && node["view"]["name"] == "Urgent"
    })
    .await;
    assert_eq!(
        created["view"]["color"], "#ff0000",
        "the colour is lowercased"
    );
    assert_eq!(created["view"]["description"], "");
    let urgent = id_of(&created["view"]);

    for (name, color, code) in [
        ("urgent", "#00ff00", "LABEL_NAME_TAKEN"),
        ("", "#00ff00", "INVALID_LABEL"),
        (&"x".repeat(101), "#00ff00", "INVALID_LABEL"),
        ("Blue", "blue", "INVALID_LABEL"),
        ("Blue", "#00f", "INVALID_LABEL"),
    ] {
        let refused = create_label(&world, &manager, name, color).await;
        assert_eq!(error_code(&refused), code, "{name:?} {color}");
    }
    ok(&create_label(&world, &manager, "Reviewed", "#00ff00").await);
    let listed = world.labels(&reader).await;
    assert_eq!(
        listed.len(),
        2,
        "any principal of the host reads the catalogue"
    );
    assert_eq!(
        listed[0]["name"], "Urgent",
        "listed in id order: the first created first"
    );
    assert_eq!(world.labels(&runner).await.len(), 2);

    ok(&world
        .gql(
            &manager,
            "mutation($id:UUID!,$d:String){workspaceUpdateLabel(id:$id,description:$d){success}}",
            serde_json::json!({ "id": urgent, "d": "Needs an answer this week" }),
        )
        .await);
    let updated = next_delta(&mut live, "workspaceLabelsChanged", |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "Updated"
    })
    .await;
    assert_eq!(updated["view"]["description"], "Needs an answer this week");
    let unchanged = world
        .gql(
            &manager,
            "mutation($id:UUID!,$d:String){workspaceUpdateLabel(id:$id,description:$d){success}}",
            serde_json::json!({ "id": urgent, "d": "Needs an answer this week" }),
        )
        .await;
    assert_eq!(error_code(&unchanged), "NOTHING_TO_CHANGE");
    let taken = world
        .gql(
            &manager,
            "mutation($id:UUID!,$n:String){workspaceUpdateLabel(id:$id,name:$n){success}}",
            serde_json::json!({ "id": urgent, "n": "REVIEWED" }),
        )
        .await;
    assert_eq!(error_code(&taken), "LABEL_NAME_TAKEN");
    let unknown = world
        .gql(
            &manager,
            "mutation($id:UUID!){workspaceDeleteLabel(id:$id){success}}",
            serde_json::json!({ "id": Uuid::now_v7() }),
        )
        .await;
    assert_eq!(error_code(&unknown), "LABEL_NOT_FOUND");
    ok(&world
        .gql(
            &manager,
            "mutation($id:UUID!){workspaceDeleteLabel(id:$id){success}}",
            serde_json::json!({ "id": urgent }),
        )
        .await);
    let removed = next_delta(&mut live, "workspaceLabelsChanged", |node| {
        node["__typename"] == "DriveRemove"
    })
    .await;
    assert_eq!(removed["key"], urgent.to_string());
    assert_eq!(world.labels(&reader).await.len(), 1);

    world.cleanup().await;
}

#[tokio::test]
async fn a_files_labels_are_a_target_set_that_survives_a_move_and_loses_a_deleted_label() {
    let world = World::start("pod-labels-files").await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    let outsider = passport(Uuid::now_v7());
    ok(&create_label(&world, &manager, "Urgent", "#ff0000").await);
    ok(&create_label(&world, &manager, "Reviewed", "#00ff00").await);
    let labels = world.labels(&owner).await;
    let by_name = |name: &str| id_of(labels.iter().find(|label| label["name"] == name).unwrap());
    let (reviewed, urgent) = (by_name("Reviewed"), by_name("Urgent"));
    let from = world.create_workspace(&owner, "from").await;
    let to = world.create_workspace(&owner, "to").await;
    let file_id = upload(
        &world,
        &owner,
        &UploadRequest::text(from, "", "tagged.txt", BYTES),
    )
    .await;
    world.await_source_promoted(file_id).await;
    let mut files = drive_subscription(&world, &owner, from).await;

    let foreign = set_labels(&world, &outsider, file_id, &[urgent]).await;
    assert_eq!(error_code(&foreign), "NOT_THE_WORKSPACE_OWNER");
    let unknown = set_labels(&world, &owner, file_id, &[urgent, Uuid::now_v7()]).await;
    assert_eq!(error_code(&unknown), "LABEL_NOT_FOUND");
    ok(&set_labels(&world, &owner, file_id, &[urgent, reviewed, urgent]).await);
    let tagged = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "LabelsChanged"
    })
    .await;
    assert_eq!(
        tagged["view"]["labelIds"],
        serde_json::json!([reviewed, urgent]),
        "the set is stored once and listed by label name"
    );
    let again = set_labels(&world, &owner, file_id, &[reviewed, urgent]).await;
    assert_eq!(
        error_code(&again),
        "NOTHING_TO_CHANGE",
        "the same target set is not silently acked"
    );
    ok(&set_labels(&world, &owner, file_id, &[urgent]).await);
    let narrowed = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "LabelsChanged"
    })
    .await;
    assert_eq!(narrowed["view"]["labelIds"], serde_json::json!([urgent]));
    assert_eq!(
        world.file(&owner, file_id).await["affordances"]["setLabels"]["allowed"],
        true
    );

    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!,$d:UUID){workspaceUpdateFile(fileId:$f,driveId:$d){success}}",
            serde_json::json!({ "f": file_id, "d": to }),
        )
        .await);
    assert_eq!(
        world.file(&owner, file_id).await["labelIds"],
        serde_json::json!([urgent]),
        "labels travel with the file across the host's drives"
    );

    let mut to_session = drive_subscription(&world, &owner, to).await;
    ok(&world
        .gql(
            &manager,
            "mutation($id:UUID!){workspaceDeleteLabel(id:$id){success}}",
            serde_json::json!({ "id": urgent }),
        )
        .await);
    let detached = next_drive_delta(&mut to_session, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "LabelsChanged"
    })
    .await;
    assert_eq!(detached["cause"]["detached"], urgent.to_string());
    assert_eq!(detached["view"]["labelIds"], serde_json::json!([]));
    let already_empty = set_labels(&world, &owner, file_id, &[]).await;
    assert_eq!(error_code(&already_empty), "NOTHING_TO_CHANGE");
    to_session.expect_silence(Duration::from_secs(1)).await;

    world.cleanup().await;
}
