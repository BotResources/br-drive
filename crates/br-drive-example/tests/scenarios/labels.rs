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

    let accented = "é".repeat(100);
    ok(&create_label(&world, &manager, &accented, "#0000ff").await);
    assert!(
        world
            .labels(&reader)
            .await
            .iter()
            .any(|label| label["name"] == accented),
        "a 100-character name is accepted whatever its byte length"
    );
    let long_description = world
        .gql(
            &manager,
            "mutation($id:UUID!,$d:String){workspaceCreateLabel(id:$id,name:\"Wordy\",color:\"#000000\",description:$d){success}}",
            serde_json::json!({ "id": Uuid::now_v7(), "d": "é".repeat(600) }),
        )
        .await;
    assert_eq!(error_code(&long_description), "INVALID_LABEL");
    let long_rename = world
        .gql(
            &manager,
            "mutation($id:UUID!,$d:String){workspaceUpdateLabel(id:$id,description:$d){success}}",
            serde_json::json!({ "id": urgent, "d": "é".repeat(600) }),
        )
        .await;
    assert_eq!(
        error_code(&long_rename),
        "INVALID_LABEL",
        "an update is bounded like a create, not by the database check"
    );
    for (name, color, code) in [
        ("urgent", "#00ff00", "LABEL_NAME_TAKEN"),
        ("", "#00ff00", "INVALID_LABEL"),
        (&"x".repeat(101), "#00ff00", "INVALID_LABEL"),
        (&"é".repeat(101), "#00ff00", "INVALID_LABEL"),
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
        3,
        "any principal of the host reads the catalogue"
    );
    assert_eq!(
        listed[0]["name"], "Urgent",
        "listed in id order: the first created first"
    );
    assert!(
        listed[0].get("createdBy").is_none(),
        "the creator's id is not on the wire"
    );
    assert_eq!(world.labels(&runner).await.len(), 3);

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
    assert_eq!(world.labels(&reader).await.len(), 2);

    world.cleanup().await;
}

#[tokio::test]
async fn two_concurrent_creates_of_one_label_name_answer_exactly_one_name_taken() {
    let world = World::start("pod-labels-concurrent").await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");

    let (left, right) = tokio::join!(
        create_label(&world, &manager, "Same Name", "#111111"),
        create_label(&world, &manager, "same name", "#222222"),
    );
    let mut codes: Vec<String> = [&left, &right]
        .into_iter()
        .map(|response| {
            if response.get("errors").is_none() {
                ok(response);
                "OK".to_string()
            } else {
                error_code(response)
            }
        })
        .collect();
    codes.sort();
    assert_eq!(
        codes,
        vec!["LABEL_NAME_TAKEN".to_string(), "OK".to_string()],
        "the advisory lock serializes the two saves: {left} / {right}"
    );
    assert_eq!(world.labels(&manager).await.len(), 1);

    world.cleanup().await;
}

#[tokio::test]
async fn deleting_a_label_on_more_files_than_the_threshold_resets_the_file_sessions() {
    let world = World::start("pod-labels-bulk-delete").await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let owner = passport(Uuid::now_v7());
    ok(&create_label(&world, &manager, "Everywhere", "#ff00ff").await);
    let everywhere = id_of(&world.labels(&owner).await[0]);
    let drive = world.create_workspace(&owner, "library").await;
    let mut files = Vec::new();
    for index in 0..4 {
        let name = format!("file-{index}.txt");
        let file_id = upload(
            &world,
            &owner,
            &UploadRequest::text(drive, "", &name, BYTES),
        )
        .await;
        ok(&set_labels(&world, &owner, file_id, &[everywhere]).await);
        files.push(file_id);
    }
    for file_id in &files {
        world.await_source_promoted(*file_id).await;
    }
    let mut session = drive_subscription(&world, &owner, drive).await;

    ok(&world
        .gql(
            &manager,
            "mutation($id:UUID!){workspaceDeleteLabel(id:$id){success}}",
            serde_json::json!({ "id": everywhere }),
        )
        .await);
    let reset = next_drive_delta(&mut session, |node| {
        node["__typename"] == "DriveReset" && node["views"].as_array().unwrap().len() == 4
    })
    .await;
    assert!(
        reset["views"]
            .as_array()
            .unwrap()
            .iter()
            .all(|view| view["labelIds"] == serde_json::json!([])),
        "past the threshold the session is reset from the committed state: {reset}"
    );
    for file_id in &files {
        assert_eq!(
            world.file(&owner, *file_id).await["labelIds"],
            serde_json::json!([])
        );
    }

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
