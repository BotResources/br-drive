use std::collections::BTreeSet;

use uuid::Uuid;

use crate::harness::upload::{UploadRequest, request, ticket, upload};
use crate::harness::{World, error_code, ok, passport};

const BYTES: &[u8] = b"concurrent bytes";

#[tokio::test]
async fn two_concurrent_uploads_of_one_name_into_one_folder_serialize_on_the_drive() {
    let world = World::start("pod-concurrent-upload").await;
    let owner = passport(Uuid::now_v7());
    let drive = world.create_workspace(&owner, "library").await;
    let request_a = UploadRequest::text(drive, "shared", "report.pdf", BYTES);
    let request_b = UploadRequest::text(drive, "shared", "report.pdf", b"other bytes");
    let (first, second) = (Uuid::now_v7(), Uuid::now_v7());

    let (a, b) = tokio::join!(
        request(&world, &owner, first, &request_a),
        request(&world, &owner, second, &request_b),
    );
    ticket(&a);
    ticket(&b);

    let mut names = BTreeSet::new();
    for id in [first, second] {
        names.insert(
            world.file(&owner, id).await["name"]
                .as_str()
                .unwrap()
                .to_string(),
        );
    }
    assert_eq!(
        names,
        BTreeSet::from(["report.pdf".to_string(), "report (1).pdf".to_string()]),
        "both requests are acked and the second derives its counter under the drive lock"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn two_concurrent_renames_onto_one_name_answer_exactly_one_name_taken() {
    let world = World::start("pod-concurrent-rename").await;
    let owner = passport(Uuid::now_v7());
    let drive = world.create_workspace(&owner, "library").await;
    let a = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "docs", "a.txt", BYTES),
    )
    .await;
    let b = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "docs", "b.txt", BYTES),
    )
    .await;

    let rename = |file: Uuid| {
        world.gql(
            &owner,
            "mutation($f:UUID!,$n:String){workspaceUpdateFile(fileId:$f,name:$n){success}}",
            serde_json::json!({ "f": file, "n": "winner.txt" }),
        )
    };
    let (left, right) = tokio::join!(rename(a), rename(b));
    let codes: Vec<String> = [&left, &right]
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
    let mut sorted = codes.clone();
    sorted.sort();
    assert_eq!(
        sorted,
        vec!["NAME_TAKEN".to_string(), "OK".to_string()],
        "one rename wins and the other is refused with a code, never an internal error: {codes:?}"
    );

    let names: BTreeSet<String> = world
        .drive_files(&owner, drive)
        .await
        .iter()
        .map(|file| file["name"].as_str().unwrap().to_string())
        .collect();
    assert!(names.contains("winner.txt"));
    assert_eq!(names.len(), 2);

    world.cleanup().await;
}
