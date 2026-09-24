use std::time::Duration;

use uuid::Uuid;

use crate::harness::archive::ARCHIVE_RUNNER_SCOPE;
use crate::harness::runner::{RUNNER_SCOPE, Report, install_render_rule, report};
use crate::harness::upload::{Ticket, UploadRequest, post_bytes, sha256_hex, upload};
use crate::harness::{ArchiveHost, JobsStandIn, World, manager_passport, ok, service_passport};

const BYTES: &[u8] = b"personal bytes";
const REDACTED: Uuid = Uuid::nil();

async fn created_by(world: &World, table: &str, column: &str, id: Uuid) -> Uuid {
    sqlx::query_scalar(&format!("SELECT {column} FROM {table} WHERE id = $1"))
        .bind(id)
        .fetch_one(&world.db.app)
        .await
        .expect("read the row's person")
}

#[tokio::test]
async fn anonymising_a_person_rewrites_every_id_they_left_and_keeps_their_files() {
    let world = World::start("pod-erase-anonymise").await;
    let person_id = Uuid::now_v7();
    let person = manager_passport(person_id, "Ada");
    let runner = service_passport(&[RUNNER_SCOPE]);
    install_render_rule(&world, &person).await;
    let drive = world.create_workspace(&person, "mine").await;
    let file_id = upload(
        &world,
        &person,
        &UploadRequest::text(drive, "", "mine.txt", BYTES),
    )
    .await;
    let job_id = world.await_job(file_id).await;
    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id,
            pages: vec![(1, "a page")],
            origin: None,
            indexer: None,
            done: false,
        },
    )
    .await);
    ok(&world
        .gql(
            &person,
            "mutation($id:UUID!,$n:String!,$c:String!){workspaceCreateLabel(id:$id,name:$n,color:$c){success}}",
            serde_json::json!({ "id": Uuid::now_v7(), "n": "Mine", "c": "#123456" }),
        )
        .await);
    let label = Uuid::parse_str(world.labels(&person).await[0]["id"].as_str().unwrap()).unwrap();
    ok(&world
        .gql(
            &person,
            "mutation($f:UUID!,$l:[UUID!]!){workspaceSetFileLabels(fileId:$f,labelIds:$l){success}}",
            serde_json::json!({ "f": file_id, "l": [label] }),
        )
        .await);
    let ruleset =
        Uuid::parse_str(world.rulesets(&person).await[0]["id"].as_str().unwrap()).unwrap();

    let outcome = world.erase(person_id).await;
    assert!(outcome.fresh);
    assert!(
        outcome.rows_erased >= 6,
        "drive, file, page, label, link, rule: {outcome:?}"
    );
    assert_eq!(outcome.blobs_purged, 0, "anonymising keeps every object");

    assert_eq!(
        created_by(&world, "drive.drive", "created_by", drive).await,
        REDACTED
    );
    assert_eq!(
        created_by(&world, "drive.file", "created_by", file_id).await,
        REDACTED
    );
    assert_eq!(
        created_by(&world, "drive.label", "created_by", label).await,
        REDACTED
    );
    assert_eq!(
        created_by(&world, "drive.ruleset", "created_by", ruleset).await,
        REDACTED
    );
    let page_by: Uuid =
        sqlx::query_scalar("SELECT updated_by FROM drive.file_page WHERE file_id = $1")
            .bind(file_id)
            .fetch_one(&world.db.app)
            .await
            .unwrap();
    assert!(
        page_by != REDACTED && page_by != person_id,
        "the runner wrote the page, not the person: {page_by}"
    );
    let link_by: Uuid =
        sqlx::query_scalar("SELECT created_by FROM drive.file_label WHERE file_id = $1")
            .bind(file_id)
            .fetch_one(&world.db.app)
            .await
            .unwrap();
    assert_eq!(link_by, REDACTED);
    let initiator: serde_json::Value =
        sqlx::query_scalar("SELECT triggered_by FROM drive.file_job WHERE job_id = $1")
            .bind(job_id)
            .fetch_one(&world.db.app)
            .await
            .unwrap();
    assert_eq!(initiator["id"], REDACTED.to_string());
    assert!(
        initiator["display_name"].is_null(),
        "the display name is gone: {initiator}"
    );
    let file = world.file(&person, file_id).await;
    assert_eq!(
        file["processingState"], "PROCESSING",
        "the file itself stays"
    );
    assert_eq!(file["createdBy"], REDACTED.to_string());
    assert_eq!(world.file_pages(&person, file_id).await.len(), 1);

    let replay = world.erase(person_id).await;
    assert!(
        !replay.fresh,
        "a second erase of the same person is absorbed"
    );
    assert_eq!(
        created_by(&world, "drive.file", "created_by", file_id).await,
        REDACTED
    );

    world.cleanup().await;
}

#[tokio::test]
async fn deleting_a_person_removes_their_files_and_purges_their_objects() {
    let world = World::start("pod-erase-delete-a").await;
    let archive = ArchiveHost::start(&world, "pod-erase-delete-b").await;
    let jobs = JobsStandIn::attach(&world).await;
    let person_id = Uuid::now_v7();
    let person = manager_passport(person_id, "Bea");
    let other_id = Uuid::now_v7();
    let other = manager_passport(other_id, "Cy");
    let runner = service_passport(&[ARCHIVE_RUNNER_SCOPE]);
    ok(&archive
        .gql(
            &world,
            &person,
            "mutation($id:UUID!){archiveVaultCreateRuleset(id:$id,name:\"render\",trigger:UPLOAD,mediaTypes:[\"text/plain\"],steps:[{runnerType:\"render\"}],isDefault:true){id}}",
            serde_json::json!({ "id": Uuid::now_v7() }),
        )
        .await);
    let drive = archive.drive_for(person_id).await;
    let others_drive = archive.drive_for(other_id).await;

    let mine = archive_upload(&world, &archive, &person, drive, "mine.txt").await;
    let theirs = archive_upload(&world, &archive, &other, others_drive, "theirs.txt").await;
    let my_job = archive.job_of(mine).await.expect("my chain started");
    let their_job = archive.job_of(theirs).await.expect("their chain started");
    let create = jobs.await_create(mine).await;
    assert_eq!(create.job_id, my_job);
    assert_eq!(jobs.await_create(theirs).await.job_id, their_job);
    ok(&archive
        .gql(
            &world,
            &runner,
            "mutation($f:UUID!,$j:UUID!){archiveVaultRunnerReport(fileId:$f,jobId:$j,pages:[{number:1,markdown:\"mine\"}]){success}}",
            serde_json::json!({ "f": mine, "j": my_job }),
        )
        .await);
    let source = archive_source(&archive, mine).await;

    let outcome = archive.erase(person_id).await;
    assert!(outcome.fresh);
    assert!(
        outcome.rows_erased >= 2,
        "the file row and the drive's created_by: {outcome:?}"
    );
    assert_eq!(outcome.blobs_purged, 1, "the source object is purged");
    let late = archive
        .gql(
            &world,
            &runner,
            "mutation($f:UUID!,$j:UUID!){archiveVaultRunnerReport(fileId:$f,jobId:$j,pages:[{number:2,markdown:\"late\"}]){success}}",
            serde_json::json!({ "f": mine, "j": my_job }),
        )
        .await;
    assert_eq!(
        crate::harness::error_code(&late),
        "FILE_NOT_FOUND",
        "the runner of the erased file is refused from now on"
    );
    assert!(
        archive.file_state(&world, &person, mine).await.is_null(),
        "the person's file is gone"
    );
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM drive.file WHERE id = $1")
        .bind(mine)
        .fetch_one(&archive.db.app)
        .await
        .unwrap();
    assert_eq!(rows, 0);
    let blob: Option<String> =
        sqlx::query_scalar("SELECT state FROM service_engine.blob WHERE id = $1")
            .bind(source)
            .fetch_optional(&archive.db.app)
            .await
            .unwrap();
    assert!(
        blob.is_none() || blob.as_deref() == Some("orphaned"),
        "the object is purged or on its way: {blob:?}"
    );
    let drive_by: Uuid = sqlx::query_scalar("SELECT created_by FROM drive.drive WHERE id = $1")
        .bind(drive)
        .fetch_one(&archive.db.app)
        .await
        .unwrap();
    assert_eq!(
        drive_by, REDACTED,
        "the drive is the host's to delete; its person is anonymised"
    );
    let their_file = archive.file_state(&world, &other, theirs).await;
    assert_eq!(
        their_file["processingState"], "PROCESSING",
        "the other person's file and job are untouched"
    );
    assert_eq!(archive.job_of(theirs).await, Some(their_job));
    jobs.expect_no_command(Duration::from_millis(500)).await;

    let replay = archive.erase(person_id).await;
    assert!(!replay.fresh);
    assert_eq!(replay.blobs_purged, 0);

    archive.shutdown().await;
    world.cleanup().await;
}

async fn archive_upload(
    world: &World,
    archive: &ArchiveHost,
    owner: &str,
    drive: Uuid,
    name: &str,
) -> Uuid {
    let file_id = Uuid::now_v7();
    let response = archive
        .gql(
            world,
            owner,
            "mutation($f:UUID!,$d:UUID!,$p:String!,$n:String!,$m:String!,$s:ByteCount!,$h:String!){\
             archiveVaultRequestUpload(fileId:$f,driveId:$d,path:$p,name:$n,mediaType:$m,size:$s,sha256:$h){fileId url fields}}",
            serde_json::json!({
                "f": file_id, "d": drive, "p": "", "n": name, "m": "text/plain",
                "s": BYTES.len(), "h": sha256_hex(BYTES),
            }),
        )
        .await;
    let post = &ok(&response)["archiveVaultRequestUpload"];
    let ticket = Ticket {
        file_id,
        url: post["url"].as_str().unwrap().to_string(),
        fields: post["fields"].as_object().unwrap().clone(),
    };
    let status = post_bytes(world, &ticket, BYTES, name).await;
    assert!((200..300).contains(&status));
    ok(&archive
        .gql(
            world,
            owner,
            "mutation($f:UUID!){archiveVaultCommitUpload(fileId:$f){success}}",
            serde_json::json!({ "f": file_id }),
        )
        .await);
    file_id
}

async fn archive_source(archive: &ArchiveHost, file_id: Uuid) -> Uuid {
    sqlx::query_scalar("SELECT blob_ref FROM drive.file WHERE id = $1")
        .bind(file_id)
        .fetch_one(&archive.db.app)
        .await
        .expect("the archive file's source")
}
