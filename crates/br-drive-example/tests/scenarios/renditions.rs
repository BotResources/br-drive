//! File renditions: a runner hands back a binary file derived from the
//! source (`<p>RunnerRequestRenditionUpload`), stored in the host's object
//! storage beside it. It is replaced by name only when the new object lands,
//! survives the chain's conclusion, is read by a later step through
//! `<p>RunnerContext` and by a reader through `<p>RenditionAccess`, and its
//! object leaves storage with its file on every delete path.

use std::time::Duration;

use uuid::Uuid;

use crate::harness::archive::{ARCHIVE_RUNNER_SCOPE, ArchiveHost};
use crate::harness::runner::{
    INDEX, RENDER, RUNNER_SCOPE, Report, RuleSpec, context, create_ruleset, install_render_rule,
    rendition_ticket, report, request_rendition, upload_rendition,
};
use crate::harness::upload::{Ticket, UploadRequest, post_bytes, sha256_hex, upload_processed};
use crate::harness::{
    Fact, JobsStandIn, World, drive_subscription, error_code, manager_passport, next_drive_delta,
    ok, passport, service_passport,
};
use crate::poll_until;

const SOURCE: &[u8] = b"a database dump, as uploaded";
const DUCKDB: &[u8] = b"DUCK an analytic database derived from the dump";
const PARQUET: &[u8] = b"PAR1 a columnar export derived from the dump";
const NAME: &str = "base.duckdb";

fn of_kind<'a>(facts: &'a [Fact], kind: &str) -> Vec<&'a Fact> {
    facts
        .iter()
        .filter(|fact| fact.event_type == kind)
        .collect()
}

async fn file_facts(world: &World, file_id: Uuid) -> Vec<Fact> {
    world.facts("drive_file", serde_json::json!(file_id)).await
}

/// A file under a one-step rule, its job running.
async fn file_with_job(
    world: &World,
    jobs: &JobsStandIn,
    owner: &str,
    drive: Uuid,
    path: &str,
    name: &str,
) -> (Uuid, Uuid) {
    let file_id = upload_processed(
        world,
        owner,
        &UploadRequest::text(drive, path, name, SOURCE),
    )
    .await;
    let job_id = jobs.await_create(file_id).await.job_id;
    (file_id, job_id)
}

/// Uploads a rendition and waits until it landed; answers its blob and its
/// object key.
async fn landed_rendition(
    world: &World,
    runner: &str,
    file_id: Uuid,
    job_id: Uuid,
    name: &str,
    bytes: &[u8],
) -> (Uuid, String) {
    let key = upload_rendition(world, runner, file_id, job_id, name, "duckdb", bytes).await;
    let (reference, _, _, _) = poll_until!(Duration::from_secs(15), {
        world
            .rendition_row(file_id, name)
            .await
            .filter(|(_, _, _, landed)| *landed)
    });
    (reference, key)
}

async fn get(world: &World, url: &str) -> Vec<u8> {
    world
        .http
        .get(url)
        .send()
        .await
        .expect("GET the object")
        .bytes()
        .await
        .expect("read the object")
        .to_vec()
}

fn access_url(response: &serde_json::Value) -> Option<String> {
    ok(response)["workspaceRenditionAccess"]
        .as_str()
        .map(str::to_string)
}

#[tokio::test]
async fn a_rendition_lands_and_is_served_to_the_runner_and_the_reader_never_to_anyone_else() {
    // Given: a file whose job runs, its owner watching the drive
    let world = World::start("pod-rendition-land").await;
    let jobs = JobsStandIn::attach(&world).await;
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    install_render_rule(&world, &manager_passport(Uuid::now_v7(), "Ada")).await;
    let drive = world.create_workspace(&owner, "library").await;
    let (file_id, job_id) = file_with_job(&world, &jobs, &owner, drive, "", "dump.txt").await;
    let mut files = drive_subscription(&world, &owner, drive).await;

    // When: the runner asks to hand back a rendition
    let ticket = rendition_ticket(
        &request_rendition(&world, &runner, file_id, job_id, NAME, "duckdb", DUCKDB).await,
    );

    // Then: the request is announced, and nothing is readable before the
    // object lands
    next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "RenditionRequested"
    })
    .await;
    assert_eq!(
        world.file(&owner, file_id).await["renditions"],
        serde_json::json!([]),
        "a rendition is listed once landed"
    );
    assert!(access_url(&world.rendition_access(&owner, file_id, NAME).await).is_none());
    // (the context answers once the engine reaper promoted the source)
    let before = poll_until!(Duration::from_secs(15), {
        let response = context(&world, &runner, file_id, job_id).await;
        (response.get("errors").is_none())
            .then(|| response["data"]["workspaceRunnerContext"].clone())
    });
    assert_eq!(before["renditions"], serde_json::json!([]));

    // When: its bytes land
    let status = post_bytes(&world, &ticket, DUCKDB, NAME).await;
    assert!(
        (200..300).contains(&status),
        "the rendition lands: {status}"
    );

    // Then: the drive view lists it, announced as available
    let available = next_drive_delta(&mut files, |node| {
        node["__typename"] == "DriveUpsert" && node["cause"]["kind"] == "RenditionAvailable"
    })
    .await;
    assert_eq!(available["cause"]["name"], NAME);
    assert_eq!(available["view"]["renditions"][0]["name"], NAME);
    assert_eq!(available["view"]["renditions"][0]["kind"], "duckdb");
    let listed = world.file(&owner, file_id).await["renditions"].clone();
    assert_eq!(listed.as_array().unwrap().len(), 1);
    assert_eq!(listed[0]["name"], NAME);
    assert_eq!(listed[0]["kind"], "duckdb");
    assert_eq!(listed[0]["mediaType"], "application/octet-stream");
    assert_eq!(listed[0]["sizeBytes"], DUCKDB.len());
    assert!(listed[0]["landedAt"].is_string());

    // And: the runner reads it through its context, a fresh GET at each call
    let first = ok(&context(&world, &runner, file_id, job_id).await)["workspaceRunnerContext"]
        ["renditions"]
        .clone();
    assert_eq!(first.as_array().unwrap().len(), 1);
    assert_eq!(first[0]["name"], NAME);
    assert_eq!(first[0]["kind"], "duckdb");
    assert_eq!(first[0]["sizeBytes"], DUCKDB.len());
    assert_eq!(get(&world, first[0]["url"].as_str().unwrap()).await, DUCKDB);
    let again = ok(&context(&world, &runner, file_id, job_id).await)["workspaceRunnerContext"]
        ["renditions"][0]["url"]
        .as_str()
        .expect("a presigned GET on every call")
        .to_string();
    assert_eq!(get(&world, &again).await, DUCKDB);

    // And: the reader gets an attachment GET on it
    let url = access_url(&world.rendition_access(&owner, file_id, NAME).await)
        .expect("the owner reads the rendition");
    assert!(
        url.contains("response-content-disposition=attachment"),
        "a rendition is a download: {url}"
    );
    assert_eq!(get(&world, &url).await, DUCKDB);
    assert!(
        access_url(
            &world
                .rendition_access(&owner, file_id, "other.duckdb")
                .await
        )
        .is_none(),
        "an unknown name resolves to nothing"
    );
    assert!(
        access_url(&world.rendition_access(&owner, Uuid::now_v7(), NAME).await).is_none(),
        "an unknown file resolves to nothing"
    );

    // And: a person without access to the drive, and the runner through the
    // drive views, read nothing
    let outsider = passport(Uuid::now_v7());
    assert!(access_url(&world.rendition_access(&outsider, file_id, NAME).await).is_none());
    assert!(world.file(&outsider, file_id).await.is_null());
    assert!(access_url(&world.rendition_access(&runner, file_id, NAME).await).is_none());
    assert!(world.file(&runner, file_id).await.is_null());
    assert!(world.drive_files(&runner, drive).await.is_empty());

    // And: the host holds a fact for the request and one for the landing
    let facts = file_facts(&world, file_id).await;
    let issued = of_kind(&facts, "RenditionUploadTicketIssued");
    assert_eq!(issued.len(), 1, "{facts:?}");
    assert_eq!(issued[0].payload["name"], NAME);
    assert_eq!(issued[0].payload["rendition_kind"], "duckdb");
    assert!(issued[0].is_runner);
    let landed = of_kind(&facts, "RenditionAvailable");
    assert_eq!(landed.len(), 1, "{facts:?}");
    assert_eq!(landed[0].payload["name"], NAME);
    assert_eq!(landed[0].payload["rendition_kind"], "duckdb");
    assert!(of_kind(&facts, "RenditionReplaced").is_empty());

    world.cleanup().await;
}

#[tokio::test]
async fn a_later_step_reads_the_rendition_and_a_final_report_with_no_pages_keeps_it() {
    // Given: a two-step rule — a converter, then a step reading its output
    let world = World::start("pod-rendition-chain").await;
    let jobs = JobsStandIn::attach(&world).await;
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    ok(&create_ruleset(
        &world,
        &manager_passport(Uuid::now_v7(), "Ada"),
        RuleSpec {
            name: "convert then index",
            trigger: "UPLOAD",
            media_types: &["text/plain"],
            steps: &[
                (RENDER, serde_json::json!({})),
                (INDEX, serde_json::json!({})),
            ],
            is_default: true,
        },
    )
    .await);
    let drive = world.create_workspace(&owner, "library").await;
    let (file_id, first) = file_with_job(&world, &jobs, &owner, drive, "", "dump.txt").await;

    // When: the first step hands back a rendition and ends with no page
    let (reference, key) = landed_rendition(&world, &runner, file_id, first, NAME, DUCKDB).await;
    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id: first,
            pages: Vec::new(),
            origin: None,
            indexer: None,
            done: true,
        },
    )
    .await);
    jobs.await_finish(first).await;
    let second = jobs.await_create(file_id).await.job_id;
    assert_ne!(second, first);

    // Then: the second step reads it through its own context
    let read = ok(&context(&world, &runner, file_id, second).await)["workspaceRunnerContext"]
        ["renditions"]
        .clone();
    assert_eq!(read.as_array().unwrap().len(), 1);
    assert_eq!(get(&world, read[0]["url"].as_str().unwrap()).await, DUCKDB);

    // When: the second step concludes the chain with zero pages
    ok(&report(
        &world,
        &runner,
        file_id,
        Report {
            job_id: second,
            pages: Vec::new(),
            origin: None,
            indexer: Some(("A database with no page.", 0, 0)),
            done: true,
        },
    )
    .await);

    // Then: the file is READY with no page, and the rendition is still there,
    // its object held
    let ready = world.await_state(&owner, file_id, "READY").await;
    assert_eq!(ready["pageCount"], 0);
    assert!(world.file_pages(&owner, file_id).await.is_empty());
    let file = world.file(&owner, file_id).await;
    assert_eq!(file["renditions"][0]["name"], NAME);
    let url = access_url(&world.rendition_access(&owner, file_id, NAME).await)
        .expect("the rendition survives the chain's conclusion");
    assert_eq!(get(&world, &url).await, DUCKDB);
    assert_eq!(
        world.blob_state(reference).await.as_deref(),
        Some("uploaded")
    );
    assert!(world.object_exists(&key).await);
    // And: a runner whose job ended reads nothing any more
    assert_eq!(
        error_code(&context(&world, &runner, file_id, second).await),
        "JOB_NOT_ACTIVE"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn replacing_a_rendition_keeps_the_old_object_readable_until_the_new_one_lands() {
    // Given: a landed rendition
    let world = World::start("pod-rendition-replace").await;
    let jobs = JobsStandIn::attach(&world).await;
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    install_render_rule(&world, &manager_passport(Uuid::now_v7(), "Ada")).await;
    let drive = world.create_workspace(&owner, "library").await;
    let (file_id, job_id) = file_with_job(&world, &jobs, &owner, drive, "", "dump.txt").await;
    let (old, old_key) = landed_rendition(&world, &runner, file_id, job_id, NAME, DUCKDB).await;

    // When: the runner asks to replace it, under another kind
    let replacement = rendition_ticket(
        &request_rendition(&world, &runner, file_id, job_id, NAME, "parquet", PARQUET).await,
    );

    // Then: the landed object and its kind stay current, nothing is released
    let (current, pending, kind, _) = world.rendition_row(file_id, NAME).await.unwrap();
    assert_eq!(current, old);
    assert_eq!(kind, "duckdb");
    let new = pending.expect("the replacement is pending on the row");
    assert_eq!(world.blob_state(old).await.as_deref(), Some("uploaded"));
    assert_eq!(
        world.file(&owner, file_id).await["renditions"][0]["kind"],
        "duckdb"
    );
    // And: a re-request while it is in flight is refused; the first ticket
    // stands
    let retried =
        request_rendition(&world, &runner, file_id, job_id, NAME, "parquet", PARQUET).await;
    assert_eq!(error_code(&retried), "RENDITION_UPLOAD_PENDING");
    assert_eq!(
        world.rendition_row(file_id, NAME).await.unwrap().1,
        Some(new)
    );

    // When: the replacement upload fails
    let status = post_bytes(&world, &replacement, b"not the pinned bytes", NAME).await;
    assert!(
        !(200..300).contains(&status),
        "refused by storage: {status}"
    );

    // Then: the old object is still what a reader gets
    let url = access_url(&world.rendition_access(&owner, file_id, NAME).await)
        .expect("the old rendition is still readable");
    assert_eq!(get(&world, &url).await, DUCKDB);

    // When: the replacement lands
    let status = post_bytes(&world, &replacement, PARQUET, NAME).await;
    assert!(
        (200..300).contains(&status),
        "the replacement lands: {status}"
    );

    // Then: the row swaps in the landing's transaction, the old object is
    // released, and readers get the new one under its kind
    poll_until!(Duration::from_secs(15), {
        (world.rendition_row(file_id, NAME).await.unwrap().0 == new).then_some(())
    });
    let (_, pending, kind, landed) = world.rendition_row(file_id, NAME).await.unwrap();
    assert!(pending.is_none());
    assert_eq!(kind, "parquet");
    assert!(landed);
    assert_eq!(world.blob_state(old).await.as_deref(), Some("orphaned"));
    let listed = world.file(&owner, file_id).await["renditions"].clone();
    assert_eq!(
        listed.as_array().unwrap().len(),
        1,
        "one rendition per name"
    );
    assert_eq!(listed[0]["kind"], "parquet");
    assert_eq!(listed[0]["sizeBytes"], PARQUET.len());
    let url = poll_until!(Duration::from_secs(15), {
        match access_url(&world.rendition_access(&owner, file_id, NAME).await) {
            Some(url) => (get(&world, &url).await == PARQUET).then_some(url),
            None => None,
        }
    });
    assert!(url.contains("response-content-disposition=attachment"));
    let read = ok(&context(&world, &runner, file_id, job_id).await)["workspaceRunnerContext"]
        ["renditions"]
        .clone();
    assert_eq!(read[0]["kind"], "parquet");
    assert_eq!(get(&world, read[0]["url"].as_str().unwrap()).await, PARQUET);
    // And: the reaper deletes the old object from storage
    world.await_object_gone(old, &old_key).await;
    // And: the host holds the replacement as such
    let facts = file_facts(&world, file_id).await;
    assert_eq!(of_kind(&facts, "RenditionUploadTicketIssued").len(), 2);
    assert_eq!(of_kind(&facts, "RenditionAvailable").len(), 1);
    let replaced = of_kind(&facts, "RenditionReplaced");
    assert_eq!(replaced.len(), 1, "{facts:?}");
    assert_eq!(replaced[0].payload["rendition_kind"], "parquet");

    world.cleanup().await;
}

#[tokio::test]
async fn a_rendition_request_is_refused_off_the_runner_scope_the_active_job_and_its_bounds() {
    // Given: a file whose job runs
    let world = World::start("pod-rendition-refusals").await;
    let jobs = JobsStandIn::attach(&world).await;
    let owner = passport(Uuid::now_v7());
    let runner = service_passport(&[RUNNER_SCOPE]);
    install_render_rule(&world, &manager_passport(Uuid::now_v7(), "Ada")).await;
    let drive = world.create_workspace(&owner, "library").await;
    let (file_id, job_id) = file_with_job(&world, &jobs, &owner, drive, "", "dump.txt").await;
    let host = &world;
    let request = |principal: String, job: Uuid, name: String, kind: String, size: u64| async move {
        host
            .gql(
                &principal,
                "mutation($f:UUID!,$j:UUID!,$n:String!,$k:String!,$m:String!,$s:ByteCount!,$h:String!){\
                 workspaceRunnerRequestRenditionUpload(fileId:$f,jobId:$j,name:$n,kind:$k,mediaType:$m,size:$s,sha256:$h)\
                 {fileId url fields}}",
                serde_json::json!({
                    "f": file_id, "j": job, "n": name, "k": kind,
                    "m": "application/octet-stream", "s": size, "h": sha256_hex(DUCKDB),
                }),
            )
            .await
    };
    let size = DUCKDB.len() as u64;
    let runner = runner.as_str();

    // Then: a person, and a service without the runner scope, are refused
    // before any lookup
    let unscoped = service_passport(&["workspace:other"]);
    for principal in [&owner, &unscoped] {
        let principal: &String = principal;
        assert_eq!(
            error_code(
                &request(
                    principal.clone(),
                    job_id,
                    NAME.into(),
                    "duckdb".into(),
                    size
                )
                .await
            ),
            "RUNNER_SCOPE_REQUIRED"
        );
    }
    // And: a job that is not the file's active one is refused
    assert_eq!(
        error_code(
            &request(
                runner.into(),
                Uuid::now_v7(),
                NAME.into(),
                "duckdb".into(),
                size
            )
            .await
        ),
        "JOB_NOT_ACTIVE"
    );
    // And: a name or a kind off its grammar is refused
    let long_name = "a".repeat(65);
    for name in [
        "",
        ".duckdb",
        "Base.duckdb",
        "a/b",
        "a b",
        long_name.as_str(),
    ] {
        assert_eq!(
            error_code(&request(runner.into(), job_id, name.into(), "duckdb".into(), size).await),
            "INVALID_RENDITION_NAME",
            "{name:?}"
        );
    }
    let long_kind = "a".repeat(33);
    for kind in ["", "DuckDB", "duck-db", "2fast", long_kind.as_str()] {
        assert_eq!(
            error_code(&request(runner.into(), job_id, NAME.into(), kind.into(), size).await),
            "INVALID_RENDITION_KIND",
            "{kind:?}"
        );
    }
    // And: past the host's RENDITION_MAX_BYTES the request is refused (the
    // example host bounds renditions at 2 MiB, images at 1 MiB)
    assert_eq!(
        error_code(&request(runner.into(), job_id, NAME.into(), "duckdb".into(), 3 << 20).await),
        "FILE_TOO_LARGE"
    );
    let within = request(
        runner.into(),
        job_id,
        "big.duckdb".into(),
        "duckdb".into(),
        3 << 19,
    )
    .await;
    assert!(
        within.get("errors").is_none(),
        "a rendition above the image bound but within its own is accepted: {within}"
    );

    // And: nothing was stored for any refusal
    let rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM drive.file_rendition WHERE file_id = $1 AND name <> 'big.duckdb'",
    )
    .bind(file_id)
    .fetch_one(&world.db.app)
    .await
    .unwrap();
    assert_eq!(rows, 0);
    let facts = file_facts(&world, file_id).await;
    assert_eq!(of_kind(&facts, "RenditionUploadTicketIssued").len(), 1);

    world.cleanup().await;
}

/// The objects of a file's renditions — a landed one, and a replacement
/// requested but never uploaded, pending on the row — after its runner
/// handed them back.
async fn file_with_renditions(
    world: &World,
    jobs: &JobsStandIn,
    owner: &str,
    runner: &str,
    drive: Uuid,
    path: &str,
    name: &str,
) -> (Uuid, Vec<(Uuid, String)>) {
    let (file_id, job_id) = file_with_job(world, jobs, owner, drive, path, name).await;
    let landed = landed_rendition(world, runner, file_id, job_id, NAME, DUCKDB).await;
    let ticket = rendition_ticket(
        &request_rendition(world, runner, file_id, job_id, NAME, "duckdb", PARQUET).await,
    );
    let (_, pending, _, _) = world.rendition_row(file_id, NAME).await.unwrap();
    let pending = pending.expect("a replacement in flight");
    (
        file_id,
        vec![
            landed,
            (pending, ticket.fields["key"].as_str().unwrap().to_string()),
        ],
    )
}

#[tokio::test]
async fn deleting_a_file_a_folder_or_a_drive_deletes_its_rendition_objects_from_storage() {
    // Given: four files holding renditions — one deleted alone, one with its
    // folder, one with its drive, one with a drive deleted from a reaction —
    // each with a replacement requested and never uploaded
    let world = World::start("pod-rendition-delete").await;
    let jobs = JobsStandIn::attach(&world).await;
    let owner = manager_passport(Uuid::now_v7(), "Ada");
    let runner = service_passport(&[RUNNER_SCOPE]);
    install_render_rule(&world, &owner).await;
    let library = world.create_workspace(&owner, "library").await;
    let archive = world.create_workspace(&owner, "archive").await;
    let retracted = world.create_workspace(&owner, "retracted").await;
    let (alone, alone_objects) =
        file_with_renditions(&world, &jobs, &owner, &runner, library, "", "alone.txt").await;
    let (filed, filed_objects) = file_with_renditions(
        &world,
        &jobs,
        &owner,
        &runner,
        library,
        "dumps",
        "filed.txt",
    )
    .await;
    let (archived, archived_objects) =
        file_with_renditions(&world, &jobs, &owner, &runner, archive, "", "archived.txt").await;
    let (gone, gone_objects) =
        file_with_renditions(&world, &jobs, &owner, &runner, retracted, "", "gone.txt").await;
    let keeper = file_with_renditions(&world, &jobs, &owner, &runner, library, "", "keeper.txt")
        .await
        .1;

    // When: the file is deleted, then the folder, then a drive, then the
    // other drive from the host's retract reaction
    ok(&world
        .gql(
            &owner,
            "mutation($f:UUID!){workspaceDeleteFile(fileId:$f){success}}",
            serde_json::json!({ "f": alone }),
        )
        .await);
    ok(&world
        .gql(
            &owner,
            "mutation($d:UUID!){workspaceDeleteFolder(driveId:$d,prefix:\"dumps/\"){success}}",
            serde_json::json!({ "d": library }),
        )
        .await);
    ok(&world
        .gql(
            &owner,
            "mutation($id:UUID!){workspaceDelete(id:$id){success}}",
            serde_json::json!({ "id": archive }),
        )
        .await);
    let message = world.send_retract(retracted).await;
    world.await_consumed(message).await;

    // Then: every rendition object of each file is released by its gesture
    for (file, objects) in [
        (alone, &alone_objects),
        (filed, &filed_objects),
        (archived, &archived_objects),
        (gone, &gone_objects),
    ] {
        assert!(world.file(&owner, file).await.is_null());
        for (reference, _) in objects {
            assert_eq!(
                world.blob_state(*reference).await.as_deref(),
                Some("orphaned"),
                "the rendition object of {file} is released with it"
            );
        }
    }
    // And: once past RENDITION_ORPHAN_AFTER the reaper deletes them from
    // storage
    for objects in [
        &alone_objects,
        &filed_objects,
        &archived_objects,
        &gone_objects,
    ] {
        for (reference, key) in objects {
            world.await_object_gone(*reference, key).await;
        }
    }
    // And: the file left in place keeps its rendition
    let (landed, landed_key) = &keeper[0];
    assert_ne!(world.blob_state(*landed).await.as_deref(), Some("orphaned"));
    assert!(world.object_exists(landed_key).await);

    world.cleanup().await;
}

#[tokio::test]
async fn erasing_a_person_purges_the_rendition_objects_of_their_files() {
    // Given: a host erasing by deletion, and a file of the person whose
    // runner handed back a rendition
    let world = World::start("pod-rendition-erase-a").await;
    let archive = ArchiveHost::start(&world, "pod-rendition-erase-b").await;
    let jobs = JobsStandIn::attach(&world).await;
    let person_id = Uuid::now_v7();
    let person = manager_passport(person_id, "Bea");
    let runner = service_passport(&[ARCHIVE_RUNNER_SCOPE]);
    ok(&archive
        .gql(
            &world,
            &person,
            "mutation($id:UUID!){archiveVaultCreateRuleset(id:$id,name:\"convert\",trigger:UPLOAD,mediaTypes:[\"text/plain\"],steps:[{runnerType:\"render\"}],isDefault:true){id}}",
            serde_json::json!({ "id": Uuid::now_v7() }),
        )
        .await);
    let drive = archive.drive_for(person_id).await;
    let file_id = archive_upload(&world, &archive, &person, drive).await;
    let job_id = jobs.await_create(file_id).await.job_id;
    let response = archive
        .gql(
            &world,
            &runner,
            "mutation($f:UUID!,$j:UUID!,$n:String!,$k:String!,$m:String!,$s:ByteCount!,$h:String!){\
             archiveVaultRunnerRequestRenditionUpload(fileId:$f,jobId:$j,name:$n,kind:$k,mediaType:$m,size:$s,sha256:$h)\
             {fileId url fields}}",
            serde_json::json!({
                "f": file_id, "j": job_id, "n": NAME, "k": "duckdb",
                "m": "application/octet-stream", "s": DUCKDB.len(), "h": sha256_hex(DUCKDB),
            }),
        )
        .await;
    let ticket = ticket_of(&response, "archiveVaultRunnerRequestRenditionUpload");
    let status = post_bytes(&world, &ticket, DUCKDB, NAME).await;
    assert!((200..300).contains(&status));
    let rendition: Uuid = poll_until!(Duration::from_secs(15), {
        sqlx::query_scalar(
            "SELECT blob_ref FROM drive.file_rendition WHERE file_id = $1 AND landed_at IS NOT NULL",
        )
        .bind(file_id)
        .fetch_optional(&archive.db.app)
        .await
        .unwrap()
    });
    let key = ticket.fields["key"].as_str().unwrap().to_string();
    assert!(world.minio.object_exists(&archive.bucket, &key).await);

    // When: the person is erased
    let outcome = archive.erase(person_id).await;

    // Then: the source and the rendition objects are purged, and the
    // rendition row is gone with the file
    assert_eq!(
        outcome.blobs_purged, 2,
        "the source and the rendition objects: {outcome:?}"
    );
    poll_until!(Duration::from_secs(30), {
        (!world.minio.object_exists(&archive.bucket, &key).await).then_some(())
    });
    let rows: i64 =
        sqlx::query_scalar("SELECT count(*) FROM drive.file_rendition WHERE file_id = $1")
            .bind(file_id)
            .fetch_one(&archive.db.app)
            .await
            .unwrap();
    assert_eq!(rows, 0);
    let blob: Option<String> =
        sqlx::query_scalar("SELECT state FROM service_engine.blob WHERE id = $1")
            .bind(rendition)
            .fetch_optional(&archive.db.app)
            .await
            .unwrap();
    assert!(
        blob.is_none() || blob.as_deref() == Some("orphaned"),
        "the rendition blob is purged: {blob:?}"
    );

    archive.shutdown().await;
    world.cleanup().await;
}

fn ticket_of(response: &serde_json::Value, root: &str) -> Ticket {
    crate::harness::runner::image_ticket_of(response, root)
}

async fn archive_upload(world: &World, archive: &ArchiveHost, owner: &str, drive: Uuid) -> Uuid {
    let file_id = Uuid::now_v7();
    let response = archive
        .gql(
            world,
            owner,
            "mutation($f:UUID!,$d:UUID!,$p:String!,$n:String!,$m:String!,$s:ByteCount!,$h:String!){\
             archiveVaultRequestUpload(fileId:$f,driveId:$d,path:$p,name:$n,mediaType:$m,size:$s,sha256:$h){fileId url fields}}",
            serde_json::json!({
                "f": file_id, "d": drive, "p": "", "n": "dump.txt", "m": "text/plain",
                "s": SOURCE.len(), "h": sha256_hex(SOURCE),
            }),
        )
        .await;
    let ticket = ticket_of(&response, "archiveVaultRequestUpload");
    let status = post_bytes(world, &ticket, SOURCE, "dump.txt").await;
    assert!((200..300).contains(&status));
    for gesture in [
        "mutation($f:UUID!){archiveVaultCommitUpload(fileId:$f){success}}",
        "mutation($f:UUID!){archiveVaultProcessFile(fileId:$f){success}}",
    ] {
        ok(&archive
            .gql(world, owner, gesture, serde_json::json!({ "f": file_id }))
            .await);
    }
    file_id
}
