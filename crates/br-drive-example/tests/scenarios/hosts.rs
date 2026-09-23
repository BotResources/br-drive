use std::time::Duration;

use contract_jobs::catalog::RunnerTypeLifecycle;
use uuid::Uuid;

use crate::harness::archive::ARCHIVE_RUNNER_SCOPE;
use crate::harness::runner::{RENDER, RUNNER_SCOPE, Report, install_render_rule, report};
use crate::harness::upload::{Ticket, UploadRequest, post_bytes, sha256_hex, upload};
use crate::harness::{ArchiveHost, JobsStandIn, World, manager_passport, ok, service_passport};

const BYTES: &[u8] = b"two hosts, one broker";

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
    assert!(
        (200..300).contains(&status),
        "the archive upload lands: {status}"
    );
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

#[tokio::test]
async fn two_hosts_on_one_broker_each_receive_every_jobs_fact_about_their_own_jobs() {
    let world = World::start("pod-two-hosts-a").await;
    let archive = ArchiveHost::start(&world, "pod-two-hosts-b").await;
    let jobs = JobsStandIn::attach(&world).await;
    let manager = manager_passport(Uuid::now_v7(), "Ada");
    let archivist_id = Uuid::now_v7();
    let archivist = manager_passport(archivist_id, "Bea");
    let workspace_runner = service_passport(&[RUNNER_SCOPE]);
    let archive_runner = service_passport(&[ARCHIVE_RUNNER_SCOPE]);

    install_render_rule(&world, &jobs, &manager).await;
    jobs.declare_runner_type(RENDER, RunnerTypeLifecycle::Active)
        .await;
    archive.await_known_runner_type(RENDER).await;
    ok(&archive
        .gql(
            &world,
            &archivist,
            "mutation($id:UUID!){archiveVaultCreateRuleset(id:$id,name:\"render\",trigger:UPLOAD,mediaTypes:[\"text/plain\"],steps:[{runnerType:\"render\"}],isDefault:true){id unknownRunnerTypes}}",
            serde_json::json!({ "id": Uuid::now_v7() }),
        )
        .await);

    let workspace_drive = world.create_workspace(&manager, "library").await;
    let archive_drive = archive.drive_for(archivist_id).await;
    let workspace_file = upload(
        &world,
        &manager,
        &UploadRequest::text(workspace_drive, "", "ours.txt", BYTES),
    )
    .await;
    let archive_file =
        archive_upload(&world, &archive, &archivist, archive_drive, "theirs.txt").await;

    let job_w = world.await_job(workspace_file).await;
    let job_a = archive
        .job_of(archive_file)
        .await
        .expect("the archive host minted a job");
    let create_w = jobs.await_create(workspace_file).await;
    let create_a = jobs.await_create(archive_file).await;
    assert_eq!(create_w.source_bc.as_deref(), Some("workspace"));
    assert_eq!(create_a.source_bc.as_deref(), Some("archive"));
    assert_eq!(create_a.producer, "archive");
    let config = create_a.config.expect("the archive job config");
    assert_eq!(
        config["context_root"], "archiveVaultRunnerContext",
        "the root names follow the engine's camel-casing of a two-word prefix"
    );
    assert_eq!(
        config["image_upload_root"],
        "archiveVaultRunnerRequestImageUpload"
    );
    assert_eq!(config["report_root"], "archiveVaultRunnerReport");
    assert_eq!(config["host"], "archive");

    // Every fact goes to both hosts through their own durables: each one sees
    // its job's facts and ignores the other's.
    let run = Uuid::now_v7();
    for job in [job_w, job_a] {
        jobs.queue(job, RENDER).await;
        jobs.start(job, run).await;
        jobs.declare_plan(job, run, &["render"]).await;
        jobs.start_step(job, run, 0, "render").await;
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let ours = world.file(&manager, workspace_file).await;
        let theirs = archive.file_state(&world, &archivist, archive_file).await;
        // The plan and the current step are projected by two independent
        // reactions over two facts, in no guaranteed order: wait for both
        // before reading either, or the faster one ends the poll alone.
        let done = |file: &serde_json::Value| {
            file["progress"]["currentLabel"] == "render"
                && file["progress"]["plan"] == serde_json::json!(["render"])
        };
        if done(&ours) && done(&theirs) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "both hosts must see their own step: {ours} / {theirs}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    ok(&report(
        &world,
        &workspace_runner,
        workspace_file,
        Report {
            job_id: job_w,
            pages: vec![(1, "ours")],
            origin: None,
            indexer: None,
            done: true,
        },
    )
    .await);
    ok(&archive
        .gql(
            &world,
            &archive_runner,
            "mutation($f:UUID!,$j:UUID!){archiveVaultRunnerReport(fileId:$f,jobId:$j,pages:[{number:1,markdown:\"theirs\"}],done:true){success}}",
            serde_json::json!({ "f": archive_file, "j": job_a }),
        )
        .await);
    jobs.await_finish(job_w).await;
    jobs.await_finish(job_a).await;
    jobs.complete(job_w).await;
    jobs.complete(job_a).await;
    world.await_state(&manager, workspace_file, "READY").await;
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let theirs = archive.file_state(&world, &archivist, archive_file).await;
        if theirs["processingState"] == "READY" {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the archive host's chain must land READY: {theirs}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    jobs.expect_no_command(Duration::from_secs(1)).await;

    archive.shutdown().await;
    world.cleanup().await;
}
