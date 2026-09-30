//! `br_drive::delete_drive_in_reaction`: a host deletes a drive from a
//! **reaction** — the example host's `retract` command, what a roster sends
//! when the person a workspace belongs to leaves — with the effects of
//! `delete_drive`: the files, their objects, their running jobs cancelled and
//! their facts handed to the host, under the reaction's message.

use std::collections::HashSet;
use std::time::Duration;

use uuid::Uuid;

use crate::harness::runner::install_render_rule;
use crate::harness::upload::{UploadRequest, upload, upload_processed};
use crate::harness::{JobsStandIn, World, drive_subscription, manager_passport, ok, quiet};

const BYTES: &[u8] = b"a document of a workspace retracted";

async fn drive_rows(world: &World, drive: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM drive.drive WHERE id = $1")
        .bind(drive)
        .fetch_one(&world.db.app)
        .await
        .expect("count the drive rows")
}

async fn workspace(world: &World, passport: &str, id: Uuid) -> serde_json::Value {
    ok(&world
        .gql(
            passport,
            "query($id:UUID!){workspaceWorkspace(id:$id){id}}",
            serde_json::json!({ "id": id }),
        )
        .await)["workspaceWorkspace"]
        .clone()
}

#[tokio::test]
async fn a_drive_deleted_from_a_reaction_takes_its_files_cancels_their_jobs_and_hands_their_facts()
{
    // Given: a workspace whose drive holds a running chain and a stored file,
    // its owner watching the drive
    let world = World::start("pod-retract").await;
    let jobs = JobsStandIn::attach(&world).await;
    let owner = manager_passport(Uuid::now_v7(), "Ada");
    install_render_rule(&world, &owner).await;
    let drive = world.create_workspace(&owner, "personal").await;
    let running = upload_processed(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "running.txt", BYTES),
    )
    .await;
    let running_job = jobs.await_create(running).await.job_id;
    let stored = upload(
        &world,
        &owner,
        &UploadRequest::text(drive, "", "stored.txt", BYTES),
    )
    .await;
    let sources = [
        world.source_of(running).await,
        world.source_of(stored).await,
    ];
    let mut files = drive_subscription(&world, &owner, drive).await;
    quiet(&mut files).await;

    // When: the roster's retract command reaches the host
    let message = world.send_retract(drive).await;
    world.await_consumed(message).await;

    // Then: the workspace, its drive and its files are gone, their objects
    // released
    assert!(workspace(&world, &owner, drive).await.is_null());
    assert_eq!(drive_rows(&world, drive).await, 0);
    for (file, source) in [running, stored].into_iter().zip(sources) {
        assert!(world.file(&owner, file).await.is_null());
        assert_eq!(world.blob_state(source).await.as_deref(), Some("orphaned"));
    }
    // And: Jobs is asked to cancel the running job
    assert_eq!(jobs.await_cancel(running_job).await.job_id, running_job);
    // And: each file's last fact is `DriveDeleted`, caused by the command
    for file in [running, stored] {
        let facts = world.facts("drive_file", serde_json::json!(file)).await;
        let last = facts.last().expect("the file's facts");
        assert_eq!(last.event_type, "DriveDeleted");
        assert_eq!(last.causation_id, Some(message));
        assert_eq!(last.actor_kind, "service");
        assert_eq!(last.seq, facts.len() as i64, "gap-free to the end");
    }
    // And: the owner's live window loses both files
    let mut gone = HashSet::new();
    while gone.len() < 2 {
        let delta = files.next_payload(Duration::from_secs(15)).await;
        let node = &delta["workspaceDriveChanged"];
        match node["__typename"].as_str() {
            Some("DriveRemove") => {
                gone.insert(node["key"].as_str().unwrap_or_default().to_string());
            }
            Some("DriveReset") if node["views"].as_array().is_some_and(Vec::is_empty) => {
                gone.insert(running.to_string());
                gone.insert(stored.to_string());
            }
            _ => {}
        }
    }
    assert!(gone.contains(&running.to_string()) && gone.contains(&stored.to_string()));

    // And: the command redelivered changes nothing
    let again = world.send_retract(drive).await;
    world.await_consumed(again).await;
    jobs.expect_no_command(Duration::from_secs(1)).await;

    world.cleanup().await;
}

#[tokio::test]
async fn a_drive_past_the_reset_threshold_is_deleted_from_a_reaction_all_the_same() {
    // Given: a workspace whose drive holds more files than the host's reset
    // threshold (3), its owner watching the drive
    let world = World::start("pod-retract-many").await;
    let owner = manager_passport(Uuid::now_v7(), "Ada");
    let drive = world.create_workspace(&owner, "personal").await;
    let mut ids = Vec::new();
    for index in 0..5 {
        let name = format!("file-{index}.txt");
        ids.push(
            upload(
                &world,
                &owner,
                &UploadRequest::text(drive, "", &name, BYTES),
            )
            .await,
        );
    }
    let mut files = drive_subscription(&world, &owner, drive).await;
    quiet(&mut files).await;

    // When: the retract command reaches the host
    let message = world.send_retract(drive).await;
    world.await_consumed(message).await;

    // Then: every file is gone with its fact — the reaction stays within the
    // engine's impact budget, one impact per file never staged
    for id in &ids {
        assert!(world.file(&owner, *id).await.is_null());
        let facts = world.facts("drive_file", serde_json::json!(id)).await;
        assert_eq!(
            facts.last().map(|fact| fact.event_type.as_str()),
            Some("DriveDeleted")
        );
    }
    assert_eq!(drive_rows(&world, drive).await, 0);
    // And: the owner's session catches up through the host's visibility
    // change: the files leave its window
    let mut gone = HashSet::new();
    while gone.len() < ids.len() {
        let delta = files.next_payload(Duration::from_secs(15)).await;
        let node = &delta["workspaceDriveChanged"];
        match node["__typename"].as_str() {
            Some("DriveRemove") => {
                gone.insert(node["key"].as_str().unwrap_or_default().to_string());
            }
            Some("DriveReset") if node["views"].as_array().is_some_and(Vec::is_empty) => {
                gone.extend(ids.iter().map(Uuid::to_string));
            }
            _ => {}
        }
    }

    world.cleanup().await;
}
