//! The Jobs double's own contract: it refuses what Jobs refuses, so a scenario
//! that violates Jobs' contract fails instead of passing. Checked against
//! `svc-jobs` (`app/create.rs`) and `bc-jobs` (`commands/job/create.rs`,
//! `domain/job/parenting.rs`, `commands/job/source.rs`) at `contract-jobs`
//! 0.5.0.

use contract_jobs::command::{CreateJob, TriggeredBy};
use uuid::Uuid;

use crate::harness::runner::RENDER;
use crate::harness::{JobsStandIn, World};

const PRODUCER: &str = "another-producer";

fn create(source_entity_id: Uuid, parent_job_id: Option<Uuid>) -> CreateJob {
    CreateJob {
        job_id: Uuid::now_v7(),
        runner_type: RENDER.to_string(),
        producer: PRODUCER.to_string(),
        config: None,
        parent_job_id,
        triggered_by: None,
        source_bc: Some(PRODUCER.to_string()),
        source_entity_id: Some(source_entity_id),
        max_attempts: None,
    }
}

async fn refused(jobs: &JobsStandIn, create: &CreateJob) -> String {
    jobs.submit_create(create).await;
    jobs.await_rejection(create.job_id).await.reason_code
}

#[tokio::test]
async fn the_jobs_double_refuses_a_settled_deleted_unknown_or_self_parent_and_a_busy_source() {
    // Given: the double judging creates the way Jobs does
    let world = World::start("pod-jobs-double").await;
    let jobs = JobsStandIn::attach(&world).await;

    // When: a job is created, then settles, then is named as a parent
    let parent = create(Uuid::now_v7(), None);
    jobs.submit_create(&parent).await;
    jobs.await_create(parent.source_entity_id.unwrap()).await;
    assert!(jobs.is_live(parent.job_id), "a fresh root job is accepted");
    jobs.complete(parent.job_id).await;
    let child = create(Uuid::now_v7(), Some(parent.job_id));
    jobs.submit_create(&child).await;

    // Then: the child is refused, naming the terminal parent
    let terminal = jobs.await_rejection(child.job_id).await;
    assert_eq!(terminal.reason_code, "parent_job_terminal");
    assert_eq!(terminal.params["parentJobId"], parent.job_id.to_string());
    assert!(!jobs.is_live(child.job_id));

    // And: a deleted, an unknown and a self-named parent are refused too
    let deleted = create(Uuid::now_v7(), None);
    jobs.submit_create(&deleted).await;
    jobs.await_create(deleted.source_entity_id.unwrap()).await;
    jobs.complete(deleted.job_id).await;
    jobs.delete_job(deleted.job_id);
    assert_eq!(
        refused(&jobs, &create(Uuid::now_v7(), Some(deleted.job_id))).await,
        "parent_job_deleted"
    );
    assert_eq!(
        refused(&jobs, &create(Uuid::now_v7(), Some(Uuid::now_v7()))).await,
        "parent_job_unknown"
    );
    let mut own = create(Uuid::now_v7(), None);
    own.parent_job_id = Some(own.job_id);
    assert_eq!(refused(&jobs, &own).await, "self_reference");

    // And: a second live job on one source entity is refused, naming the first
    let entity = Uuid::now_v7();
    let first = create(entity, None);
    jobs.submit_create(&first).await;
    jobs.await_create(entity).await;
    let second = create(entity, None);
    jobs.submit_create(&second).await;
    let busy = jobs.await_rejection(second.job_id).await;
    assert_eq!(busy.reason_code, "duplicate_active_entity");
    assert_eq!(busy.params["activeJobId"], first.job_id.to_string());
    assert_eq!(busy.params["sourceEntityId"], entity.to_string());
    assert_eq!(busy.params["sourceBc"], PRODUCER);

    // And: an id reused for another declaration is refused, the same one absorbed
    let mut reused = create(Uuid::now_v7(), None);
    reused.job_id = first.job_id;
    assert_eq!(refused(&jobs, &reused).await, "id_reuse");

    world.cleanup().await;
}

#[tokio::test]
async fn the_jobs_double_refuses_the_inputs_jobs_cannot_build_a_job_from() {
    // Given: the double
    let world = World::start("pod-jobs-double-inputs").await;
    let jobs = JobsStandIn::attach(&world).await;

    // When / Then: ids that are not UUIDv7 are refused
    let mut job_v4 = create(Uuid::now_v7(), None);
    job_v4.job_id = Uuid::new_v4();
    assert_eq!(refused(&jobs, &job_v4).await, "not_uuid_v7");
    assert_eq!(
        refused(&jobs, &create(Uuid::new_v4(), None)).await,
        "not_uuid_v7"
    );
    let mut anonymous_v4 = create(Uuid::now_v7(), None);
    anonymous_v4.triggered_by = Some(TriggeredBy::Anonymous(Uuid::nil()));
    assert_eq!(refused(&jobs, &anonymous_v4).await, "not_uuid_v7");

    // And: a blank or over-long display name is refused
    let mut blank = create(Uuid::now_v7(), None);
    blank.triggered_by = Some(TriggeredBy::Identified {
        id: Uuid::now_v7(),
        display_name: "   ".into(),
    });
    assert_eq!(refused(&jobs, &blank).await, "blank_value");
    let mut long = create(Uuid::now_v7(), None);
    long.triggered_by = Some(TriggeredBy::Identified {
        id: Uuid::now_v7(),
        display_name: "x".repeat(513),
    });
    assert_eq!(refused(&jobs, &long).await, "value_too_long");

    // And: a source named by another bc than the producer, or half named, is refused
    let mut foreign = create(Uuid::now_v7(), None);
    foreign.source_bc = Some("someone-else".into());
    assert_eq!(refused(&jobs, &foreign).await, "corrupt_state");
    let mut half = create(Uuid::now_v7(), None);
    half.source_bc = None;
    assert_eq!(refused(&jobs, &half).await, "blank_value");

    world.cleanup().await;
}
