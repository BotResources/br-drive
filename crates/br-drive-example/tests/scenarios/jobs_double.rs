//! The Jobs double's own contract: it refuses what Jobs refuses, so a scenario
//! that violates Jobs' contract fails instead of passing. Checked against
//! `svc-jobs` (`app/create.rs`) and `bc-jobs` (`commands/job/create.rs`,
//! `domain/job/parenting.rs`, `commands/job/source.rs`) at `contract-jobs`
//! 0.5.0, and its passport rules (`app/create/declare.rs`, `resolve_passport`,
//! `resolve_triggered_by`) at `contract-jobs` 0.7.0.

use std::time::Duration;

use br_core_auth::{AuthMethod, Passport, PassportClaims};
use contract_jobs::CMD_JOB_CREATE_V1;
use contract_jobs::command::TriggeredBy;
use contract_jobs::command_v3::CreateJobV3 as CreateJob;
use futures_util::FutureExt;
use uuid::Uuid;

use crate::harness::runner::RENDER;
use crate::harness::{JobsStandIn, World};

const PRODUCER: &str = "another-producer";

fn user_passport(user: Uuid) -> serde_json::Value {
    serde_json::to_value(Passport::human(
        user,
        false,
        true,
        AuthMethod::Jwt,
        None,
        PassportClaims::new(),
    ))
    .unwrap()
}

fn create(source_entity_id: Uuid, parent_job_id: Option<Uuid>) -> CreateJob {
    CreateJob {
        job_id: Uuid::now_v7(),
        runner_type: RENDER.to_string(),
        producer: PRODUCER.to_string(),
        config: None,
        payload: None,
        passport: Some(
            serde_json::to_value(Passport::service(Uuid::now_v7(), PassportClaims::new())).unwrap(),
        ),
        triggered_by: None,
        parent_job_id,
        initial_job_id: None,
        mode: None,
        caller_address: None,
        acting_run_id: None,
        acting_instance_key: None,
        source_bc: Some(PRODUCER.to_string()),
        source_entity_id: Some(source_entity_id),
        max_attempts: None,
        priority: None,
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

#[tokio::test]
async fn the_jobs_double_refuses_a_root_job_without_a_passport_or_naming_someone_else() {
    // Given: the double
    let world = World::start("pod-jobs-double-passport").await;
    let jobs = JobsStandIn::attach(&world).await;

    // When / Then: a root job without a passport is refused
    let mut bare = create(Uuid::now_v7(), None);
    bare.passport = None;
    assert_eq!(refused(&jobs, &bare).await, "passport_required");

    // And: a passport that is not a Passport is refused
    let mut garbled = create(Uuid::now_v7(), None);
    garbled.passport = Some(serde_json::json!({ "kind": "robot" }));
    assert_eq!(refused(&jobs, &garbled).await, "passport_malformed");

    // And: a triggered_by that is not the passport's user is refused, and so
    // is any one beside a service passport
    let user = Uuid::now_v7();
    let mut other = create(Uuid::now_v7(), None);
    other.passport = Some(user_passport(user));
    other.triggered_by = Some(TriggeredBy::Anonymous(Uuid::now_v7()));
    assert_eq!(refused(&jobs, &other).await, "passport_user_mismatch");
    let mut beside_service = create(Uuid::now_v7(), None);
    beside_service.triggered_by = Some(TriggeredBy::Anonymous(user));
    assert_eq!(
        refused(&jobs, &beside_service).await,
        "passport_user_mismatch"
    );

    // And: the passport's own user, named or not, is accepted
    let mut own = create(Uuid::now_v7(), None);
    own.passport = Some(user_passport(user));
    own.triggered_by = Some(TriggeredBy::Identified {
        id: user,
        display_name: "Ada".into(),
    });
    jobs.submit_create(&own).await;
    jobs.await_create(own.source_entity_id.unwrap()).await;

    world.cleanup().await;
}

/// The message of a panic a read of the double raised.
async fn panic_of(read: impl std::future::Future<Output = ()>) -> String {
    let panic = std::panic::AssertUnwindSafe(read)
        .catch_unwind()
        .await
        .expect_err("the read fails the scenario");
    panic
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_default()
}

#[tokio::test]
async fn a_job_create_v1_fails_the_scenario_loudly_instead_of_passing_unseen() {
    // Given: the double
    let world = World::start("pod-jobs-double-v1").await;
    let jobs = JobsStandIn::attach(&world).await;

    // When: a producer still on v1 sends a root job — no passport on v1
    let entity = Uuid::now_v7();
    let mut legacy = serde_json::to_value(create(entity, None)).unwrap();
    legacy.as_object_mut().unwrap().remove("passport");
    jobs.submit_command(CMD_JOB_CREATE_V1, "job.create", 1, legacy)
        .await;

    // Then: proving that no command reached jobs fails, naming the v1 create
    let refused = panic_of(jobs.expect_no_command(Duration::from_secs(5))).await;
    assert!(refused.contains("job.create v1"), "{refused}");

    // And: awaiting a create fails on the breach too, not on a timeout
    let waited = panic_of(async {
        jobs.await_create(entity).await;
    })
    .await;
    assert!(waited.contains("job.create v1"), "{waited}");

    // And: the breach is the one recorded (acknowledged here, or the double's
    // drop would fail the scenario)
    let violations = jobs.take_violations();
    assert_eq!(violations.len(), 1, "{violations:?}");
    // The v1 create itself may still sit in the inbox: a check that panicked
    // before its read left it there, one that panicked after consumed it.
    if let Some((subject, _)) = jobs.next_command(Duration::from_millis(200)).await {
        assert_eq!(subject, CMD_JOB_CREATE_V1);
    }
    jobs.expect_no_command(Duration::from_millis(200)).await;

    world.cleanup().await;
}
