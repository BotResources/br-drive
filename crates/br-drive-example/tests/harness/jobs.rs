use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::consumer::pull::Config as PullConfig;
use async_nats::jetstream::consumer::{AckPolicy, DeliverPolicy};
use br_core_integration::{
    Actor, EventCoords, EventMetadata, IntegrationCommand, IntegrationEvent, ServiceAccountId,
};
use chrono::Utc;
use contract_jobs::command::{CancelJob, CreateJob, FinishJob};
use contract_jobs::event::{
    EVENT_TYPE_CANCELLED, EVENT_TYPE_COMPLETED, EVENT_TYPE_CREATION_REJECTED, EVENT_TYPE_FAILED,
    EVENT_TYPE_PLAN_DECLARED, EVENT_TYPE_QUEUED, EVENT_TYPE_STARTED, EVENT_TYPE_STEP_STARTED,
    FailureReport, JobCancelled, JobCompleted, JobCreationRejected, JobFailed, JobPlanDeclared,
    JobQueued, JobStarted, JobStepStarted, REASON_DUPLICATE_ACTIVE_ENTITY, REASON_ID_REUSE,
};
use contract_jobs::{
    CMD_JOB_CANCEL_V2, CMD_JOB_CREATE_V1, CMD_JOB_FINISH_V2, evt_job_cancelled_v1_coords,
    evt_job_completed_v1_coords, evt_job_creation_rejected_v1_coords, evt_job_failed_v1_coords,
    evt_job_plan_declared_v1_coords, evt_job_queued_v1_coords, evt_job_started_v1_coords,
    evt_job_step_started_v1_coords,
};
use futures_util::StreamExt;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use service_engine::nats::INTEGRATION_CMD;
use uuid::Uuid;

use super::World;

const EVENT_VERSION: u8 = 1;

/// What the double knows of one job: enough to judge a `job.create` the way
/// Jobs does (`bc-jobs` `create_job`: a terminal or deleted parent is refused,
/// a second live job on one source entity is refused).
#[derive(Debug, Clone)]
struct Held {
    /// The declaration Jobs recorded; absent for a job the test planted.
    declared: Option<CreateJob>,
    source: Option<(String, Uuid)>,
    terminal: bool,
    deleted: bool,
}

#[derive(Debug, Default)]
struct Ledger {
    jobs: HashMap<Uuid, Held>,
    rejections: HashMap<Uuid, JobCreationRejected>,
    /// Runner types Jobs knows and no longer accepts jobs for. Any other type,
    /// known or not, is accepted — an unknown one is then never dispatched.
    retired: std::collections::HashSet<String>,
    /// Drop every `job.cancel` unread, as Jobs does with a cancel it consumes
    /// before the creation of the job it names.
    dropping_cancels: bool,
}

/// The input checks `svc-jobs` runs before the domain (`app/create.rs::build`
/// and the value objects of `bc-jobs`): UUIDv7 ids, a source named whole and by
/// its producer, a display name that is not blank and at most 512 characters.
fn malformed(create: &CreateJob) -> Option<(&'static str, &'static str)> {
    let v7 = |id: Uuid| id.get_version_num() == 7;
    if !v7(create.job_id) {
        return Some(("not_uuid_v7", "job_id"));
    }
    match (&create.source_bc, create.source_entity_id) {
        (Some(bc), Some(entity)) => {
            if bc != &create.producer {
                return Some(("corrupt_state", "source_bc_is_not_the_producer"));
            }
            if !v7(entity) {
                return Some(("not_uuid_v7", "source_entity_id"));
            }
        }
        (None, None) => {}
        _ => return Some(("blank_value", "source_entity_id")),
    }
    if let Some(parent) = create.parent_job_id
        && !v7(parent)
    {
        return Some(("not_uuid_v7", "parent_job_id"));
    }
    if let Some(user) = &create.triggered_by {
        if !v7(user.id()) {
            return Some(("not_uuid_v7", "triggered_by"));
        }
        let display_name = user
            .display_name()
            .map(str::to_owned)
            .unwrap_or_else(|| user.id().to_string());
        if display_name.trim().is_empty() {
            return Some(("blank_value", "display_name"));
        }
        if display_name.chars().count() > 512 {
            return Some(("value_too_long", "display_name"));
        }
    }
    None
}

impl Ledger {
    /// The verdict Jobs would give, in Jobs' order (input, runner type, id
    /// reuse, source, parent): `None` to accept, else the rejection.
    fn judge(&self, create: &CreateJob) -> Option<JobCreationRejected> {
        let reject = |reason_code: &str, mut params: serde_json::Map<String, Value>| {
            params.insert("jobId".into(), Value::String(create.job_id.to_string()));
            if let (Some(bc), Some(entity)) = (&create.source_bc, create.source_entity_id) {
                params.insert("sourceEntityId".into(), Value::String(entity.to_string()));
                params.insert("sourceBc".into(), Value::String(bc.clone()));
            }
            Some(JobCreationRejected {
                job_id: create.job_id,
                reason_code: reason_code.to_string(),
                params: Value::Object(params),
            })
        };
        let named = |key: &str, value: String| {
            serde_json::Map::from_iter([(key.to_string(), Value::String(value))])
        };
        if let Some((code, field)) = malformed(create) {
            return reject(code, named("field", field.to_string()));
        }
        if self.retired.contains(&create.runner_type) {
            return reject(
                "runner_type_retired",
                named("runnerType", create.runner_type.clone()),
            );
        }
        if let Some(existing) = self.jobs.get(&create.job_id) {
            return match &existing.declared {
                Some(declared) if declared == create => None,
                _ => reject(REASON_ID_REUSE, serde_json::Map::new()),
            };
        }
        if let Some(source) = source_of(create) {
            let active = self
                .jobs
                .iter()
                .filter(|(_, held)| !held.terminal && held.source.as_ref() == Some(&source))
                .map(|(id, _)| *id)
                .max();
            if let Some(active) = active {
                return reject(
                    REASON_DUPLICATE_ACTIVE_ENTITY,
                    named("activeJobId", active.to_string()),
                );
            }
        }
        if let Some(parent) = create.parent_job_id {
            if parent == create.job_id {
                return reject("self_reference", named("field", "parent_job_id".into()));
            }
            let parent_named = || named("parentJobId", parent.to_string());
            match self.jobs.get(&parent) {
                None => return reject("parent_job_unknown", parent_named()),
                Some(held) if held.deleted => return reject("parent_job_deleted", parent_named()),
                Some(held) if held.terminal => {
                    return reject("parent_job_terminal", parent_named());
                }
                Some(_) => {}
            }
        }
        None
    }

    fn accept(&mut self, create: &CreateJob) {
        self.jobs.entry(create.job_id).or_insert(Held {
            declared: Some(create.clone()),
            source: source_of(create),
            terminal: false,
            deleted: false,
        });
    }

    /// Marks `job_id` terminal; `true` when it was live.
    fn settle(&mut self, job_id: Uuid) -> bool {
        match self.jobs.get_mut(&job_id) {
            Some(held) if !held.terminal => {
                held.terminal = true;
                true
            }
            _ => false,
        }
    }
}

fn source_of(create: &CreateJob) -> Option<(String, Uuid)> {
    Some((create.source_bc.clone()?, create.source_entity_id?))
}

pub struct JobsStandIn {
    js: async_nats::jetstream::Context,
    /// Every command addressed to jobs, in arrival order, once judged.
    inbox: tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<(String, Value)>>,
    /// Commands read while waiting for another one, kept for a later wait.
    skipped: tokio::sync::Mutex<std::collections::VecDeque<(String, Value)>>,
    ledger: Arc<std::sync::Mutex<Ledger>>,
    actor: Uuid,
    drain: tokio::task::JoinHandle<()>,
}

impl Drop for JobsStandIn {
    fn drop(&mut self) {
        self.drain.abort();
    }
}

fn event_subject(coords: &EventCoords) -> String {
    format!(
        "integration.evt.{}.{}.{}.v{}",
        coords.producer.as_str(),
        coords.aggregate.as_str(),
        coords.fact.as_str(),
        coords.version
    )
}

async fn publish_fact<T: Serialize>(
    js: &async_nats::jetstream::Context,
    actor: Uuid,
    coords: EventCoords,
    event_type: &str,
    payload: T,
) {
    let event = IntegrationEvent::new(
        Uuid::now_v7(),
        event_type,
        EVENT_VERSION,
        Utc::now(),
        EventMetadata::new(
            Actor::Service(ServiceAccountId::from(actor)),
            Uuid::now_v7(),
        ),
        payload,
    );
    let bytes = serde_json::to_vec(&event).expect("a jobs fact serializes");
    js.publish(event_subject(&coords), bytes.into())
        .await
        .expect("publish the jobs fact")
        .await
        .expect("the stream acks the jobs fact");
}

/// Reads every command addressed to jobs as it arrives, judges each
/// `job.create` against the ledger and answers a refused one with
/// `creation_rejected` — as Jobs would, before any test looks at it — then
/// hands the command on to the test.
async fn drain(
    mut commands: async_nats::jetstream::consumer::pull::Stream,
    js: async_nats::jetstream::Context,
    actor: Uuid,
    ledger: Arc<std::sync::Mutex<Ledger>>,
    inbox: tokio::sync::mpsc::UnboundedSender<(String, Value)>,
) {
    while let Some(next) = commands.next().await {
        let message = next.expect("read a command");
        message.ack().await.expect("ack the command");
        let subject = message.subject.to_string();
        let envelope: IntegrationCommand<Value> =
            serde_json::from_slice(&message.payload).expect("an integration command envelope");
        let payload = envelope.payload;
        let rejection = match subject.as_str() {
            CMD_JOB_CREATE_V1 => {
                let create: CreateJob =
                    serde_json::from_value(payload.clone()).expect("a job.create decodes");
                let mut ledger = ledger.lock().expect("the ledger lock");
                let verdict = ledger.judge(&create);
                match &verdict {
                    Some(rejection) => {
                        ledger.rejections.insert(create.job_id, rejection.clone());
                    }
                    None => ledger.accept(&create),
                }
                verdict
            }
            CMD_JOB_FINISH_V2 => {
                let finish: FinishJob =
                    serde_json::from_value(payload.clone()).expect("a job.finish decodes");
                ledger
                    .lock()
                    .expect("the ledger lock")
                    .settle(finish.job_id);
                None
            }
            CMD_JOB_CANCEL_V2 => {
                let cancel: CancelJob =
                    serde_json::from_value(payload.clone()).expect("a job.cancel decodes");
                let was_live = {
                    let mut ledger = ledger.lock().expect("the ledger lock");
                    !ledger.dropping_cancels && ledger.settle(cancel.job_id)
                };
                // Jobs cancels a live job at once and says so; a cancel of a
                // settled or unknown job changes nothing.
                if was_live {
                    publish_fact(
                        &js,
                        actor,
                        evt_job_cancelled_v1_coords().unwrap(),
                        EVENT_TYPE_CANCELLED,
                        JobCancelled {
                            job_id: cancel.job_id,
                        },
                    )
                    .await;
                }
                None
            }
            _ => None,
        };
        if let Some(rejection) = rejection {
            publish_fact(
                &js,
                actor,
                evt_job_creation_rejected_v1_coords().unwrap(),
                EVENT_TYPE_CREATION_REJECTED,
                rejection,
            )
            .await;
        }
        if inbox.send((subject, payload)).is_err() {
            return;
        }
    }
}

impl JobsStandIn {
    pub async fn attach(world: &World) -> Self {
        let client = async_nats::connect(world.nats_server.url())
            .await
            .expect("dial the ephemeral broker as the jobs stand-in");
        let js = async_nats::jetstream::new(client);
        let stream = js
            .get_stream(INTEGRATION_CMD)
            .await
            .expect("the integration command stream exists");
        let consumer = stream
            .create_consumer(PullConfig {
                name: Some(format!("jobs-standin-{}", Uuid::now_v7().simple())),
                filter_subject: "integration.cmd.jobs.>".to_string(),
                deliver_policy: DeliverPolicy::New,
                ack_policy: AckPolicy::Explicit,
                ..Default::default()
            })
            .await
            .expect("the stand-in observes the commands addressed to jobs");
        let commands = consumer.messages().await.expect("open the command stream");
        let actor = Uuid::now_v7();
        let ledger = Arc::new(std::sync::Mutex::new(Ledger::default()));
        let (sender, inbox) = tokio::sync::mpsc::unbounded_channel();
        let drain = tokio::spawn(drain(commands, js.clone(), actor, ledger.clone(), sender));
        Self {
            js,
            inbox: tokio::sync::Mutex::new(inbox),
            skipped: tokio::sync::Mutex::new(std::collections::VecDeque::new()),
            ledger,
            actor,
            drain,
        }
    }

    /// A live job Jobs holds on `source_entity_id` that the host knows nothing
    /// of — what a lost `job.finish` or a restored host database leaves behind.
    pub fn hold_source(&self, source_bc: &str, source_entity_id: Uuid) -> Uuid {
        let job_id = Uuid::now_v7();
        self.ledger.lock().expect("the ledger lock").jobs.insert(
            job_id,
            Held {
                declared: None,
                source: Some((source_bc.to_string(), source_entity_id)),
                terminal: false,
                deleted: false,
            },
        );
        job_id
    }

    /// While `dropping`, every `job.cancel` is dropped: the job stays live and
    /// no `cancelled` follows.
    pub fn drop_cancels(&self, dropping: bool) {
        self.ledger
            .lock()
            .expect("the ledger lock")
            .dropping_cancels = dropping;
    }

    /// Jobs retires `runner_type`: it refuses every new job of it at creation.
    pub fn retire_runner_type(&self, runner_type: &str) {
        self.ledger
            .lock()
            .expect("the ledger lock")
            .retired
            .insert(runner_type.to_string());
    }

    /// A settled job an administrator deleted from Jobs' ledger (Jobs never
    /// deletes live work).
    pub fn delete_job(&self, job_id: Uuid) {
        assert!(!self.is_live(job_id), "Jobs deletes settled jobs only");
        if let Some(held) = self
            .ledger
            .lock()
            .expect("the ledger lock")
            .jobs
            .get_mut(&job_id)
        {
            held.deleted = true;
        }
    }

    /// Whether the double still counts `job_id` as live.
    pub fn is_live(&self, job_id: Uuid) -> bool {
        self.ledger
            .lock()
            .expect("the ledger lock")
            .jobs
            .get(&job_id)
            .is_some_and(|held| !held.terminal)
    }

    /// The rejection the double answered for `job_id`, awaited for a while.
    pub async fn await_rejection(&self, job_id: Uuid) -> JobCreationRejected {
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            if let Some(rejection) = self
                .ledger
                .lock()
                .expect("the ledger lock")
                .rejections
                .get(&job_id)
                .cloned()
            {
                return rejection;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the double never judged job {job_id}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Whether the double refused `job_id` (checked at once).
    pub fn rejected(&self, job_id: Uuid) -> bool {
        self.ledger
            .lock()
            .expect("the ledger lock")
            .rejections
            .contains_key(&job_id)
    }

    /// Sends a `job.create` to jobs as another producer would.
    pub async fn submit_create(&self, create: &CreateJob) {
        let command = IntegrationCommand::new(
            Uuid::now_v7(),
            "job.create",
            1,
            Utc::now(),
            EventMetadata::new(
                Actor::Service(ServiceAccountId::from(Uuid::now_v7())),
                Uuid::now_v7(),
            ),
            serde_json::to_value(create).expect("a job.create encodes"),
        );
        self.js
            .publish(
                CMD_JOB_CREATE_V1,
                serde_json::to_vec(&command).expect("encode").into(),
            )
            .await
            .expect("publish the job.create")
            .await
            .expect("the stream acks the job.create");
    }

    fn settle(&self, job_id: Uuid) {
        self.ledger.lock().expect("the ledger lock").settle(job_id);
    }

    /// Whether the double refused `job_id`, at once.
    fn refused(&self, job_id: Uuid) -> Option<JobCreationRejected> {
        self.ledger
            .lock()
            .expect("the ledger lock")
            .rejections
            .get(&job_id)
            .cloned()
    }

    async fn publish<T: Serialize>(&self, coords: EventCoords, event_type: &str, payload: T) {
        publish_fact(&self.js, self.actor, coords, event_type, payload).await;
    }

    pub async fn queue(&self, job_id: Uuid, runner_type: &str) {
        self.publish(
            evt_job_queued_v1_coords().unwrap(),
            EVENT_TYPE_QUEUED,
            JobQueued {
                job_id,
                runner_type: runner_type.to_string(),
            },
        )
        .await;
    }

    pub async fn start(&self, job_id: Uuid, run_id: Uuid) {
        self.publish(
            evt_job_started_v1_coords().unwrap(),
            EVENT_TYPE_STARTED,
            JobStarted { job_id, run_id },
        )
        .await;
    }

    pub async fn declare_plan(&self, job_id: Uuid, run_id: Uuid, steps: &[&str]) {
        self.publish(
            evt_job_plan_declared_v1_coords().unwrap(),
            EVENT_TYPE_PLAN_DECLARED,
            JobPlanDeclared {
                job_id,
                run_id,
                steps: steps.iter().map(|s| s.to_string()).collect(),
            },
        )
        .await;
    }

    pub async fn start_step(&self, job_id: Uuid, run_id: Uuid, index: u32, label: &str) {
        self.start_step_at(job_id, run_id, index, label, Utc::now())
            .await;
    }

    /// The same fact with a pinned instant: publishing it twice is a redelivery.
    pub async fn start_step_at(
        &self,
        job_id: Uuid,
        run_id: Uuid,
        index: u32,
        label: &str,
        started_at: chrono::DateTime<Utc>,
    ) {
        self.publish(
            evt_job_step_started_v1_coords().unwrap(),
            EVENT_TYPE_STEP_STARTED,
            JobStepStarted {
                job_id,
                run_id,
                index,
                label: label.to_string(),
                started_at,
            },
        )
        .await;
    }

    pub async fn complete(&self, job_id: Uuid) {
        self.settle(job_id);
        self.publish(
            evt_job_completed_v1_coords().unwrap(),
            EVENT_TYPE_COMPLETED,
            JobCompleted { job_id },
        )
        .await;
    }

    pub async fn reject_creation(&self, job_id: Uuid, reason_code: &str) {
        self.reject_creation_with(job_id, reason_code, Value::Object(Default::default()))
            .await;
    }

    pub async fn reject_creation_with(&self, job_id: Uuid, reason_code: &str, params: Value) {
        self.settle(job_id);
        self.publish(
            evt_job_creation_rejected_v1_coords().unwrap(),
            EVENT_TYPE_CREATION_REJECTED,
            JobCreationRejected {
                job_id,
                reason_code: reason_code.to_string(),
                params,
            },
        )
        .await;
    }

    pub async fn fail(&self, job_id: Uuid, failure_cause: &str, reason_code: Option<&str>) {
        self.settle(job_id);
        self.publish(
            evt_job_failed_v1_coords().unwrap(),
            EVENT_TYPE_FAILED,
            JobFailed {
                job_id,
                failure_cause: failure_cause.to_string(),
                failure_report: reason_code.map(|reason_code| FailureReport {
                    kind: "PERMANENT".to_string(),
                    reason_code: reason_code.to_string(),
                    params: Value::Object(Default::default()),
                    diagnostic: serde_json::json!({ "log": "the runner said so" }),
                }),
                note: None,
            },
        )
        .await;
    }

    pub async fn cancel(&self, job_id: Uuid) {
        self.settle(job_id);
        self.publish(
            evt_job_cancelled_v1_coords().unwrap(),
            EVENT_TYPE_CANCELLED,
            JobCancelled { job_id },
        )
        .await;
    }

    /// The next command addressed to jobs: one skipped by an earlier wait
    /// first, then the stream.
    pub async fn next_command(&self, within: Duration) -> Option<(String, Value)> {
        if let Some(skipped) = self.skipped.lock().await.pop_front() {
            return Some(skipped);
        }
        self.read_command(within).await
    }

    async fn read_command(&self, within: Duration) -> Option<(String, Value)> {
        let mut inbox = self.inbox.lock().await;
        match tokio::time::timeout(within, inbox.recv()).await {
            Ok(Some(command)) => Some(command),
            // A closed inbox is a drain that stopped: no absence can be proven.
            Ok(None) => panic!("the jobs double's drain stopped; see the log above"),
            Err(_) => None,
        }
    }

    async fn await_command<T: DeserializeOwned>(
        &self,
        subject: &str,
        matches: impl Fn(&T) -> bool,
    ) -> T {
        let wanted = |found: &str, payload: &Value| -> Option<T> {
            if found != subject {
                return None;
            }
            let decoded: T = serde_json::from_value(payload.clone()).expect("the command decodes");
            matches(&decoded).then_some(decoded)
        };
        {
            let mut skipped = self.skipped.lock().await;
            if let Some(index) = skipped
                .iter()
                .position(|(found, payload)| wanted(found, payload).is_some())
            {
                let (found, payload) = skipped.remove(index).expect("the index is in range");
                return wanted(&found, &payload).expect("checked by position");
            }
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            let Some((found, payload)) = self.read_command(remaining).await else {
                panic!("no {subject} command reached jobs in time");
            };
            if let Some(decoded) = wanted(&found, &payload) {
                return decoded;
            }
            self.skipped.lock().await.push_back((found, payload));
        }
    }

    /// The next `job.create` for `file_id`, which the double accepted — a
    /// refusal nobody expected fails the scenario here, not three steps later.
    pub async fn await_create(&self, file_id: Uuid) -> CreateJob {
        let create = self.next_create(file_id).await;
        if let Some(refusal) = self.refused(create.job_id) {
            panic!(
                "jobs refused the create of {file_id}: {} {}",
                refusal.reason_code, refusal.params
            );
        }
        create
    }

    /// The next `job.create` for `file_id`, which the double refused.
    pub async fn await_refused_create(&self, file_id: Uuid) -> (CreateJob, JobCreationRejected) {
        let create = self.next_create(file_id).await;
        let refusal = self
            .refused(create.job_id)
            .unwrap_or_else(|| panic!("jobs accepted the create of {file_id}"));
        (create, refusal)
    }

    async fn next_create(&self, file_id: Uuid) -> CreateJob {
        self.await_command::<CreateJob>(CMD_JOB_CREATE_V1, |create| {
            create.source_entity_id == Some(file_id)
        })
        .await
    }

    pub async fn await_finish(&self, job_id: Uuid) -> FinishJob {
        self.await_command::<FinishJob>(CMD_JOB_FINISH_V2, |finish| finish.job_id == job_id)
            .await
    }

    pub async fn await_cancel(&self, job_id: Uuid) -> CancelJob {
        self.await_command::<CancelJob>(CMD_JOB_CANCEL_V2, |cancel| cancel.job_id == job_id)
            .await
    }

    pub async fn expect_no_command(&self, within: Duration) {
        if let Some((subject, payload)) = self.next_command(within).await {
            panic!("jobs received an unexpected {subject}: {payload}");
        }
    }
}
