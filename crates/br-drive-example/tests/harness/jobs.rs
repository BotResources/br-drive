use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::consumer::pull::Config as PullConfig;
use async_nats::jetstream::consumer::{AckPolicy, DeliverPolicy};
use br_core_integration::{
    Actor, EventCoords, EventMetadata, IntegrationCommand, IntegrationEvent, ServiceAccountId,
};
use chrono::Utc;
use contract_jobs::catalog::{RunnerType, RunnerTypeLifecycle, runner_type_key};
use contract_jobs::command::{CancelJob, CreateJob, FinishJob};
use contract_jobs::event::{
    EVENT_TYPE_CANCELLED, EVENT_TYPE_COMPLETED, EVENT_TYPE_CREATION_REJECTED, EVENT_TYPE_FAILED,
    EVENT_TYPE_PLAN_DECLARED, EVENT_TYPE_QUEUED, EVENT_TYPE_STARTED, EVENT_TYPE_STEP_STARTED,
    FailureReport, JobCancelled, JobCompleted, JobCreationRejected, JobFailed, JobPlanDeclared,
    JobQueued, JobStarted, JobStepStarted, REASON_DUPLICATE_ACTIVE_ENTITY,
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
use service_engine::nats::{INTEGRATION_CMD, KV_PUBLISHED_LANGUAGE};
use uuid::Uuid;

use super::World;

const EVENT_VERSION: u8 = 1;

/// What the double knows of one job: enough to judge a `job.create` the way
/// Jobs does (`bc-jobs` `create_job`: a terminal or deleted parent is refused,
/// a second live job on one source entity is refused).
#[derive(Debug, Clone)]
struct Held {
    source: Option<(String, Uuid)>,
    terminal: bool,
    deleted: bool,
}

#[derive(Debug, Default)]
struct Ledger {
    jobs: HashMap<Uuid, Held>,
    rejections: HashMap<Uuid, JobCreationRejected>,
}

impl Ledger {
    /// The verdict Jobs would give: `None` to accept, else the rejection.
    fn judge(&self, create: &CreateJob) -> Option<JobCreationRejected> {
        let reject = |reason_code: &str, mut params: serde_json::Map<String, Value>| {
            params.insert("jobId".into(), Value::String(create.job_id.to_string()));
            Some(JobCreationRejected {
                job_id: create.job_id,
                reason_code: reason_code.to_string(),
                params: Value::Object(params),
            })
        };
        if self.jobs.contains_key(&create.job_id) {
            return None;
        }
        if let Some(parent) = create.parent_job_id {
            let named = || {
                serde_json::Map::from_iter([(
                    "parentJobId".to_string(),
                    Value::String(parent.to_string()),
                )])
            };
            match self.jobs.get(&parent) {
                None => return reject("parent_job_unknown", named()),
                Some(held) if held.deleted => return reject("parent_job_deleted", named()),
                Some(held) if held.terminal => return reject("parent_job_terminal", named()),
                Some(_) => {}
            }
        }
        let source = source_of(create)?;
        let active = self
            .jobs
            .iter()
            .find(|(_, held)| !held.terminal && held.source.as_ref() == Some(&source));
        if let Some((active, _)) = active {
            return reject(
                REASON_DUPLICATE_ACTIVE_ENTITY,
                serde_json::Map::from_iter([
                    ("activeJobId".to_string(), Value::String(active.to_string())),
                    (
                        "sourceEntityId".to_string(),
                        Value::String(source.1.to_string()),
                    ),
                    ("sourceBc".to_string(), Value::String(source.0.clone())),
                ]),
            );
        }
        None
    }

    fn accept(&mut self, create: &CreateJob) {
        self.jobs.entry(create.job_id).or_insert(Held {
            source: source_of(create),
            terminal: false,
            deleted: false,
        });
    }

    fn settle(&mut self, job_id: Uuid) {
        if let Some(held) = self.jobs.get_mut(&job_id) {
            held.terminal = true;
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
                ledger
                    .lock()
                    .expect("the ledger lock")
                    .settle(cancel.job_id);
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
                source: Some((source_bc.to_string(), source_entity_id)),
                terminal: false,
                deleted: false,
            },
        );
        job_id
    }

    /// A job an administrator deleted from Jobs' ledger.
    pub fn delete_job(&self, job_id: Uuid) {
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

    pub async fn declare_runner_type(&self, runner_type: &str, lifecycle: RunnerTypeLifecycle) {
        let kv = self
            .js
            .get_key_value(KV_PUBLISHED_LANGUAGE)
            .await
            .expect("the published-language bucket exists");
        let entry = RunnerType {
            runner_type: runner_type.to_string(),
            lifecycle,
            version: 1,
        };
        kv.put(
            runner_type_key(runner_type),
            serde_json::to_vec(&entry).unwrap().into(),
        )
        .await
        .expect("publish the runner type");
    }

    pub async fn publish_catalogue_noise(&self, runner_type: &str, value: Value) {
        let kv = self
            .js
            .get_key_value(KV_PUBLISHED_LANGUAGE)
            .await
            .expect("the published-language bucket exists");
        kv.put(
            runner_type_key(runner_type),
            serde_json::to_vec(&value).unwrap().into(),
        )
        .await
        .expect("publish the noise");
    }

    pub async fn retire_runner_type(&self, runner_type: &str) {
        let kv = self
            .js
            .get_key_value(KV_PUBLISHED_LANGUAGE)
            .await
            .expect("the published-language bucket exists");
        kv.delete(runner_type_key(runner_type))
            .await
            .expect("retire the runner type");
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
        tokio::time::timeout(within, inbox.recv()).await.ok()?
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

    pub async fn await_create(&self, file_id: Uuid) -> CreateJob {
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
