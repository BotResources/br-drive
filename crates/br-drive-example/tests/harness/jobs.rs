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
    JobQueued, JobStarted, JobStepStarted,
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

pub struct JobsStandIn {
    js: async_nats::jetstream::Context,
    commands: tokio::sync::Mutex<async_nats::jetstream::consumer::pull::Stream>,
    /// Commands read while waiting for another one, kept for a later wait.
    skipped: tokio::sync::Mutex<std::collections::VecDeque<(String, Value)>>,
    actor: Uuid,
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
        Self {
            js,
            commands: tokio::sync::Mutex::new(commands),
            skipped: tokio::sync::Mutex::new(std::collections::VecDeque::new()),
            actor: Uuid::now_v7(),
        }
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
        let event = IntegrationEvent::new(
            Uuid::now_v7(),
            event_type,
            EVENT_VERSION,
            Utc::now(),
            EventMetadata::new(
                Actor::Service(ServiceAccountId::from(self.actor)),
                Uuid::now_v7(),
            ),
            payload,
        );
        let bytes = serde_json::to_vec(&event).expect("a jobs fact serializes");
        self.js
            .publish(event_subject(&coords), bytes.into())
            .await
            .expect("publish the jobs fact")
            .await
            .expect("the stream acks the jobs fact");
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
        self.publish(
            evt_job_completed_v1_coords().unwrap(),
            EVENT_TYPE_COMPLETED,
            JobCompleted { job_id },
        )
        .await;
    }

    pub async fn reject_creation(&self, job_id: Uuid, reason_code: &str) {
        self.publish(
            evt_job_creation_rejected_v1_coords().unwrap(),
            EVENT_TYPE_CREATION_REJECTED,
            JobCreationRejected {
                job_id,
                reason_code: reason_code.to_string(),
                params: Value::Object(Default::default()),
            },
        )
        .await;
    }

    pub async fn fail(&self, job_id: Uuid, failure_cause: &str, reason_code: Option<&str>) {
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
        let mut messages = self.commands.lock().await;
        let next = tokio::time::timeout(within, messages.next()).await.ok()??;
        let message = next.expect("read a command");
        message.ack().await.expect("ack the command");
        let subject = message.subject.to_string();
        let envelope: IntegrationCommand<Value> =
            serde_json::from_slice(&message.payload).expect("an integration command envelope");
        Some((subject, envelope.payload))
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
