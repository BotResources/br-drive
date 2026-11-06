//! A file's processing (`drive.file_processing`): one state row per processed
//! file — its last job, the chain that job belongs to, where its runner says
//! it is — and the events that moved it, handed to the host as facts of the
//! `drive_file_processing` noun. No row: the file was never processed (PENDING
//! while its upload is not confirmed, READY once it is).
//!
//! The processing rules, as 0.4.0 held them:
//! - a file has at most one live job: a chain is started only on a settled
//!   file, the next step's job only once Jobs ended the previous one — the
//!   previous step's final report mints its id and holds it
//!   (`NextStepHeld`), Jobs' end of the previous job creates it (0.8.1) —
//!   both under the file's row lock;
//! - the first end of a job wins (the runner's final report or declared
//!   failure, Jobs' failed, cancelled or creation_rejected); every later fact
//!   about it — and any fact about an earlier job of the file — is kept as
//!   `JobFactIgnored` and moves nothing;
//! - a user's cancel crossing the runner's final report stops the chain
//!   before its next step;
//! - a chain's first step is never asked of Jobs before the engine promoted
//!   the file's source: started on a source not yet promoted, the step's job
//!   id is minted and the step awaits the promotion (`SourceAwaited`, 0.8.2).

use std::marker::PhantomData;

use chrono::{DateTime, SubsecRound, Utc};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::name::NounName;
use service_engine::persistence::{Aggregate, Persistence, PersistenceStyle};
use service_engine::wire::Noun;
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::commands::Initiator;
use crate::facts::{self, DriveEvent, FactMeta, Pending, SoftEda, Stamped};
use crate::file::{FileStatus, ProcessingState};
use crate::host::DriveHost;
use crate::ruleset::{RulesetStep, Trigger};

/// The `processingError` of a file whose job was cancelled.
pub const CANCELLED: &str = "cancelled";

/// The `processingError` of a chain stopped before its next step because the
/// Passport of its initiator is no longer held (a chain started before 0.7.0,
/// an initiator erased while it ran): no step is ever sent to Jobs without one.
pub const PASSPORT_MISSING: &str = "passport_missing";

/// The `processingError` of a chain stopped before its next step because the
/// Passport held for it no longer reads as a Passport.
pub const PASSPORT_UNREADABLE: &str = "passport_unreadable";

/// The names of what is known of a job, as the entries of
/// [`FileJob::events`] name them.
pub mod kind {
    pub const QUEUED: &str = "queued";
    pub const CREATION_REJECTED: &str = "creation_rejected";
    pub const STARTED: &str = "started";
    pub const PLAN_DECLARED: &str = "plan_declared";
    pub const STEP_STARTED: &str = "step_started";
    pub const COMPLETED: &str = "completed";
    pub const FAILED: &str = "failed";
    pub const CANCELLED: &str = "cancelled";
    /// A user asked to cancel the job; Jobs confirms with `cancelled`.
    pub const CANCEL_REQUESTED: &str = "cancel_requested";
    /// The runner's final report: the job's end.
    pub const REPORTED_DONE: &str = "reported_done";
    /// The runner declared the job failed, with its reason.
    pub const REPORTED_FAILED: &str = "reported_failed";
    /// The job is the chain's first step, decided but not yet asked of Jobs:
    /// it waits for the engine to promote the file's source. Since 0.8.2.
    pub const AWAITING_SOURCE: &str = "awaiting_source";
}

/// The noun of a file's processing: keyed by the file's id.
pub struct FileProcessingNoun;

impl Noun for FileProcessingNoun {
    type Key = Uuid;
    const NAME: NounName = NounName::from_static("drive_file_processing");
}

/// The payload schema version of [`ProcessingEvent`].
pub const PROCESSING_EVENT_VERSION: i32 = 1;

/// What happened to a file's processing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind")]
#[non_exhaustive]
pub enum ProcessingEvent {
    /// A gesture started a chain: the plan the file now runs.
    ChainStarted {
        trigger: Trigger,
        ruleset_id: Option<Uuid>,
        steps: Vec<RulesetStep>,
        /// The priority every step's job asks Jobs for, 1 to 5; `None` (left
        /// out of the payload) when the gesture named none. Since 0.9.0.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        priority: Option<i16>,
    },
    /// The job of a chain step was asked of Jobs (`job.create`).
    JobCreated {
        job_id: Uuid,
        step_index: i32,
        runner_type: String,
    },
    /// The runner's final report ended the job.
    JobReportedDone { job_id: Uuid },
    /// The runner declared the job failed.
    JobReportedFailed {
        job_id: Uuid,
        reason_code: String,
        message: Option<String>,
    },
    /// Jobs failed the job: `reason_code` is the runner's reported code, else
    /// Jobs' cause.
    JobFailed {
        job_id: Uuid,
        failure_cause: String,
        reason_code: String,
        note: Option<String>,
    },
    /// Jobs cancelled the job.
    JobCancelled { job_id: Uuid },
    /// Jobs refused to create the job.
    JobCreationRejected {
        job_id: Uuid,
        reason_code: String,
        params: serde_json::Value,
    },
    /// The library sent Jobs the cancel of the running job (`job.cancel`),
    /// on a user's gesture; Jobs' `cancelled` settles it.
    JobCancelSent { job_id: Uuid },
    /// The chain stopped before step `step_index`: a user's cancel was
    /// pending when `after_job_id`, the previous step's job, was reported
    /// done, or arrived while the step was held (`NextStepHeld`), or the host
    /// froze the drive while it was held, so the step's job was never created
    /// nor asked of Jobs.
    StepSkipped {
        step_index: i32,
        runner_type: String,
        after_job_id: Uuid,
    },
    /// The runner declared its plan, in a run of the job.
    JobPlanDeclared {
        job_id: Uuid,
        run_id: Uuid,
        labels: Vec<String>,
    },
    /// The runner started a step of its plan, in a run of the job.
    JobStepStarted {
        job_id: Uuid,
        run_id: Uuid,
        index: i32,
        label: String,
        started_at: DateTime<Utc>,
    },
    /// Jobs queued the job.
    JobQueued { job_id: Uuid, runner_type: String },
    /// A run of the job started.
    JobStarted { job_id: Uuid, run_id: Uuid },
    /// Jobs completed the job — only ever after the library's own
    /// `job.finish`: information, and, when the chain's next step was held
    /// for it, what creates that step (`JobCreated` follows).
    JobCompleted { job_id: Uuid },
    /// The chain stopped before step `step_index`: when `after_job_id`, the
    /// previous step's job, was reported done, the Passport the chain acts
    /// under was missing or unreadable (`reason_code`, [`PASSPORT_MISSING`] or
    /// [`PASSPORT_UNREADABLE`]), so the step's job was never created nor asked
    /// of Jobs and the file is FAILED with that code. Since 0.7.0.
    StepAbandoned {
        step_index: i32,
        runner_type: String,
        after_job_id: Uuid,
        reason_code: String,
    },
    /// A fact received about a job that changes nothing any more: an end
    /// after the job's first end, a progress fact of an ended job or one
    /// already known, any fact about an earlier job of the file. `received`
    /// is the fact as it would have been recorded.
    JobFactIgnored {
        job_id: Uuid,
        received: serde_json::Value,
        why: String,
    },
    /// The host froze the file's drive while the job ran: the job ended
    /// here, cancelled, right after its `job.cancel` was sent
    /// (`JobCancelSent`); Jobs' own `cancelled`, when it comes, changes
    /// nothing. Since 0.5.1.
    JobCancelledOnFreeze { job_id: Uuid },
    /// The previous step's job, `after_job_id`, was reported done and Jobs
    /// told (`job.finish`): the chain's next step, `step_index`, is decided
    /// and its job id minted, `job_id`, but its `job.create` waits for Jobs'
    /// own end of `after_job_id` — Jobs holds one live job per file and
    /// judges a create and a finish on separate consumers, in no order. The
    /// file stays PROCESSING; `JobCreated` with that id follows when Jobs'
    /// end arrives, or `StepSkipped` / `StepAbandoned` instead. Since 0.8.1.
    NextStepHeld {
        step_index: i32,
        runner_type: String,
        job_id: Uuid,
        after_job_id: Uuid,
    },
    /// The chain's first step, `step_index`, is decided and its job id
    /// minted, `job_id` — the file's last job — but the file's source is not
    /// promoted yet (the engine serves no download before its reaper verified
    /// the object), so its `job.create` waits for the promotion: `JobCreated`
    /// with that id follows when it reaches the library, or
    /// `AwaitedStepDropped` instead. The file is PROCESSING. Since 0.8.2.
    SourceAwaited {
        step_index: i32,
        runner_type: String,
        job_id: Uuid,
    },
    /// The chain stopped before its first step, `job_id`, which awaited the
    /// source's promotion (`SourceAwaited`) and was never asked of Jobs: a
    /// user's cancel or a freeze (`cancelled`), or, at the promotion, the
    /// chain's Passport missing or unreadable (`reason_code`). The file is
    /// FAILED with that code. Since 0.8.2.
    AwaitedStepDropped {
        step_index: i32,
        runner_type: String,
        job_id: Uuid,
        reason_code: String,
    },
}

impl DriveEvent for ProcessingEvent {
    const NOUN: &'static str = "drive_file_processing";
    const VERSION: i32 = PROCESSING_EVENT_VERSION;

    fn kind(&self) -> &'static str {
        match self {
            Self::ChainStarted { .. } => "ChainStarted",
            Self::JobCreated { .. } => "JobCreated",
            Self::JobReportedDone { .. } => "JobReportedDone",
            Self::JobReportedFailed { .. } => "JobReportedFailed",
            Self::JobFailed { .. } => "JobFailed",
            Self::JobCancelled { .. } => "JobCancelled",
            Self::JobCancelledOnFreeze { .. } => "JobCancelledOnFreeze",
            Self::JobCreationRejected { .. } => "JobCreationRejected",
            Self::JobCancelSent { .. } => "JobCancelSent",
            Self::StepSkipped { .. } => "StepSkipped",
            Self::StepAbandoned { .. } => "StepAbandoned",
            Self::NextStepHeld { .. } => "NextStepHeld",
            Self::SourceAwaited { .. } => "SourceAwaited",
            Self::AwaitedStepDropped { .. } => "AwaitedStepDropped",
            Self::JobPlanDeclared { .. } => "JobPlanDeclared",
            Self::JobStepStarted { .. } => "JobStepStarted",
            Self::JobQueued { .. } => "JobQueued",
            Self::JobStarted { .. } => "JobStarted",
            Self::JobCompleted { .. } => "JobCompleted",
            Self::JobFactIgnored { .. } => "JobFactIgnored",
        }
    }
}

/// Why a received fact changed nothing (`JobFactIgnored.why`).
pub mod why {
    /// The job had already ended: its first end won.
    pub const JOB_ALREADY_ENDED: &str = "job_already_ended";
    /// The fact is about an earlier job of the file.
    pub const NOT_THE_CURRENT_JOB: &str = "not_the_current_job";
    /// What the fact says is already the job's state (a redelivery).
    pub const ALREADY_KNOWN: &str = "already_known";
}

/// One job of a file, as the file's reads see it: the last one carries the
/// file's processing state.
#[derive(Debug, Clone, PartialEq)]
pub struct FileJob {
    pub job_id: Uuid,
    /// The index of the chain step the job runs, in the file's `steps`.
    pub step_index: i32,
    /// The gesture that started the job's chain; `None` for a job carried
    /// over from 0.1.
    pub trigger: Option<Trigger>,
    /// The principal whose gesture started the chain, as sent to Jobs.
    pub triggered_by: Option<Initiator>,
    /// What the job's reads need while it runs, as entries `{kind, ...}`: the
    /// current plan (`plan_declared`, `steps`), the latest step started
    /// (`step_started`, `index`, `label`, `started_at`) and a cancel request
    /// (`cancel_requested`); once the job was reported done while the chain's
    /// next step is held, `reported_done` (and a cancel request) instead;
    /// while the job is a first step awaiting its source, `awaiting_source`.
    /// Empty once the processing ended.
    pub events: Vec<serde_json::Value>,
    pub created_at: DateTime<Utc>,
}

/// Where the runner of a job says it is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunProgress {
    pub plan: Vec<String>,
    pub current_index: Option<i32>,
    pub current_label: Option<String>,
    pub at: Option<DateTime<Utc>>,
}

fn entries_of<'a>(
    events: &'a [serde_json::Value],
    wanted: &'a str,
) -> impl Iterator<Item = &'a serde_json::Value> + 'a {
    events
        .iter()
        .filter(move |entry| entry.get("kind").and_then(serde_json::Value::as_str) == Some(wanted))
}

impl FileJob {
    /// Whether the job was reported done while its file still processes:
    /// the chain's next step is held until Jobs ends this job, so it runs no
    /// more and is not the file's active job. Since 0.8.1.
    pub fn reported_done(&self) -> bool {
        entries_of(&self.events, kind::REPORTED_DONE)
            .next()
            .is_some()
    }

    /// Whether the job is the chain's first step, decided but not yet asked
    /// of Jobs: it waits for the engine to promote the file's source, so it
    /// runs nothing yet and is not the file's active job. Since 0.8.2.
    pub fn awaiting_source(&self) -> bool {
        entries_of(&self.events, kind::AWAITING_SOURCE)
            .next()
            .is_some()
    }

    /// Whether a user asked to cancel this job.
    pub fn cancel_requested(&self) -> bool {
        entries_of(&self.events, kind::CANCEL_REQUESTED)
            .next()
            .is_some()
    }

    /// The plan of the last `plan_declared`, and the latest `step_started` —
    /// by its start instant, then by index.
    pub fn progress(&self) -> RunProgress {
        let plan = entries_of(&self.events, kind::PLAN_DECLARED)
            .last()
            .and_then(|entry| entry.get("steps"))
            .and_then(|steps| serde_json::from_value::<Vec<String>>(steps.clone()).ok())
            .unwrap_or_default();
        let step = entries_of(&self.events, kind::STEP_STARTED)
            .filter_map(|entry| {
                let at = entry
                    .get("started_at")
                    .and_then(|at| serde_json::from_value::<DateTime<Utc>>(at.clone()).ok())?;
                let index = entry.get("index").and_then(serde_json::Value::as_i64)?;
                let label = entry
                    .get("label")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                Some((at, index, label))
            })
            .max_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
        RunProgress {
            plan,
            current_index: step
                .as_ref()
                .map(|(_, index, _)| i32::try_from(*index).unwrap_or(i32::MAX)),
            current_label: step.as_ref().map(|(_, _, label)| label.clone()),
            at: step.map(|(at, _, _)| at),
        }
    }
}

/// What the file's reads know of its running job: the current plan, the
/// latest step started, the first cancel request.
#[derive(Debug, Default)]
pub(crate) struct RunningFacts {
    pub plan: Option<Vec<String>>,
    pub step: Option<(i32, String, DateTime<Utc>)>,
    pub cancel_requested_at: Option<DateTime<Utc>>,
    /// The job was reported done and the chain's next step is held until
    /// Jobs ends it: the job no longer runs, its plan and step are gone.
    pub reported_done: bool,
    /// The job is the chain's first step awaiting the source's promotion:
    /// nothing of it ran.
    pub awaiting_source: bool,
}

impl RunningFacts {
    /// The facts as [`FileJob::events`] entries.
    pub(crate) fn entries(self) -> Vec<serde_json::Value> {
        let reported_done = self.reported_done;
        let mut entries = Vec::new();
        if self.awaiting_source {
            entries.push(serde_json::json!({ "kind": kind::AWAITING_SOURCE }));
        }
        if reported_done {
            entries.push(serde_json::json!({ "kind": kind::REPORTED_DONE }));
        } else if let Some(plan) = self.plan {
            entries.push(serde_json::json!({ "kind": kind::PLAN_DECLARED, "steps": plan }));
        }
        if let Some((index, label, started_at)) = self.step.filter(|_| !reported_done) {
            entries.push(serde_json::json!({
                "kind": kind::STEP_STARTED,
                "index": index,
                "label": label,
                "started_at": started_at,
            }));
        }
        if let Some(at) = self.cancel_requested_at {
            entries.push(serde_json::json!({ "kind": kind::CANCEL_REQUESTED, "at": at }));
        }
        entries
    }
}

/// A fact Jobs sent about a job of the file.
#[derive(Debug, Clone)]
pub(crate) enum Received {
    Queued {
        runner_type: String,
    },
    Started {
        run_id: Uuid,
    },
    Completed,
    PlanDeclared {
        run_id: Uuid,
        labels: Vec<String>,
    },
    StepStarted {
        run_id: Uuid,
        index: i32,
        label: String,
        started_at: DateTime<Utc>,
    },
    Failed {
        failure_cause: String,
        reason_code: String,
        note: Option<String>,
    },
    Cancelled,
    CreationRejected {
        reason_code: String,
        params: serde_json::Value,
    },
}

impl Received {
    /// The fact with its instants at the store's precision (microseconds):
    /// Jobs may send nanoseconds, `drive.file_processing` keeps microseconds,
    /// so a redelivered step compares equal to the one stored before it.
    fn at_store_precision(self) -> Self {
        match self {
            Self::StepStarted {
                run_id,
                index,
                label,
                started_at,
            } => Self::StepStarted {
                run_id,
                index,
                label,
                started_at: started_at.trunc_subsecs(6),
            },
            other => other,
        }
    }

    /// The fact as its own event.
    fn event(&self, job_id: Uuid) -> ProcessingEvent {
        match self.clone() {
            Self::Queued { runner_type } => ProcessingEvent::JobQueued {
                job_id,
                runner_type,
            },
            Self::Started { run_id } => ProcessingEvent::JobStarted { job_id, run_id },
            Self::Completed => ProcessingEvent::JobCompleted { job_id },
            Self::PlanDeclared { run_id, labels } => ProcessingEvent::JobPlanDeclared {
                job_id,
                run_id,
                labels,
            },
            Self::StepStarted {
                run_id,
                index,
                label,
                started_at,
            } => ProcessingEvent::JobStepStarted {
                job_id,
                run_id,
                index,
                label,
                started_at,
            },
            Self::Failed {
                failure_cause,
                reason_code,
                note,
            } => ProcessingEvent::JobFailed {
                job_id,
                failure_cause,
                reason_code,
                note,
            },
            Self::Cancelled => ProcessingEvent::JobCancelled { job_id },
            Self::CreationRejected {
                reason_code,
                params,
            } => ProcessingEvent::JobCreationRejected {
                job_id,
                reason_code,
                params,
            },
        }
    }
}

/// What a received fact did to the file's processing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Applied {
    /// It ended the running job: the file's status moved.
    Ended,
    /// It moved what the running job's runner says (its plan, its step).
    Progressed,
    /// It is recorded as itself and moves nothing a reader sees.
    Noted,
    /// It changes nothing any more: recorded as `JobFactIgnored`.
    Ignored,
    /// Jobs ended the job the chain's held next step waits for: the caller
    /// moves the chain on (`create_job` with the held id, `skip_step` or
    /// `abandon_step`), in the same transaction.
    Released,
}

/// A file's processing: its last job and where the chain stands.
pub struct FileProcessing<H> {
    pub(crate) file_id: Uuid,
    pub(crate) version: i64,
    pub(crate) state: ProcessingState,
    pub(crate) job_id: Uuid,
    pub(crate) step_index: i32,
    pub(crate) trigger: Option<Trigger>,
    pub(crate) triggered_by: Option<Initiator>,
    pub(crate) job_created_at: DateTime<Utc>,
    pub(crate) error_code: Option<String>,
    pub(crate) error_message: Option<String>,
    pub(crate) plan: Option<Vec<String>>,
    pub(crate) plan_index: Option<i32>,
    pub(crate) plan_label: Option<String>,
    pub(crate) plan_at: Option<DateTime<Utc>>,
    pub(crate) cancel_requested_at: Option<DateTime<Utc>>,
    pub(crate) past_job_ids: Vec<Uuid>,
    pub(crate) updated_at: DateTime<Utc>,
    /// The Passport the running chain acts under, as its initiator presented
    /// it: set when a chain starts, `None` from any end on. Never in a fact, a
    /// view or a log (the `Debug` of the state leaves it out).
    pub(crate) passport: Option<serde_json::Value>,
    /// The minted id of the chain's next step's job, held from the previous
    /// step's final report until Jobs ends that step's job; `None` otherwise.
    /// Set only while PROCESSING, with `job_id` reported done. Since 0.8.1.
    pub(crate) next_job_id: Option<Uuid>,
    /// The file's last job is the chain's first step, minted but not yet
    /// asked of Jobs: it waits for the source's promotion. Set only while
    /// PROCESSING. Since 0.8.2.
    pub(crate) awaiting_source: bool,
    /// The priority the running chain asks Jobs for at every step, as the
    /// gesture that started it named it (1 to 5); `None` when it named none,
    /// and from any end on. Since 0.9.0.
    pub(crate) priority: Option<i16>,
    pending: Pending<ProcessingEvent>,
    host: PhantomData<fn() -> H>,
}

impl<H> Clone for FileProcessing<H> {
    fn clone(&self) -> Self {
        Self {
            file_id: self.file_id,
            version: self.version,
            state: self.state,
            job_id: self.job_id,
            step_index: self.step_index,
            trigger: self.trigger,
            triggered_by: self.triggered_by.clone(),
            job_created_at: self.job_created_at,
            error_code: self.error_code.clone(),
            error_message: self.error_message.clone(),
            plan: self.plan.clone(),
            plan_index: self.plan_index,
            plan_label: self.plan_label.clone(),
            plan_at: self.plan_at,
            cancel_requested_at: self.cancel_requested_at,
            past_job_ids: self.past_job_ids.clone(),
            updated_at: self.updated_at,
            passport: self.passport.clone(),
            next_job_id: self.next_job_id,
            awaiting_source: self.awaiting_source,
            priority: self.priority,
            pending: self.pending.clone(),
            host: PhantomData,
        }
    }
}

impl<H> std::fmt::Debug for FileProcessing<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileProcessing")
            .field("file_id", &self.file_id)
            .field("version", &self.version)
            .field("state", &self.state)
            .field("job_id", &self.job_id)
            .field("step_index", &self.step_index)
            .field("next_job_id", &self.next_job_id)
            .field("awaiting_source", &self.awaiting_source)
            .field("priority", &self.priority)
            .finish_non_exhaustive()
    }
}

impl<H> FileProcessing<H> {
    /// The processing of a file never processed, before its first chain: no
    /// job yet. Only ever saved once `start_chain` gave it one.
    pub(crate) fn blank(file_id: Uuid, at: DateTime<Utc>) -> Self {
        Self {
            file_id,
            version: 0,
            state: ProcessingState::Ready,
            job_id: Uuid::nil(),
            step_index: 0,
            trigger: None,
            triggered_by: None,
            job_created_at: at,
            error_code: None,
            error_message: None,
            plan: None,
            plan_index: None,
            plan_label: None,
            plan_at: None,
            cancel_requested_at: None,
            past_job_ids: Vec::new(),
            updated_at: at,
            passport: None,
            next_job_id: None,
            awaiting_source: false,
            priority: None,
            pending: Pending::default(),
            host: PhantomData,
        }
    }

    pub fn file_id(&self) -> Uuid {
        self.file_id
    }

    pub fn version(&self) -> i64 {
        self.version
    }

    fn base_version(&self) -> i64 {
        self.version - self.pending.len()
    }

    fn push(&mut self, event: ProcessingEvent, meta: &FactMeta) {
        self.pending.push(&mut self.version, event, meta);
    }

    /// Whether the file's last job is still running: PROCESSING, not
    /// reported done with the chain's next step held, and not a first step
    /// still awaiting the source.
    pub(crate) fn is_running(&self) -> bool {
        self.state == ProcessingState::Processing
            && self.next_job_id.is_none()
            && !self.awaiting_source
    }

    /// Whether the file's last job is the chain's first step awaiting the
    /// source's promotion.
    pub(crate) fn awaits_source(&self) -> bool {
        self.awaiting_source
    }

    /// The id of the chain's next step's job, held until Jobs ends the
    /// file's last job.
    pub(crate) fn held_job_id(&self) -> Option<Uuid> {
        self.next_job_id
    }

    /// The file's status as its reads see it.
    pub(crate) fn status(&self) -> FileStatus {
        let facts = if self.state == ProcessingState::Processing {
            RunningFacts {
                plan: self.plan.clone(),
                step: self.plan_at.map(|at| {
                    (
                        self.plan_index.unwrap_or_default(),
                        self.plan_label.clone().unwrap_or_default(),
                        at,
                    )
                }),
                cancel_requested_at: self.cancel_requested_at,
                reported_done: self.next_job_id.is_some(),
                awaiting_source: self.awaiting_source,
            }
        } else {
            RunningFacts::default()
        };
        FileStatus {
            state: self.state,
            error: self.error_code.clone(),
            last_job: Some(FileJob {
                job_id: self.job_id,
                step_index: self.step_index,
                trigger: self.trigger,
                triggered_by: self.triggered_by.clone(),
                events: facts.entries(),
                created_at: self.job_created_at,
            }),
        }
    }

    /// A gesture started a chain on the settled file, acting under its
    /// initiator's `passport` and asking Jobs for `priority` until the chain
    /// ends.
    pub(crate) fn start_chain(
        &mut self,
        trigger: Trigger,
        ruleset_id: Option<Uuid>,
        steps: Vec<RulesetStep>,
        passport: serde_json::Value,
        priority: Option<i16>,
        meta: &FactMeta,
    ) {
        self.passport = Some(passport);
        self.priority = priority;
        self.updated_at = meta.occurred_at;
        self.push(
            ProcessingEvent::ChainStarted {
                trigger,
                ruleset_id,
                steps,
                priority,
            },
            meta,
        );
    }

    /// Makes `job_id` the file's running job, the chain's step `step_index`,
    /// acting under `passport` (the chain's own, carried over from the
    /// previous step).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_job(
        &mut self,
        job_id: Uuid,
        step_index: i32,
        runner_type: String,
        trigger: Option<Trigger>,
        triggered_by: Option<Initiator>,
        passport: serde_json::Value,
        meta: &FactMeta,
    ) {
        if self.job_id != job_id {
            self.retire_current_job();
        }
        self.passport = Some(passport);
        self.next_job_id = None;
        self.awaiting_source = false;
        self.state = ProcessingState::Processing;
        self.job_id = job_id;
        self.step_index = step_index;
        self.trigger = trigger;
        self.triggered_by = triggered_by;
        self.job_created_at = meta.occurred_at;
        self.updated_at = meta.occurred_at;
        self.push(
            ProcessingEvent::JobCreated {
                job_id,
                step_index,
                runner_type,
            },
            meta,
        );
    }

    /// The chain's first step, `step_index`, becomes the file's last job as
    /// `job_id`, minted now, but is not asked of Jobs: the file's source is
    /// not promoted yet. The file is PROCESSING and the chain keeps its
    /// Passport; [`Self::create_job`] with the same id follows at the
    /// promotion, or [`Self::drop_awaited_step`].
    pub(crate) fn await_source(
        &mut self,
        job_id: Uuid,
        step_index: i32,
        runner_type: String,
        trigger: Option<Trigger>,
        triggered_by: Option<Initiator>,
        meta: &FactMeta,
    ) {
        self.retire_current_job();
        self.next_job_id = None;
        self.awaiting_source = true;
        self.state = ProcessingState::Processing;
        self.job_id = job_id;
        self.step_index = step_index;
        self.trigger = trigger;
        self.triggered_by = triggered_by;
        self.job_created_at = meta.occurred_at;
        self.updated_at = meta.occurred_at;
        self.push(
            ProcessingEvent::SourceAwaited {
                step_index,
                runner_type,
                job_id,
            },
            meta,
        );
    }

    /// The chain stops before its first step, which awaited the source: the
    /// step is never asked of Jobs and the file is FAILED `reason_code`.
    pub(crate) fn drop_awaited_step(
        &mut self,
        runner_type: String,
        reason_code: &str,
        meta: &FactMeta,
    ) {
        let job_id = self.job_id;
        let step_index = self.step_index;
        self.end(Some((reason_code.to_string(), None)), meta);
        self.push(
            ProcessingEvent::AwaitedStepDropped {
                step_index,
                runner_type,
                job_id,
                reason_code: reason_code.to_string(),
            },
            meta,
        );
    }

    /// The current job becomes an earlier one: its late facts are still
    /// recognised (and ignored). A blank processing has no job to retire.
    fn retire_current_job(&mut self) {
        if !self.job_id.is_nil() && !self.past_job_ids.contains(&self.job_id) {
            self.past_job_ids.push(self.job_id);
        }
        self.error_code = None;
        self.error_message = None;
        self.plan = None;
        self.plan_index = None;
        self.plan_label = None;
        self.plan_at = None;
        self.cancel_requested_at = None;
    }

    /// The running job ended: READY on success, else FAILED with `error`. The
    /// chain's Passport and priority go with it, and a held next step: the
    /// chain is over.
    fn end(&mut self, error: Option<(String, Option<String>)>, meta: &FactMeta) {
        self.passport = None;
        self.priority = None;
        self.next_job_id = None;
        self.awaiting_source = false;
        match error {
            Some((code, message)) => {
                self.state = ProcessingState::Failed;
                self.error_code = Some(code);
                self.error_message = message;
            }
            None => {
                self.state = ProcessingState::Ready;
                self.error_code = None;
                self.error_message = None;
            }
        }
        self.updated_at = meta.occurred_at;
    }

    /// The runner's final report ended the running job. The chain moves on
    /// from the caller: `skip_step`, `abandon_step`, or nothing (READY); a
    /// next step to run is [`Self::report_done_holding`].
    pub(crate) fn report_done(&mut self, meta: &FactMeta) {
        let job_id = self.job_id;
        self.end(None, meta);
        self.push(ProcessingEvent::JobReportedDone { job_id }, meta);
    }

    /// The runner's final report ended the running job and the chain has a
    /// next step, `step_index`: its job id, `next_job_id`, is minted and
    /// held until Jobs ends this job (`Applied::Released`). The file stays
    /// PROCESSING, the chain's Passport stays held, a cancel request stays
    /// pending — the job is just no longer the running one.
    pub(crate) fn report_done_holding(
        &mut self,
        step_index: i32,
        runner_type: String,
        next_job_id: Uuid,
        meta: &FactMeta,
    ) {
        let job_id = self.job_id;
        self.next_job_id = Some(next_job_id);
        self.updated_at = meta.occurred_at;
        self.push(ProcessingEvent::JobReportedDone { job_id }, meta);
        self.push(
            ProcessingEvent::NextStepHeld {
                step_index,
                runner_type,
                job_id: next_job_id,
                after_job_id: job_id,
            },
            meta,
        );
    }

    /// The runner declared the running job failed.
    pub(crate) fn report_failed(
        &mut self,
        reason_code: &str,
        message: Option<&str>,
        meta: &FactMeta,
    ) {
        let job_id = self.job_id;
        self.end(
            Some((reason_code.to_string(), message.map(str::to_string))),
            meta,
        );
        self.push(
            ProcessingEvent::JobReportedFailed {
                job_id,
                reason_code: reason_code.to_string(),
                message: message.map(str::to_string),
            },
            meta,
        );
    }

    /// A user's cancel crossed the final report of the running job: the
    /// chain stops before step `step_index`, which is never launched, and the
    /// file is FAILED `cancelled`. The state keeps the ended job as the file's
    /// last one — no job id is minted for a step that never ran.
    pub(crate) fn skip_step(&mut self, step_index: i32, runner_type: String, meta: &FactMeta) {
        let after_job_id = self.job_id;
        self.end(Some((CANCELLED.to_string(), None)), meta);
        self.push(
            ProcessingEvent::StepSkipped {
                step_index,
                runner_type,
                after_job_id,
            },
            meta,
        );
    }

    /// The chain stopped before step `step_index` because its Passport is
    /// missing or unreadable (`reason_code`): the step is never launched and
    /// the file is FAILED with that code. Like `skip_step`, the ended job
    /// stays the file's last one.
    pub(crate) fn abandon_step(
        &mut self,
        step_index: i32,
        runner_type: String,
        reason_code: &str,
        meta: &FactMeta,
    ) {
        let after_job_id = self.job_id;
        self.end(Some((reason_code.to_string(), None)), meta);
        self.push(
            ProcessingEvent::StepAbandoned {
                step_index,
                runner_type,
                after_job_id,
                reason_code: reason_code.to_string(),
            },
            meta,
        );
    }

    /// The Passport the running chain acts under, taken out of the state —
    /// what the next step's job carries.
    pub(crate) fn take_passport(&mut self) -> Option<serde_json::Value> {
        self.passport.take()
    }

    /// A user asked to cancel the running job; the first request is the one
    /// the reads show.
    pub(crate) fn request_cancel(&mut self, meta: &FactMeta) {
        let job_id = self.job_id;
        self.cancel_requested_at.get_or_insert(meta.occurred_at);
        self.updated_at = meta.occurred_at;
        self.push(ProcessingEvent::JobCancelSent { job_id }, meta);
    }

    /// The host froze the file's drive: the cancel of the running job is
    /// sent (`JobCancelSent`, the caller stages `job.cancel`) and the job ends
    /// here, cancelled (`JobCancelledOnFreeze`) — the file is FAILED
    /// `cancelled` at once, so the runner's next call meets a job that no
    /// longer runs, and Jobs' own `cancelled` is recorded as ignored.
    pub(crate) fn cancel_on_freeze(&mut self, meta: &FactMeta) {
        let job_id = self.job_id;
        self.request_cancel(meta);
        self.end(Some((CANCELLED.to_string(), None)), meta);
        self.push(ProcessingEvent::JobCancelledOnFreeze { job_id }, meta);
    }

    /// Whether a user asked to cancel the running job.
    pub(crate) fn cancel_requested(&self) -> bool {
        self.cancel_requested_at.is_some()
    }

    /// Records a Jobs fact about `job_id`, one of the file's jobs, and says
    /// what it did. Nothing received is dropped: a fact that changes nothing
    /// any more is recorded as `JobFactIgnored`.
    pub(crate) fn receive(&mut self, job_id: Uuid, fact: Received, meta: &FactMeta) -> Applied {
        let fact = fact.at_store_precision();
        let event = fact.event(job_id);
        if job_id != self.job_id {
            return self.ignore(job_id, &event, why::NOT_THE_CURRENT_JOB, meta);
        }
        if self.next_job_id.is_some() {
            // The job was reported done and the chain's next step waits for
            // Jobs to end it: any end Jobs settles — its `completed`, or a
            // `failed` / `cancelled` it settled before the `job.finish` — frees
            // the file in Jobs. The report was the job's end: a Jobs end other
            // than `completed` is kept as ignored, as it always was.
            match fact {
                Received::Completed => {
                    self.push(event, meta);
                    return Applied::Released;
                }
                Received::Failed { .. }
                | Received::Cancelled
                | Received::CreationRejected { .. } => {
                    self.ignore(job_id, &event, why::JOB_ALREADY_ENDED, meta);
                    return Applied::Released;
                }
                _ => {}
            }
        }
        let running = self.is_running();
        match fact {
            Received::Queued { .. } | Received::Started { .. } | Received::Completed => {
                self.push(event, meta);
                Applied::Noted
            }
            Received::PlanDeclared { labels, .. } => {
                if !running {
                    return self.ignore(job_id, &event, why::JOB_ALREADY_ENDED, meta);
                }
                if self.plan.as_ref() == Some(&labels) {
                    return self.ignore(job_id, &event, why::ALREADY_KNOWN, meta);
                }
                self.plan = Some(labels);
                self.push(event, meta);
                Applied::Progressed
            }
            Received::StepStarted {
                index,
                label,
                started_at,
                ..
            } => {
                if !running {
                    return self.ignore(job_id, &event, why::JOB_ALREADY_ENDED, meta);
                }
                let later = match (self.plan_at, self.plan_index) {
                    (Some(at), current) => (started_at, index) > (at, current.unwrap_or_default()),
                    (None, _) => true,
                };
                if !later {
                    return self.ignore(job_id, &event, why::ALREADY_KNOWN, meta);
                }
                self.plan_index = Some(index);
                self.plan_label = Some(label);
                self.plan_at = Some(started_at);
                self.push(event, meta);
                Applied::Progressed
            }
            Received::Failed {
                ref reason_code,
                ref note,
                ..
            } => {
                if !running {
                    return self.ignore(job_id, &event, why::JOB_ALREADY_ENDED, meta);
                }
                self.end(Some((reason_code.clone(), note.clone())), meta);
                self.push(event, meta);
                Applied::Ended
            }
            Received::Cancelled => {
                if !running {
                    return self.ignore(job_id, &event, why::JOB_ALREADY_ENDED, meta);
                }
                self.end(Some((CANCELLED.to_string(), None)), meta);
                self.push(event, meta);
                Applied::Ended
            }
            Received::CreationRejected {
                ref reason_code, ..
            } => {
                if !running {
                    return self.ignore(job_id, &event, why::JOB_ALREADY_ENDED, meta);
                }
                self.end(Some((reason_code.clone(), None)), meta);
                self.push(event, meta);
                Applied::Ended
            }
        }
    }

    fn ignore(
        &mut self,
        job_id: Uuid,
        received: &ProcessingEvent,
        why: &str,
        meta: &FactMeta,
    ) -> Applied {
        let received = serde_json::to_value(received).unwrap_or(serde_json::Value::Null);
        self.push(
            ProcessingEvent::JobFactIgnored {
                job_id,
                received,
                why: why.to_string(),
            },
            meta,
        );
        Applied::Ignored
    }

    #[cfg(test)]
    pub(crate) fn pending(&self) -> &[Stamped<ProcessingEvent>] {
        self.pending.as_slice()
    }
}

/// The columns of `drive.file_processing`.
const COLUMNS: &str = "file_id, version, state, job_id, step_index, trigger, triggered_by_id, \
     triggered_by_name, job_created_at, error_code, error_message, plan, plan_index, plan_label, \
     plan_at, cancel_requested_at, past_job_ids, updated_at, passport, next_job_id, awaiting_source, \
     priority";

fn config_error(error: impl std::fmt::Display) -> EngineError {
    EngineError::Config(error.to_string())
}

fn row_to_processing<H>(row: &sqlx::postgres::PgRow) -> Result<FileProcessing<H>, EngineError> {
    let state: String = row.get("state");
    let trigger: Option<String> = row.get("trigger");
    let triggered_by_id: Option<Uuid> = row.get("triggered_by_id");
    Ok(FileProcessing {
        file_id: row.get("file_id"),
        version: row.get("version"),
        state: ProcessingState::from_db_str(&state).map_err(config_error)?,
        job_id: row.get("job_id"),
        step_index: row.get("step_index"),
        trigger: trigger
            .as_deref()
            .map(Trigger::from_db_str)
            .transpose()
            .map_err(config_error)?,
        triggered_by: triggered_by_id.map(|id| Initiator {
            id,
            display_name: row.get("triggered_by_name"),
        }),
        job_created_at: row.get("job_created_at"),
        error_code: row.get("error_code"),
        error_message: row.get("error_message"),
        plan: row.get("plan"),
        plan_index: row.get("plan_index"),
        plan_label: row.get("plan_label"),
        plan_at: row.get("plan_at"),
        cancel_requested_at: row.get("cancel_requested_at"),
        past_job_ids: row.get("past_job_ids"),
        updated_at: row.get("updated_at"),
        passport: row.get("passport"),
        next_job_id: row.get("next_job_id"),
        awaiting_source: row.get("awaiting_source"),
        priority: row.get("priority"),
        pending: Pending::default(),
        host: PhantomData,
    })
}

/// The file whose processing knows `job_id` — as its last job or an earlier
/// one — if any.
pub(crate) async fn file_of_job(
    conn: &mut PgConnection,
    job_id: Uuid,
) -> Result<Option<Uuid>, EngineError> {
    let row = sqlx::query(
        "SELECT file_id FROM drive.file_processing \
         WHERE job_id = $1 OR past_job_ids @> ARRAY[$1]::uuid[] LIMIT 1",
    )
    .bind(job_id)
    .fetch_optional(conn)
    .await?;
    Ok(row.map(|row| row.get("file_id")))
}

pub struct FileProcessingStore<H>(PhantomData<fn() -> H>);

impl<H: DriveHost> Persistence for FileProcessingStore<H> {
    type Aggregate = FileProcessing<H>;
    type Key = Uuid;
    type Event = Stamped<ProcessingEvent>;

    const STYLE: PersistenceStyle = PersistenceStyle::SoftEda;

    fn lock<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query("SELECT 1 FROM drive.file_processing WHERE file_id = $1 FOR UPDATE")
                .bind(key)
                .execute(conn)
                .await?;
            Ok(())
        })
    }

    fn read_many<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [Uuid],
    ) -> BoxFuture<'a, Result<Vec<(Uuid, FileProcessing<H>)>, EngineError>> {
        Box::pin(async move {
            let rows = sqlx::query(&format!(
                "SELECT {COLUMNS} FROM drive.file_processing WHERE file_id = ANY($1)"
            ))
            .bind(keys)
            .fetch_all(conn)
            .await?;
            rows.iter()
                .map(|row| row_to_processing(row).map(|state| (state.file_id, state)))
                .collect()
        })
    }

    fn save<'a>(
        conn: &'a mut PgConnection,
        processing: &'a FileProcessing<H>,
        events: &'a [Stamped<ProcessingEvent>],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            upsert(conn, processing).await?;
            hand_facts::<H>(conn, processing, events).await
        })
    }

    fn create<'a>(
        conn: &'a mut PgConnection,
        processing: &'a FileProcessing<H>,
        events: &'a [Stamped<ProcessingEvent>],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            upsert(conn, processing).await?;
            hand_facts::<H>(conn, processing, events).await
        })
    }
}

async fn upsert<H>(conn: &mut PgConnection, state: &FileProcessing<H>) -> Result<(), EngineError> {
    let (by_id, by_name) = match &state.triggered_by {
        Some(initiator) => (Some(initiator.id), initiator.display_name.clone()),
        None => (None, None),
    };
    sqlx::query(&format!(
        "INSERT INTO drive.file_processing ({COLUMNS}) VALUES \
         ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22) \
         ON CONFLICT (file_id) DO UPDATE SET version = EXCLUDED.version, state = EXCLUDED.state, \
           job_id = EXCLUDED.job_id, step_index = EXCLUDED.step_index, \
           trigger = EXCLUDED.trigger, triggered_by_id = EXCLUDED.triggered_by_id, \
           triggered_by_name = EXCLUDED.triggered_by_name, \
           job_created_at = EXCLUDED.job_created_at, error_code = EXCLUDED.error_code, \
           error_message = EXCLUDED.error_message, plan = EXCLUDED.plan, \
           plan_index = EXCLUDED.plan_index, plan_label = EXCLUDED.plan_label, \
           plan_at = EXCLUDED.plan_at, cancel_requested_at = EXCLUDED.cancel_requested_at, \
           past_job_ids = EXCLUDED.past_job_ids, updated_at = EXCLUDED.updated_at, \
           passport = EXCLUDED.passport, next_job_id = EXCLUDED.next_job_id, \
           awaiting_source = EXCLUDED.awaiting_source, priority = EXCLUDED.priority"
    ))
    .bind(state.file_id)
    .bind(state.version)
    .bind(state.state.as_str())
    .bind(state.job_id)
    .bind(state.step_index)
    .bind(state.trigger.map(Trigger::as_str))
    .bind(by_id)
    .bind(by_name)
    .bind(state.job_created_at)
    .bind(&state.error_code)
    .bind(&state.error_message)
    .bind(&state.plan)
    .bind(state.plan_index)
    .bind(&state.plan_label)
    .bind(state.plan_at)
    .bind(state.cancel_requested_at)
    .bind(&state.past_job_ids)
    .bind(state.updated_at)
    .bind(&state.passport)
    .bind(state.next_job_id)
    .bind(state.awaiting_source)
    .bind(state.priority)
    .execute(conn)
    .await?;
    Ok(())
}

async fn hand_facts<H: DriveHost>(
    conn: &mut PgConnection,
    state: &FileProcessing<H>,
    events: &[Stamped<ProcessingEvent>],
) -> Result<(), EngineError> {
    let key = facts::uuid_key(state.file_id);
    let facts = facts::facts_of(&key, state.base_version(), events)?;
    facts::hand::<H>(conn, &facts).await
}

impl<H: DriveHost> Aggregate for FileProcessing<H> {
    type Store = FileProcessingStore<H>;

    fn key(&self) -> Uuid {
        self.file_id
    }

    fn pending_events(&self) -> &[Stamped<ProcessingEvent>] {
        self.pending.as_slice()
    }
}

impl<H: DriveHost> SoftEda for FileProcessing<H> {
    fn clear_pending(&mut self) {
        self.pending.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::facts::ActorKind;

    fn meta() -> FactMeta {
        FactMeta {
            actor_id: Uuid::now_v7(),
            actor_kind: ActorKind::Service,
            is_runner: false,
            impersonator_id: None,
            correlation_id: Uuid::now_v7(),
            causation_id: None,
            occurred_at: Utc::now(),
        }
    }

    fn passport() -> serde_json::Value {
        serde_json::json!({ "kind": "service", "service_account_id": Uuid::now_v7(), "claims": {} })
    }

    fn running() -> FileProcessing<()> {
        let meta = meta();
        let mut processing = FileProcessing::<()>::blank(Uuid::now_v7(), meta.occurred_at);
        processing.start_chain(
            Trigger::Upload,
            None,
            Vec::new(),
            passport(),
            Some(4),
            &meta,
        );
        processing.create_job(
            Uuid::now_v7(),
            0,
            "render".into(),
            Some(Trigger::Upload),
            None,
            passport(),
            &meta,
        );
        processing
    }

    fn kinds(processing: &FileProcessing<()>) -> Vec<&'static str> {
        processing
            .pending()
            .iter()
            .map(|stamped| stamped.event.kind())
            .collect()
    }

    #[test]
    fn a_chain_start_is_two_events_and_the_file_processes() {
        let processing = running();
        assert_eq!(kinds(&processing), vec!["ChainStarted", "JobCreated"]);
        assert_eq!(processing.version, 2);
        assert_eq!(processing.base_version(), 0);
        assert!(processing.is_running());
        assert!(processing.past_job_ids.is_empty(), "a blank has no job");
    }

    #[test]
    fn the_first_end_wins_and_a_later_one_is_kept_as_ignored() {
        let mut processing = running();
        let job = processing.job_id;
        processing.report_done(&meta());
        assert_eq!(processing.state, ProcessingState::Ready);
        let applied = processing.receive(
            job,
            Received::Failed {
                failure_cause: "x".into(),
                reason_code: "late".into(),
                note: None,
            },
            &meta(),
        );
        assert_eq!(applied, Applied::Ignored);
        assert_eq!(processing.state, ProcessingState::Ready);
        assert!(processing.error_code.is_none());
        let Some(Stamped {
            event: ProcessingEvent::JobFactIgnored { received, why, .. },
            ..
        }) = processing.pending().last()
        else {
            panic!("the late end is recorded as ignored");
        };
        assert_eq!(why, why::JOB_ALREADY_ENDED);
        assert_eq!(received["kind"], "JobFailed");
    }

    #[test]
    fn a_freeze_sends_the_cancel_and_ends_the_job_there_so_jobs_cancelled_changes_nothing() {
        let mut processing = running();
        let job = processing.job_id;
        processing.cancel_on_freeze(&meta());
        assert_eq!(
            kinds(&processing),
            vec![
                "ChainStarted",
                "JobCreated",
                "JobCancelSent",
                "JobCancelledOnFreeze"
            ]
        );
        assert_eq!(processing.state, ProcessingState::Failed);
        assert_eq!(processing.error_code.as_deref(), Some(CANCELLED));
        assert_eq!(
            processing.receive(job, Received::Cancelled, &meta()),
            Applied::Ignored,
            "Jobs' own cancelled confirms a job already ended"
        );
        assert_eq!(processing.state, ProcessingState::Failed);
    }

    #[test]
    fn a_fact_of_an_earlier_job_is_ignored_and_a_repeated_progress_too() {
        let mut processing = running();
        let first = processing.job_id;
        processing.report_done(&meta());
        processing.create_job(
            Uuid::now_v7(),
            1,
            "index".into(),
            None,
            None,
            passport(),
            &meta(),
        );
        assert_eq!(processing.past_job_ids, vec![first]);
        assert_eq!(
            processing.receive(first, Received::Completed, &meta()),
            Applied::Ignored
        );
        let current = processing.job_id;
        let run_id = Uuid::now_v7();
        let plan = Received::PlanDeclared {
            run_id,
            labels: vec!["a".into()],
        };
        assert_eq!(
            processing.receive(current, plan.clone(), &meta()),
            Applied::Progressed
        );
        assert_eq!(processing.receive(current, plan, &meta()), Applied::Ignored);
        let at = Utc::now();
        let step = |index| Received::StepStarted {
            run_id,
            index,
            label: "a".into(),
            started_at: at,
        };
        assert_eq!(
            processing.receive(current, step(1), &meta()),
            Applied::Progressed
        );
        assert_eq!(
            processing.receive(current, step(0), &meta()),
            Applied::Ignored,
            "an older step never moves the progress back"
        );
        assert_eq!(processing.plan_index, Some(1));
    }

    #[test]
    fn a_cancel_crossing_the_final_report_skips_the_next_step_on_the_ended_job() {
        let mut processing = running();
        let ran = processing.job_id;
        processing.request_cancel(&meta());
        assert!(processing.cancel_requested());
        processing.report_done(&meta());
        processing.skip_step(1, "index".into(), &meta());
        assert_eq!(processing.state, ProcessingState::Failed);
        assert_eq!(processing.error_code.as_deref(), Some(CANCELLED));
        assert_eq!(
            processing.job_id, ran,
            "no job is minted for a step never launched"
        );
        assert_eq!(processing.step_index, 0);
        assert_eq!(
            kinds(&processing)[2..],
            ["JobCancelSent", "JobReportedDone", "StepSkipped"]
        );
        let Some(Stamped {
            event:
                ProcessingEvent::StepSkipped {
                    step_index,
                    runner_type,
                    after_job_id,
                },
            ..
        }) = processing.pending().last()
        else {
            panic!("the skipped step is a fact");
        };
        assert_eq!(
            (*step_index, runner_type.as_str(), *after_job_id),
            (1, "index", ran)
        );
        assert_eq!(
            processing.receive(ran, Received::Cancelled, &meta()),
            Applied::Ignored,
            "Jobs' late cancel of the ended job moves nothing"
        );
    }

    #[test]
    fn a_final_report_with_a_next_step_holds_it_until_jobs_ends_the_job() {
        let mut processing = running();
        let ran = processing.job_id;
        let held = Uuid::now_v7();
        processing.report_done_holding(1, "index".into(), held, &meta());
        assert_eq!(processing.state, ProcessingState::Processing);
        assert!(!processing.is_running(), "the reported job runs no more");
        assert_eq!(processing.held_job_id(), Some(held));
        assert!(processing.passport.is_some(), "the chain still holds it");
        assert_eq!(kinds(&processing)[2..], ["JobReportedDone", "NextStepHeld"]);
        let status = processing.status();
        assert_eq!(status.state, ProcessingState::Processing);
        let last = status.last_job.unwrap();
        assert_eq!(last.job_id, ran, "no job runs the held step yet");
        assert!(last.reported_done());
        assert!(last.progress().plan.is_empty());

        let run_id = Uuid::now_v7();
        assert_eq!(
            processing.receive(
                ran,
                Received::PlanDeclared {
                    run_id,
                    labels: vec!["late".into()]
                },
                &meta()
            ),
            Applied::Ignored,
            "a late progress fact of the reported job moves nothing"
        );
        assert_eq!(
            processing.receive(ran, Received::Completed, &meta()),
            Applied::Released
        );
        assert_eq!(processing.held_job_id(), Some(held), "the caller moves on");
        processing.create_job(held, 1, "index".into(), None, None, passport(), &meta());
        assert_eq!(processing.held_job_id(), None);
        assert!(processing.is_running());
        assert_eq!(processing.past_job_ids, vec![ran]);
        assert_eq!(
            processing.receive(ran, Received::Completed, &meta()),
            Applied::Ignored,
            "a redelivered completed finds the step already created"
        );
    }

    #[test]
    fn any_end_jobs_settles_releases_a_held_step_and_only_completed_is_recorded_as_itself() {
        for fact in [
            Received::Cancelled,
            Received::Failed {
                failure_cause: "x".into(),
                reason_code: "x".into(),
                note: None,
            },
            Received::CreationRejected {
                reason_code: "x".into(),
                params: serde_json::json!({}),
            },
        ] {
            let mut processing = running();
            let ran = processing.job_id;
            processing.report_done_holding(1, "index".into(), Uuid::now_v7(), &meta());
            assert_eq!(processing.receive(ran, fact, &meta()), Applied::Released);
            assert_eq!(processing.state, ProcessingState::Processing);
            assert!(processing.error_code.is_none(), "the report was the end");
            assert_eq!(kinds(&processing).last(), Some(&"JobFactIgnored"));
        }
        let mut processing = running();
        processing.report_done_holding(1, "index".into(), Uuid::now_v7(), &meta());
        assert_eq!(
            processing.receive(Uuid::now_v7(), Received::Completed, &meta()),
            Applied::Ignored,
            "another job's end releases nothing"
        );
        assert!(processing.held_job_id().is_some());
    }

    #[test]
    fn a_held_step_skipped_on_a_cancel_ends_the_chain_on_the_reported_job() {
        let mut processing = running();
        let ran = processing.job_id;
        processing.report_done_holding(1, "index".into(), Uuid::now_v7(), &meta());
        processing.request_cancel(&meta());
        assert_eq!(
            processing.receive(ran, Received::Completed, &meta()),
            Applied::Released
        );
        processing.skip_step(1, "index".into(), &meta());
        assert_eq!(processing.state, ProcessingState::Failed);
        assert_eq!(processing.error_code.as_deref(), Some(CANCELLED));
        assert_eq!(processing.job_id, ran);
        assert_eq!(processing.held_job_id(), None);
        assert!(processing.passport.is_none());
        assert_eq!(
            processing.receive(ran, Received::Completed, &meta()),
            Applied::Noted,
            "a redelivered completed after the skip only confirms"
        );
    }

    #[test]
    fn no_fact_of_the_library_is_named_as_a_request() {
        let mut processing = running();
        processing.request_cancel(&meta());
        processing.report_done(&meta());
        processing.skip_step(1, "index".into(), &meta());
        for kind in kinds(&processing) {
            for word in ["Requested", "Wanted", "Needed"] {
                assert!(!kind.ends_with(word), "{kind} is not a fact");
            }
        }
    }

    /// Jobs' instants carry nanoseconds on Linux; Postgres keeps
    /// microseconds. A step replayed after the state was stored and loaded
    /// again must still read as the step already known.
    #[test]
    fn a_step_replayed_after_a_store_round_trip_is_already_known_whatever_its_nanoseconds() {
        use chrono::Timelike;
        let at = Utc::now().with_nanosecond(123_456_789).unwrap();
        let run_id = Uuid::now_v7();
        let step = || Received::StepStarted {
            run_id,
            index: 0,
            label: "render".into(),
            started_at: at,
        };
        let plan = || Received::PlanDeclared {
            run_id,
            labels: vec!["render".into()],
        };
        let mut processing = running();
        let job = processing.job_id;
        assert_eq!(
            processing.receive(job, step(), &meta()),
            Applied::Progressed
        );
        assert_eq!(
            processing.plan_at,
            Some(at.trunc_subsecs(6)),
            "the step is kept at the store's precision"
        );
        // What a reload from `drive.file_processing` gives back.
        processing.plan_at = processing.plan_at.map(|at| at.trunc_subsecs(6));
        // Any order of the plan and the replays: two moves, then nothing.
        assert_eq!(
            processing.receive(job, plan(), &meta()),
            Applied::Progressed
        );
        assert_eq!(processing.receive(job, step(), &meta()), Applied::Ignored);
        assert_eq!(processing.receive(job, plan(), &meta()), Applied::Ignored);
        assert_eq!(processing.receive(job, step(), &meta()), Applied::Ignored);
        let mut reordered = running();
        let job = reordered.job_id;
        assert_eq!(reordered.receive(job, plan(), &meta()), Applied::Progressed);
        assert_eq!(reordered.receive(job, plan(), &meta()), Applied::Ignored);
        assert_eq!(reordered.receive(job, step(), &meta()), Applied::Progressed);
        reordered.plan_at = reordered.plan_at.map(|at| at.trunc_subsecs(6));
        assert_eq!(reordered.receive(job, step(), &meta()), Applied::Ignored);
    }

    #[test]
    fn the_status_shows_the_running_facts_only_while_the_job_runs() {
        let mut processing = running();
        processing.request_cancel(&meta());
        let status = processing.status();
        assert_eq!(status.state, ProcessingState::Processing);
        assert!(status.last_job.as_ref().unwrap().cancel_requested());
        processing.report_failed("broken", Some("why"), &meta());
        let status = processing.status();
        assert_eq!(status.state, ProcessingState::Failed);
        assert_eq!(status.error.as_deref(), Some("broken"));
        assert!(status.last_job.unwrap().events.is_empty());
    }

    #[test]
    fn progress_reads_the_last_plan_and_the_latest_step_whatever_their_arrival_order() {
        let early = Utc::now();
        let late = early + chrono::TimeDelta::seconds(5);
        let job = FileJob {
            job_id: Uuid::now_v7(),
            step_index: 0,
            trigger: None,
            triggered_by: None,
            events: vec![
                serde_json::json!({ "kind": "plan_declared", "steps": ["a", "b"] }),
                serde_json::json!({ "kind": "step_started", "index": 1, "label": "b", "started_at": late }),
                serde_json::json!({ "kind": "step_started", "index": 0, "label": "a", "started_at": early }),
            ],
            created_at: Utc::now(),
        };
        let progress = job.progress();
        assert_eq!(progress.plan, vec!["a", "b"]);
        assert_eq!(progress.current_index, Some(1));
        assert_eq!(progress.current_label.as_deref(), Some("b"));
        assert_eq!(progress.at, Some(late));
    }

    #[test]
    fn the_passport_is_held_only_while_the_chain_runs() {
        let mut processing = running();
        assert!(processing.passport.is_some(), "a running chain holds it");
        assert_eq!(processing.priority, Some(4), "and its priority");
        processing.report_done(&meta());
        assert!(processing.passport.is_none(), "READY drops it");
        assert!(processing.priority.is_none(), "READY drops the priority");

        let mut failed = running();
        failed.report_failed("broken", None, &meta());
        assert!(failed.passport.is_none(), "a reported failure drops it");
        assert!(failed.priority.is_none(), "and the priority");

        for fact in [
            Received::Cancelled,
            Received::Failed {
                failure_cause: "x".into(),
                reason_code: "x".into(),
                note: None,
            },
            Received::CreationRejected {
                reason_code: "x".into(),
                params: serde_json::json!({}),
            },
        ] {
            let mut ended = running();
            let job = ended.job_id;
            assert_eq!(ended.receive(job, fact, &meta()), Applied::Ended);
            assert!(ended.passport.is_none(), "an end from Jobs drops it");
            assert!(ended.priority.is_none(), "and the priority");
        }

        let mut frozen = running();
        frozen.cancel_on_freeze(&meta());
        assert!(frozen.passport.is_none(), "a freeze drops it");
        assert!(frozen.priority.is_none(), "and the priority");

        let mut skipped = running();
        skipped.request_cancel(&meta());
        skipped.report_done(&meta());
        skipped.skip_step(1, "index".into(), &meta());
        assert!(skipped.passport.is_none(), "a cancelled chain drops it");
        assert!(skipped.priority.is_none(), "and the priority");
    }

    #[test]
    fn the_chain_started_fact_names_a_priority_only_when_one_was_asked() {
        let meta = meta();
        let mut asked = FileProcessing::<()>::blank(Uuid::now_v7(), meta.occurred_at);
        asked.start_chain(
            Trigger::Upload,
            None,
            Vec::new(),
            passport(),
            Some(5),
            &meta,
        );
        let payload = serde_json::to_value(&asked.pending()[0].event).unwrap();
        assert_eq!(payload["priority"], serde_json::json!(5));

        let mut none = FileProcessing::<()>::blank(Uuid::now_v7(), meta.occurred_at);
        none.start_chain(Trigger::Upload, None, Vec::new(), passport(), None, &meta);
        let payload = serde_json::to_value(&none.pending()[0].event).unwrap();
        assert!(
            payload.get("priority").is_none(),
            "a chain asking for nothing keeps the 0.8 payload: {payload}"
        );
        let read: ProcessingEvent = serde_json::from_value(serde_json::json!({
            "kind": "ChainStarted", "trigger": "UPLOAD", "ruleset_id": null, "steps": []
        }))
        .unwrap();
        assert!(matches!(
            read,
            ProcessingEvent::ChainStarted { priority: None, .. }
        ));
    }

    #[test]
    fn a_chain_without_a_readable_passport_ends_failed_before_its_next_step() {
        let mut processing = running();
        let ran = processing.job_id;
        processing.take_passport();
        processing.report_done(&meta());
        processing.abandon_step(1, "index".into(), PASSPORT_UNREADABLE, &meta());
        assert_eq!(processing.state, ProcessingState::Failed);
        assert_eq!(processing.error_code.as_deref(), Some(PASSPORT_UNREADABLE));
        assert_eq!(processing.job_id, ran, "no job is minted for the step");
        assert!(processing.passport.is_none());
        let Some(Stamped {
            event:
                ProcessingEvent::StepAbandoned {
                    step_index,
                    reason_code,
                    after_job_id,
                    ..
                },
            ..
        }) = processing.pending().last()
        else {
            panic!("the abandoned step is a fact");
        };
        assert_eq!(
            (*step_index, reason_code.as_str(), *after_job_id),
            (1, PASSPORT_UNREADABLE, ran)
        );
        let facts = serde_json::to_string(
            &processing
                .pending()
                .iter()
                .map(|stamped| &stamped.event)
                .collect::<Vec<_>>(),
        )
        .unwrap();
        assert!(
            !facts.contains("service_account_id") && !facts.contains("claims"),
            "no fact carries the passport: {facts}"
        );
    }

    fn awaiting() -> (FileProcessing<()>, Uuid) {
        let meta = meta();
        let mut processing = FileProcessing::<()>::blank(Uuid::now_v7(), meta.occurred_at);
        processing.start_chain(
            Trigger::Upload,
            None,
            Vec::new(),
            passport(),
            Some(2),
            &meta,
        );
        let job = Uuid::now_v7();
        processing.await_source(job, 0, "render".into(), Some(Trigger::Upload), None, &meta);
        (processing, job)
    }

    #[test]
    fn a_first_step_awaiting_the_source_processes_but_runs_nothing() {
        let (processing, job) = awaiting();
        assert_eq!(kinds(&processing), vec!["ChainStarted", "SourceAwaited"]);
        assert_eq!(processing.state, ProcessingState::Processing);
        assert!(processing.awaits_source());
        assert!(!processing.is_running(), "no job runs before the promotion");
        assert!(
            processing.passport.is_some(),
            "the chain keeps its passport"
        );
        assert_eq!(processing.held_job_id(), None, "not a held next step");
        let status = processing.status();
        let last = status.last_job.expect("the minted step is the last job");
        assert_eq!(last.job_id, job);
        assert_eq!(last.step_index, 0);
        assert!(last.awaiting_source());
        assert!(!last.reported_done());
        assert_eq!(last.progress().plan, Vec::<String>::new());
    }

    #[test]
    fn the_promotion_creates_the_awaited_step_with_its_minted_id() {
        let (mut processing, job) = awaiting();
        let meta = meta();
        processing.create_job(
            job,
            0,
            "render".into(),
            Some(Trigger::Upload),
            None,
            passport(),
            &meta,
        );
        assert_eq!(
            kinds(&processing),
            vec!["ChainStarted", "SourceAwaited", "JobCreated"]
        );
        assert!(!processing.awaits_source());
        assert!(processing.is_running());
        assert_eq!(processing.job_id, job);
        assert!(
            processing.past_job_ids.is_empty(),
            "the awaited step is the created job, not an earlier one"
        );
        assert!(!processing.status().last_job.unwrap().awaiting_source());
    }

    #[test]
    fn a_dropped_awaited_step_fails_the_file_and_lets_its_passport_go() {
        let (mut processing, job) = awaiting();
        processing.drop_awaited_step("render".into(), CANCELLED, &meta());
        assert_eq!(processing.state, ProcessingState::Failed);
        assert_eq!(processing.error_code.as_deref(), Some(CANCELLED));
        assert!(!processing.awaits_source());
        assert!(processing.passport.is_none());
        let Some(Stamped {
            event:
                ProcessingEvent::AwaitedStepDropped {
                    step_index,
                    job_id,
                    reason_code,
                    runner_type,
                },
            ..
        }) = processing.pending().last()
        else {
            panic!("the dropped step is a fact");
        };
        assert_eq!(
            (
                *step_index,
                *job_id,
                reason_code.as_str(),
                runner_type.as_str()
            ),
            (0, job, CANCELLED, "render")
        );
    }
}
