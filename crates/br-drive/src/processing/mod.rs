//! The ruleset chain over Jobs: one job per step, staged through the engine
//! outbox, ended by the runner (its final report or its declared failure) and
//! by Jobs' own terminal facts, whichever comes first.

mod chain;
mod commands;
mod reactions;
mod roots;
mod state;

pub use chain::{ChainPlan, cancel_active_job};
pub(crate) use chain::{load_processing, report_done, report_failed, start_chain};
pub use commands::Initiator;
pub(crate) use commands::JobCancel;
pub use reactions::{
    CancelledFact, CompletedFact, CreationRejectedFact, DURABLE_CANCELLED, DURABLE_COMPLETED,
    DURABLE_CREATION_REJECTED, DURABLE_FAILED, DURABLE_PLAN_DECLARED, DURABLE_QUEUED,
    DURABLE_STARTED, DURABLE_STEP_STARTED, FailedFact, PlanDeclaredFact, QueuedFact, StartedFact,
    StepStartedFact, on_cancelled, on_completed, on_creation_rejected, on_failed, on_plan_declared,
    on_queued, on_started, on_step_started,
};
pub use roots::{RootNames, declare_roots};
pub(crate) use state::RunningFacts;
pub use state::{
    CANCELLED, FileJob, FileProcessing, FileProcessingNoun, FileProcessingStore,
    PROCESSING_EVENT_VERSION, ProcessingEvent, RunProgress, kind as job_event,
    why as ignored_because,
};

/// The name of one of the library's durables on the host's NATS consumers: every
/// host service gets its own consumer on the shared integration streams, so two
/// hosts on one cluster never split the facts between them.
pub fn durable(service: &str, suffix: &str) -> String {
    format!("{service}-drive-{suffix}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_durable_is_namespaced_by_the_host_service() {
        assert_eq!(
            durable("workspace", DURABLE_COMPLETED),
            "workspace-drive-job-completed"
        );
        assert_ne!(
            durable("workspace", DURABLE_COMPLETED),
            durable("archive", DURABLE_COMPLETED)
        );
    }
}
