//! The ruleset chain over Jobs: one job per step, staged through the engine
//! outbox, advanced by Jobs' facts and the runner's final report.

mod chain;
mod commands;
mod log;
mod reactions;
mod roots;

pub(crate) use chain::refresh_status;
pub use chain::{ChainPlan, cancel_active_job, finish_job, start_chain, wipe_rendition};
pub use commands::Initiator;
pub(crate) use commands::JobCancel;
pub use log::{FileJob, RunProgress, kind as job_event};
pub(crate) use log::{append as append_job_event, entry as job_entry};
pub use reactions::{
    CancelledFact, CompletedFact, CreationRejectedFact, DURABLE_CANCELLED, DURABLE_COMPLETED,
    DURABLE_CREATION_REJECTED, DURABLE_FAILED, DURABLE_PLAN_DECLARED, DURABLE_QUEUED,
    DURABLE_STARTED, DURABLE_STEP_STARTED, FailedFact, PlanDeclaredFact, QueuedFact, StartedFact,
    StepStartedFact, on_cancelled, on_completed, on_creation_rejected, on_failed, on_plan_declared,
    on_queued, on_started, on_step_started,
};
pub use roots::{RootNames, declare_roots};

/// The `processingError` of a file whose job Jobs cancelled.
pub const CANCELLED: &str = "cancelled";

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
