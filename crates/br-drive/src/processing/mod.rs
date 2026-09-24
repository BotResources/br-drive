//! The ruleset chain over Jobs: one job per step, staged through the engine
//! outbox, advanced by Jobs' facts and the runner's final report.

mod backstop;
mod chain;
mod commands;
mod reactions;
mod roots;

pub(crate) use backstop::run_alive;
pub use backstop::{
    LAUNCH_RETRY_AFTER, LAUNCH_RETRY_CAP, LAUNCH_RETRY_DURABLE, LaunchRetry, STEP_DEADLINE_DURABLE,
    StepDeadline, launch_retry, pickup_timeout, retry_delay, step_deadline, step_timeout,
};
pub use chain::{
    ChainPlan, advance, cancel_active_job, finish_active_job, start_chain, wipe_rendition,
};
pub use commands::Initiator;
pub use reactions::{
    CancelledFact, CompletedFact, CreationRejectedFact, DURABLE_CANCELLED, DURABLE_COMPLETED,
    DURABLE_CREATION_REJECTED, DURABLE_FAILED, DURABLE_PLAN_DECLARED, DURABLE_QUEUED,
    DURABLE_STARTED, DURABLE_STEP_STARTED, FailedFact, PlanDeclaredFact, QueuedFact, StartedFact,
    StepStartedFact, on_cancelled, on_completed, on_creation_rejected, on_failed, on_plan_declared,
    on_queued, on_started, on_step_started,
};
pub use roots::{RootNames, declare_roots};

pub const RUNNER_TYPE_UNAVAILABLE: &str = "runner_type_unavailable";
#[deprecated(
    since = "0.2.0",
    note = "no longer raised: a step fired before the first catalogue scan is deferred"
)]
pub const CATALOGUE_NOT_WATCHED: &str = "catalogue_not_watched";
/// The reason a file lands FAILED when a step outlives its deadline
/// (`DriveHost::PICKUP_TIMEOUT` or `DriveHost::STEP_TIMEOUT`).
pub const TIMED_OUT: &str = "timed_out";
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
