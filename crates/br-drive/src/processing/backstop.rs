//! The two scheduled messages that keep a chain from sitting in PROCESSING
//! forever: the step deadline, and the retry of a launch deferred until the
//! host's first catalogue scan. Both name the step they were scheduled for by
//! its index and the instant it was entered, so a message outliving its step is
//! a no-op, redelivered or not.

use std::marker::PhantomData;
use std::time::Duration;

use br_core_integration::{Aggregate as BcAggregate, Bc, CommandCoords, Verb};
use chrono::{DateTime, TimeDelta, Utc};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::inbound::{ReactionCoordinates, ReactionMessage};
use service_engine::pipeline::{Ops, Reaction};
use service_engine::time::Timestamp;
use uuid::Uuid;

use super::TIMED_OUT;
use super::chain::{Launched, cancel_active_job, mark_failed, outcome_cause, stage_job};
use crate::fault::DriveReactionFault;
use crate::file::{File, FileCause, FileRow, ProcessingState};
use crate::host::DriveHost;
use crate::upload::UPLOAD_DEADLINE_AGGREGATE;

pub const STEP_DEADLINE_VERB: &str = "step-deadline";
pub const STEP_DEADLINE_DURABLE: &str = "step-deadline";
pub const LAUNCH_RETRY_VERB: &str = "launch-retry";
pub const LAUNCH_RETRY_DURABLE: &str = "launch-retry";

/// How long a deferred launch first waits before asking the catalogue again;
/// every further retry waits twice as long, up to `LAUNCH_RETRY_CAP`.
pub const LAUNCH_RETRY_AFTER: Duration = Duration::from_secs(5);
pub const LAUNCH_RETRY_CAP: Duration = Duration::from_secs(5 * 60);

/// The wait before retry number `attempt` (0-based) of a deferred launch.
pub fn retry_delay(attempt: u32) -> Duration {
    LAUNCH_RETRY_AFTER
        .checked_mul(1u32.checked_shl(attempt).unwrap_or(u32::MAX))
        .map_or(LAUNCH_RETRY_CAP, |delay| delay.min(LAUNCH_RETRY_CAP))
}

/// `DriveHost::STEP_TIMEOUT` as a delta the scheduler accepts, checked once at
/// registration so a launch never meets an unusable value.
pub fn step_timeout<H: DriveHost>() -> Result<TimeDelta, EngineError> {
    TimeDelta::from_std(H::STEP_TIMEOUT)
        .ok()
        .filter(|timeout| *timeout > TimeDelta::zero())
        .filter(|timeout| Utc::now().checked_add_signed(*timeout).is_some())
        .ok_or_else(|| {
            EngineError::Config(
                "DriveHost::STEP_TIMEOUT must be positive and fit a scheduled deadline".into(),
            )
        })
}

fn coordinates<H: DriveHost>(verb: &'static str) -> ReactionCoordinates {
    ReactionCoordinates::Command(CommandCoords {
        receiver: Bc::new(H::SERVICE).expect("the host service name is a valid bc"),
        aggregate: BcAggregate::new(UPLOAD_DEADLINE_AGGREGATE).expect("a static aggregate segment"),
        verb: Verb::new(verb).expect("a static verb segment"),
        version: 1,
    })
}

/// Whether `file` is still inside the step a message was scheduled for: the
/// same index, entered at the same instant (a reprocess re-entering the same
/// index gets a new instant), and still PROCESSING.
fn names_the_step<H>(file: &FileRow<H>, step: i32, entered_at: DateTime<Utc>) -> bool {
    file.processing_state == ProcessingState::Processing
        && file.step_index == Some(step)
        && file.step_entered_at == Some(entered_at)
}

#[derive(Serialize, Deserialize)]
pub struct StepDeadline<H> {
    pub file_id: Uuid,
    pub step: i32,
    pub entered_at: DateTime<Utc>,
    #[serde(skip)]
    host: PhantomData<fn() -> H>,
}

impl<H> StepDeadline<H> {
    pub fn new(file_id: Uuid, step: i32, entered_at: DateTime<Utc>) -> Self {
        Self {
            file_id,
            step,
            entered_at,
            host: PhantomData,
        }
    }
}

impl<H: DriveHost> ReactionMessage for StepDeadline<H> {
    fn coordinates() -> ReactionCoordinates {
        coordinates::<H>(STEP_DEADLINE_VERB)
    }

    fn decode(payload: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(payload)
    }
}

#[derive(Serialize, Deserialize)]
pub struct LaunchRetry<H> {
    pub file_id: Uuid,
    pub step: i32,
    pub entered_at: DateTime<Utc>,
    /// How many retries came before this one: the next waits `retry_delay`.
    #[serde(default)]
    pub attempt: u32,
    #[serde(skip)]
    host: PhantomData<fn() -> H>,
}

impl<H> LaunchRetry<H> {
    pub fn new(file_id: Uuid, step: i32, entered_at: DateTime<Utc>, attempt: u32) -> Self {
        Self {
            file_id,
            step,
            entered_at,
            attempt,
            host: PhantomData,
        }
    }
}

impl<H: DriveHost> ReactionMessage for LaunchRetry<H> {
    fn coordinates() -> ReactionCoordinates {
        coordinates::<H>(LAUNCH_RETRY_VERB)
    }

    fn decode(payload: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(payload)
    }
}

/// Schedules retry number `attempt` of the launch of the step `file` is in.
pub(super) fn schedule_retry<H: DriveHost>(
    cx: &mut Ops<'_>,
    file: &FileRow<H>,
    attempt: u32,
) -> Result<(), EngineError> {
    let (Some(step), Some(entered_at)) = (file.step_index, file.step_entered_at) else {
        return Err(EngineError::Config(
            "a launch is retried only for a file inside a step".into(),
        ));
    };
    let delay = TimeDelta::from_std(retry_delay(attempt)).unwrap_or(TimeDelta::minutes(5));
    cx.schedule_at(
        cx.now() + delay,
        LaunchRetry::<H>::new(file.id, step, entered_at, attempt),
    )
}

/// The deadline of the step `file` just entered: `STEP_TIMEOUT` after its last
/// sign of life (its entry, until Jobs or the runner says more).
pub(super) fn schedule_deadline<H: DriveHost>(
    cx: &mut Ops<'_>,
    file: &FileRow<H>,
) -> Result<(), EngineError> {
    let (Some(step), Some(entered_at)) = (file.step_index, file.step_entered_at) else {
        return Err(EngineError::Config(
            "a deadline is scheduled only for a file inside a step".into(),
        ));
    };
    let alive = file.step_alive_at.unwrap_or(entered_at);
    cx.schedule_at(
        Timestamp::from_utc(alive + step_timeout::<H>()?),
        StepDeadline::<H>::new(file.id, step, entered_at),
    )
}

/// The step fell silent for `DriveHost::STEP_TIMEOUT`: whatever job it holds is
/// cancelled and the file lands FAILED `timed_out`, open to a reprocess. Jobs
/// never fails a job no live runner picked up, so this is the way out of that
/// state. A step that showed a sign of life since the message was scheduled
/// gets a later deadline instead.
pub fn step_deadline<'r, H: DriveHost>(
    cx: &'r mut Reaction<'r>,
    message: StepDeadline<H>,
) -> BoxFuture<'r, Result<(), DriveReactionFault>> {
    Box::pin(async move {
        let Some(mut file) = cx.load::<FileRow<H>>(&message.file_id).await? else {
            return Ok(());
        };
        if !names_the_step(&file, message.step, message.entered_at) {
            return Ok(());
        }
        let alive = file.step_alive_at.unwrap_or(message.entered_at);
        if cx.now().as_datetime() < alive + step_timeout::<H>()? {
            schedule_deadline(cx, &file)?;
            return Ok(());
        }
        cancel_active_job(cx, &file)?;
        // The cancelled job stays known: Jobs may still count it as live when
        // the next launch asks for a job, and that launch cancels it again.
        file.stray_job_id = file.job_id.or(file.stray_job_id);
        mark_failed(&mut file, TIMED_OUT);
        file.updated_at = cx.now().as_datetime();
        cx.save(&file).await?;
        cx.impact_caused::<File, _>(
            &file.id,
            FileCause::ProcessingFailed {
                reason: TIMED_OUT.to_string(),
            },
        )?;
        Ok(())
    })
}

/// A deferred launch asks again: the job is staged once the catalogue has been
/// scanned, or the next retry is scheduled, later.
pub fn launch_retry<'r, H: DriveHost>(
    cx: &'r mut Reaction<'r>,
    message: LaunchRetry<H>,
) -> BoxFuture<'r, Result<(), DriveReactionFault>> {
    Box::pin(async move {
        let Some(mut file) = cx.load::<FileRow<H>>(&message.file_id).await? else {
            return Ok(());
        };
        if !names_the_step(&file, message.step, message.entered_at) || file.job_id.is_some() {
            return Ok(());
        }
        let launched = stage_job(cx, &mut file, message.attempt.saturating_add(1)).await?;
        if launched == Launched::Deferred {
            // Still unscanned: the next retry is staged and nothing else moved.
            return Ok(());
        }
        file.updated_at = cx.now().as_datetime();
        cx.save(&file).await?;
        let step = usize::try_from(message.step).unwrap_or(0);
        cx.impact_caused::<File, _>(&file.id, outcome_cause(&file, launched, step))?;
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_deferred_launch_backs_off_from_five_seconds_to_five_minutes() {
        assert_eq!(retry_delay(0), Duration::from_secs(5));
        assert_eq!(retry_delay(1), Duration::from_secs(10));
        assert_eq!(retry_delay(5), Duration::from_secs(160));
        assert_eq!(retry_delay(6), LAUNCH_RETRY_CAP);
        assert_eq!(retry_delay(40), LAUNCH_RETRY_CAP);
    }

    #[test]
    fn a_step_message_names_one_entry_of_one_step_of_a_processing_file() {
        let entered = Utc::now().trunc_subsecs(6);
        let mut file = crate::file::tests_support::processing_file::<()>(1, entered);
        assert!(names_the_step(&file, 1, entered));
        assert!(!names_the_step(&file, 0, entered), "another step");
        assert!(
            !names_the_step(&file, 1, entered + TimeDelta::microseconds(1)),
            "the same step re-entered later"
        );
        // A message survives a JSON round trip with its instant intact.
        let message = StepDeadline::<()>::new(file.id, 1, entered);
        let back: StepDeadline<()> =
            serde_json::from_slice(&serde_json::to_vec(&message).unwrap()).unwrap();
        assert!(names_the_step(&file, back.step, back.entered_at));
        for state in [ProcessingState::Ready, ProcessingState::Failed] {
            file.processing_state = state;
            assert!(!names_the_step(&file, 1, entered), "{state:?}");
        }
    }

    use chrono::SubsecRound;
}
