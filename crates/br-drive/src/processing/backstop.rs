//! The two scheduled messages that keep a chain from sitting in PROCESSING
//! forever: the step deadline, and the retry of a launch deferred until the
//! host's first catalogue scan. Both name the step they were scheduled for by
//! its index and the instant it was entered, so a message outliving its step is
//! a no-op.

use std::marker::PhantomData;
use std::time::Duration;

use br_core_integration::{Aggregate as BcAggregate, Bc, CommandCoords, Verb};
use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::inbound::{ReactionCoordinates, ReactionMessage};
use service_engine::pipeline::Reaction;
use uuid::Uuid;

use super::TIMED_OUT;
use super::chain::{Launched, cancel_active_job, mark_failed, outcome_cause, stage_job};
use crate::fault::DriveReactionFault;
use crate::file::{File, FileCause, FileRow, ProcessingState};
use crate::host::DriveHost;

pub const BACKSTOP_AGGREGATE: &str = "drive_file";
pub const STEP_DEADLINE_VERB: &str = "step-deadline";
pub const STEP_DEADLINE_DURABLE: &str = "step-deadline";
pub const LAUNCH_RETRY_VERB: &str = "launch-retry";
pub const LAUNCH_RETRY_DURABLE: &str = "launch-retry";

/// How long a deferred launch waits before asking the catalogue again.
pub const LAUNCH_RETRY_AFTER: Duration = Duration::from_secs(5);

fn coordinates<H: DriveHost>(verb: &'static str) -> ReactionCoordinates {
    ReactionCoordinates::Command(CommandCoords {
        receiver: Bc::new(H::SERVICE).expect("the host service name is a valid bc"),
        aggregate: BcAggregate::new(BACKSTOP_AGGREGATE).expect("a static aggregate segment"),
        verb: Verb::new(verb).expect("a static verb segment"),
        version: 1,
    })
}

macro_rules! step_message {
    ($name:ident, $verb:expr) => {
        #[derive(Serialize, Deserialize)]
        pub struct $name<H> {
            pub file_id: Uuid,
            pub step: i32,
            pub entered_at: DateTime<Utc>,
            #[serde(skip)]
            host: PhantomData<fn() -> H>,
        }

        impl<H> $name<H> {
            pub fn new(file_id: Uuid, step: i32, entered_at: DateTime<Utc>) -> Self {
                Self {
                    file_id,
                    step,
                    entered_at,
                    host: PhantomData,
                }
            }

            /// Whether the file is still inside the very step this message was
            /// scheduled for.
            fn names_the_step_of(&self, file: &FileRow<H>) -> bool {
                file.processing_state == ProcessingState::Processing
                    && file.step_index == Some(self.step)
                    && file.step_entered_at == Some(self.entered_at)
            }
        }

        impl<H: DriveHost> ReactionMessage for $name<H> {
            fn coordinates() -> ReactionCoordinates {
                coordinates::<H>($verb)
            }

            fn decode(payload: &[u8]) -> Result<Self, serde_json::Error> {
                serde_json::from_slice(payload)
            }
        }
    };
}

step_message!(StepDeadline, STEP_DEADLINE_VERB);
step_message!(LaunchRetry, LAUNCH_RETRY_VERB);

/// The step outlived `DriveHost::STEP_TIMEOUT`: whatever job it holds is
/// cancelled and the file lands FAILED `timed_out`, open to a reprocess. Jobs
/// never fails a job no live runner ever picked up, so this is the only way out
/// of that state.
pub fn step_deadline<'r, H: DriveHost>(
    cx: &'r mut Reaction<'r>,
    message: StepDeadline<H>,
) -> BoxFuture<'r, Result<(), DriveReactionFault>> {
    Box::pin(async move {
        let Some(mut file) = cx.load::<FileRow<H>>(&message.file_id).await? else {
            return Ok(());
        };
        if !message.names_the_step_of(&file) {
            return Ok(());
        }
        cancel_active_job(cx, &file)?;
        file.stray_job_id = None;
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
/// scanned, or the retry is scheduled once more.
pub fn launch_retry<'r, H: DriveHost>(
    cx: &'r mut Reaction<'r>,
    message: LaunchRetry<H>,
) -> BoxFuture<'r, Result<(), DriveReactionFault>> {
    Box::pin(async move {
        let Some(mut file) = cx.load::<FileRow<H>>(&message.file_id).await? else {
            return Ok(());
        };
        if !message.names_the_step_of(&file) || file.job_id.is_some() {
            return Ok(());
        }
        let launched = stage_job(cx, &mut file).await?;
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
