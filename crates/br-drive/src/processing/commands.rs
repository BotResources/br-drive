use br_core_integration::CommandCoords;
use contract_jobs::command::{CancelJob, CreateJob, FinishJob, TriggeredBy};
use serde::{Deserialize, Serialize};
use service_engine::pipeline::OutboundCommand;
use uuid::Uuid;

use crate::host::DriveHost;

macro_rules! outgoing {
    ($name:ident, $payload:ty, $coords:path) => {
        #[derive(Serialize)]
        pub struct $name {
            #[serde(flatten)]
            pub payload: $payload,
        }

        impl OutboundCommand for $name {
            fn coords(&self) -> CommandCoords {
                $coords().expect("the published jobs coordinates are valid")
            }

            fn command_id(&self) -> Uuid {
                Uuid::now_v7()
            }
        }
    };
}

outgoing!(
    JobCreate,
    CreateJob,
    contract_jobs::cmd_job_create_v1_coords
);
outgoing!(
    JobFinish,
    FinishJob,
    contract_jobs::cmd_job_finish_v2_coords
);
outgoing!(
    JobCancel,
    CancelJob,
    contract_jobs::cmd_job_cancel_v2_coords
);

/// The principal whose gesture started a chain; every step of the chain names
/// it as the job's `triggered_by`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Initiator {
    pub id: Uuid,
    pub display_name: Option<String>,
}

impl Initiator {
    pub fn of<H: DriveHost>(principal: &H) -> Self {
        Self {
            id: principal.id().as_uuid(),
            display_name: principal.display_name(),
        }
    }

    pub(super) fn triggered_by(&self) -> TriggeredBy {
        match &self.display_name {
            Some(display_name) => TriggeredBy::Identified {
                id: self.id,
                display_name: display_name.clone(),
            },
            None => TriggeredBy::Anonymous(self.id),
        }
    }
}
