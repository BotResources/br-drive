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

/// The longest display name Jobs accepts on `triggered_by`.
pub const MAX_DISPLAY_NAME_CHARS: usize = 512;

impl Initiator {
    pub fn of<H: DriveHost>(principal: &H) -> Self {
        Self {
            id: principal.id().as_uuid(),
            display_name: principal.display_name(),
        }
    }

    /// What `job.create` says of the initiator, in the shape Jobs accepts: a
    /// UUIDv7 id (an erased initiator, the nil id, is not named at all), and a
    /// display name trimmed, non-blank and at most 512 characters.
    pub(super) fn triggered_by(&self) -> Option<TriggeredBy> {
        if self.id.get_version_num() != 7 {
            return None;
        }
        let display_name = self
            .display_name
            .as_deref()
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(|name| {
                name.chars()
                    .take(MAX_DISPLAY_NAME_CHARS)
                    .collect::<String>()
            });
        Some(match display_name {
            Some(display_name) => TriggeredBy::Identified {
                id: self.id,
                display_name,
            },
            None => TriggeredBy::Anonymous(self.id),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn initiator(id: Uuid, display_name: Option<&str>) -> Initiator {
        Initiator {
            id,
            display_name: display_name.map(str::to_string),
        }
    }

    #[test]
    fn the_initiator_is_named_to_jobs_only_in_a_shape_jobs_accepts() {
        let id = Uuid::now_v7();
        assert_eq!(
            initiator(id, Some("  Ada ")).triggered_by(),
            Some(TriggeredBy::Identified {
                id,
                display_name: "Ada".into()
            })
        );
        assert_eq!(
            initiator(id, Some("   ")).triggered_by(),
            Some(TriggeredBy::Anonymous(id)),
            "a blank name is no name"
        );
        let long = "é".repeat(600);
        let Some(TriggeredBy::Identified { display_name, .. }) =
            initiator(id, Some(&long)).triggered_by()
        else {
            panic!("a long name is kept, cut");
        };
        assert_eq!(display_name.chars().count(), MAX_DISPLAY_NAME_CHARS);
        assert_eq!(
            initiator(Uuid::nil(), Some("Erased")).triggered_by(),
            None,
            "an erased initiator is not named"
        );
        assert_eq!(initiator(Uuid::new_v4(), None).triggered_by(), None);
    }
}
