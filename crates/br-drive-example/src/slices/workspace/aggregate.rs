use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use service_engine::Cohort;
use service_engine::gate::{Gate, Reason};
use service_engine::name::NounName;
use service_engine::visibility::{Cohorts, Visibility};
use service_engine::wire::Noun;
use uuid::Uuid;

use crate::kernel::AppPrincipal;

pub const NOT_THE_OWNER: Reason = Reason::new("NOT_THE_WORKSPACE_OWNER");

pub const OWNER_DIM: &str = "workspace_owner";

pub struct Workspace;

impl Noun for Workspace {
    type Key = Uuid;
    const NAME: NounName = NounName::from_static("workspace");
}

/// A workspace is the object its drive hangs off (same id): it refreshes when
/// the drive's files change.
#[cfg(feature = "drive")]
impl br_drive::DriveOwnerNoun for Workspace {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum WorkspaceCause {
    Created,
    Deleted,
    Transferred { to: Uuid },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceRow {
    pub id: Uuid,
    pub owner_id: Uuid,
    pub name: String,
    pub created_at: DateTime<Utc>,
    /// Read-side only, never written: the files of the workspace's drive, and
    /// how many are READY — refreshed live because the kernel names this noun
    /// as `DriveHost::DriveOwner`.
    #[serde(default)]
    pub file_count: i64,
    #[serde(default)]
    pub ready_file_count: i64,
}

service_engine::gated! {
    WorkspaceRow, AppPrincipal;
    "delete" => fn delete_gate(this, principal) {
        if principal.user() == this.owner_id {
            Gate::allowed()
        } else {
            Gate::blocked(NOT_THE_OWNER)
        }
    }
    "transfer" => fn transfer_gate(this, principal) {
        if principal.user() == this.owner_id {
            Gate::allowed()
        } else {
            Gate::blocked(NOT_THE_OWNER)
        }
    }
}

impl WorkspaceRow {
    pub fn transfer(&mut self, principal: &AppPrincipal, to: Uuid) -> Result<WorkspaceCause, Reason> {
        self.transfer_gate(principal).require()?;
        self.owner_id = to;
        Ok(WorkspaceCause::Transferred { to })
    }
}

impl Visibility for Workspace {
    type Row = WorkspaceRow;
    type Principal = AppPrincipal;

    fn cohorts(row: &WorkspaceRow) -> Cohorts {
        vec![Cohort::uuid(OWNER_DIM, row.owner_id)]
    }

    fn memberships(principal: &AppPrincipal) -> Cohorts {
        vec![Cohort::uuid(OWNER_DIM, principal.user())]
    }
}
