#[cfg(feature = "drive")]
pub mod drive;
pub mod error;
pub mod facts;
pub mod principal;

pub use error::{AppFault, ReactionFault};
pub use facts::{HostSettings, OwnedWorkspaces, PrincipalFacts};
pub use principal::AppPrincipal;

pub const OWNERSHIP_DEP: u8 = 0;
