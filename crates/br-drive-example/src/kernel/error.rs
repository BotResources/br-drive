use service_engine::error::EngineError;
use service_engine::gate::Reason;
use service_engine::inbound::{Disposition, ReactionError, sqlx_is_terminal};
use service_engine::pipeline::MutationFault;

pub const WORKSPACE_NOT_FOUND: Reason = Reason::new("WORKSPACE_NOT_FOUND");

#[derive(Debug, thiserror::Error)]
pub enum AppFault {
    #[error("refused: {}", .0.code())]
    Refused(Reason),
    #[error("the store or engine failed")]
    Engine(#[from] EngineError),
}

impl MutationFault for AppFault {
    fn reason(&self) -> Option<Reason> {
        match self {
            Self::Refused(reason) => Some(*reason),
            Self::Engine(_) => None,
        }
    }
}

impl From<Reason> for AppFault {
    fn from(reason: Reason) -> Self {
        Self::Refused(reason)
    }
}

#[cfg(feature = "drive")]
impl From<br_drive::DriveFault> for AppFault {
    fn from(fault: br_drive::DriveFault) -> Self {
        match fault {
            br_drive::DriveFault::Refused(reason) => Self::Refused(reason),
            br_drive::DriveFault::Engine(error) => Self::Engine(error),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReactionFault {
    #[error("terminal: {0}")]
    Terminal(String),
    #[error("the store failed")]
    Store(#[from] EngineError),
}

impl ReactionError for ReactionFault {
    fn disposition(&self) -> Disposition {
        match self {
            Self::Terminal(_) => Disposition::Terminal,
            Self::Store(EngineError::Db(db)) if sqlx_is_terminal(db) => Disposition::Terminal,
            Self::Store(_) => Disposition::Retry,
        }
    }
}

#[cfg(feature = "drive")]
impl From<br_drive::DriveFault> for ReactionFault {
    fn from(fault: br_drive::DriveFault) -> Self {
        match fault {
            br_drive::DriveFault::Refused(reason) => Self::Terminal(reason.code().to_string()),
            br_drive::DriveFault::Engine(error) => Self::from(error),
        }
    }
}
