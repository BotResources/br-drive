use service_engine::error::EngineError;
use service_engine::gate::Reason;
use service_engine::inbound::{Disposition, ReactionError, sqlx_is_terminal};
use service_engine::pipeline::MutationFault;

pub const WORKSPACE_NOT_FOUND: Reason = Reason::new("WORKSPACE_NOT_FOUND");

#[derive(Debug, thiserror::Error)]
pub enum AppFault {
    #[error("refused: {}", .0.code())]
    Refused(Reason),
    #[error("engine: {0}")]
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

#[derive(Debug, thiserror::Error)]
pub enum ReactionFault {
    #[error("terminal: {0}")]
    Terminal(String),
    #[error("store: {0}")]
    Store(String),
}

impl ReactionError for ReactionFault {
    fn disposition(&self) -> Disposition {
        match self {
            Self::Terminal(_) => Disposition::Terminal,
            Self::Store(_) => Disposition::Retry,
        }
    }
}

impl From<EngineError> for ReactionFault {
    fn from(error: EngineError) -> Self {
        match &error {
            EngineError::Db(db) if sqlx_is_terminal(db) => Self::Terminal(error.to_string()),
            _ => Self::Store(error.to_string()),
        }
    }
}
