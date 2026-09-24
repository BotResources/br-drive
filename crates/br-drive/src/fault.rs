use service_engine::error::EngineError;
use service_engine::gate::Reason;
use service_engine::inbound::{Disposition, ReactionError, sqlx_is_terminal};
use service_engine::pipeline::MutationFault;

pub mod codes {
    use service_engine::gate::Reason;

    pub const DRIVE_NOT_FOUND: Reason = Reason::new("DRIVE_NOT_FOUND");
    pub const FILE_NOT_FOUND: Reason = Reason::new("FILE_NOT_FOUND");
    pub const FOLDER_NOT_FOUND: Reason = Reason::new("FOLDER_NOT_FOUND");
    pub const FILE_PROTECTED: Reason = Reason::new("FILE_PROTECTED");
    pub const FILE_NOT_PENDING: Reason = Reason::new("FILE_NOT_PENDING");
    pub const FILE_NOT_READY: Reason = Reason::new("FILE_NOT_READY");
    pub const FILE_TOO_LARGE: Reason = Reason::new("FILE_TOO_LARGE");
    pub const UPLOAD_NOT_LANDED: Reason = Reason::new("UPLOAD_NOT_LANDED");
    pub const INVALID_SHA256: Reason = Reason::new("INVALID_SHA256");
    pub const INVALID_MEDIA_TYPE: Reason = Reason::new("INVALID_MEDIA_TYPE");
    pub const INVALID_PATH: Reason = Reason::new("INVALID_PATH");
    pub const INVALID_NAME: Reason = Reason::new("INVALID_NAME");
    pub const INVALID_TITLE: Reason = Reason::new("INVALID_TITLE");
    pub const NAME_TAKEN: Reason = Reason::new("NAME_TAKEN");
    pub const FOLDER_INTO_ITSELF: Reason = Reason::new("FOLDER_INTO_ITSELF");
    pub const NOTHING_TO_CHANGE: Reason = Reason::new("NOTHING_TO_CHANGE");
    pub const RUNNER_SCOPE_REQUIRED: Reason = Reason::new("RUNNER_SCOPE_REQUIRED");
    pub const JOB_NOT_ACTIVE: Reason = Reason::new("JOB_NOT_ACTIVE");
    pub const SOURCE_NOT_AVAILABLE: Reason = Reason::new("SOURCE_NOT_AVAILABLE");
    pub const INVALID_IMAGE_NAME: Reason = Reason::new("INVALID_IMAGE_NAME");
    pub const INVALID_PAGE: Reason = Reason::new("INVALID_PAGE");
    pub const INVALID_PAGE_ORIGIN: Reason = Reason::new("INVALID_PAGE_ORIGIN");
    pub const PAGE_NOT_FOUND: Reason = Reason::new("PAGE_NOT_FOUND");
    pub const INDEXER_FIELDS_TOGETHER: Reason = Reason::new("INDEXER_FIELDS_TOGETHER");
    pub const INVALID_INDEXER_VALUE: Reason = Reason::new("INVALID_INDEXER_VALUE");
    pub const BATCH_TOO_LARGE: Reason = Reason::new("BATCH_TOO_LARGE");
    pub const IMAGE_UPLOAD_PENDING: Reason = Reason::new("IMAGE_UPLOAD_PENDING");
    pub const KEY_REUSED: Reason = Reason::new("KEY_REUSED");
    pub const RULESET_NOT_FOUND: Reason = Reason::new("RULESET_NOT_FOUND");
    pub const RULESET_NAME_TAKEN: Reason = Reason::new("RULESET_NAME_TAKEN");
    pub const INVALID_RULESET: Reason = Reason::new("INVALID_RULESET");
    pub const DEFAULT_ALREADY_SET: Reason = Reason::new("DEFAULT_ALREADY_SET");
    pub const RULESET_MISMATCH: Reason = Reason::new("RULESET_MISMATCH");
    pub const NO_RULESET_MATCHES: Reason = Reason::new("NO_RULESET_MATCHES");
    pub const RUNNER_TYPE_UNAVAILABLE: Reason = Reason::new("RUNNER_TYPE_UNAVAILABLE");
    pub const FILE_PROCESSING: Reason = Reason::new("FILE_PROCESSING");
    pub const CATALOGUE_NOT_WATCHED: Reason = Reason::new("CATALOGUE_NOT_WATCHED");
    pub const LABEL_NOT_FOUND: Reason = Reason::new("LABEL_NOT_FOUND");
    pub const LABEL_NAME_TAKEN: Reason = Reason::new("LABEL_NAME_TAKEN");
    pub const INVALID_LABEL: Reason = Reason::new("INVALID_LABEL");
}

#[derive(Debug, thiserror::Error)]
pub enum DriveFault {
    #[error("refused: {}", .0.code())]
    Refused(Reason),
    #[error("engine: {0}")]
    Engine(EngineError),
}

impl MutationFault for DriveFault {
    fn reason(&self) -> Option<Reason> {
        match self {
            Self::Refused(reason) => Some(*reason),
            Self::Engine(_) => None,
        }
    }
}

impl DriveFault {
    pub fn into_graphql(self) -> async_graphql::Error {
        match self {
            Self::Refused(reason) => {
                service_engine::coded_error(reason.code(), "the drive refused the request")
            }
            Self::Engine(error) => async_graphql::Error::new(error.to_string()),
        }
    }
}

impl From<Reason> for DriveFault {
    fn from(reason: Reason) -> Self {
        Self::Refused(reason)
    }
}

impl From<EngineError> for DriveFault {
    fn from(error: EngineError) -> Self {
        match error {
            EngineError::KeyReused { .. } => Self::Refused(codes::KEY_REUSED),
            EngineError::BlobOverPolicy { .. } => Self::Refused(codes::FILE_TOO_LARGE),
            EngineError::PolicyRefused { code } => match Reason::parse(code) {
                Ok(reason) => Self::Refused(reason),
                Err(_) => Self::Engine(EngineError::PolicyRefused { code }),
            },
            other => Self::Engine(other),
        }
    }
}

impl From<sqlx::Error> for DriveFault {
    fn from(error: sqlx::Error) -> Self {
        Self::Engine(EngineError::from(error))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DriveReactionFault {
    #[error("terminal: {0}")]
    Terminal(String),
    #[error("store: {0}")]
    Store(String),
}

impl ReactionError for DriveReactionFault {
    fn disposition(&self) -> Disposition {
        match self {
            Self::Terminal(_) => Disposition::Terminal,
            Self::Store(_) => Disposition::Retry,
        }
    }
}

impl From<EngineError> for DriveReactionFault {
    fn from(error: EngineError) -> Self {
        match &error {
            EngineError::Db(db) if sqlx_is_terminal(db) => Self::Terminal(error.to_string()),
            _ => Self::Store(error.to_string()),
        }
    }
}

impl From<DriveFault> for DriveReactionFault {
    fn from(fault: DriveFault) -> Self {
        match fault {
            DriveFault::Refused(reason) => Self::Terminal(reason.code().to_string()),
            DriveFault::Engine(error) => Self::from(error),
        }
    }
}
