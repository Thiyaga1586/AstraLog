use core::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControllerError {
    Invalid(&'static str),
    InvalidOwned(String),
    LimitExceeded {
        what: &'static str,
        actual: usize,
        max: usize,
    },
    Busy(&'static str),
    NotLeader {
        leader: Option<u64>,
    },
    Io(String),
    Raft(String),
    ChecksumMismatch {
        context: &'static str,
        expected: u32,
        actual: u32,
    },
}

impl fmt::Display for ControllerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => f.write_str(message),
            Self::InvalidOwned(message) => f.write_str(message),
            Self::LimitExceeded { what, actual, max } => {
                write!(f, "{what} exceeds limit: actual={actual}, max={max}")
            }
            Self::Busy(message) => write!(f, "controller busy: {message}"),
            Self::NotLeader { leader } => match leader {
                Some(leader) => write!(f, "not controller leader; current leader={leader}"),
                None => f.write_str("not controller leader; current leader unknown"),
            },
            Self::Io(message) => write!(f, "I/O error: {message}"),
            Self::Raft(message) => write!(f, "Raft error: {message}"),
            Self::ChecksumMismatch {
                context,
                expected,
                actual,
            } => write!(
                f,
                "{context} checksum mismatch: expected={expected:#010x}, actual={actual:#010x}"
            ),
        }
    }
}

impl std::error::Error for ControllerError {}

impl From<std::io::Error> for ControllerError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value.to_string())
    }
}

impl From<astralog_raft::RaftError> for ControllerError {
    fn from(value: astralog_raft::RaftError) -> Self {
        Self::Raft(value.to_string())
    }
}

pub type Result<T> = core::result::Result<T, ControllerError>;
