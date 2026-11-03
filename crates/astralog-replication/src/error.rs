use std::{fmt, io};

#[derive(Debug)]
pub enum ReplicationError {
    Io(io::Error),
    Storage(astralog_storage::StorageError),
    Raft(astralog_raft::RaftError),
    Invalid(&'static str),
    InvalidOwned(String),
    Busy(&'static str),
    LimitExceeded {
        what: &'static str,
        actual: usize,
        max: usize,
    },
    ChecksumMismatch {
        context: &'static str,
        expected: u32,
        actual: u32,
    },
}

impl fmt::Display for ReplicationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "replication I/O error: {error}"),
            Self::Storage(error) => write!(f, "replication storage error: {error}"),
            Self::Raft(error) => write!(f, "replication Raft error: {error}"),
            Self::Invalid(message) => write!(f, "invalid replication data: {message}"),
            Self::InvalidOwned(message) => write!(f, "invalid replication data: {message}"),
            Self::Busy(message) => write!(f, "replication resource busy: {message}"),
            Self::LimitExceeded { what, actual, max } => {
                write!(f, "replication {what} exceeds limit: {actual} > {max}")
            }
            Self::ChecksumMismatch {
                context,
                expected,
                actual,
            } => write!(
                f,
                "{context} CRC32C mismatch: expected {expected:#010x}, got {actual:#010x}"
            ),
        }
    }
}

impl std::error::Error for ReplicationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Storage(error) => Some(error),
            Self::Raft(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for ReplicationError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<astralog_storage::StorageError> for ReplicationError {
    fn from(value: astralog_storage::StorageError) -> Self {
        Self::Storage(value)
    }
}

impl From<astralog_raft::RaftError> for ReplicationError {
    fn from(value: astralog_raft::RaftError) -> Self {
        Self::Raft(value)
    }
}

pub type Result<T> = std::result::Result<T, ReplicationError>;
