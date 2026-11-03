use std::{fmt, io};

pub type Result<T> = std::result::Result<T, StorageError>;

#[derive(Debug)]
pub enum StorageError {
    Io(io::Error),
    InvalidFormat(&'static str),
    InvalidFormatOwned(String),
    UnsupportedVersion(u16),
    ChecksumMismatch {
        context: &'static str,
        expected: u32,
        actual: u32,
    },
    LimitExceeded {
        what: &'static str,
        actual: usize,
        max: usize,
    },
    ArithmeticOverflow(&'static str),
    Corruption {
        reason: String,
    },
    CommittedCorruption {
        raft_index: u64,
        reason: String,
    },
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "storage I/O error: {err}"),
            Self::InvalidFormat(message) => write!(f, "invalid storage format: {message}"),
            Self::InvalidFormatOwned(message) => write!(f, "invalid storage format: {message}"),
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported storage format version {version}")
            }
            Self::ChecksumMismatch {
                context,
                expected,
                actual,
            } => write!(
                f,
                "{context} CRC32C mismatch: expected {expected:#010x}, got {actual:#010x}",
            ),
            Self::LimitExceeded { what, actual, max } => {
                write!(f, "{what} exceeds limit: {actual} > {max}")
            }
            Self::ArithmeticOverflow(what) => {
                write!(f, "integer overflow while computing {what}")
            }
            Self::Corruption { reason } => write!(f, "storage corruption: {reason}"),
            Self::CommittedCorruption { raft_index, reason } => {
                write!(
                    f,
                    "committed corruption at Raft index {raft_index}: {reason}",
                )
            }
        }
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for StorageError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}
