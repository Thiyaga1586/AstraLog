use std::{fmt, io};

use astralog_controller::ControllerError;
use astralog_protocol::ProtocolError;
use astralog_raft::RaftError;
use astralog_replication::ReplicationError;
use astralog_storage::StorageError;

#[derive(Debug)]
pub enum BrokerError {
    Io(io::Error),
    Protocol(ProtocolError),
    Raft(RaftError),
    Replication(ReplicationError),
    Storage(StorageError),
    Controller(ControllerError),
    Catalog(&'static str),
    CatalogOwned(String),
    InvalidConfig(&'static str),
    Entropy(getrandom::Error),
    TaskJoin(String),
    Metadata(&'static str),
}

impl fmt::Display for BrokerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "I/O error: {err}"),
            Self::Protocol(err) => write!(f, "protocol error: {err}"),
            Self::Raft(err) => write!(f, "Raft error: {err}"),
            Self::Replication(err) => write!(f, "replication error: {err}"),
            Self::Storage(err) => write!(f, "storage error: {err}"),
            Self::Controller(err) => write!(f, "controller error: {err}"),
            Self::Catalog(message) => write!(f, "catalog error: {message}"),
            Self::CatalogOwned(message) => write!(f, "catalog error: {message}"),
            Self::InvalidConfig(message) => write!(f, "invalid broker config: {message}"),
            Self::Entropy(err) => write!(f, "OS entropy error: {err}"),
            Self::TaskJoin(message) => write!(f, "blocking task failed: {message}"),
            Self::Metadata(message) => write!(f, "metadata cache error: {message}"),
        }
    }
}

impl std::error::Error for BrokerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            Self::Protocol(err) => Some(err),
            Self::Raft(err) => Some(err),
            Self::Replication(err) => Some(err),
            Self::Storage(err) => Some(err),
            Self::Controller(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for BrokerError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<ProtocolError> for BrokerError {
    fn from(value: ProtocolError) -> Self {
        Self::Protocol(value)
    }
}

impl From<RaftError> for BrokerError {
    fn from(value: RaftError) -> Self {
        Self::Raft(value)
    }
}

impl From<StorageError> for BrokerError {
    fn from(value: StorageError) -> Self {
        Self::Storage(value)
    }
}

impl From<ReplicationError> for BrokerError {
    fn from(value: ReplicationError) -> Self {
        Self::Replication(value)
    }
}

impl From<ControllerError> for BrokerError {
    fn from(value: ControllerError) -> Self {
        Self::Controller(value)
    }
}

impl From<getrandom::Error> for BrokerError {
    fn from(value: getrandom::Error) -> Self {
        Self::Entropy(value)
    }
}

pub type Result<T> = std::result::Result<T, BrokerError>;
