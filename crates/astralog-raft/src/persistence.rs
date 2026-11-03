use crate::{HardState, LogEntry, LogIndex, PersistenceToken, SnapshotMetadata};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersistenceOp {
    UpdateHardState(HardState),
    TruncateSuffix { from: LogIndex },
    Append(Vec<LogEntry>),
    AdvanceCommit { to: LogIndex },
    InstallSnapshot(SnapshotMetadata),
    DiscardPrefix { through: LogIndex },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistenceBatch {
    pub token: PersistenceToken,
    pub operations: Vec<PersistenceOp>,
}

impl PersistenceBatch {
    pub fn new(token: PersistenceToken, operations: Vec<PersistenceOp>) -> Self {
        Self { token, operations }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistenceFailure {
    pub message: String,
}

impl PersistenceFailure {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}
