#![forbid(unsafe_code)]

mod configuration;
mod engine;
mod entry;
mod error;
mod message;
mod persistence;
mod state;
mod types;

#[cfg(test)]
mod formal_model;
#[cfg(test)]
mod simulation;

pub use configuration::{
    ConfigurationChange, EffectiveConfiguration, JointConfiguration, StableConfiguration,
};
pub use engine::{Effect, Input, RaftCore};
pub use entry::{EntryKind, LogEntry};
pub use error::{RaftError, Result};
pub use message::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    PreVoteRequest, PreVoteResponse, RaftMessage, RequestVoteRequest, RequestVoteResponse,
    TimeoutNow,
};
pub use persistence::{PersistenceBatch, PersistenceFailure, PersistenceOp};
pub use state::{
    ElectionRound, HardState, LeadershipTransfer, PreVoteDecision, PreVoteRound, Progress,
    RaftLimits, RecoveredState, ReplicationMode, Role, SnapshotMetadata, TimingConfig,
    VolatileState, VoteDecision,
};
pub use types::{LogIndex, NodeId, PersistenceToken, ProposalId, SnapshotId, Term};
