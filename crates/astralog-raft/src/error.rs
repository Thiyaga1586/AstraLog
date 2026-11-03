use core::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftError {
    ZeroNodeId,
    TermOverflow,
    LogIndexOverflow,
    EmptyVoterSet,
    DuplicateMember(NodeIdValue),
    MemberInBothRoles(NodeIdValue),
    InvalidJointConfiguration,
    InvalidHardState(&'static str),
    InvalidSnapshot(&'static str),
    InvalidRecoveredLog(&'static str),
    InvalidTiming(&'static str),
    InvalidElectionTimeoutSample,
    InvalidElectionStart,
    InvalidLeaderTransition,
    AppendEntriesEntriesUnavailable,
    AppendEntriesCommitAdvancementUnavailable,
    InvalidAppendEntries(&'static str),
    CommittedLogConflict,
    InvalidConfigurationChange(&'static str),
    InvalidSnapshotState(&'static str),
    InvalidLeadershipTransfer(&'static str),
    PersistenceTokenOverflow,
    PersistenceAlreadyInFlight,
    PersistenceQueueFull,
    UnexpectedPersistenceToken,
    InvalidAppliedIndex,
    CoreStopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeIdValue(pub u64);

impl fmt::Display for RaftError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroNodeId => f.write_str("Raft node id zero is reserved"),
            Self::TermOverflow => f.write_str("Raft term overflow"),
            Self::LogIndexOverflow => f.write_str("Raft log index overflow"),
            Self::EmptyVoterSet => f.write_str("Raft voter set must not be empty"),
            Self::DuplicateMember(id) => write!(f, "duplicate Raft member {}", id.0),
            Self::MemberInBothRoles(id) => {
                write!(f, "Raft member {} cannot be voter and learner", id.0)
            }
            Self::InvalidJointConfiguration => f.write_str("invalid Raft joint configuration"),
            Self::InvalidHardState(message) => write!(f, "invalid Raft hard state: {message}"),
            Self::InvalidSnapshot(message) => write!(f, "invalid Raft snapshot: {message}"),
            Self::InvalidRecoveredLog(message) => {
                write!(f, "invalid recovered Raft log: {message}")
            }
            Self::InvalidTiming(message) => write!(f, "invalid Raft timing: {message}"),
            Self::InvalidElectionTimeoutSample => {
                f.write_str("election timeout sample is outside configured range")
            }
            Self::InvalidElectionStart => {
                f.write_str("real election may start only from a qualified pre-vote quorum")
            }
            Self::InvalidLeaderTransition => f.write_str(
                "leader transition requires a qualified election quorum in current term",
            ),
            Self::AppendEntriesEntriesUnavailable => f.write_str(
                "non-empty AppendEntries is unavailable before the log-mutation checkpoint",
            ),
            Self::AppendEntriesCommitAdvancementUnavailable => f.write_str(
                "AppendEntries commit advancement is unavailable before the commit checkpoint",
            ),
            Self::InvalidAppendEntries(message) => {
                write!(f, "invalid AppendEntries request: {message}")
            }
            Self::CommittedLogConflict => {
                f.write_str("AppendEntries conflicts with committed Raft history")
            }
            Self::InvalidConfigurationChange(message) => {
                write!(f, "invalid Raft configuration change: {message}")
            }
            Self::InvalidSnapshotState(message) => {
                write!(f, "invalid Raft snapshot state: {message}")
            }
            Self::InvalidLeadershipTransfer(message) => {
                write!(f, "invalid Raft leadership transfer: {message}")
            }
            Self::PersistenceTokenOverflow => f.write_str("Raft persistence token overflow"),
            Self::PersistenceAlreadyInFlight => {
                f.write_str("Raft persistence barrier already in flight")
            }
            Self::PersistenceQueueFull => f.write_str("Raft pending persistence queue is full"),
            Self::UnexpectedPersistenceToken => {
                f.write_str("unexpected Raft persistence completion token")
            }
            Self::InvalidAppliedIndex => f.write_str("invalid applied-through index"),
            Self::CoreStopped => f.write_str("Raft core is stopped"),
        }
    }
}

impl std::error::Error for RaftError {}

pub type Result<T> = std::result::Result<T, RaftError>;
