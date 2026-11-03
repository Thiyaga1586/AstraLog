use crate::{LogEntry, LogIndex, NodeId, SnapshotMetadata, Term};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreVoteRequest {
    pub prospective_term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreVoteResponse {
    pub prospective_term: Term,
    pub voter_id: NodeId,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVoteRequest {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVoteResponse {
    pub term: Term,
    pub voter_id: NodeId,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesRequest {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry>,
    pub leader_commit: LogIndex,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub follower_id: NodeId,
    pub success: bool,
    pub match_index: LogIndex,
    pub conflict_term: Option<Term>,
    pub conflict_index: LogIndex,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallSnapshotRequest {
    pub term: Term,
    pub leader_id: NodeId,
    pub snapshot: SnapshotMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallSnapshotResponse {
    pub term: Term,
    pub follower_id: NodeId,
    pub success: bool,
    pub match_index: LogIndex,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeoutNow {
    pub term: Term,
    pub leader_id: NodeId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftMessage {
    PreVoteRequest(PreVoteRequest),
    PreVoteResponse(PreVoteResponse),
    RequestVoteRequest(RequestVoteRequest),
    RequestVoteResponse(RequestVoteResponse),
    AppendEntriesRequest(AppendEntriesRequest),
    AppendEntriesResponse(AppendEntriesResponse),
    InstallSnapshotRequest(InstallSnapshotRequest),
    InstallSnapshotResponse(InstallSnapshotResponse),
    TimeoutNow(TimeoutNow),
}
