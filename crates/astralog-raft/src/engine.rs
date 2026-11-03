use std::collections::VecDeque;

use crate::{
    AppendEntriesRequest, AppendEntriesResponse, ConfigurationChange, EffectiveConfiguration,
    ElectionRound, EntryKind, HardState, InstallSnapshotRequest, InstallSnapshotResponse, LogEntry,
    LogIndex, NodeId, PersistenceBatch, PersistenceFailure, PersistenceOp, PersistenceToken,
    PreVoteDecision, PreVoteRequest, PreVoteResponse, PreVoteRound, Progress, ProposalId,
    RaftError, RaftLimits, RaftMessage, RecoveredState, ReplicationMode, RequestVoteRequest,
    RequestVoteResponse, Result, Role, SnapshotMetadata, Term, TimeoutNow, TimingConfig,
    VolatileState, VoteDecision,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PrefixCheck {
    Match,
    Missing {
        conflict_index: LogIndex,
    },
    TermMismatch {
        conflict_term: Term,
        conflict_index: LogIndex,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Input {
    Tick {
        reset_election_timeout: u64,
    },
    Step(RaftMessage),
    BeginElection {
        prospective_term: Term,
    },
    WinElection {
        term: Term,
    },
    Propose {
        id: ProposalId,
        payload: Vec<u8>,
    },
    ProposeConfiguration {
        id: ProposalId,
        change: ConfigurationChange,
    },
    TransferLeadership {
        target: NodeId,
    },
    PersistenceCompleted(PersistenceToken),
    PersistenceFailed {
        token: PersistenceToken,
        error: PersistenceFailure,
    },
    AppliedThrough(LogIndex),
    SnapshotBuilt(SnapshotMetadata),
    RequestSnapshot(LogIndex),
    SnapshotPersisted(PersistenceToken),
    Shutdown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    Persist(PersistenceBatch),
    Send {
        target: NodeId,
        message: RaftMessage,
    },
    Apply(Vec<LogEntry>),
    BuildSnapshot(LogIndex),
    InstallSnapshotToStateMachine(SnapshotMetadata),
    RoleChanged {
        role: Role,
        term: crate::Term,
        leader: Option<NodeId>,
    },
    ProposalResult {
        id: ProposalId,
        accepted: bool,
        leader: Option<NodeId>,
    },
    PreVoteQuorumReached {
        prospective_term: Term,
    },
    ElectionQuorumReached {
        term: Term,
    },
    Fatal(String),
}

#[derive(Debug)]
enum PendingDurableAction {
    StartCampaign {
        hard_state: HardState,
    },
    RespondToVote {
        hard_state: HardState,
        target: NodeId,
        response: RequestVoteResponse,
        reset_election_timer: bool,
    },
    FenceHigherTerm {
        hard_state: HardState,
    },
    PersistLeaderBarrier {
        entry: LogEntry,
    },
    ProcessAppendEntriesAfterTerm {
        hard_state: HardState,
        request: AppendEntriesRequest,
    },
    PersistFollowerAppend {
        leader_id: NodeId,
        resulting_log: Vec<LogEntry>,
        match_index: LogIndex,
        commit_to: LogIndex,
    },
    PersistLeaderEntry {
        entry: LogEntry,
        proposal_id: Option<ProposalId>,
    },
    PersistCommit {
        to: LogIndex,
    },
    PersistLocalSnapshot {
        metadata: SnapshotMetadata,
        retained_log: Vec<LogEntry>,
    },
    ProcessInstallSnapshotAfterTerm {
        hard_state: HardState,
        request: InstallSnapshotRequest,
    },
    PersistInstalledSnapshot {
        leader_id: NodeId,
        metadata: SnapshotMetadata,
        retained_log: Vec<LogEntry>,
        commit_to: LogIndex,
    },
    ProcessTimeoutNowAfterTerm {
        hard_state: HardState,
        message: TimeoutNow,
    },
}

#[derive(Debug)]
struct PendingAppendPersistence {
    token: PersistenceToken,
    entry: LogEntry,
    proposal_id: Option<ProposalId>,
    logical_bytes: usize,
}

#[derive(Debug)]
pub struct RaftCore {
    local_node: NodeId,
    hard_state: HardState,
    volatile: VolatileState,
    stable_log: Vec<LogEntry>,
    snapshot: SnapshotMetadata,
    timing: TimingConfig,
    limits: RaftLimits,
    in_flight_persistence: Option<PersistenceToken>,
    pending_durable_action: Option<PendingDurableAction>,
    pending_append_persistence: VecDeque<PendingAppendPersistence>,
    pending_append_bytes: usize,
    next_persistence_token: u64,
    effects: VecDeque<Effect>,
    pending_snapshot_build: Option<LogIndex>,
    stopped: bool,
}

impl RaftCore {
    pub fn from_recovered(
        local_node: NodeId,
        recovered: RecoveredState,
        timing: TimingConfig,
        randomized_election_timeout: u64,
    ) -> Result<Self> {
        Self::from_recovered_with_limits(
            local_node,
            recovered,
            timing,
            randomized_election_timeout,
            RaftLimits::default(),
        )
    }

    pub fn from_recovered_with_limits(
        local_node: NodeId,
        recovered: RecoveredState,
        timing: TimingConfig,
        randomized_election_timeout: u64,
        limits: RaftLimits,
    ) -> Result<Self> {
        recovered.validate()?;
        if !timing.contains_election_timeout(randomized_election_timeout) {
            return Err(RaftError::InvalidElectionTimeoutSample);
        }
        let mut volatile = VolatileState::recovered(
            local_node,
            &recovered.hard_state,
            &recovered.snapshot,
            recovered.effective_configuration.clone(),
            randomized_election_timeout,
        );
        volatile.pending_configuration_index = recovered.pending_configuration_index;
        let recovery_replay = recovered
            .log
            .iter()
            .filter(|entry| {
                entry.index > recovered.snapshot.last_included_index
                    && entry.index <= recovered.hard_state.durable_commit_index
            })
            .cloned()
            .collect::<Vec<_>>();
        let mut effects = VecDeque::new();
        if !recovery_replay.is_empty() {
            effects.push_back(Effect::Apply(recovery_replay));
        }
        Ok(Self {
            local_node,
            hard_state: recovered.hard_state,
            volatile,
            stable_log: recovered.log,
            snapshot: recovered.snapshot,
            timing,
            limits,
            in_flight_persistence: None,
            pending_durable_action: None,
            pending_append_persistence: VecDeque::new(),
            pending_append_bytes: 0,
            next_persistence_token: 1,
            effects,
            pending_snapshot_build: None,
            stopped: false,
        })
    }

    pub const fn local_node(&self) -> NodeId {
        self.local_node
    }

    pub const fn hard_state(&self) -> &HardState {
        &self.hard_state
    }

    pub const fn volatile_state(&self) -> &VolatileState {
        &self.volatile
    }

    pub const fn snapshot(&self) -> &SnapshotMetadata {
        &self.snapshot
    }

    pub fn stable_log(&self) -> &[LogEntry] {
        &self.stable_log
    }

    pub const fn effective_configuration(&self) -> &EffectiveConfiguration {
        &self.volatile.effective_configuration
    }

    pub fn in_flight_persistence(&self) -> Option<PersistenceToken> {
        self.in_flight_persistence.or_else(|| {
            self.pending_append_persistence
                .front()
                .map(|pending| pending.token)
        })
    }

    pub fn pending_persistence_len(&self) -> usize {
        usize::from(self.in_flight_persistence.is_some()) + self.pending_append_persistence.len()
    }

    pub const fn pending_persistence_bytes(&self) -> usize {
        self.pending_append_bytes
    }

    pub fn next_staged_log_index(&self) -> Result<LogIndex> {
        self.logical_last_log_position().0.checked_next()
    }

    pub const fn is_stopped(&self) -> bool {
        self.stopped
    }

    pub const fn timing(&self) -> TimingConfig {
        self.timing
    }

    pub fn drain_effects(&mut self) -> impl Iterator<Item = Effect> + '_ {
        self.effects.drain(..)
    }

    #[cfg(test)]
    fn stage_persistence(&mut self, batch: PersistenceBatch) -> Result<()> {
        if self.stopped {
            return Err(RaftError::CoreStopped);
        }
        if self.in_flight_persistence.is_some() {
            return Err(RaftError::PersistenceAlreadyInFlight);
        }
        self.in_flight_persistence = Some(batch.token);
        self.effects.push_back(Effect::Persist(batch));
        Ok(())
    }

    fn next_internal_persistence_token(&mut self) -> Result<PersistenceToken> {
        let token = PersistenceToken::new(self.next_persistence_token);
        self.next_persistence_token = self
            .next_persistence_token
            .checked_add(1)
            .ok_or(RaftError::PersistenceTokenOverflow)?;
        Ok(token)
    }

    fn stage_hard_state_action(
        &mut self,
        hard_state: HardState,
        action: PendingDurableAction,
    ) -> Result<()> {
        if self.in_flight_persistence.is_some()
            || self.pending_durable_action.is_some()
            || !self.pending_append_persistence.is_empty()
        {
            return Err(RaftError::PersistenceAlreadyInFlight);
        }
        let token = self.next_internal_persistence_token()?;
        self.in_flight_persistence = Some(token);
        self.pending_durable_action = Some(action);
        self.effects
            .push_back(Effect::Persist(PersistenceBatch::new(
                token,
                vec![PersistenceOp::UpdateHardState(hard_state)],
            )));
        Ok(())
    }

    #[cfg(test)]
    fn observe_current_term_leader_contact(
        &mut self,
        leader_id: NodeId,
        leader_term: Term,
    ) -> Result<()> {
        if self.stopped {
            return Err(RaftError::CoreStopped);
        }
        if self.hard_state.current_term == Term::ZERO || leader_term != self.hard_state.current_term
        {
            return Ok(());
        }
        self.accept_current_term_leader_authority(leader_id);
        Ok(())
    }

    pub fn handle_input(&mut self, input: Input) -> Result<()> {
        if self.stopped && !matches!(input, Input::Shutdown) {
            return Err(RaftError::CoreStopped);
        }
        if self.in_flight_persistence.is_some()
            && matches!(
                input,
                Input::Tick { .. }
                    | Input::Step(_)
                    | Input::BeginElection { .. }
                    | Input::WinElection { .. }
                    | Input::Propose { .. }
                    | Input::ProposeConfiguration { .. }
                    | Input::TransferLeadership { .. }
                    | Input::SnapshotBuilt(_)
                    | Input::RequestSnapshot(_)
            )
        {
            return Err(RaftError::PersistenceAlreadyInFlight);
        }
        if !self.pending_append_persistence.is_empty()
            && matches!(
                input,
                Input::Tick { .. }
                    | Input::Step(_)
                    | Input::BeginElection { .. }
                    | Input::WinElection { .. }
                    | Input::ProposeConfiguration { .. }
                    | Input::TransferLeadership { .. }
                    | Input::SnapshotBuilt(_)
                    | Input::RequestSnapshot(_)
            )
        {
            return Err(RaftError::PersistenceAlreadyInFlight);
        }
        match input {
            Input::PersistenceCompleted(token) | Input::SnapshotPersisted(token) => {
                self.complete_persistence(token)?;
            }
            Input::PersistenceFailed { token, error } => {
                if self.in_flight_persistence == Some(token) {
                    self.in_flight_persistence = None;
                    self.pending_durable_action = None;
                } else if self
                    .pending_append_persistence
                    .front()
                    .is_some_and(|pending| pending.token == token)
                {
                    self.pending_append_persistence.clear();
                    self.pending_append_bytes = 0;
                } else {
                    return Err(RaftError::UnexpectedPersistenceToken);
                }
                self.stopped = true;
                self.effects.push_back(Effect::Fatal(error.message));
            }
            Input::AppliedThrough(index) => {
                if index < self.volatile.last_applied || index > self.volatile.commit_index {
                    return Err(RaftError::InvalidAppliedIndex);
                }
                self.volatile.last_applied = index;
            }
            Input::Shutdown => {
                self.stopped = true;
            }
            Input::Tick {
                reset_election_timeout,
            } => self.handle_tick(reset_election_timeout)?,
            Input::Step(message) => self.handle_message(message)?,
            Input::BeginElection { prospective_term } => {
                self.begin_election(prospective_term)?;
            }
            Input::WinElection { term } => {
                self.win_election(term)?;
            }
            Input::Propose { id, payload } => {
                self.handle_application_proposal(id, payload)?;
            }
            Input::ProposeConfiguration { id, change } => {
                self.handle_configuration_proposal(id, change)?;
            }
            Input::TransferLeadership { target } => {
                self.handle_transfer_leadership(target)?;
            }
            Input::SnapshotBuilt(metadata) => {
                self.handle_snapshot_built(metadata)?;
            }
            Input::RequestSnapshot(index) => {
                self.handle_snapshot_request(index)?;
            }
        }
        Ok(())
    }

    fn complete_persistence(&mut self, token: PersistenceToken) -> Result<()> {
        if self.in_flight_persistence.is_none()
            && self
                .pending_append_persistence
                .front()
                .is_some_and(|pending| pending.token == token)
        {
            let pending = self
                .pending_append_persistence
                .pop_front()
                .ok_or(RaftError::UnexpectedPersistenceToken)?;
            self.pending_append_bytes = self
                .pending_append_bytes
                .checked_sub(pending.logical_bytes)
                .ok_or(RaftError::InvalidHardState(
                    "pending persistence byte accounting underflow",
                ))?;
            self.finish_leader_entry_persistence(pending.entry, pending.proposal_id)?;
            return Ok(());
        }
        if self.in_flight_persistence != Some(token) {
            return Err(RaftError::UnexpectedPersistenceToken);
        }
        self.in_flight_persistence = None;

        let Some(action) = self.pending_durable_action.take() else {
            return Ok(());
        };

        match action {
            PendingDurableAction::StartCampaign { hard_state } => {
                self.finish_start_campaign(hard_state)?;
            }
            PendingDurableAction::RespondToVote {
                hard_state,
                target,
                response,
                reset_election_timer,
            } => {
                let term_changed = hard_state.current_term != self.hard_state.current_term;
                self.hard_state = hard_state;
                if term_changed {
                    self.step_down_for_term_change();
                    self.effects.push_back(Effect::RoleChanged {
                        role: self.volatile.role,
                        term: self.hard_state.current_term,
                        leader: None,
                    });
                } else if response.vote_granted {
                    self.step_down_after_vote_grant();
                }
                if reset_election_timer {
                    self.volatile.election_elapsed = 0;
                }
                self.effects.push_back(Effect::Send {
                    target,
                    message: RaftMessage::RequestVoteResponse(response),
                });
            }
            PendingDurableAction::FenceHigherTerm { hard_state } => {
                self.hard_state = hard_state;
                self.step_down_for_term_change();
                self.effects.push_back(Effect::RoleChanged {
                    role: self.volatile.role,
                    term: self.hard_state.current_term,
                    leader: None,
                });
            }
            PendingDurableAction::PersistLeaderBarrier { entry } => {
                self.finish_leader_barrier_persistence(entry)?;
            }
            PendingDurableAction::ProcessAppendEntriesAfterTerm {
                hard_state,
                request,
            } => {
                let previous_role = self.volatile.role;
                self.hard_state = hard_state;
                self.step_down_for_term_change();
                if previous_role != self.volatile.role {
                    self.effects.push_back(Effect::RoleChanged {
                        role: self.volatile.role,
                        term: self.hard_state.current_term,
                        leader: None,
                    });
                }
                self.process_current_term_append_entries(request)?;
            }
            PendingDurableAction::PersistFollowerAppend {
                leader_id,
                resulting_log,
                match_index,
                commit_to,
            } => {
                self.stable_log = resulting_log;
                self.reconstruct_membership_from_durable_log()?;
                self.finish_commit_persistence(commit_to)?;
                self.effects.push_back(Effect::Send {
                    target: leader_id,
                    message: RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
                        term: self.hard_state.current_term,
                        follower_id: self.local_node,
                        success: true,
                        match_index,
                        conflict_term: None,
                        conflict_index: LogIndex::ZERO,
                    }),
                });
            }
            PendingDurableAction::PersistLeaderEntry { entry, proposal_id } => {
                self.finish_leader_entry_persistence(entry, proposal_id)?;
            }
            PendingDurableAction::PersistCommit { to } => {
                self.finish_commit_persistence(to)?;
                if self.volatile.role == Role::Leader {
                    self.send_replication_to_all()?;
                }
            }
            PendingDurableAction::PersistLocalSnapshot {
                metadata,
                retained_log,
            } => {
                self.snapshot = metadata;
                self.stable_log = retained_log;
            }
            PendingDurableAction::ProcessInstallSnapshotAfterTerm {
                hard_state,
                request,
            } => {
                let previous_role = self.volatile.role;
                self.hard_state = hard_state;
                self.step_down_for_term_change();
                if previous_role != self.volatile.role {
                    self.effects.push_back(Effect::RoleChanged {
                        role: self.volatile.role,
                        term: self.hard_state.current_term,
                        leader: None,
                    });
                }
                self.process_current_term_install_snapshot(request)?;
            }
            PendingDurableAction::PersistInstalledSnapshot {
                leader_id,
                metadata,
                retained_log,
                commit_to,
            } => {
                self.snapshot = metadata.clone();
                self.stable_log = retained_log;
                if commit_to > self.hard_state.durable_commit_index {
                    self.hard_state.durable_commit_index = commit_to;
                }
                self.volatile.commit_index = self.volatile.commit_index.max(commit_to);
                self.volatile.last_applied =
                    self.volatile.last_applied.max(metadata.last_included_index);
                self.reconstruct_membership_from_durable_log()?;
                self.effects
                    .push_back(Effect::InstallSnapshotToStateMachine(metadata.clone()));
                self.effects.push_back(Effect::Send {
                    target: leader_id,
                    message: RaftMessage::InstallSnapshotResponse(InstallSnapshotResponse {
                        term: self.hard_state.current_term,
                        follower_id: self.local_node,
                        success: true,
                        match_index: metadata.last_included_index,
                    }),
                });
                self.emit_apply_if_needed();
            }
            PendingDurableAction::ProcessTimeoutNowAfterTerm {
                hard_state,
                message,
            } => {
                self.hard_state = hard_state;
                self.step_down_for_term_change();
                self.process_current_term_timeout_now(message)?;
            }
        }
        Ok(())
    }

    fn handle_tick(&mut self, reset_election_timeout: u64) -> Result<()> {
        if !self
            .timing
            .contains_election_timeout(reset_election_timeout)
        {
            return Err(RaftError::InvalidElectionTimeoutSample);
        }

        if self.volatile.role == Role::Learner {
            self.volatile.election_elapsed = self.volatile.election_elapsed.saturating_add(1);
            return Ok(());
        }

        if self.volatile.role == Role::Leader {
            return self.handle_leader_tick();
        }

        if !matches!(
            self.volatile.role,
            Role::Follower | Role::PreCandidate | Role::Candidate
        ) {
            return Ok(());
        }

        self.volatile.election_elapsed = self.volatile.election_elapsed.saturating_add(1);
        if self.volatile.election_elapsed < self.volatile.randomized_election_timeout {
            return Ok(());
        }

        self.start_pre_vote(reset_election_timeout)
    }

    fn handle_leader_tick(&mut self) -> Result<()> {
        self.volatile.heartbeat_elapsed = self.volatile.heartbeat_elapsed.saturating_add(1);
        self.volatile.election_elapsed = self.volatile.election_elapsed.saturating_add(1);

        if self
            .volatile
            .leadership_transfer
            .as_ref()
            .is_some_and(|transfer| {
                !self
                    .volatile
                    .effective_configuration
                    .contains_voter(transfer.target)
            })
        {
            self.volatile.leadership_transfer = None;
        }

        if let Some(transfer) = self.volatile.leadership_transfer.as_mut() {
            transfer.elapsed = transfer.elapsed.saturating_add(1);
            if transfer.elapsed >= self.volatile.randomized_election_timeout {
                self.volatile.leadership_transfer = None;
            }
        }

        if self.volatile.election_elapsed >= self.volatile.randomized_election_timeout {
            let mut active = std::collections::BTreeSet::new();
            active.insert(self.local_node);
            for (node, progress) in &self.volatile.progress {
                if *node != self.local_node && progress.recently_active {
                    active.insert(*node);
                }
            }

            if !self
                .volatile
                .effective_configuration
                .has_voter_quorum(&active)
            {
                self.volatile.role = Role::Follower;
                self.volatile.leader_id = None;
                self.volatile.progress.clear();
                self.volatile.leadership_transfer = None;
                self.volatile.election_elapsed = 0;
                self.effects.push_back(Effect::RoleChanged {
                    role: Role::Follower,
                    term: self.hard_state.current_term,
                    leader: None,
                });
                return Ok(());
            }

            self.volatile.election_elapsed = 0;
            for (node, progress) in &mut self.volatile.progress {
                progress.recently_active = *node == self.local_node;
            }
        }

        if self.volatile.heartbeat_elapsed >= self.timing.heartbeat_interval {
            self.volatile.heartbeat_elapsed = 0;
            self.send_replication_to_all()?;
        }
        Ok(())
    }

    fn start_pre_vote(&mut self, reset_election_timeout: u64) -> Result<()> {
        if !self
            .volatile
            .effective_configuration
            .contains_voter(self.local_node)
        {
            return Ok(());
        }

        let prospective_term = self.hard_state.current_term.checked_next()?;
        let was_pre_candidate = self.volatile.role == Role::PreCandidate;
        self.volatile.role = Role::PreCandidate;
        self.volatile.leader_id = None;
        self.volatile.election_elapsed = 0;
        self.volatile.randomized_election_timeout = reset_election_timeout;

        let mut responses = std::collections::BTreeMap::new();
        responses.insert(self.local_node, PreVoteDecision::Granted);
        self.volatile.pre_vote = Some(PreVoteRound {
            prospective_term,
            responses,
            quorum_reached: false,
        });

        if !was_pre_candidate {
            self.effects.push_back(Effect::RoleChanged {
                role: Role::PreCandidate,
                term: self.hard_state.current_term,
                leader: None,
            });
        }

        let (last_log_index, last_log_term) = self.last_log_position();
        for target in self.volatile.effective_configuration.voters() {
            if target == self.local_node {
                continue;
            }
            self.effects.push_back(Effect::Send {
                target,
                message: RaftMessage::PreVoteRequest(PreVoteRequest {
                    prospective_term,
                    candidate_id: self.local_node,
                    last_log_index,
                    last_log_term,
                }),
            });
        }

        self.maybe_emit_pre_vote_quorum();
        Ok(())
    }

    fn handle_message(&mut self, message: RaftMessage) -> Result<()> {
        match message {
            RaftMessage::PreVoteRequest(request) => self.handle_pre_vote_request(request),
            RaftMessage::PreVoteResponse(response) => self.handle_pre_vote_response(response),
            RaftMessage::RequestVoteRequest(request) => self.handle_request_vote_request(request),
            RaftMessage::RequestVoteResponse(response) => {
                self.handle_request_vote_response(response)
            }
            RaftMessage::AppendEntriesRequest(request) => {
                self.handle_append_entries_request(request)
            }
            RaftMessage::AppendEntriesResponse(response) => {
                self.handle_append_entries_response(response)
            }
            RaftMessage::InstallSnapshotRequest(request) => {
                self.handle_install_snapshot_request(request)
            }
            RaftMessage::InstallSnapshotResponse(response) => {
                self.handle_install_snapshot_response(response)
            }
            RaftMessage::TimeoutNow(message) => self.handle_timeout_now(message),
        }
    }

    fn handle_pre_vote_request(&mut self, request: PreVoteRequest) -> Result<()> {
        let eligible_requester = self
            .volatile
            .effective_configuration
            .contains_voter(request.candidate_id);
        let local_is_voter = self
            .volatile
            .effective_configuration
            .contains_voter(self.local_node);
        let prospective_is_newer = request.prospective_term > self.hard_state.current_term;
        let requester_is_up_to_date =
            self.log_is_at_least_as_up_to_date(request.last_log_term, request.last_log_index);
        let recent_leader = self.volatile.leader_id.is_some()
            && self.volatile.election_elapsed < self.volatile.randomized_election_timeout;

        let vote_granted = local_is_voter
            && eligible_requester
            && prospective_is_newer
            && requester_is_up_to_date
            && !recent_leader;

        self.effects.push_back(Effect::Send {
            target: request.candidate_id,
            message: RaftMessage::PreVoteResponse(PreVoteResponse {
                prospective_term: request.prospective_term,
                voter_id: self.local_node,
                vote_granted,
            }),
        });
        Ok(())
    }

    fn handle_pre_vote_response(&mut self, response: PreVoteResponse) -> Result<()> {
        if self.volatile.role != Role::PreCandidate
            || !self
                .volatile
                .effective_configuration
                .contains_voter(response.voter_id)
        {
            return Ok(());
        }

        let Some(round) = self.volatile.pre_vote.as_mut() else {
            return Ok(());
        };
        if response.prospective_term != round.prospective_term || round.quorum_reached {
            return Ok(());
        }
        round
            .responses
            .entry(response.voter_id)
            .or_insert(if response.vote_granted {
                PreVoteDecision::Granted
            } else {
                PreVoteDecision::Rejected
            });
        self.maybe_emit_pre_vote_quorum();
        Ok(())
    }

    fn begin_election(&mut self, prospective_term: Term) -> Result<()> {
        if self.volatile.role != Role::PreCandidate
            || !self
                .volatile
                .effective_configuration
                .contains_voter(self.local_node)
        {
            return Err(RaftError::InvalidElectionStart);
        }

        let Some(round) = self.volatile.pre_vote.as_ref() else {
            return Err(RaftError::InvalidElectionStart);
        };
        if !round.quorum_reached || round.prospective_term != prospective_term {
            return Err(RaftError::InvalidElectionStart);
        }
        if self.hard_state.current_term.checked_next()? != prospective_term {
            return Err(RaftError::InvalidElectionStart);
        }

        let hard_state = HardState {
            current_term: prospective_term,
            voted_for: Some(self.local_node),
            durable_commit_index: self.hard_state.durable_commit_index,
        };
        self.stage_hard_state_action(
            hard_state.clone(),
            PendingDurableAction::StartCampaign { hard_state },
        )
    }

    fn finish_start_campaign(&mut self, hard_state: HardState) -> Result<()> {
        let term = hard_state.current_term;
        self.hard_state = hard_state;
        self.volatile.role = Role::Candidate;
        self.volatile.leader_id = None;
        self.volatile.pre_vote = None;
        self.volatile.election_elapsed = 0;
        self.volatile.heartbeat_elapsed = 0;
        self.volatile.leadership_transfer = None;

        let mut responses = std::collections::BTreeMap::new();
        responses.insert(self.local_node, VoteDecision::Granted);
        self.volatile.election = Some(ElectionRound {
            term,
            responses,
            quorum_reached: false,
        });
        self.effects.push_back(Effect::RoleChanged {
            role: Role::Candidate,
            term,
            leader: None,
        });

        let (last_log_index, last_log_term) = self.last_log_position();
        for target in self.volatile.effective_configuration.voters() {
            if target == self.local_node {
                continue;
            }
            self.effects.push_back(Effect::Send {
                target,
                message: RaftMessage::RequestVoteRequest(RequestVoteRequest {
                    term,
                    candidate_id: self.local_node,
                    last_log_index,
                    last_log_term,
                }),
            });
        }
        self.maybe_emit_election_quorum();
        Ok(())
    }

    fn handle_request_vote_request(&mut self, request: RequestVoteRequest) -> Result<()> {
        if !self
            .volatile
            .effective_configuration
            .contains_voter(request.candidate_id)
            || request.term == Term::ZERO
        {
            self.send_request_vote_response(request.candidate_id, false);
            return Ok(());
        }

        if request.term < self.hard_state.current_term {
            self.send_request_vote_response(request.candidate_id, false);
            return Ok(());
        }

        let local_is_voter = self
            .volatile
            .effective_configuration
            .contains_voter(self.local_node);
        let requester_is_up_to_date =
            self.log_is_at_least_as_up_to_date(request.last_log_term, request.last_log_index);

        if request.term > self.hard_state.current_term {
            let vote_granted = local_is_voter && requester_is_up_to_date;
            let hard_state = HardState {
                current_term: request.term,
                voted_for: vote_granted.then_some(request.candidate_id),
                durable_commit_index: self.hard_state.durable_commit_index,
            };
            let response = RequestVoteResponse {
                term: request.term,
                voter_id: self.local_node,
                vote_granted,
            };
            return self.stage_hard_state_action(
                hard_state.clone(),
                PendingDurableAction::RespondToVote {
                    hard_state,
                    target: request.candidate_id,
                    response,
                    reset_election_timer: vote_granted,
                },
            );
        }

        let vote_granted = local_is_voter
            && requester_is_up_to_date
            && self
                .hard_state
                .voted_for
                .is_none_or(|voted_for| voted_for == request.candidate_id);

        if !vote_granted {
            self.send_request_vote_response(request.candidate_id, false);
            return Ok(());
        }

        if self.hard_state.voted_for == Some(request.candidate_id) {
            self.volatile.election_elapsed = 0;
            self.step_down_after_vote_grant();
            self.send_request_vote_response(request.candidate_id, true);
            return Ok(());
        }

        let hard_state = HardState {
            current_term: request.term,
            voted_for: Some(request.candidate_id),
            durable_commit_index: self.hard_state.durable_commit_index,
        };
        let response = RequestVoteResponse {
            term: request.term,
            voter_id: self.local_node,
            vote_granted: true,
        };
        self.stage_hard_state_action(
            hard_state.clone(),
            PendingDurableAction::RespondToVote {
                hard_state,
                target: request.candidate_id,
                response,
                reset_election_timer: true,
            },
        )
    }

    fn handle_request_vote_response(&mut self, response: RequestVoteResponse) -> Result<()> {
        if !self
            .volatile
            .effective_configuration
            .contains_voter(response.voter_id)
        {
            return Ok(());
        }

        if response.term > self.hard_state.current_term {
            let hard_state = HardState {
                current_term: response.term,
                voted_for: None,
                durable_commit_index: self.hard_state.durable_commit_index,
            };
            return self.stage_hard_state_action(
                hard_state.clone(),
                PendingDurableAction::FenceHigherTerm { hard_state },
            );
        }
        if response.term < self.hard_state.current_term || self.volatile.role != Role::Candidate {
            return Ok(());
        }

        let Some(round) = self.volatile.election.as_mut() else {
            return Ok(());
        };
        if round.term != response.term || round.quorum_reached {
            return Ok(());
        }
        round
            .responses
            .entry(response.voter_id)
            .or_insert(if response.vote_granted {
                VoteDecision::Granted
            } else {
                VoteDecision::Rejected
            });
        self.maybe_emit_election_quorum();
        Ok(())
    }

    fn handle_append_entries_request(&mut self, request: AppendEntriesRequest) -> Result<()> {
        let leader_is_voter = self
            .volatile
            .effective_configuration
            .contains_voter(request.leader_id);

        if request.term == Term::ZERO || !leader_is_voter {
            self.send_append_entries_rejection(
                request.leader_id,
                None,
                self.last_log_position().0.checked_next()?,
            );
            return Ok(());
        }

        if request.term < self.hard_state.current_term {
            self.send_append_entries_rejection(
                request.leader_id,
                None,
                self.last_log_position().0.checked_next()?,
            );
            return Ok(());
        }

        if request.term > self.hard_state.current_term {
            let hard_state = HardState {
                current_term: request.term,
                voted_for: None,
                durable_commit_index: self.hard_state.durable_commit_index,
            };
            return self.stage_hard_state_action(
                hard_state.clone(),
                PendingDurableAction::ProcessAppendEntriesAfterTerm {
                    hard_state,
                    request,
                },
            );
        }

        self.process_current_term_append_entries(request)
    }

    fn handle_append_entries_response(&mut self, response: AppendEntriesResponse) -> Result<()> {
        let responder_is_member = self
            .volatile
            .effective_configuration
            .contains_voter(response.follower_id)
            || self
                .volatile
                .effective_configuration
                .contains_learner(response.follower_id);
        if !responder_is_member {
            return Ok(());
        }

        if response.term > self.hard_state.current_term {
            let hard_state = HardState {
                current_term: response.term,
                voted_for: None,
                durable_commit_index: self.hard_state.durable_commit_index,
            };
            return self.stage_hard_state_action(
                hard_state.clone(),
                PendingDurableAction::FenceHigherTerm { hard_state },
            );
        }
        if response.term < self.hard_state.current_term || self.volatile.role != Role::Leader {
            return Ok(());
        }

        let leader_last = self.last_log_position().0;
        let snapshot_floor = self.snapshot.last_included_index.checked_next()?;
        let Some(progress) = self.volatile.progress.get_mut(&response.follower_id) else {
            return Ok(());
        };
        progress.recently_active = true;

        if response.success {
            if response.match_index > leader_last {
                return Err(RaftError::InvalidAppendEntries(
                    "follower success match index exceeds leader durable log",
                ));
            }
            if response.match_index > progress.match_index {
                progress.match_index = response.match_index;
                progress.next_index = response.match_index.checked_next()?;
            }
            progress.mode = ReplicationMode::Replicate;
            self.maybe_finish_transfer_catch_up(response.follower_id)?;
            self.maybe_advance_leader_commit()?;
            if self.in_flight_persistence.is_none()
                && self
                    .volatile
                    .progress
                    .get(&response.follower_id)
                    .is_some_and(|peer| peer.next_index <= leader_last)
            {
                self.send_replication_to(response.follower_id)?;
            }
            return Ok(());
        }

        let mut next = response.conflict_index;
        if let Some(conflict_term) = response.conflict_term
            && let Some(last_in_term) = self.last_index_in_term(conflict_term)
        {
            next = last_in_term.checked_next()?;
        }
        let needs_snapshot = next <= self.snapshot.last_included_index;
        if next < snapshot_floor {
            next = snapshot_floor;
        }
        let progress = self
            .volatile
            .progress
            .get_mut(&response.follower_id)
            .ok_or(RaftError::InvalidAppendEntries("missing peer progress"))?;
        progress.next_index = next;
        progress.mode = if needs_snapshot {
            ReplicationMode::Snapshot
        } else {
            ReplicationMode::Probe
        };
        self.send_replication_to(response.follower_id)
    }

    fn send_replication_to_all(&mut self) -> Result<()> {
        if self.volatile.role != Role::Leader {
            return Ok(());
        }
        let peers = self
            .volatile
            .progress
            .keys()
            .copied()
            .filter(|node| *node != self.local_node)
            .collect::<Vec<_>>();
        for peer in peers {
            self.send_replication_to(peer)?;
        }
        Ok(())
    }

    fn send_replication_to(&mut self, target: NodeId) -> Result<()> {
        if self.volatile.role != Role::Leader || target == self.local_node {
            return Ok(());
        }
        let Some(progress) = self.volatile.progress.get(&target).cloned() else {
            return Ok(());
        };

        if progress.mode == ReplicationMode::Snapshot
            || progress.next_index <= self.snapshot.last_included_index
        {
            self.effects.push_back(Effect::Send {
                target,
                message: RaftMessage::InstallSnapshotRequest(InstallSnapshotRequest {
                    term: self.hard_state.current_term,
                    leader_id: self.local_node,
                    snapshot: self.snapshot.clone(),
                }),
            });
            return Ok(());
        }

        let leader_last = self.last_log_position().0;
        let next_index = if progress.next_index > leader_last.checked_next()? {
            leader_last.checked_next()?
        } else {
            progress.next_index
        };
        if next_index == LogIndex::ZERO {
            return Err(RaftError::InvalidAppendEntries(
                "leader peer next_index cannot be zero",
            ));
        }
        let prev_log_index = LogIndex::new(next_index.get() - 1);
        let prev_log_term = self
            .term_at(prev_log_index)
            .ok_or(RaftError::InvalidAppendEntries(
                "leader no longer retains peer prefix",
            ))?;
        let entries = bounded_replication_suffix(
            &self.stable_log,
            next_index,
            self.limits.max_replication_send_bytes,
        );

        self.effects.push_back(Effect::Send {
            target,
            message: RaftMessage::AppendEntriesRequest(AppendEntriesRequest {
                term: self.hard_state.current_term,
                leader_id: self.local_node,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.volatile.commit_index,
            }),
        });
        Ok(())
    }

    fn last_index_in_term(&self, term: Term) -> Option<LogIndex> {
        self.stable_log
            .iter()
            .rev()
            .find(|entry| entry.term == term)
            .map(|entry| entry.index)
            .or_else(|| {
                (self.snapshot.last_included_term == term)
                    .then_some(self.snapshot.last_included_index)
            })
    }

    fn maybe_advance_leader_commit(&mut self) -> Result<()> {
        if self.volatile.role != Role::Leader
            || self.in_flight_persistence.is_some()
            || !self.pending_append_persistence.is_empty()
        {
            return Ok(());
        }
        let target = self
            .stable_log
            .iter()
            .rev()
            .find(|entry| {
                entry.index > self.volatile.commit_index
                    && entry.term == self.hard_state.current_term
                    && self
                        .volatile
                        .effective_configuration
                        .replication_quorum_at(entry.index, &self.volatile.progress)
            })
            .map(|entry| entry.index);
        let Some(to) = target else {
            return Ok(());
        };

        let token = self.next_internal_persistence_token()?;
        self.in_flight_persistence = Some(token);
        self.pending_durable_action = Some(PendingDurableAction::PersistCommit { to });
        self.effects
            .push_back(Effect::Persist(PersistenceBatch::new(
                token,
                vec![PersistenceOp::AdvanceCommit { to }],
            )));
        Ok(())
    }

    fn maybe_finish_transfer_catch_up(&mut self, peer: NodeId) -> Result<()> {
        let leader_last = self.last_log_position().0;
        let caught_up = self
            .volatile
            .progress
            .get(&peer)
            .is_some_and(|progress| progress.match_index >= leader_last);
        let Some(transfer) = self.volatile.leadership_transfer.as_mut() else {
            return Ok(());
        };
        if transfer.target != peer || transfer.timeout_now_sent || !caught_up {
            return Ok(());
        }
        transfer.timeout_now_sent = true;
        self.effects.push_back(Effect::Send {
            target: peer,
            message: RaftMessage::TimeoutNow(TimeoutNow {
                term: self.hard_state.current_term,
                leader_id: self.local_node,
            }),
        });
        Ok(())
    }

    fn process_current_term_append_entries(&mut self, request: AppendEntriesRequest) -> Result<()> {
        if request.term != self.hard_state.current_term {
            return Ok(());
        }

        if !self.accept_current_term_leader_authority(request.leader_id) {
            self.send_append_entries_rejection(
                request.leader_id,
                None,
                self.last_log_position().0.checked_next()?,
            );
            return Ok(());
        }

        match self.check_prefix(request.prev_log_index, request.prev_log_term)? {
            PrefixCheck::Missing { conflict_index } => {
                self.send_append_entries_rejection(request.leader_id, None, conflict_index);
                return Ok(());
            }
            PrefixCheck::TermMismatch {
                conflict_term,
                conflict_index,
            } => {
                self.send_append_entries_rejection(
                    request.leader_id,
                    Some(conflict_term),
                    conflict_index,
                );
                return Ok(());
            }
            PrefixCheck::Match => {}
        }

        self.validate_incoming_entries(&request)?;

        let mut resulting_log = self.stable_log.clone();
        let mut operations = Vec::new();
        let mut append_from = request.entries.len();

        for (offset, incoming) in request.entries.iter().enumerate() {
            if let Some(local) = self.log_entry(incoming.index) {
                if local.term == incoming.term {
                    if local.kind != incoming.kind {
                        self.fatal_safety("same Raft term/index has different entry payload");
                        return Ok(());
                    }
                    continue;
                }

                if incoming.index <= self.volatile.commit_index {
                    self.fatal_safety("AppendEntries attempted to truncate committed history");
                    return Ok(());
                }

                let truncate_position = resulting_log
                    .iter()
                    .position(|entry| entry.index == incoming.index)
                    .ok_or(RaftError::InvalidAppendEntries(
                        "conflicting retained entry disappeared during analysis",
                    ))?;
                resulting_log.truncate(truncate_position);
                operations.push(PersistenceOp::TruncateSuffix {
                    from: incoming.index,
                });
                append_from = offset;
                break;
            }

            append_from = offset;
            break;
        }

        if append_from < request.entries.len() {
            let append = request.entries[append_from..].to_vec();
            resulting_log.extend(append.iter().cloned());
            operations.push(PersistenceOp::Append(append));
        }

        let match_index = request
            .entries
            .last()
            .map_or(request.prev_log_index, |entry| entry.index);
        // A newly elected leader may not yet know that this follower already
        // durably learned a higher commit index from the previous leader. The
        // follower must never regress its committed prefix merely because the
        // incoming leader_commit is stale. Preserve the local durable commit
        // while still bounding any forward movement by match_index.
        let commit_to = self
            .volatile
            .commit_index
            .max(std::cmp::min(request.leader_commit, match_index));
        let _ = self.derive_membership_for_log(&resulting_log, commit_to)?;
        if commit_to > self.volatile.commit_index {
            operations.push(PersistenceOp::AdvanceCommit { to: commit_to });
        }

        if operations.is_empty() {
            self.effects.push_back(Effect::Send {
                target: request.leader_id,
                message: RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
                    term: self.hard_state.current_term,
                    follower_id: self.local_node,
                    success: true,
                    match_index,
                    conflict_term: None,
                    conflict_index: LogIndex::ZERO,
                }),
            });
            return Ok(());
        }

        let token = self.next_internal_persistence_token()?;
        self.in_flight_persistence = Some(token);
        self.pending_durable_action = Some(PendingDurableAction::PersistFollowerAppend {
            leader_id: request.leader_id,
            resulting_log,
            match_index,
            commit_to,
        });
        self.effects
            .push_back(Effect::Persist(PersistenceBatch::new(token, operations)));
        Ok(())
    }

    fn handle_install_snapshot_request(&mut self, request: InstallSnapshotRequest) -> Result<()> {
        let leader_is_voter = self
            .volatile
            .effective_configuration
            .contains_voter(request.leader_id);
        if request.term == Term::ZERO || !leader_is_voter {
            self.send_install_snapshot_response(
                request.leader_id,
                false,
                self.snapshot.last_included_index,
            );
            return Ok(());
        }
        if request.term < self.hard_state.current_term {
            self.send_install_snapshot_response(
                request.leader_id,
                false,
                self.snapshot.last_included_index,
            );
            return Ok(());
        }
        if request.term > self.hard_state.current_term {
            let hard_state = HardState {
                current_term: request.term,
                voted_for: None,
                durable_commit_index: self.hard_state.durable_commit_index,
            };
            return self.stage_hard_state_action(
                hard_state.clone(),
                PendingDurableAction::ProcessInstallSnapshotAfterTerm {
                    hard_state,
                    request,
                },
            );
        }
        self.process_current_term_install_snapshot(request)
    }

    fn process_current_term_install_snapshot(
        &mut self,
        request: InstallSnapshotRequest,
    ) -> Result<()> {
        if request.term != self.hard_state.current_term {
            return Ok(());
        }
        if !self.accept_current_term_leader_authority(request.leader_id) {
            self.send_install_snapshot_response(
                request.leader_id,
                false,
                self.snapshot.last_included_index,
            );
            return Ok(());
        }

        let incoming = request.snapshot;
        SnapshotMetadata::new(
            incoming.last_included_index,
            incoming.last_included_term,
            incoming.configuration.clone(),
            incoming.snapshot_id,
        )?;
        if incoming.last_included_index <= self.snapshot.last_included_index {
            self.send_install_snapshot_response(
                request.leader_id,
                true,
                self.snapshot.last_included_index,
            );
            return Ok(());
        }

        if incoming.last_included_index <= self.volatile.commit_index
            && self
                .term_at(incoming.last_included_index)
                .is_some_and(|term| term != incoming.last_included_term)
        {
            self.send_install_snapshot_response(request.leader_id, false, LogIndex::ZERO);
            return Ok(());
        }

        let matching_boundary =
            self.term_at(incoming.last_included_index) == Some(incoming.last_included_term);
        let retained_log = if matching_boundary {
            self.stable_log
                .iter()
                .filter(|entry| entry.index > incoming.last_included_index)
                .cloned()
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let commit_to = self.volatile.commit_index.max(incoming.last_included_index);

        let mut recovered_hard_state = self.hard_state.clone();
        recovered_hard_state.durable_commit_index =
            recovered_hard_state.durable_commit_index.max(commit_to);
        RecoveredState::from_persisted(
            recovered_hard_state,
            incoming.clone(),
            retained_log.clone(),
        )?;

        let mut operations = vec![PersistenceOp::InstallSnapshot(incoming.clone())];
        if !matching_boundary && !self.stable_log.is_empty() {
            operations.push(PersistenceOp::TruncateSuffix {
                from: incoming.last_included_index.checked_next()?,
            });
        }
        operations.push(PersistenceOp::DiscardPrefix {
            through: incoming.last_included_index,
        });
        if commit_to > self.hard_state.durable_commit_index {
            operations.push(PersistenceOp::AdvanceCommit { to: commit_to });
        }

        let token = self.next_internal_persistence_token()?;
        self.in_flight_persistence = Some(token);
        self.pending_durable_action = Some(PendingDurableAction::PersistInstalledSnapshot {
            leader_id: request.leader_id,
            metadata: incoming,
            retained_log,
            commit_to,
        });
        self.effects
            .push_back(Effect::Persist(PersistenceBatch::new(token, operations)));
        Ok(())
    }

    fn handle_install_snapshot_response(
        &mut self,
        response: InstallSnapshotResponse,
    ) -> Result<()> {
        let responder_is_member = self
            .volatile
            .effective_configuration
            .contains_voter(response.follower_id)
            || self
                .volatile
                .effective_configuration
                .contains_learner(response.follower_id);
        if !responder_is_member {
            return Ok(());
        }
        if response.term > self.hard_state.current_term {
            let hard_state = HardState {
                current_term: response.term,
                voted_for: None,
                durable_commit_index: self.hard_state.durable_commit_index,
            };
            return self.stage_hard_state_action(
                hard_state.clone(),
                PendingDurableAction::FenceHigherTerm { hard_state },
            );
        }
        if response.term < self.hard_state.current_term || self.volatile.role != Role::Leader {
            return Ok(());
        }

        let Some(progress) = self.volatile.progress.get_mut(&response.follower_id) else {
            return Ok(());
        };
        progress.recently_active = true;
        if !response.success {
            return Ok(());
        }
        if response.match_index > progress.match_index {
            progress.match_index = response.match_index;
        }
        progress.next_index = progress.match_index.checked_next()?;
        progress.mode = ReplicationMode::Probe;
        self.maybe_finish_transfer_catch_up(response.follower_id)?;
        self.maybe_advance_leader_commit()?;
        if self.in_flight_persistence.is_none() {
            self.send_replication_to(response.follower_id)?;
        }
        Ok(())
    }

    fn send_install_snapshot_response(
        &mut self,
        leader_id: NodeId,
        success: bool,
        match_index: LogIndex,
    ) {
        self.effects.push_back(Effect::Send {
            target: leader_id,
            message: RaftMessage::InstallSnapshotResponse(InstallSnapshotResponse {
                term: self.hard_state.current_term,
                follower_id: self.local_node,
                success,
                match_index,
            }),
        });
    }

    fn handle_snapshot_request(&mut self, index: LogIndex) -> Result<()> {
        if self.pending_snapshot_build.is_some() {
            return Err(RaftError::InvalidSnapshotState(
                "snapshot build already pending",
            ));
        }
        if index <= self.snapshot.last_included_index
            || index > self.volatile.last_applied
            || index > self.volatile.commit_index
        {
            return Err(RaftError::InvalidSnapshotState(
                "snapshot index must be newer and applied/committed",
            ));
        }
        if !matches!(
            self.volatile.effective_configuration,
            EffectiveConfiguration::Stable(_)
        ) || self.volatile.pending_configuration_index.is_some()
        {
            return Err(RaftError::InvalidSnapshotState(
                "local snapshot requires stable resolved membership",
            ));
        }
        if self.term_at(index).is_none() {
            return Err(RaftError::InvalidSnapshotState(
                "snapshot index term is not retained",
            ));
        }
        self.pending_snapshot_build = Some(index);
        self.effects.push_back(Effect::BuildSnapshot(index));
        Ok(())
    }

    fn handle_snapshot_built(&mut self, metadata: SnapshotMetadata) -> Result<()> {
        let Some(expected_index) = self.pending_snapshot_build.take() else {
            return Err(RaftError::InvalidSnapshotState(
                "snapshot completion arrived without request",
            ));
        };
        if metadata.last_included_index != expected_index
            || self.term_at(expected_index) != Some(metadata.last_included_term)
        {
            return Err(RaftError::InvalidSnapshotState(
                "snapshot metadata does not match requested durable history",
            ));
        }
        SnapshotMetadata::new(
            metadata.last_included_index,
            metadata.last_included_term,
            metadata.configuration.clone(),
            metadata.snapshot_id,
        )?;
        let EffectiveConfiguration::Stable(snapshot_config) =
            self.configuration_at(expected_index)?
        else {
            return Err(RaftError::InvalidSnapshotState(
                "snapshot boundary is inside an unresolved joint configuration",
            ));
        };
        if metadata.configuration != snapshot_config {
            return Err(RaftError::InvalidSnapshotState(
                "snapshot membership does not match stable configuration at snapshot index",
            ));
        }

        let retained_log = self
            .stable_log
            .iter()
            .filter(|entry| entry.index > expected_index)
            .cloned()
            .collect::<Vec<_>>();
        let token = self.next_internal_persistence_token()?;
        self.in_flight_persistence = Some(token);
        self.pending_durable_action = Some(PendingDurableAction::PersistLocalSnapshot {
            metadata: metadata.clone(),
            retained_log,
        });
        self.effects
            .push_back(Effect::Persist(PersistenceBatch::new(
                token,
                vec![
                    PersistenceOp::InstallSnapshot(metadata),
                    PersistenceOp::DiscardPrefix {
                        through: expected_index,
                    },
                ],
            )));
        Ok(())
    }

    fn handle_transfer_leadership(&mut self, target: NodeId) -> Result<()> {
        if self.volatile.role != Role::Leader {
            return Err(RaftError::InvalidLeadershipTransfer(
                "local node is not leader",
            ));
        }
        if target == self.local_node {
            return Err(RaftError::InvalidLeadershipTransfer(
                "leadership transfer target cannot be self",
            ));
        }
        if !self.volatile.effective_configuration.contains_voter(target) {
            return Err(RaftError::InvalidLeadershipTransfer(
                "leadership transfer target is not an eligible voter",
            ));
        }
        if self.volatile.leadership_transfer.is_some() {
            return Err(RaftError::InvalidLeadershipTransfer(
                "another leadership transfer is already active",
            ));
        }
        self.volatile.leadership_transfer = Some(crate::LeadershipTransfer {
            target,
            elapsed: 0,
            timeout_now_sent: false,
        });
        self.maybe_finish_transfer_catch_up(target)?;
        if self
            .volatile
            .leadership_transfer
            .as_ref()
            .is_some_and(|transfer| !transfer.timeout_now_sent)
        {
            self.send_replication_to(target)?;
        }
        Ok(())
    }

    fn handle_timeout_now(&mut self, message: TimeoutNow) -> Result<()> {
        if !self
            .volatile
            .effective_configuration
            .contains_voter(message.leader_id)
            || !self
                .volatile
                .effective_configuration
                .contains_voter(self.local_node)
        {
            return Ok(());
        }
        if message.term < self.hard_state.current_term {
            return Ok(());
        }
        if message.term > self.hard_state.current_term {
            let hard_state = HardState {
                current_term: message.term,
                voted_for: None,
                durable_commit_index: self.hard_state.durable_commit_index,
            };
            return self.stage_hard_state_action(
                hard_state.clone(),
                PendingDurableAction::ProcessTimeoutNowAfterTerm {
                    hard_state,
                    message,
                },
            );
        }
        self.process_current_term_timeout_now(message)
    }

    fn process_current_term_timeout_now(&mut self, message: TimeoutNow) -> Result<()> {
        if message.term != self.hard_state.current_term
            || !self.accept_current_term_leader_authority(message.leader_id)
        {
            return Ok(());
        }
        let next_term = self.hard_state.current_term.checked_next()?;
        let hard_state = HardState {
            current_term: next_term,
            voted_for: Some(self.local_node),
            durable_commit_index: self.hard_state.durable_commit_index,
        };
        self.stage_hard_state_action(
            hard_state.clone(),
            PendingDurableAction::StartCampaign { hard_state },
        )
    }

    fn accept_current_term_leader_authority(&mut self, leader_id: NodeId) -> bool {
        if !self
            .volatile
            .effective_configuration
            .contains_voter(leader_id)
        {
            return false;
        }
        if self.volatile.role == Role::Leader && leader_id != self.local_node {
            return false;
        }
        if self
            .volatile
            .leader_id
            .is_some_and(|current| current != leader_id)
        {
            return false;
        }

        let previous_role = self.volatile.role;
        self.volatile.leader_id = Some(leader_id);
        self.volatile.election_elapsed = 0;
        self.volatile.pre_vote = None;

        if self.volatile.role != Role::Leader {
            if self.volatile.role != Role::Learner {
                self.volatile.role = Role::Follower;
            }
            self.volatile.election = None;
            self.volatile.progress.clear();
        }

        if previous_role != self.volatile.role {
            self.effects.push_back(Effect::RoleChanged {
                role: self.volatile.role,
                term: self.hard_state.current_term,
                leader: Some(leader_id),
            });
        }
        true
    }

    fn check_prefix(&self, prev_log_index: LogIndex, prev_log_term: Term) -> Result<PrefixCheck> {
        let snapshot_index = self.snapshot.last_included_index;
        if prev_log_index < snapshot_index {
            return Ok(PrefixCheck::Missing {
                conflict_index: snapshot_index.checked_next()?,
            });
        }

        if prev_log_index == snapshot_index {
            if prev_log_term == self.snapshot.last_included_term {
                return Ok(PrefixCheck::Match);
            }
            return Ok(PrefixCheck::TermMismatch {
                conflict_term: self.snapshot.last_included_term,
                conflict_index: snapshot_index,
            });
        }

        let Some(entry) = self
            .stable_log
            .iter()
            .find(|entry| entry.index == prev_log_index)
        else {
            return Ok(PrefixCheck::Missing {
                conflict_index: self.last_log_position().0.checked_next()?,
            });
        };

        if entry.term == prev_log_term {
            return Ok(PrefixCheck::Match);
        }

        let conflict_index = self
            .stable_log
            .iter()
            .find(|candidate| candidate.term == entry.term)
            .map_or(entry.index, |candidate| candidate.index);
        Ok(PrefixCheck::TermMismatch {
            conflict_term: entry.term,
            conflict_index,
        })
    }

    fn send_append_entries_rejection(
        &mut self,
        leader_id: NodeId,
        conflict_term: Option<Term>,
        conflict_index: LogIndex,
    ) {
        self.effects.push_back(Effect::Send {
            target: leader_id,
            message: RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
                term: self.hard_state.current_term,
                follower_id: self.local_node,
                success: false,
                match_index: LogIndex::ZERO,
                conflict_term,
                conflict_index,
            }),
        });
    }

    fn validate_incoming_entries(&self, request: &AppendEntriesRequest) -> Result<()> {
        let mut expected = request.prev_log_index;
        for entry in &request.entries {
            expected = expected.checked_next()?;
            if entry.index != expected {
                return Err(RaftError::InvalidAppendEntries(
                    "entries are not contiguous from prev_log_index + 1",
                ));
            }
            if entry.term == Term::ZERO {
                return Err(RaftError::InvalidAppendEntries(
                    "real log entries must have non-zero term",
                ));
            }
        }
        Ok(())
    }

    fn log_entry(&self, index: LogIndex) -> Option<&LogEntry> {
        self.stable_log.iter().find(|entry| entry.index == index)
    }

    fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index == self.snapshot.last_included_index {
            return Some(self.snapshot.last_included_term);
        }
        self.log_entry(index).map(|entry| entry.term)
    }

    fn fatal_safety(&mut self, message: impl Into<String>) {
        self.stopped = true;
        self.in_flight_persistence = None;
        self.pending_durable_action = None;
        self.pending_append_persistence.clear();
        self.pending_append_bytes = 0;
        self.effects.push_back(Effect::Fatal(message.into()));
    }

    fn reconstruct_membership_from_durable_log(&mut self) -> Result<()> {
        let (effective, pending) =
            self.derive_membership_for_log(&self.stable_log, self.volatile.commit_index)?;

        self.volatile.effective_configuration = effective;
        self.volatile.pending_configuration_index = pending;
        self.reconcile_progress_membership()?;
        Ok(())
    }

    fn derive_membership_for_log(
        &self,
        log: &[LogEntry],
        commit_index: LogIndex,
    ) -> Result<(EffectiveConfiguration, Option<LogIndex>)> {
        let mut effective = EffectiveConfiguration::Stable(self.snapshot.configuration.clone());
        let mut pending = None;

        for entry in log {
            let EntryKind::Configuration(change) = &entry.kind else {
                continue;
            };
            match change {
                ConfigurationChange::EnterJoint { old, new } => {
                    match &effective {
                        EffectiveConfiguration::Stable(current) if current == old => {}
                        _ => {
                            return Err(RaftError::InvalidConfigurationChange(
                                "EnterJoint old configuration does not match durable effective configuration",
                            ));
                        }
                    }
                    effective = EffectiveConfiguration::Joint(crate::JointConfiguration::new(
                        old.clone(),
                        new.clone(),
                    )?);
                    pending = Some(entry.index);
                }
                ConfigurationChange::LeaveJoint { new } => {
                    match &effective {
                        EffectiveConfiguration::Joint(joint) if joint.new_config() == new => {}
                        _ => {
                            return Err(RaftError::InvalidConfigurationChange(
                                "LeaveJoint does not match the active joint configuration",
                            ));
                        }
                    }
                    effective = EffectiveConfiguration::Stable(new.clone());
                    pending = Some(entry.index);
                }
            }
        }

        if matches!(effective, EffectiveConfiguration::Stable(_))
            && pending.is_some_and(|index| index <= commit_index)
        {
            pending = None;
        }
        Ok((effective, pending))
    }

    fn configuration_at(&self, index: LogIndex) -> Result<EffectiveConfiguration> {
        if index < self.snapshot.last_included_index || index > self.last_log_position().0 {
            return Err(RaftError::InvalidSnapshotState(
                "configuration lookup index is outside retained history",
            ));
        }
        let prefix = self
            .stable_log
            .iter()
            .filter(|entry| entry.index <= index)
            .cloned()
            .collect::<Vec<_>>();
        self.derive_membership_for_log(&prefix, index)
            .map(|(configuration, _)| configuration)
    }

    fn reconcile_progress_membership(&mut self) -> Result<()> {
        if self.volatile.role != Role::Leader {
            return Ok(());
        }
        let members = self.volatile.effective_configuration.all_members();
        let durable_last = self.last_log_position().0;
        let next_index = durable_last.checked_next()?;
        self.volatile
            .progress
            .retain(|node, _| members.contains(node));
        for member in members {
            self.volatile.progress.entry(member).or_insert(Progress {
                match_index: if member == self.local_node {
                    durable_last
                } else {
                    LogIndex::ZERO
                },
                next_index,
                recently_active: member == self.local_node,
                mode: if member == self.local_node {
                    ReplicationMode::Replicate
                } else {
                    ReplicationMode::Probe
                },
            });
        }
        Ok(())
    }

    fn finish_commit_persistence(&mut self, to: LogIndex) -> Result<()> {
        if to < self.volatile.commit_index {
            return Err(RaftError::InvalidHardState(
                "durable commit index cannot move backward",
            ));
        }
        if to > self.last_log_position().0 {
            return Err(RaftError::InvalidHardState(
                "durable commit index exceeds durable history",
            ));
        }
        self.hard_state.durable_commit_index = to;
        self.volatile.commit_index = to;

        if matches!(
            self.volatile.effective_configuration,
            EffectiveConfiguration::Stable(_)
        ) && self
            .volatile
            .pending_configuration_index
            .is_some_and(|index| index <= to)
        {
            self.volatile.pending_configuration_index = None;
        }

        if matches!(
            self.volatile.effective_configuration,
            EffectiveConfiguration::Stable(_)
        ) && self.volatile.pending_configuration_index.is_none()
            && self.volatile.role != Role::Leader
        {
            let next_role = if self
                .volatile
                .effective_configuration
                .contains_learner(self.local_node)
                && !self
                    .volatile
                    .effective_configuration
                    .contains_voter(self.local_node)
            {
                Role::Learner
            } else {
                Role::Follower
            };
            if self.volatile.role != next_role {
                self.volatile.role = next_role;
                self.volatile.pre_vote = None;
                self.volatile.election = None;
                self.volatile.progress.clear();
                self.volatile.election_elapsed = 0;
                self.volatile.heartbeat_elapsed = 0;
                self.volatile.leadership_transfer = None;
                self.effects.push_back(Effect::RoleChanged {
                    role: next_role,
                    term: self.hard_state.current_term,
                    leader: self.volatile.leader_id,
                });
            }
        }

        if self.volatile.role == Role::Leader
            && matches!(
                self.volatile.effective_configuration,
                EffectiveConfiguration::Stable(_)
            )
            && !self
                .volatile
                .effective_configuration
                .contains_voter(self.local_node)
        {
            self.volatile.role = if self
                .volatile
                .effective_configuration
                .contains_learner(self.local_node)
            {
                Role::Learner
            } else {
                Role::Follower
            };
            self.volatile.leader_id = None;
            self.volatile.progress.clear();
            self.volatile.leadership_transfer = None;
            self.effects.push_back(Effect::RoleChanged {
                role: self.volatile.role,
                term: self.hard_state.current_term,
                leader: None,
            });
        }

        self.emit_apply_if_needed();
        Ok(())
    }

    fn emit_apply_if_needed(&mut self) {
        let entries = self
            .stable_log
            .iter()
            .filter(|entry| {
                entry.index > self.volatile.last_applied
                    && entry.index <= self.volatile.commit_index
            })
            .cloned()
            .collect::<Vec<_>>();
        if !entries.is_empty() {
            self.effects.push_back(Effect::Apply(entries));
        }
    }

    fn win_election(&mut self, term: Term) -> Result<()> {
        if self.volatile.role != Role::Candidate
            || self.hard_state.current_term != term
            || self.hard_state.voted_for != Some(self.local_node)
            || !self
                .volatile
                .effective_configuration
                .contains_voter(self.local_node)
        {
            return Err(RaftError::InvalidLeaderTransition);
        }

        let Some(election) = self.volatile.election.as_ref() else {
            return Err(RaftError::InvalidLeaderTransition);
        };
        if election.term != term || !election.quorum_reached {
            return Err(RaftError::InvalidLeaderTransition);
        }

        let durable_last_index = self.last_log_position().0;
        let barrier_index = durable_last_index.checked_next()?;
        let next_index = barrier_index;
        let barrier = LogEntry::new(term, barrier_index, EntryKind::LeaderBarrier);
        let token = self.next_internal_persistence_token()?;

        let mut progress = std::collections::BTreeMap::new();
        for member in self.volatile.effective_configuration.all_members() {
            let is_self = member == self.local_node;
            progress.insert(
                member,
                Progress {
                    match_index: if is_self {
                        durable_last_index
                    } else {
                        LogIndex::ZERO
                    },
                    next_index,
                    recently_active: is_self,
                    mode: if is_self {
                        ReplicationMode::Replicate
                    } else {
                        ReplicationMode::Probe
                    },
                },
            );
        }

        self.volatile.role = Role::Leader;
        self.volatile.leader_id = Some(self.local_node);
        self.volatile.election_elapsed = 0;
        self.volatile.heartbeat_elapsed = 0;
        self.volatile.pre_vote = None;
        self.volatile.election = None;
        self.volatile.leadership_transfer = None;
        self.volatile.progress = progress;
        self.in_flight_persistence = Some(token);
        self.pending_durable_action = Some(PendingDurableAction::PersistLeaderBarrier {
            entry: barrier.clone(),
        });

        self.effects.push_back(Effect::RoleChanged {
            role: Role::Leader,
            term,
            leader: Some(self.local_node),
        });
        self.effects
            .push_back(Effect::Persist(PersistenceBatch::new(
                token,
                vec![PersistenceOp::Append(vec![barrier])],
            )));
        Ok(())
    }

    fn finish_leader_barrier_persistence(&mut self, entry: LogEntry) -> Result<()> {
        if self.volatile.role != Role::Leader
            || self.volatile.leader_id != Some(self.local_node)
            || entry.term != self.hard_state.current_term
            || !matches!(&entry.kind, EntryKind::LeaderBarrier)
        {
            return Err(RaftError::InvalidLeaderTransition);
        }

        let expected_index = self.last_log_position().0.checked_next()?;
        if entry.index != expected_index {
            return Err(RaftError::InvalidLeaderTransition);
        }

        self.stable_log.push(entry.clone());
        let self_progress = self
            .volatile
            .progress
            .get_mut(&self.local_node)
            .ok_or(RaftError::InvalidLeaderTransition)?;
        self_progress.match_index = entry.index;
        self_progress.next_index = entry.index.checked_next()?;
        self_progress.recently_active = true;
        self_progress.mode = ReplicationMode::Replicate;
        self.send_replication_to_all()?;
        self.maybe_advance_leader_commit()?;
        Ok(())
    }

    fn handle_application_proposal(&mut self, id: ProposalId, payload: Vec<u8>) -> Result<()> {
        if self.volatile.role != Role::Leader || self.volatile.leadership_transfer.is_some() {
            self.effects.push_back(Effect::ProposalResult {
                id,
                accepted: false,
                leader: self.volatile.leader_id,
            });
            return Ok(());
        }

        let uncommitted = self.uncommitted_application_bytes();
        if uncommitted.saturating_add(payload.len()) > self.limits.max_uncommitted_application_bytes
        {
            self.effects.push_back(Effect::ProposalResult {
                id,
                accepted: false,
                leader: self.volatile.leader_id,
            });
            return Ok(());
        }

        let entry = LogEntry::new(
            self.hard_state.current_term,
            self.logical_last_log_position().0.checked_next()?,
            EntryKind::Application(payload),
        );
        self.stage_leader_entry(entry, Some(id))
    }

    fn handle_configuration_proposal(
        &mut self,
        id: ProposalId,
        change: ConfigurationChange,
    ) -> Result<()> {
        if self.volatile.role != Role::Leader || self.volatile.leadership_transfer.is_some() {
            self.effects.push_back(Effect::ProposalResult {
                id,
                accepted: false,
                leader: self.volatile.leader_id,
            });
            return Ok(());
        }

        if !self.configuration_change_is_allowed(&change) {
            self.effects.push_back(Effect::ProposalResult {
                id,
                accepted: false,
                leader: self.volatile.leader_id,
            });
            return Ok(());
        }

        let entry = LogEntry::new(
            self.hard_state.current_term,
            self.last_log_position().0.checked_next()?,
            EntryKind::Configuration(change),
        );
        self.stage_leader_entry(entry, Some(id))
    }

    fn stage_leader_entry(
        &mut self,
        entry: LogEntry,
        proposal_id: Option<ProposalId>,
    ) -> Result<()> {
        let groupable = matches!(entry.kind, EntryKind::Application(_));
        if self.in_flight_persistence.is_some() {
            return Err(RaftError::PersistenceAlreadyInFlight);
        }
        if !groupable && !self.pending_append_persistence.is_empty() {
            return Err(RaftError::PersistenceAlreadyInFlight);
        }
        if groupable {
            let expected = self.logical_last_log_position().0.checked_next()?;
            if entry.index != expected {
                return Err(RaftError::InvalidAppendEntries(
                    "staged leader append is not contiguous",
                ));
            }
            let logical_bytes = match &entry.kind {
                EntryKind::Application(payload) => payload.len(),
                _ => 0,
            };
            if self.pending_append_persistence.len() >= self.limits.max_pending_persistence_batches
                || self
                    .pending_append_bytes
                    .checked_add(logical_bytes)
                    .is_none_or(|bytes| bytes > self.limits.max_pending_persistence_bytes)
            {
                return Err(RaftError::PersistenceQueueFull);
            }
            let token = self.next_internal_persistence_token()?;
            self.pending_append_bytes = self
                .pending_append_bytes
                .checked_add(logical_bytes)
                .ok_or(RaftError::PersistenceQueueFull)?;
            self.pending_append_persistence
                .push_back(PendingAppendPersistence {
                    token,
                    entry: entry.clone(),
                    proposal_id,
                    logical_bytes,
                });
            self.effects
                .push_back(Effect::Persist(PersistenceBatch::new(
                    token,
                    vec![PersistenceOp::Append(vec![entry])],
                )));
            return Ok(());
        }
        let token = self.next_internal_persistence_token()?;
        self.in_flight_persistence = Some(token);
        self.pending_durable_action = Some(PendingDurableAction::PersistLeaderEntry {
            entry: entry.clone(),
            proposal_id,
        });
        self.effects
            .push_back(Effect::Persist(PersistenceBatch::new(
                token,
                vec![PersistenceOp::Append(vec![entry])],
            )));
        Ok(())
    }

    fn finish_leader_entry_persistence(
        &mut self,
        entry: LogEntry,
        proposal_id: Option<ProposalId>,
    ) -> Result<()> {
        if self.volatile.role != Role::Leader || entry.term != self.hard_state.current_term {
            return Err(RaftError::InvalidLeaderTransition);
        }
        if entry.index != self.last_log_position().0.checked_next()? {
            return Err(RaftError::InvalidAppendEntries(
                "leader local append is not contiguous",
            ));
        }

        self.stable_log.push(entry.clone());
        if let Some(self_progress) = self.volatile.progress.get_mut(&self.local_node) {
            self_progress.match_index = entry.index;
            self_progress.next_index = entry.index.checked_next()?;
            self_progress.recently_active = true;
            self_progress.mode = ReplicationMode::Replicate;
        }

        if matches!(entry.kind, EntryKind::Configuration(_)) {
            self.reconstruct_membership_from_durable_log()?;
        }

        if let Some(id) = proposal_id {
            self.effects.push_back(Effect::ProposalResult {
                id,
                accepted: true,
                leader: Some(self.local_node),
            });
        }
        self.send_replication_to_all()?;
        self.maybe_advance_leader_commit()?;
        Ok(())
    }

    fn configuration_change_is_allowed(&self, change: &ConfigurationChange) -> bool {
        match change {
            ConfigurationChange::EnterJoint { old, new } => {
                if self.volatile.pending_configuration_index.is_some() {
                    return false;
                }
                let EffectiveConfiguration::Stable(current) =
                    &self.volatile.effective_configuration
                else {
                    return false;
                };
                if current != old {
                    return false;
                }

                for voter in new.voters() {
                    if old.contains_voter(voter) {
                        continue;
                    }
                    if !old.contains_learner(voter)
                        || !self.volatile.progress.get(&voter).is_some_and(|progress| {
                            progress.match_index >= self.volatile.commit_index
                        })
                    {
                        return false;
                    }
                }
                true
            }
            ConfigurationChange::LeaveJoint { new } => {
                let EffectiveConfiguration::Joint(joint) = &self.volatile.effective_configuration
                else {
                    return false;
                };
                if joint.new_config() != new {
                    return false;
                }
                self.volatile
                    .pending_configuration_index
                    .is_some_and(|index| {
                        index <= self.volatile.commit_index
                            && self.log_entry(index).is_some_and(|entry| {
                                matches!(
                                    entry.kind,
                                    EntryKind::Configuration(
                                        ConfigurationChange::EnterJoint { .. }
                                    )
                                )
                            })
                    })
            }
        }
    }

    fn uncommitted_application_bytes(&self) -> usize {
        let durable = self
            .stable_log
            .iter()
            .filter(|entry| entry.index > self.volatile.commit_index)
            .filter_map(|entry| match &entry.kind {
                EntryKind::Application(payload) => Some(payload.len()),
                _ => None,
            })
            .sum::<usize>();
        let staged = self
            .pending_append_persistence
            .iter()
            .filter_map(|pending| match &pending.entry.kind {
                EntryKind::Application(payload) => Some(payload.len()),
                _ => None,
            })
            .sum::<usize>();
        durable.saturating_add(staged)
    }

    fn send_request_vote_response(&mut self, target: NodeId, vote_granted: bool) {
        self.effects.push_back(Effect::Send {
            target,
            message: RaftMessage::RequestVoteResponse(RequestVoteResponse {
                term: self.hard_state.current_term,
                voter_id: self.local_node,
                vote_granted,
            }),
        });
    }

    fn step_down_for_term_change(&mut self) {
        self.volatile.role = if self
            .volatile
            .effective_configuration
            .contains_voter(self.local_node)
        {
            Role::Follower
        } else {
            Role::Learner
        };
        self.volatile.leader_id = None;
        self.volatile.pre_vote = None;
        self.volatile.election = None;
        self.volatile.progress.clear();
        self.volatile.election_elapsed = 0;
        self.volatile.heartbeat_elapsed = 0;
        self.volatile.leadership_transfer = None;
    }

    fn step_down_after_vote_grant(&mut self) {
        let previous_role = self.volatile.role;
        self.volatile.leader_id = None;
        self.volatile.pre_vote = None;
        self.volatile.election = None;
        self.volatile.progress.clear();
        self.volatile.heartbeat_elapsed = 0;
        self.volatile.leadership_transfer = None;
        if previous_role != Role::Learner {
            self.volatile.role = Role::Follower;
        }
        if previous_role != self.volatile.role {
            self.effects.push_back(Effect::RoleChanged {
                role: self.volatile.role,
                term: self.hard_state.current_term,
                leader: None,
            });
        }
    }

    fn maybe_emit_election_quorum(&mut self) {
        let Some(round) = self.volatile.election.as_mut() else {
            return;
        };
        if round.quorum_reached {
            return;
        }
        let granted = round.granted_voters();
        if self
            .volatile
            .effective_configuration
            .has_election_quorum(&granted)
        {
            round.quorum_reached = true;
            self.effects
                .push_back(Effect::ElectionQuorumReached { term: round.term });
        }
    }

    fn maybe_emit_pre_vote_quorum(&mut self) {
        let Some(round) = self.volatile.pre_vote.as_mut() else {
            return;
        };
        if round.quorum_reached {
            return;
        }
        let granted = round.granted_voters();
        if self
            .volatile
            .effective_configuration
            .has_election_quorum(&granted)
        {
            round.quorum_reached = true;
            self.effects.push_back(Effect::PreVoteQuorumReached {
                prospective_term: round.prospective_term,
            });
        }
    }

    fn last_log_position(&self) -> (LogIndex, Term) {
        self.stable_log.last().map_or(
            (
                self.snapshot.last_included_index,
                self.snapshot.last_included_term,
            ),
            |entry| (entry.index, entry.term),
        )
    }

    fn logical_last_log_position(&self) -> (LogIndex, Term) {
        self.pending_append_persistence.back().map_or_else(
            || self.last_log_position(),
            |pending| (pending.entry.index, pending.entry.term),
        )
    }

    fn log_is_at_least_as_up_to_date(
        &self,
        candidate_term: Term,
        candidate_index: LogIndex,
    ) -> bool {
        let (local_index, local_term) = self.last_log_position();
        candidate_term > local_term
            || (candidate_term == local_term && candidate_index >= local_index)
    }
}

fn replication_entry_bytes(entry: &LogEntry) -> usize {
    match &entry.kind {
        EntryKind::Application(payload) => 48_usize.saturating_add(payload.len()),
        EntryKind::LeaderBarrier => 48,
        EntryKind::Configuration(_) => 256,
    }
}

fn bounded_replication_suffix(
    stable_log: &[LogEntry],
    next_index: LogIndex,
    max_bytes: usize,
) -> Vec<LogEntry> {
    let mut entries = Vec::new();
    let mut used_bytes = 0_usize;
    for entry in stable_log.iter().filter(|entry| entry.index >= next_index) {
        let candidate = used_bytes.saturating_add(replication_entry_bytes(entry));
        // Preserve forward progress for a single already-accepted retained
        // entry even when its historical size is larger than a newly lowered
        // scheduling window. Every accepted entry is independently bounded by
        // the embedding command/storage contract.
        if !entries.is_empty() && candidate > max_bytes {
            break;
        }
        entries.push(entry.clone());
        used_bytes = candidate;
        if used_bytes >= max_bytes {
            break;
        }
    }
    entries
}

#[cfg(test)]
mod tests {
    use crate::{
        Effect, EntryKind, JointConfiguration, LogEntry, PersistenceOp, SnapshotId,
        StableConfiguration, Term,
    };

    use super::*;

    fn node(value: u64) -> NodeId {
        NodeId::new(value).unwrap()
    }

    #[test]
    fn p8f_replication_effect_clones_only_a_bounded_history_window() {
        let term = Term::new(3);
        let log = (1..=10_u64)
            .map(|index| {
                LogEntry::new(
                    term,
                    LogIndex::new(index),
                    EntryKind::Application(vec![0xA5; 64]),
                )
            })
            .collect::<Vec<_>>();

        let bounded = bounded_replication_suffix(&log, LogIndex::FIRST, 300);
        assert_eq!(bounded.len(), 2);
        assert_eq!(bounded[0].index, LogIndex::FIRST);
        assert_eq!(bounded[1].index, LogIndex::new(2));
        assert!(bounded.iter().map(replication_entry_bytes).sum::<usize>() <= 300);

        let later = bounded_replication_suffix(&log, LogIndex::new(7), 300);
        assert_eq!(
            later.first().map(|entry| entry.index),
            Some(LogIndex::new(7))
        );

        let oversized = vec![LogEntry::new(
            term,
            LogIndex::FIRST,
            EntryKind::Application(vec![0x5A; 512]),
        )];
        assert_eq!(
            bounded_replication_suffix(&oversized, LogIndex::FIRST, 128).len(),
            1
        );
    }

    fn timing() -> TimingConfig {
        TimingConfig::new(1, 5, 10).unwrap()
    }

    fn recovered_with(
        config: EffectiveConfiguration,
        log: Vec<LogEntry>,
        term: Term,
    ) -> RecoveredState {
        let snapshot_config = match &config {
            EffectiveConfiguration::Stable(config) => config.clone(),
            EffectiveConfiguration::Joint(config) => config.old().clone(),
        };
        let snapshot = SnapshotMetadata::new(
            LogIndex::ZERO,
            Term::ZERO,
            snapshot_config,
            SnapshotId::ZERO,
        )
        .unwrap();
        RecoveredState {
            hard_state: HardState {
                current_term: term,
                voted_for: None,
                durable_commit_index: LogIndex::ZERO,
            },
            snapshot,
            log,
            effective_configuration: config,
            pending_configuration_index: None,
        }
    }

    fn recovered() -> RecoveredState {
        recovered_with(
            EffectiveConfiguration::Stable(
                StableConfiguration::new([node(1), node(2), node(3)], []).unwrap(),
            ),
            Vec::new(),
            Term::ZERO,
        )
    }

    fn core(local: u64) -> RaftCore {
        RaftCore::from_recovered(node(local), recovered(), timing(), 5).unwrap()
    }

    fn tick(core: &mut RaftCore, count: usize) {
        for _ in 0..count {
            core.handle_input(Input::Tick {
                reset_election_timeout: 6,
            })
            .unwrap();
        }
    }

    fn pre_vote_response(core: &mut RaftCore) -> PreVoteResponse {
        let effects = core.drain_effects().collect::<Vec<_>>();
        assert_eq!(effects.len(), 1);
        match effects.into_iter().next().unwrap() {
            Effect::Send {
                message: RaftMessage::PreVoteResponse(response),
                ..
            } => response,
            other => panic!("unexpected effect: {other:?}"),
        }
    }

    fn reach_pre_vote_quorum(core: &mut RaftCore, prospective_term: Term) {
        tick(core, 5);
        let _ = core.drain_effects().collect::<Vec<_>>();
        core.handle_input(Input::Step(RaftMessage::PreVoteResponse(PreVoteResponse {
            prospective_term,
            voter_id: node(2),
            vote_granted: true,
        })))
        .unwrap();
        assert_eq!(
            core.drain_effects().collect::<Vec<_>>(),
            vec![Effect::PreVoteQuorumReached { prospective_term }]
        );
    }

    fn staged_hard_state(effect: &Effect) -> (PersistenceToken, HardState) {
        let Effect::Persist(batch) = effect else {
            panic!("expected persistence effect, got {effect:?}");
        };
        assert_eq!(batch.operations.len(), 1);
        let PersistenceOp::UpdateHardState(hard_state) = &batch.operations[0] else {
            panic!(
                "expected hard-state persistence, got {:?}",
                batch.operations[0]
            );
        };
        (batch.token, hard_state.clone())
    }

    fn begin_three_voter_campaign(core: &mut RaftCore, term: Term) -> PersistenceToken {
        reach_pre_vote_quorum(core, term);
        core.handle_input(Input::BeginElection {
            prospective_term: term,
        })
        .unwrap();
        let effects = core.drain_effects().collect::<Vec<_>>();
        assert_eq!(effects.len(), 1);
        staged_hard_state(&effects[0]).0
    }

    fn reach_three_voter_election_quorum(core: &mut RaftCore, term: Term) {
        let token = begin_three_voter_campaign(core, term);
        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        let _ = core.drain_effects().collect::<Vec<_>>();
        core.handle_input(Input::Step(RaftMessage::RequestVoteResponse(
            RequestVoteResponse {
                term,
                voter_id: node(2),
                vote_granted: true,
            },
        )))
        .unwrap();
        assert_eq!(
            core.drain_effects().collect::<Vec<_>>(),
            vec![Effect::ElectionQuorumReached { term }]
        );
        assert_eq!(core.volatile_state().role, Role::Candidate);
        assert!(
            core.volatile_state()
                .election
                .as_ref()
                .is_some_and(|round| round.quorum_reached)
        );
    }

    fn staged_single_append(effect: &Effect) -> (PersistenceToken, LogEntry) {
        let Effect::Persist(batch) = effect else {
            panic!("expected persistence effect, got {effect:?}");
        };
        assert_eq!(batch.operations.len(), 1);
        let PersistenceOp::Append(entries) = &batch.operations[0] else {
            panic!("expected append persistence, got {:?}", batch.operations[0]);
        };
        assert_eq!(entries.len(), 1);
        (batch.token, entries[0].clone())
    }

    fn heartbeat(
        term: Term,
        leader_id: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        leader_commit: LogIndex,
    ) -> AppendEntriesRequest {
        AppendEntriesRequest {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries: Vec::new(),
            leader_commit,
        }
    }

    fn append_entries_response(core: &mut RaftCore) -> AppendEntriesResponse {
        let effects = core.drain_effects().collect::<Vec<_>>();
        assert_eq!(effects.len(), 1);
        match effects.into_iter().next().unwrap() {
            Effect::Send {
                message: RaftMessage::AppendEntriesResponse(response),
                ..
            } => response,
            other => panic!("unexpected effect: {other:?}"),
        }
    }

    #[test]
    fn foundation_stages_only_one_persistence_barrier() {
        let mut core = core(1);
        let first = PersistenceBatch::new(
            PersistenceToken::new(1),
            vec![PersistenceOp::Append(vec![LogEntry::new(
                Term::new(1),
                LogIndex::FIRST,
                EntryKind::LeaderBarrier,
            )])],
        );
        core.stage_persistence(first.clone()).unwrap();
        assert_eq!(
            core.stage_persistence(PersistenceBatch::new(PersistenceToken::new(2), Vec::new())),
            Err(RaftError::PersistenceAlreadyInFlight)
        );
        assert_eq!(
            core.drain_effects().collect::<Vec<_>>(),
            vec![Effect::Persist(first)]
        );
        core.handle_input(Input::PersistenceCompleted(PersistenceToken::new(1)))
            .unwrap();
        assert_eq!(core.in_flight_persistence(), None);
    }

    #[test]
    fn wrong_persistence_completion_cannot_cross_barrier() {
        let mut core = core(1);
        core.stage_persistence(PersistenceBatch::new(PersistenceToken::new(7), Vec::new()))
            .unwrap();
        assert_eq!(
            core.handle_input(Input::PersistenceCompleted(PersistenceToken::new(8))),
            Err(RaftError::UnexpectedPersistenceToken)
        );
        assert_eq!(core.in_flight_persistence(), Some(PersistenceToken::new(7)));
    }

    #[test]
    fn persistence_failure_is_fatal_and_never_reported_as_success() {
        let mut core = core(1);
        core.stage_persistence(PersistenceBatch::new(PersistenceToken::new(5), Vec::new()))
            .unwrap();
        let _ = core.drain_effects().collect::<Vec<_>>();
        core.handle_input(Input::PersistenceFailed {
            token: PersistenceToken::new(5),
            error: PersistenceFailure::new("simulated fsync failure"),
        })
        .unwrap();
        assert!(core.is_stopped());
        assert_eq!(
            core.drain_effects().collect::<Vec<_>>(),
            vec![Effect::Fatal("simulated fsync failure".into())]
        );
    }

    #[test]
    fn follower_proposal_is_rejected_without_hidden_forwarding() {
        let mut core = core(1);
        core.handle_input(Input::Propose {
            id: ProposalId::new(1),
            payload: b"not-leader".to_vec(),
        })
        .unwrap();
        assert_eq!(
            core.drain_effects().collect::<Vec<_>>(),
            vec![Effect::ProposalResult {
                id: ProposalId::new(1),
                accepted: false,
                leader: None,
            }]
        );
        assert_eq!(core.volatile_state().role, Role::Follower);
        assert_eq!(core.hard_state().current_term, Term::ZERO);
    }

    #[test]
    fn timeout_enters_pre_candidate_and_sends_requests_in_node_order() {
        let mut core = core(1);
        let hard_before = core.hard_state().clone();

        tick(&mut core, 4);
        assert_eq!(core.volatile_state().role, Role::Follower);
        assert!(core.drain_effects().next().is_none());

        tick(&mut core, 1);
        assert_eq!(core.volatile_state().role, Role::PreCandidate);
        assert_eq!(core.volatile_state().election_elapsed, 0);
        assert_eq!(core.volatile_state().randomized_election_timeout, 6);
        assert_eq!(core.hard_state(), &hard_before);

        let effects = core.drain_effects().collect::<Vec<_>>();
        assert_eq!(effects.len(), 3);
        assert!(matches!(
            effects[0],
            Effect::RoleChanged {
                role: Role::PreCandidate,
                term: Term::ZERO,
                leader: None
            }
        ));
        let targets = effects[1..]
            .iter()
            .map(|effect| match effect {
                Effect::Send {
                    target,
                    message: RaftMessage::PreVoteRequest(request),
                } => {
                    assert_eq!(request.prospective_term, Term::new(1));
                    assert_eq!(request.candidate_id, node(1));
                    assert_eq!(request.last_log_index, LogIndex::ZERO);
                    assert_eq!(request.last_log_term, Term::ZERO);
                    target.get()
                }
                other => panic!("unexpected effect: {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(targets, vec![2, 3]);
        assert!(
            !effects
                .iter()
                .any(|effect| matches!(effect, Effect::Persist(_)))
        );
    }

    #[test]
    fn single_voter_reaches_pre_vote_quorum_without_real_election() {
        let config =
            EffectiveConfiguration::Stable(StableConfiguration::new([node(1)], []).unwrap());
        let mut core = RaftCore::from_recovered(
            node(1),
            recovered_with(config, Vec::new(), Term::new(7)),
            timing(),
            5,
        )
        .unwrap();
        let hard_before = core.hard_state().clone();

        tick(&mut core, 5);
        let effects = core.drain_effects().collect::<Vec<_>>();
        assert!(effects.iter().any(|effect| matches!(
            effect,
            Effect::PreVoteQuorumReached {
                prospective_term: term
            } if *term == Term::new(8)
        )));
        assert_eq!(core.volatile_state().role, Role::PreCandidate);
        assert_eq!(core.hard_state(), &hard_before);
        assert!(
            !effects
                .iter()
                .any(|effect| matches!(effect, Effect::Persist(_)))
        );
    }

    #[test]
    fn real_campaign_requires_matching_qualified_pre_vote_quorum() {
        let mut core = core(1);
        assert_eq!(
            core.handle_input(Input::BeginElection {
                prospective_term: Term::new(1),
            }),
            Err(RaftError::InvalidElectionStart)
        );

        tick(&mut core, 5);
        let _ = core.drain_effects().collect::<Vec<_>>();
        assert_eq!(core.volatile_state().role, Role::PreCandidate);
        assert_eq!(
            core.handle_input(Input::BeginElection {
                prospective_term: Term::new(1),
            }),
            Err(RaftError::InvalidElectionStart)
        );

        core.handle_input(Input::Step(RaftMessage::PreVoteResponse(PreVoteResponse {
            prospective_term: Term::new(1),
            voter_id: node(2),
            vote_granted: true,
        })))
        .unwrap();
        let _ = core.drain_effects().collect::<Vec<_>>();
        assert_eq!(
            core.handle_input(Input::BeginElection {
                prospective_term: Term::new(2),
            }),
            Err(RaftError::InvalidElectionStart)
        );
    }

    #[test]
    fn self_vote_is_durable_before_any_request_vote_is_sent() {
        let mut core = core(1);
        let token = begin_three_voter_campaign(&mut core, Term::new(1));

        assert_eq!(core.hard_state().current_term, Term::ZERO);
        assert_eq!(core.hard_state().voted_for, None);
        assert_eq!(core.volatile_state().role, Role::PreCandidate);
        assert_eq!(core.in_flight_persistence(), Some(token));

        assert_eq!(
            core.handle_input(Input::Step(RaftMessage::PreVoteRequest(PreVoteRequest {
                prospective_term: Term::new(2),
                candidate_id: node(2),
                last_log_index: LogIndex::ZERO,
                last_log_term: Term::ZERO,
            }))),
            Err(RaftError::PersistenceAlreadyInFlight)
        );

        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        assert_eq!(core.hard_state().current_term, Term::new(1));
        assert_eq!(core.hard_state().voted_for, Some(node(1)));
        assert_eq!(core.volatile_state().role, Role::Candidate);

        let effects = core.drain_effects().collect::<Vec<_>>();
        assert_eq!(effects.len(), 3);
        assert!(matches!(
            effects[0],
            Effect::RoleChanged {
                role: Role::Candidate,
                term,
                leader: None
            } if term == Term::new(1)
        ));
        let targets = effects[1..]
            .iter()
            .map(|effect| match effect {
                Effect::Send {
                    target,
                    message: RaftMessage::RequestVoteRequest(request),
                } => {
                    assert_eq!(request.term, Term::new(1));
                    assert_eq!(request.candidate_id, node(1));
                    target.get()
                }
                other => panic!("unexpected effect after durable self-vote: {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(targets, vec![2, 3]);
    }

    #[test]
    fn single_voter_campaign_reports_quorum_but_does_not_become_leader() {
        let config =
            EffectiveConfiguration::Stable(StableConfiguration::new([node(1)], []).unwrap());
        let mut core = RaftCore::from_recovered(
            node(1),
            recovered_with(config, Vec::new(), Term::new(4)),
            timing(),
            5,
        )
        .unwrap();
        tick(&mut core, 5);
        let _ = core.drain_effects().collect::<Vec<_>>();
        core.handle_input(Input::BeginElection {
            prospective_term: Term::new(5),
        })
        .unwrap();
        let persist = core.drain_effects().collect::<Vec<_>>();
        let (token, hard_state) = staged_hard_state(&persist[0]);
        assert_eq!(hard_state.current_term, Term::new(5));
        assert_eq!(hard_state.voted_for, Some(node(1)));

        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        let effects = core.drain_effects().collect::<Vec<_>>();
        assert!(effects.iter().any(|effect| matches!(
            effect,
            Effect::ElectionQuorumReached { term } if *term == Term::new(5)
        )));
        assert_eq!(core.volatile_state().role, Role::Candidate);
        assert_ne!(core.volatile_state().role, Role::Leader);
    }

    #[test]
    fn granted_vote_is_persisted_before_positive_response() {
        let config = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], []).unwrap(),
        );
        let mut core = RaftCore::from_recovered(
            node(1),
            recovered_with(config, Vec::new(), Term::new(3)),
            timing(),
            5,
        )
        .unwrap();

        core.handle_input(Input::Step(RaftMessage::RequestVoteRequest(
            RequestVoteRequest {
                term: Term::new(3),
                candidate_id: node(2),
                last_log_index: LogIndex::ZERO,
                last_log_term: Term::ZERO,
            },
        )))
        .unwrap();

        let effects = core.drain_effects().collect::<Vec<_>>();
        assert_eq!(effects.len(), 1);
        let (token, hard_state) = staged_hard_state(&effects[0]);
        assert_eq!(hard_state.current_term, Term::new(3));
        assert_eq!(hard_state.voted_for, Some(node(2)));
        assert_eq!(core.hard_state().voted_for, None);

        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        assert_eq!(core.hard_state().voted_for, Some(node(2)));
        assert_eq!(core.volatile_state().election_elapsed, 0);
        assert_eq!(
            core.drain_effects().collect::<Vec<_>>(),
            vec![Effect::Send {
                target: node(2),
                message: RaftMessage::RequestVoteResponse(RequestVoteResponse {
                    term: Term::new(3),
                    voter_id: node(1),
                    vote_granted: true,
                })
            }]
        );
    }

    #[test]
    fn durable_vote_survives_restart_and_prevents_double_vote_in_same_term() {
        let config = StableConfiguration::new([node(1), node(2), node(3)], []).unwrap();
        let snapshot =
            SnapshotMetadata::new(LogIndex::ZERO, Term::ZERO, config.clone(), SnapshotId::ZERO)
                .unwrap();
        let recovered = RecoveredState {
            hard_state: HardState {
                current_term: Term::new(5),
                voted_for: Some(node(2)),
                durable_commit_index: LogIndex::ZERO,
            },
            snapshot,
            log: Vec::new(),
            effective_configuration: EffectiveConfiguration::Stable(config),
            pending_configuration_index: None,
        };
        let mut core = RaftCore::from_recovered(node(1), recovered, timing(), 5).unwrap();

        core.handle_input(Input::Step(RaftMessage::RequestVoteRequest(
            RequestVoteRequest {
                term: Term::new(5),
                candidate_id: node(3),
                last_log_index: LogIndex::ZERO,
                last_log_term: Term::ZERO,
            },
        )))
        .unwrap();
        assert_eq!(
            core.drain_effects().collect::<Vec<_>>(),
            vec![Effect::Send {
                target: node(3),
                message: RaftMessage::RequestVoteResponse(RequestVoteResponse {
                    term: Term::new(5),
                    voter_id: node(1),
                    vote_granted: false,
                })
            }]
        );

        core.handle_input(Input::Step(RaftMessage::RequestVoteRequest(
            RequestVoteRequest {
                term: Term::new(5),
                candidate_id: node(2),
                last_log_index: LogIndex::ZERO,
                last_log_term: Term::ZERO,
            },
        )))
        .unwrap();
        assert_eq!(
            core.drain_effects().collect::<Vec<_>>(),
            vec![Effect::Send {
                target: node(2),
                message: RaftMessage::RequestVoteResponse(RequestVoteResponse {
                    term: Term::new(5),
                    voter_id: node(1),
                    vote_granted: true,
                })
            }]
        );
        assert_eq!(core.hard_state().voted_for, Some(node(2)));
    }

    #[test]
    fn crash_before_vote_persistence_cannot_create_two_acknowledged_votes() {
        let config = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], []).unwrap(),
        );
        let recovered = recovered_with(config.clone(), Vec::new(), Term::new(6));
        let mut first = RaftCore::from_recovered(node(1), recovered.clone(), timing(), 5).unwrap();

        first
            .handle_input(Input::Step(RaftMessage::RequestVoteRequest(
                RequestVoteRequest {
                    term: Term::new(6),
                    candidate_id: node(2),
                    last_log_index: LogIndex::ZERO,
                    last_log_term: Term::ZERO,
                },
            )))
            .unwrap();
        let effects = first.drain_effects().collect::<Vec<_>>();
        assert_eq!(effects.len(), 1);
        assert!(matches!(effects[0], Effect::Persist(_)));
        assert_eq!(first.hard_state().voted_for, None);

        let mut restarted = RaftCore::from_recovered(node(1), recovered, timing(), 5).unwrap();
        restarted
            .handle_input(Input::Step(RaftMessage::RequestVoteRequest(
                RequestVoteRequest {
                    term: Term::new(6),
                    candidate_id: node(3),
                    last_log_index: LogIndex::ZERO,
                    last_log_term: Term::ZERO,
                },
            )))
            .unwrap();
        let restarted_effects = restarted.drain_effects().collect::<Vec<_>>();
        assert_eq!(restarted_effects.len(), 1);
        let (_, hard_state) = staged_hard_state(&restarted_effects[0]);
        assert_eq!(hard_state.voted_for, Some(node(3)));
    }

    #[test]
    fn higher_term_stale_candidate_is_fenced_and_rejected_only_after_term_persistence() {
        let config = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], []).unwrap(),
        );
        let local_log = vec![
            LogEntry::new(Term::new(3), LogIndex::new(1), EntryKind::LeaderBarrier),
            LogEntry::new(Term::new(4), LogIndex::new(2), EntryKind::LeaderBarrier),
        ];
        let mut core = RaftCore::from_recovered(
            node(1),
            recovered_with(config, local_log, Term::new(4)),
            timing(),
            5,
        )
        .unwrap();

        core.handle_input(Input::Step(RaftMessage::RequestVoteRequest(
            RequestVoteRequest {
                term: Term::new(5),
                candidate_id: node(2),
                last_log_index: LogIndex::new(100),
                last_log_term: Term::new(3),
            },
        )))
        .unwrap();
        let effects = core.drain_effects().collect::<Vec<_>>();
        let (token, hard_state) = staged_hard_state(&effects[0]);
        assert_eq!(hard_state.current_term, Term::new(5));
        assert_eq!(hard_state.voted_for, None);
        assert_eq!(core.hard_state().current_term, Term::new(4));

        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        assert_eq!(core.hard_state().current_term, Term::new(5));
        assert_eq!(core.volatile_state().role, Role::Follower);
        assert_eq!(
            core.drain_effects().collect::<Vec<_>>(),
            vec![
                Effect::RoleChanged {
                    role: Role::Follower,
                    term: Term::new(5),
                    leader: None,
                },
                Effect::Send {
                    target: node(2),
                    message: RaftMessage::RequestVoteResponse(RequestVoteResponse {
                        term: Term::new(5),
                        voter_id: node(1),
                        vote_granted: false,
                    }),
                },
            ]
        );
    }

    #[test]
    fn higher_term_vote_response_fences_candidate_after_durable_term_update() {
        let mut core = core(1);
        let token = begin_three_voter_campaign(&mut core, Term::new(1));
        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        let _ = core.drain_effects().collect::<Vec<_>>();
        assert_eq!(core.volatile_state().role, Role::Candidate);

        core.handle_input(Input::Step(RaftMessage::RequestVoteResponse(
            RequestVoteResponse {
                term: Term::new(2),
                voter_id: node(2),
                vote_granted: false,
            },
        )))
        .unwrap();
        let persist = core.drain_effects().collect::<Vec<_>>();
        assert_eq!(persist.len(), 1);
        let (fence_token, hard_state) = staged_hard_state(&persist[0]);
        assert_eq!(hard_state.current_term, Term::new(2));
        assert_eq!(hard_state.voted_for, None);
        assert_eq!(core.volatile_state().role, Role::Candidate);

        core.handle_input(Input::PersistenceCompleted(fence_token))
            .unwrap();
        assert_eq!(core.hard_state().current_term, Term::new(2));
        assert_eq!(core.hard_state().voted_for, None);
        assert_eq!(core.volatile_state().role, Role::Follower);
        assert!(core.volatile_state().election.is_none());
        assert_eq!(
            core.drain_effects().collect::<Vec<_>>(),
            vec![Effect::RoleChanged {
                role: Role::Follower,
                term: Term::new(2),
                leader: None,
            }]
        );
    }

    #[test]
    fn election_quorum_is_reported_once_without_leader_transition() {
        let mut core = core(1);
        let token = begin_three_voter_campaign(&mut core, Term::new(1));
        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        let _ = core.drain_effects().collect::<Vec<_>>();

        core.handle_input(Input::Step(RaftMessage::RequestVoteResponse(
            RequestVoteResponse {
                term: Term::new(1),
                voter_id: node(2),
                vote_granted: true,
            },
        )))
        .unwrap();
        assert_eq!(
            core.drain_effects().collect::<Vec<_>>(),
            vec![Effect::ElectionQuorumReached { term: Term::new(1) }]
        );
        assert_eq!(core.volatile_state().role, Role::Candidate);

        for voter in [2, 3] {
            core.handle_input(Input::Step(RaftMessage::RequestVoteResponse(
                RequestVoteResponse {
                    term: Term::new(1),
                    voter_id: node(voter),
                    vote_granted: true,
                },
            )))
            .unwrap();
        }
        assert!(core.drain_effects().next().is_none());
        assert_eq!(core.volatile_state().role, Role::Candidate);
        assert_ne!(core.volatile_state().role, Role::Leader);
    }

    #[test]
    fn leader_transition_requires_matching_qualified_current_term_quorum() {
        let mut follower = core(1);
        assert_eq!(
            follower.handle_input(Input::WinElection { term: Term::new(1) }),
            Err(RaftError::InvalidLeaderTransition)
        );

        let mut candidate = core(1);
        let token = begin_three_voter_campaign(&mut candidate, Term::new(1));
        candidate
            .handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        let _ = candidate.drain_effects().collect::<Vec<_>>();
        assert_eq!(candidate.volatile_state().role, Role::Candidate);
        assert_eq!(
            candidate.handle_input(Input::WinElection { term: Term::new(1) }),
            Err(RaftError::InvalidLeaderTransition)
        );

        candidate
            .handle_input(Input::Step(RaftMessage::RequestVoteResponse(
                RequestVoteResponse {
                    term: Term::new(1),
                    voter_id: node(2),
                    vote_granted: true,
                },
            )))
            .unwrap();
        let _ = candidate.drain_effects().collect::<Vec<_>>();
        assert_eq!(
            candidate.handle_input(Input::WinElection { term: Term::new(2) }),
            Err(RaftError::InvalidLeaderTransition)
        );
    }

    #[test]
    fn leader_initialization_uses_pre_barrier_durable_tail_and_stages_only_local_barrier() {
        let config = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], []).unwrap(),
        );
        let log = vec![
            LogEntry::new(
                Term::new(1),
                LogIndex::new(1),
                EntryKind::Application(vec![1]),
            ),
            LogEntry::new(
                Term::new(2),
                LogIndex::new(2),
                EntryKind::Application(vec![2]),
            ),
        ];
        let mut core = RaftCore::from_recovered(
            node(1),
            recovered_with(config, log, Term::new(2)),
            timing(),
            5,
        )
        .unwrap();

        reach_three_voter_election_quorum(&mut core, Term::new(3));
        assert_eq!(core.stable_log().len(), 2);
        assert_eq!(core.volatile_state().role, Role::Candidate);

        core.handle_input(Input::WinElection { term: Term::new(3) })
            .unwrap();

        assert_eq!(core.volatile_state().role, Role::Leader);
        assert_eq!(core.volatile_state().leader_id, Some(node(1)));
        assert!(core.volatile_state().election.is_none());
        assert!(core.volatile_state().pre_vote.is_none());
        assert_eq!(core.volatile_state().commit_index, LogIndex::ZERO);
        assert_eq!(core.stable_log().len(), 2);

        let progress = &core.volatile_state().progress;
        assert_eq!(
            progress.keys().map(|id| id.get()).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(progress[&node(1)].match_index, LogIndex::new(2));
        assert_eq!(progress[&node(1)].next_index, LogIndex::new(3));
        assert!(progress[&node(1)].recently_active);
        assert_eq!(progress[&node(1)].mode, ReplicationMode::Replicate);
        for peer in [node(2), node(3)] {
            assert_eq!(progress[&peer].match_index, LogIndex::ZERO);
            assert_eq!(progress[&peer].next_index, LogIndex::new(3));
            assert!(!progress[&peer].recently_active);
            assert_eq!(progress[&peer].mode, ReplicationMode::Probe);
        }

        let effects = core.drain_effects().collect::<Vec<_>>();
        assert_eq!(effects.len(), 2);
        assert_eq!(
            effects[0],
            Effect::RoleChanged {
                role: Role::Leader,
                term: Term::new(3),
                leader: Some(node(1)),
            }
        );
        let (token, barrier) = staged_single_append(&effects[1]);
        assert_eq!(barrier.term, Term::new(3));
        assert_eq!(barrier.index, LogIndex::new(3));
        assert!(matches!(barrier.kind, EntryKind::LeaderBarrier));
        assert!(
            !effects
                .iter()
                .any(|effect| matches!(effect, Effect::Send { .. }))
        );
        assert_eq!(core.in_flight_persistence(), Some(token));
    }

    #[test]
    fn self_match_advances_to_leader_barrier_only_after_barrier_persistence() {
        let mut core = core(1);
        reach_three_voter_election_quorum(&mut core, Term::new(1));
        core.handle_input(Input::WinElection { term: Term::new(1) })
            .unwrap();
        let effects = core.drain_effects().collect::<Vec<_>>();
        let (token, barrier) = staged_single_append(&effects[1]);

        assert_eq!(core.stable_log().len(), 0);
        assert_eq!(
            core.volatile_state().progress[&node(1)].match_index,
            LogIndex::ZERO
        );
        assert_eq!(
            core.volatile_state().progress[&node(1)].next_index,
            LogIndex::FIRST
        );

        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        assert_eq!(core.stable_log(), std::slice::from_ref(&barrier));
        assert_eq!(
            core.volatile_state().progress[&node(1)].match_index,
            LogIndex::FIRST
        );
        assert_eq!(
            core.volatile_state().progress[&node(1)].next_index,
            LogIndex::new(2)
        );
        assert_eq!(core.volatile_state().commit_index, LogIndex::ZERO);
        let replication = core.drain_effects().collect::<Vec<_>>();
        assert_eq!(replication.len(), 2);
        assert_eq!(
            replication
                .iter()
                .filter_map(|effect| match effect {
                    Effect::Send {
                        target,
                        message: RaftMessage::AppendEntriesRequest(request),
                    } => {
                        assert_eq!(request.entries, vec![barrier.clone()]);
                        Some(target.get())
                    }
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
    }

    #[test]
    fn leader_progress_includes_learners_without_giving_them_authority() {
        let config = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], [node(4)]).unwrap(),
        );
        let mut core = RaftCore::from_recovered(
            node(1),
            recovered_with(config, Vec::new(), Term::ZERO),
            timing(),
            5,
        )
        .unwrap();

        reach_three_voter_election_quorum(&mut core, Term::new(1));
        core.handle_input(Input::WinElection { term: Term::new(1) })
            .unwrap();
        let progress = &core.volatile_state().progress;
        assert_eq!(
            progress.keys().map(|id| id.get()).collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        assert_eq!(progress[&node(4)].match_index, LogIndex::ZERO);
        assert_eq!(progress[&node(4)].next_index, LogIndex::FIRST);
        assert_eq!(progress[&node(4)].mode, ReplicationMode::Probe);
        assert!(core.effective_configuration().contains_learner(node(4)));
        assert!(!core.effective_configuration().contains_voter(node(4)));
    }

    #[test]
    fn barrier_persistence_failure_is_fatal_and_never_advances_self_match() {
        let mut core = core(1);
        reach_three_voter_election_quorum(&mut core, Term::new(1));
        core.handle_input(Input::WinElection { term: Term::new(1) })
            .unwrap();
        let effects = core.drain_effects().collect::<Vec<_>>();
        let (token, _) = staged_single_append(&effects[1]);

        core.handle_input(Input::PersistenceFailed {
            token,
            error: PersistenceFailure::new("simulated barrier fsync failure"),
        })
        .unwrap();
        assert!(core.is_stopped());
        assert!(core.stable_log().is_empty());
        assert_eq!(
            core.volatile_state().progress[&node(1)].match_index,
            LogIndex::ZERO
        );
        assert_eq!(
            core.drain_effects().collect::<Vec<_>>(),
            vec![Effect::Fatal("simulated barrier fsync failure".into())]
        );
    }

    #[test]
    fn leader_barrier_index_follows_snapshot_boundary_when_live_suffix_is_empty() {
        let config = StableConfiguration::new([node(1), node(2), node(3)], []).unwrap();
        let snapshot = SnapshotMetadata::new(
            LogIndex::new(8),
            Term::new(4),
            config.clone(),
            SnapshotId::new([8; 16]),
        )
        .unwrap();
        let recovered = RecoveredState {
            hard_state: HardState {
                current_term: Term::new(5),
                voted_for: None,
                durable_commit_index: LogIndex::new(8),
            },
            snapshot,
            log: Vec::new(),
            effective_configuration: EffectiveConfiguration::Stable(config),
            pending_configuration_index: None,
        };
        let mut core = RaftCore::from_recovered(node(1), recovered, timing(), 5).unwrap();

        reach_three_voter_election_quorum(&mut core, Term::new(6));
        core.handle_input(Input::WinElection { term: Term::new(6) })
            .unwrap();
        let effects = core.drain_effects().collect::<Vec<_>>();
        let (token, barrier) = staged_single_append(&effects[1]);
        assert_eq!(barrier.index, LogIndex::new(9));
        assert_eq!(
            core.volatile_state().progress[&node(1)].match_index,
            LogIndex::new(8)
        );
        assert_eq!(
            core.volatile_state().progress[&node(1)].next_index,
            LogIndex::new(9)
        );

        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        assert_eq!(
            core.volatile_state().progress[&node(1)].match_index,
            LogIndex::new(9)
        );
        assert_eq!(
            core.volatile_state().progress[&node(1)].next_index,
            LogIndex::new(10)
        );
        assert_eq!(core.volatile_state().commit_index, LogIndex::new(8));
    }

    #[test]
    fn joint_election_quorum_requires_old_and_new_majorities() {
        let old = StableConfiguration::new([node(1), node(2), node(3)], []).unwrap();
        let new = StableConfiguration::new([node(3), node(4), node(5)], []).unwrap();
        let config = EffectiveConfiguration::Joint(JointConfiguration::new(old, new).unwrap());
        let mut core = RaftCore::from_recovered(
            node(1),
            recovered_with(config, Vec::new(), Term::ZERO),
            timing(),
            5,
        )
        .unwrap();

        tick(&mut core, 5);
        let _ = core.drain_effects().collect::<Vec<_>>();
        for voter in [2, 3, 4] {
            core.handle_input(Input::Step(RaftMessage::PreVoteResponse(PreVoteResponse {
                prospective_term: Term::new(1),
                voter_id: node(voter),
                vote_granted: true,
            })))
            .unwrap();
        }
        let _ = core.drain_effects().collect::<Vec<_>>();
        core.handle_input(Input::BeginElection {
            prospective_term: Term::new(1),
        })
        .unwrap();
        let persist = core.drain_effects().collect::<Vec<_>>();
        let (token, _) = staged_hard_state(&persist[0]);
        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        let _ = core.drain_effects().collect::<Vec<_>>();

        for voter in [2, 4] {
            core.handle_input(Input::Step(RaftMessage::RequestVoteResponse(
                RequestVoteResponse {
                    term: Term::new(1),
                    voter_id: node(voter),
                    vote_granted: true,
                },
            )))
            .unwrap();
        }
        assert!(core.drain_effects().next().is_none());

        core.handle_input(Input::Step(RaftMessage::RequestVoteResponse(
            RequestVoteResponse {
                term: Term::new(1),
                voter_id: node(3),
                vote_granted: true,
            },
        )))
        .unwrap();
        assert_eq!(
            core.drain_effects().collect::<Vec<_>>(),
            vec![Effect::ElectionQuorumReached { term: Term::new(1) }]
        );
        assert_eq!(core.volatile_state().role, Role::Candidate);
    }

    #[test]
    fn foreign_request_vote_identity_cannot_raise_term() {
        let config = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], []).unwrap(),
        );
        let mut core = RaftCore::from_recovered(
            node(1),
            recovered_with(config, Vec::new(), Term::new(4)),
            timing(),
            5,
        )
        .unwrap();

        core.handle_input(Input::Step(RaftMessage::RequestVoteRequest(
            RequestVoteRequest {
                term: Term::new(99),
                candidate_id: node(9),
                last_log_index: LogIndex::ZERO,
                last_log_term: Term::ZERO,
            },
        )))
        .unwrap();
        assert_eq!(core.hard_state().current_term, Term::new(4));
        assert_eq!(core.in_flight_persistence(), None);
        assert_eq!(
            core.drain_effects().collect::<Vec<_>>(),
            vec![Effect::Send {
                target: node(9),
                message: RaftMessage::RequestVoteResponse(RequestVoteResponse {
                    term: Term::new(4),
                    voter_id: node(1),
                    vote_granted: false,
                })
            }]
        );
    }

    #[test]
    fn learner_persists_higher_term_but_never_grants_vote() {
        let config = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], [node(4)]).unwrap(),
        );
        let mut core = RaftCore::from_recovered(
            node(4),
            recovered_with(config, Vec::new(), Term::new(2)),
            timing(),
            5,
        )
        .unwrap();

        core.handle_input(Input::Step(RaftMessage::RequestVoteRequest(
            RequestVoteRequest {
                term: Term::new(3),
                candidate_id: node(1),
                last_log_index: LogIndex::ZERO,
                last_log_term: Term::ZERO,
            },
        )))
        .unwrap();
        let persist = core.drain_effects().collect::<Vec<_>>();
        let (token, hard_state) = staged_hard_state(&persist[0]);
        assert_eq!(hard_state.current_term, Term::new(3));
        assert_eq!(hard_state.voted_for, None);

        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        assert_eq!(core.volatile_state().role, Role::Learner);
        let effects = core.drain_effects().collect::<Vec<_>>();
        assert!(matches!(
            effects.as_slice(),
            [
                Effect::RoleChanged {
                    role: Role::Learner,
                    term,
                    leader: None
                },
                Effect::Send {
                    target,
                    message: RaftMessage::RequestVoteResponse(RequestVoteResponse {
                        term: response_term,
                        vote_granted: false,
                        ..
                    })
                }
            ] if *term == Term::new(3)
                && *target == node(1)
                && *response_term == Term::new(3)
        ));
    }

    #[test]
    fn higher_term_grant_persists_new_term_and_vote_atomically_before_response() {
        let config = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], []).unwrap(),
        );
        let mut core = RaftCore::from_recovered(
            node(1),
            recovered_with(config, Vec::new(), Term::new(7)),
            timing(),
            5,
        )
        .unwrap();

        core.handle_input(Input::Step(RaftMessage::RequestVoteRequest(
            RequestVoteRequest {
                term: Term::new(8),
                candidate_id: node(2),
                last_log_index: LogIndex::ZERO,
                last_log_term: Term::ZERO,
            },
        )))
        .unwrap();
        let effects = core.drain_effects().collect::<Vec<_>>();
        let (token, staged) = staged_hard_state(&effects[0]);
        assert_eq!(staged.current_term, Term::new(8));
        assert_eq!(staged.voted_for, Some(node(2)));
        assert_eq!(core.hard_state().current_term, Term::new(7));
        assert_eq!(core.hard_state().voted_for, None);

        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        assert_eq!(core.hard_state().current_term, Term::new(8));
        assert_eq!(core.hard_state().voted_for, Some(node(2)));
        assert_eq!(
            core.drain_effects().collect::<Vec<_>>(),
            vec![
                Effect::RoleChanged {
                    role: Role::Follower,
                    term: Term::new(8),
                    leader: None,
                },
                Effect::Send {
                    target: node(2),
                    message: RaftMessage::RequestVoteResponse(RequestVoteResponse {
                        term: Term::new(8),
                        voter_id: node(1),
                        vote_granted: true,
                    }),
                },
            ]
        );
    }

    #[test]
    fn campaign_persistence_failure_is_fatal_and_sends_no_request_vote() {
        let mut core = core(1);
        let token = begin_three_voter_campaign(&mut core, Term::new(1));
        assert_eq!(core.volatile_state().role, Role::PreCandidate);

        core.handle_input(Input::PersistenceFailed {
            token,
            error: PersistenceFailure::new("simulated hard-state fsync failure"),
        })
        .unwrap();
        assert!(core.is_stopped());
        assert_eq!(core.hard_state().current_term, Term::ZERO);
        assert_eq!(core.hard_state().voted_for, None);
        assert_eq!(
            core.drain_effects().collect::<Vec<_>>(),
            vec![Effect::Fatal("simulated hard-state fsync failure".into())]
        );
    }

    #[test]
    fn foreign_higher_term_vote_response_cannot_fence_candidate() {
        let mut core = core(1);
        let token = begin_three_voter_campaign(&mut core, Term::new(1));
        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        let _ = core.drain_effects().collect::<Vec<_>>();

        core.handle_input(Input::Step(RaftMessage::RequestVoteResponse(
            RequestVoteResponse {
                term: Term::new(99),
                voter_id: node(9),
                vote_granted: false,
            },
        )))
        .unwrap();
        assert_eq!(core.hard_state().current_term, Term::new(1));
        assert_eq!(core.volatile_state().role, Role::Candidate);
        assert_eq!(core.in_flight_persistence(), None);
        assert!(core.drain_effects().next().is_none());
    }

    #[test]
    fn pre_vote_grant_checks_term_membership_and_log_freshness_without_persistence() {
        let local_log = vec![
            LogEntry::new(Term::new(1), LogIndex::new(1), EntryKind::LeaderBarrier),
            LogEntry::new(Term::new(2), LogIndex::new(2), EntryKind::LeaderBarrier),
        ];
        let config = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], []).unwrap(),
        );
        let mut core = RaftCore::from_recovered(
            node(2),
            recovered_with(config, local_log, Term::new(2)),
            timing(),
            5,
        )
        .unwrap();
        let hard_before = core.hard_state().clone();

        for (request, expected) in [
            (
                PreVoteRequest {
                    prospective_term: Term::new(3),
                    candidate_id: node(1),
                    last_log_index: LogIndex::new(99),
                    last_log_term: Term::new(1),
                },
                false,
            ),
            (
                PreVoteRequest {
                    prospective_term: Term::new(3),
                    candidate_id: node(1),
                    last_log_index: LogIndex::new(2),
                    last_log_term: Term::new(2),
                },
                true,
            ),
            (
                PreVoteRequest {
                    prospective_term: Term::new(2),
                    candidate_id: node(1),
                    last_log_index: LogIndex::new(2),
                    last_log_term: Term::new(2),
                },
                false,
            ),
            (
                PreVoteRequest {
                    prospective_term: Term::new(3),
                    candidate_id: node(9),
                    last_log_index: LogIndex::new(2),
                    last_log_term: Term::new(2),
                },
                false,
            ),
        ] {
            core.handle_input(Input::Step(RaftMessage::PreVoteRequest(request)))
                .unwrap();
            assert_eq!(pre_vote_response(&mut core).vote_granted, expected);
        }
        assert_eq!(core.hard_state(), &hard_before);
    }

    #[test]
    fn recent_valid_leader_contact_suppresses_pre_vote_grant() {
        let config = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], []).unwrap(),
        );
        let mut core = RaftCore::from_recovered(
            node(2),
            recovered_with(config, Vec::new(), Term::new(1)),
            timing(),
            5,
        )
        .unwrap();
        let hard_before = core.hard_state().clone();
        core.observe_current_term_leader_contact(node(3), Term::new(1))
            .unwrap();

        core.handle_input(Input::Step(RaftMessage::PreVoteRequest(PreVoteRequest {
            prospective_term: Term::new(2),
            candidate_id: node(1),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::ZERO,
        })))
        .unwrap();
        assert!(!pre_vote_response(&mut core).vote_granted);
        assert_eq!(core.hard_state(), &hard_before);
    }

    #[test]
    fn valid_leader_contact_cancels_active_pre_vote_round() {
        let config = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], []).unwrap(),
        );
        let mut core = RaftCore::from_recovered(
            node(1),
            recovered_with(config, Vec::new(), Term::new(1)),
            timing(),
            5,
        )
        .unwrap();
        tick(&mut core, 5);
        let _ = core.drain_effects().collect::<Vec<_>>();
        assert_eq!(core.volatile_state().role, Role::PreCandidate);
        assert!(core.volatile_state().pre_vote.is_some());

        core.observe_current_term_leader_contact(node(2), Term::new(1))
            .unwrap();
        assert_eq!(core.volatile_state().role, Role::Follower);
        assert_eq!(core.volatile_state().leader_id, Some(node(2)));
        assert_eq!(core.volatile_state().election_elapsed, 0);
        assert!(core.volatile_state().pre_vote.is_none());
        assert_eq!(
            core.drain_effects().collect::<Vec<_>>(),
            vec![Effect::RoleChanged {
                role: Role::Follower,
                term: Term::new(1),
                leader: Some(node(2)),
            }]
        );
    }

    #[test]
    fn mismatched_or_synthetic_leader_contact_cannot_suppress_pre_vote() {
        let mut zero_term = core(2);
        zero_term
            .observe_current_term_leader_contact(node(3), Term::ZERO)
            .unwrap();
        assert_eq!(zero_term.volatile_state().leader_id, None);

        let config = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], []).unwrap(),
        );
        let mut term_two = RaftCore::from_recovered(
            node(2),
            recovered_with(config, Vec::new(), Term::new(2)),
            timing(),
            5,
        )
        .unwrap();
        term_two
            .observe_current_term_leader_contact(node(3), Term::new(1))
            .unwrap();
        assert_eq!(term_two.volatile_state().leader_id, None);
        term_two
            .observe_current_term_leader_contact(node(3), Term::new(3))
            .unwrap();
        assert_eq!(term_two.volatile_state().leader_id, None);
    }

    #[test]
    fn snapshot_boundary_participates_in_pre_vote_log_freshness() {
        let config = StableConfiguration::new([node(1), node(2), node(3)], []).unwrap();
        let snapshot = SnapshotMetadata::new(
            LogIndex::new(8),
            Term::new(4),
            config.clone(),
            SnapshotId::new([9; 16]),
        )
        .unwrap();
        let recovered = RecoveredState {
            hard_state: HardState {
                current_term: Term::new(5),
                voted_for: None,
                durable_commit_index: LogIndex::new(8),
            },
            snapshot,
            log: Vec::new(),
            effective_configuration: EffectiveConfiguration::Stable(config),
            pending_configuration_index: None,
        };
        let mut core = RaftCore::from_recovered(node(2), recovered, timing(), 5).unwrap();

        for (last_log_term, last_log_index, expected) in [
            (Term::new(3), LogIndex::new(100), false),
            (Term::new(4), LogIndex::new(7), false),
            (Term::new(4), LogIndex::new(8), true),
            (Term::new(5), LogIndex::new(1), true),
        ] {
            core.handle_input(Input::Step(RaftMessage::PreVoteRequest(PreVoteRequest {
                prospective_term: Term::new(6),
                candidate_id: node(1),
                last_log_index,
                last_log_term,
            })))
            .unwrap();
            assert_eq!(pre_vote_response(&mut core).vote_granted, expected);
        }
    }

    #[test]
    fn learner_neither_campaigns_nor_grants_pre_vote() {
        let config = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], [node(4)]).unwrap(),
        );
        let mut core = RaftCore::from_recovered(
            node(4),
            recovered_with(config, Vec::new(), Term::ZERO),
            timing(),
            5,
        )
        .unwrap();

        tick(&mut core, 20);
        assert_eq!(core.volatile_state().role, Role::Learner);
        assert!(core.drain_effects().next().is_none());

        core.handle_input(Input::Step(RaftMessage::PreVoteRequest(PreVoteRequest {
            prospective_term: Term::new(1),
            candidate_id: node(1),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::ZERO,
        })))
        .unwrap();
        assert!(!pre_vote_response(&mut core).vote_granted);
    }

    #[test]
    fn stable_quorum_counts_each_voter_once_and_emits_once() {
        let mut core = core(1);
        tick(&mut core, 5);
        let _ = core.drain_effects().collect::<Vec<_>>();
        let hard_before = core.hard_state().clone();

        core.handle_input(Input::Step(RaftMessage::PreVoteResponse(PreVoteResponse {
            prospective_term: Term::new(1),
            voter_id: node(2),
            vote_granted: true,
        })))
        .unwrap();
        assert_eq!(
            core.drain_effects().collect::<Vec<_>>(),
            vec![Effect::PreVoteQuorumReached {
                prospective_term: Term::new(1)
            }]
        );

        for voter in [2, 3] {
            core.handle_input(Input::Step(RaftMessage::PreVoteResponse(PreVoteResponse {
                prospective_term: Term::new(1),
                voter_id: node(voter),
                vote_granted: true,
            })))
            .unwrap();
        }
        assert!(core.drain_effects().next().is_none());
        assert_eq!(core.hard_state(), &hard_before);
        assert_eq!(core.volatile_state().role, Role::PreCandidate);
    }

    #[test]
    fn joint_pre_vote_requires_majority_of_old_and_new_voters() {
        let old = StableConfiguration::new([node(1), node(2), node(3)], []).unwrap();
        let new = StableConfiguration::new([node(3), node(4), node(5)], []).unwrap();
        let config = EffectiveConfiguration::Joint(JointConfiguration::new(old, new).unwrap());
        let mut core = RaftCore::from_recovered(
            node(1),
            recovered_with(config, Vec::new(), Term::ZERO),
            timing(),
            5,
        )
        .unwrap();
        tick(&mut core, 5);
        let _ = core.drain_effects().collect::<Vec<_>>();

        for voter in [2, 4] {
            core.handle_input(Input::Step(RaftMessage::PreVoteResponse(PreVoteResponse {
                prospective_term: Term::new(1),
                voter_id: node(voter),
                vote_granted: true,
            })))
            .unwrap();
        }
        assert!(core.drain_effects().next().is_none());

        core.handle_input(Input::Step(RaftMessage::PreVoteResponse(PreVoteResponse {
            prospective_term: Term::new(1),
            voter_id: node(3),
            vote_granted: true,
        })))
        .unwrap();
        assert_eq!(
            core.drain_effects().collect::<Vec<_>>(),
            vec![Effect::PreVoteQuorumReached {
                prospective_term: Term::new(1)
            }]
        );
    }

    #[test]
    fn stale_and_foreign_pre_vote_responses_do_not_advance_round() {
        let mut core = core(1);
        tick(&mut core, 5);
        let _ = core.drain_effects().collect::<Vec<_>>();

        for response in [
            PreVoteResponse {
                prospective_term: Term::new(2),
                voter_id: node(2),
                vote_granted: true,
            },
            PreVoteResponse {
                prospective_term: Term::new(1),
                voter_id: node(9),
                vote_granted: true,
            },
        ] {
            core.handle_input(Input::Step(RaftMessage::PreVoteResponse(response)))
                .unwrap();
        }
        assert!(core.drain_effects().next().is_none());
        assert_eq!(
            core.volatile_state()
                .pre_vote
                .as_ref()
                .unwrap()
                .responses
                .len(),
            1
        );
    }

    #[test]
    fn pre_candidate_timeout_restarts_round_with_injected_timeout_sample() {
        let mut core = core(1);
        tick(&mut core, 5);
        let _ = core.drain_effects().collect::<Vec<_>>();
        core.handle_input(Input::Step(RaftMessage::PreVoteResponse(PreVoteResponse {
            prospective_term: Term::new(1),
            voter_id: node(2),
            vote_granted: false,
        })))
        .unwrap();
        assert_eq!(
            core.volatile_state()
                .pre_vote
                .as_ref()
                .unwrap()
                .responses
                .len(),
            2
        );

        for _ in 0..6 {
            core.handle_input(Input::Tick {
                reset_election_timeout: 7,
            })
            .unwrap();
        }
        assert_eq!(core.volatile_state().randomized_election_timeout, 7);
        assert_eq!(
            core.volatile_state()
                .pre_vote
                .as_ref()
                .unwrap()
                .responses
                .len(),
            1
        );
        assert_eq!(core.hard_state().current_term, Term::ZERO);
    }

    #[test]
    fn invalid_timeout_sample_is_rejected_before_state_change() {
        let mut core = core(1);
        assert_eq!(
            core.handle_input(Input::Tick {
                reset_election_timeout: 4,
            }),
            Err(RaftError::InvalidElectionTimeoutSample)
        );
        assert_eq!(core.volatile_state().election_elapsed, 0);
    }

    #[test]
    fn stale_append_entries_heartbeat_rejects_with_current_term_without_authority_change() {
        let config = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], []).unwrap(),
        );
        let mut core = RaftCore::from_recovered(
            node(1),
            recovered_with(config, Vec::new(), Term::new(3)),
            timing(),
            5,
        )
        .unwrap();

        core.handle_input(Input::Step(RaftMessage::AppendEntriesRequest(heartbeat(
            Term::new(2),
            node(2),
            LogIndex::ZERO,
            Term::ZERO,
            LogIndex::ZERO,
        ))))
        .unwrap();

        assert_eq!(core.hard_state().current_term, Term::new(3));
        assert_eq!(core.volatile_state().leader_id, None);
        let response = append_entries_response(&mut core);
        assert_eq!(response.term, Term::new(3));
        assert!(!response.success);
        assert_eq!(response.conflict_term, None);
        assert_eq!(response.conflict_index, LogIndex::FIRST);
    }

    #[test]
    fn current_term_heartbeat_steps_candidate_down_and_establishes_leader_authority() {
        let mut core = core(1);
        let token = begin_three_voter_campaign(&mut core, Term::new(1));
        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        let _ = core.drain_effects().collect::<Vec<_>>();
        assert_eq!(core.volatile_state().role, Role::Candidate);

        core.handle_input(Input::Step(RaftMessage::AppendEntriesRequest(heartbeat(
            Term::new(1),
            node(2),
            LogIndex::ZERO,
            Term::ZERO,
            LogIndex::ZERO,
        ))))
        .unwrap();

        assert_eq!(core.volatile_state().role, Role::Follower);
        assert_eq!(core.volatile_state().leader_id, Some(node(2)));
        assert_eq!(core.volatile_state().election_elapsed, 0);
        assert!(core.volatile_state().election.is_none());
        let effects = core.drain_effects().collect::<Vec<_>>();
        assert_eq!(effects.len(), 2);
        assert_eq!(
            effects[0],
            Effect::RoleChanged {
                role: Role::Follower,
                term: Term::new(1),
                leader: Some(node(2)),
            }
        );
        assert!(matches!(
            effects[1],
            Effect::Send {
                target,
                message: RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
                    term,
                    success: true,
                    match_index,
                    ..
                })
            } if target == node(2) && term == Term::new(1) && match_index == LogIndex::ZERO
        ));
    }

    #[test]
    fn same_term_competing_leader_is_rejected_without_replacing_authority() {
        let config = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], []).unwrap(),
        );
        let mut core = RaftCore::from_recovered(
            node(1),
            recovered_with(config, Vec::new(), Term::new(2)),
            timing(),
            5,
        )
        .unwrap();

        core.handle_input(Input::Step(RaftMessage::AppendEntriesRequest(heartbeat(
            Term::new(2),
            node(2),
            LogIndex::ZERO,
            Term::ZERO,
            LogIndex::ZERO,
        ))))
        .unwrap();
        assert!(append_entries_response(&mut core).success);
        assert_eq!(core.volatile_state().leader_id, Some(node(2)));

        tick(&mut core, 2);
        let elapsed_before = core.volatile_state().election_elapsed;
        core.handle_input(Input::Step(RaftMessage::AppendEntriesRequest(heartbeat(
            Term::new(2),
            node(3),
            LogIndex::ZERO,
            Term::ZERO,
            LogIndex::ZERO,
        ))))
        .unwrap();

        let response = append_entries_response(&mut core);
        assert!(!response.success);
        assert_eq!(core.volatile_state().leader_id, Some(node(2)));
        assert_eq!(core.volatile_state().election_elapsed, elapsed_before);
    }

    #[test]
    fn higher_term_heartbeat_persists_term_before_response_and_clears_leader_progress() {
        let mut core = core(1);
        reach_three_voter_election_quorum(&mut core, Term::new(1));
        core.handle_input(Input::WinElection { term: Term::new(1) })
            .unwrap();
        let leader_effects = core.drain_effects().collect::<Vec<_>>();
        let (barrier_token, barrier) = staged_single_append(&leader_effects[1]);
        core.handle_input(Input::PersistenceCompleted(barrier_token))
            .unwrap();
        let _ = core.drain_effects().collect::<Vec<_>>();
        assert_eq!(core.volatile_state().role, Role::Leader);
        assert!(!core.volatile_state().progress.is_empty());

        core.handle_input(Input::Step(RaftMessage::AppendEntriesRequest(heartbeat(
            Term::new(2),
            node(2),
            barrier.index,
            barrier.term,
            LogIndex::ZERO,
        ))))
        .unwrap();
        let persist = core.drain_effects().collect::<Vec<_>>();
        assert_eq!(persist.len(), 1);
        let (token, hard_state) = staged_hard_state(&persist[0]);
        assert_eq!(hard_state.current_term, Term::new(2));
        assert_eq!(hard_state.voted_for, None);
        assert_eq!(core.hard_state().current_term, Term::new(1));

        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        assert_eq!(core.hard_state().current_term, Term::new(2));
        assert_eq!(core.volatile_state().role, Role::Follower);
        assert_eq!(core.volatile_state().leader_id, Some(node(2)));
        assert!(core.volatile_state().progress.is_empty());

        let effects = core.drain_effects().collect::<Vec<_>>();
        assert_eq!(effects.len(), 2);
        assert!(matches!(
            effects[0],
            Effect::RoleChanged {
                role: Role::Follower,
                term,
                leader: None
            } if term == Term::new(2)
        ));
        assert!(matches!(
            effects[1],
            Effect::Send {
                target,
                message: RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
                    term,
                    success: true,
                    match_index,
                    ..
                })
            } if target == node(2) && term == Term::new(2) && match_index == barrier.index
        ));
    }

    #[test]
    fn snapshot_boundary_heartbeat_matches_without_live_suffix() {
        let config = StableConfiguration::new([node(1), node(2), node(3)], []).unwrap();
        let snapshot = SnapshotMetadata::new(
            LogIndex::new(8),
            Term::new(4),
            config.clone(),
            SnapshotId::new([4; 16]),
        )
        .unwrap();
        let recovered = RecoveredState {
            hard_state: HardState {
                current_term: Term::new(5),
                voted_for: None,
                durable_commit_index: LogIndex::new(8),
            },
            snapshot,
            log: Vec::new(),
            effective_configuration: EffectiveConfiguration::Stable(config),
            pending_configuration_index: None,
        };
        let mut core = RaftCore::from_recovered(node(1), recovered, timing(), 5).unwrap();

        core.handle_input(Input::Step(RaftMessage::AppendEntriesRequest(heartbeat(
            Term::new(5),
            node(2),
            LogIndex::new(8),
            Term::new(4),
            LogIndex::new(8),
        ))))
        .unwrap();

        let response = append_entries_response(&mut core);
        assert!(response.success);
        assert_eq!(response.match_index, LogIndex::new(8));
        assert_eq!(core.volatile_state().commit_index, LogIndex::new(8));
        assert!(core.stable_log().is_empty());
    }

    #[test]
    fn prefix_below_snapshot_floor_rejects_with_snapshot_plus_one_hint() {
        let config = StableConfiguration::new([node(1), node(2), node(3)], []).unwrap();
        let snapshot = SnapshotMetadata::new(
            LogIndex::new(8),
            Term::new(4),
            config.clone(),
            SnapshotId::new([5; 16]),
        )
        .unwrap();
        let recovered = RecoveredState {
            hard_state: HardState {
                current_term: Term::new(5),
                voted_for: None,
                durable_commit_index: LogIndex::new(8),
            },
            snapshot,
            log: Vec::new(),
            effective_configuration: EffectiveConfiguration::Stable(config),
            pending_configuration_index: None,
        };
        let mut core = RaftCore::from_recovered(node(1), recovered, timing(), 5).unwrap();

        core.handle_input(Input::Step(RaftMessage::AppendEntriesRequest(heartbeat(
            Term::new(5),
            node(2),
            LogIndex::new(7),
            Term::new(4),
            LogIndex::new(8),
        ))))
        .unwrap();

        let response = append_entries_response(&mut core);
        assert!(!response.success);
        assert_eq!(response.conflict_term, None);
        assert_eq!(response.conflict_index, LogIndex::new(9));
    }

    #[test]
    fn missing_prefix_beyond_tail_rejects_with_last_plus_one_hint() {
        let config = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], []).unwrap(),
        );
        let log = vec![
            LogEntry::new(
                Term::new(1),
                LogIndex::new(1),
                EntryKind::Application(vec![1]),
            ),
            LogEntry::new(
                Term::new(2),
                LogIndex::new(2),
                EntryKind::Application(vec![2]),
            ),
        ];
        let mut core = RaftCore::from_recovered(
            node(1),
            recovered_with(config, log, Term::new(3)),
            timing(),
            5,
        )
        .unwrap();

        core.handle_input(Input::Step(RaftMessage::AppendEntriesRequest(heartbeat(
            Term::new(3),
            node(2),
            LogIndex::new(5),
            Term::new(2),
            LogIndex::ZERO,
        ))))
        .unwrap();

        let response = append_entries_response(&mut core);
        assert!(!response.success);
        assert_eq!(response.conflict_term, None);
        assert_eq!(response.conflict_index, LogIndex::new(3));
        assert_eq!(core.stable_log().len(), 2);
    }

    #[test]
    fn retained_term_mismatch_returns_term_and_first_retained_index_hint() {
        let config = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], []).unwrap(),
        );
        let log = vec![
            LogEntry::new(
                Term::new(1),
                LogIndex::new(1),
                EntryKind::Application(vec![1]),
            ),
            LogEntry::new(
                Term::new(2),
                LogIndex::new(2),
                EntryKind::Application(vec![2]),
            ),
            LogEntry::new(
                Term::new(2),
                LogIndex::new(3),
                EntryKind::Application(vec![3]),
            ),
            LogEntry::new(
                Term::new(3),
                LogIndex::new(4),
                EntryKind::Application(vec![4]),
            ),
        ];
        let mut core = RaftCore::from_recovered(
            node(1),
            recovered_with(config, log, Term::new(4)),
            timing(),
            5,
        )
        .unwrap();

        core.handle_input(Input::Step(RaftMessage::AppendEntriesRequest(heartbeat(
            Term::new(4),
            node(2),
            LogIndex::new(3),
            Term::new(9),
            LogIndex::ZERO,
        ))))
        .unwrap();

        let response = append_entries_response(&mut core);
        assert!(!response.success);
        assert_eq!(response.conflict_term, Some(Term::new(2)));
        assert_eq!(response.conflict_index, LogIndex::new(2));
        assert_eq!(core.stable_log().len(), 4);
    }

    #[test]
    fn snapshot_boundary_term_mismatch_returns_snapshot_term_hint() {
        let config = StableConfiguration::new([node(1), node(2), node(3)], []).unwrap();
        let snapshot = SnapshotMetadata::new(
            LogIndex::new(8),
            Term::new(4),
            config.clone(),
            SnapshotId::new([6; 16]),
        )
        .unwrap();
        let recovered = RecoveredState {
            hard_state: HardState {
                current_term: Term::new(5),
                voted_for: None,
                durable_commit_index: LogIndex::new(8),
            },
            snapshot,
            log: Vec::new(),
            effective_configuration: EffectiveConfiguration::Stable(config),
            pending_configuration_index: None,
        };
        let mut core = RaftCore::from_recovered(node(1), recovered, timing(), 5).unwrap();

        core.handle_input(Input::Step(RaftMessage::AppendEntriesRequest(heartbeat(
            Term::new(5),
            node(2),
            LogIndex::new(8),
            Term::new(3),
            LogIndex::new(8),
        ))))
        .unwrap();

        let response = append_entries_response(&mut core);
        assert!(!response.success);
        assert_eq!(response.conflict_term, Some(Term::new(4)));
        assert_eq!(response.conflict_index, LogIndex::new(8));
    }

    #[test]
    fn non_empty_append_entries_is_persisted_before_success_response() {
        let config = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], []).unwrap(),
        );
        let log = vec![LogEntry::new(
            Term::new(1),
            LogIndex::new(1),
            EntryKind::Application(vec![1]),
        )];
        let mut core = RaftCore::from_recovered(
            node(1),
            recovered_with(config, log.clone(), Term::new(2)),
            timing(),
            5,
        )
        .unwrap();
        let request = AppendEntriesRequest {
            term: Term::new(2),
            leader_id: node(2),
            prev_log_index: LogIndex::new(1),
            prev_log_term: Term::new(1),
            entries: vec![LogEntry::new(
                Term::new(2),
                LogIndex::new(2),
                EntryKind::Application(vec![2]),
            )],
            leader_commit: LogIndex::ZERO,
        };

        core.handle_input(Input::Step(RaftMessage::AppendEntriesRequest(request)))
            .unwrap();
        assert_eq!(core.stable_log(), log.as_slice());
        assert_eq!(core.volatile_state().leader_id, Some(node(2)));
        let effects = core.drain_effects().collect::<Vec<_>>();
        assert_eq!(effects.len(), 1);
        let Effect::Persist(batch) = &effects[0] else {
            panic!("expected follower append persistence");
        };
        assert!(matches!(
            batch.operations.as_slice(),
            [PersistenceOp::Append(entries)] if entries.len() == 1 && entries[0].index == LogIndex::new(2)
        ));
        let token = batch.token;
        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        assert_eq!(core.stable_log().len(), 2);
        let response = append_entries_response(&mut core);
        assert!(response.success);
        assert_eq!(response.match_index, LogIndex::new(2));
    }

    #[test]
    fn follower_append_from_new_leader_never_regresses_existing_commit() {
        let config = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], []).unwrap(),
        );
        let log = vec![
            LogEntry::new(
                Term::new(1),
                LogIndex::new(1),
                EntryKind::Application(vec![1]),
            ),
            LogEntry::new(
                Term::new(1),
                LogIndex::new(2),
                EntryKind::Application(vec![2]),
            ),
        ];
        let mut recovered = recovered_with(config, log.clone(), Term::new(2));
        recovered.hard_state.durable_commit_index = LogIndex::new(2);
        let mut core = RaftCore::from_recovered(node(1), recovered, timing(), 5).unwrap();
        let recovery = core.drain_effects().collect::<Vec<_>>();
        assert!(matches!(
            recovery.as_slice(),
            [Effect::Apply(entries)] if entries.len() == 2
        ));
        core.handle_input(Input::AppliedThrough(LogIndex::new(2)))
            .unwrap();

        core.handle_input(Input::Step(RaftMessage::AppendEntriesRequest(
            AppendEntriesRequest {
                term: Term::new(2),
                leader_id: node(2),
                prev_log_index: LogIndex::new(2),
                prev_log_term: Term::new(1),
                entries: vec![LogEntry::new(
                    Term::new(2),
                    LogIndex::new(3),
                    EntryKind::LeaderBarrier,
                )],
                // The new leader has not yet learned the globally durable
                // commit index 2 from the previous leader.
                leader_commit: LogIndex::new(1),
            },
        )))
        .unwrap();

        let effects = core.drain_effects().collect::<Vec<_>>();
        let Effect::Persist(batch) = &effects[0] else {
            panic!("new leader append must be persisted before success");
        };
        assert!(matches!(
            batch.operations.as_slice(),
            [PersistenceOp::Append(entries)]
                if entries.len() == 1 && entries[0].index == LogIndex::new(3)
        ));
        let token = batch.token;
        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();

        assert_eq!(core.volatile_state().commit_index, LogIndex::new(2));
        assert_eq!(core.hard_state().durable_commit_index, LogIndex::new(2));
        assert_eq!(core.stable_log().len(), 3);
        let response = append_entries_response(&mut core);
        assert!(response.success);
        assert_eq!(response.match_index, LogIndex::new(3));
    }

    #[test]
    fn non_empty_append_entries_with_bad_prefix_still_returns_prefix_rejection() {
        let config = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], []).unwrap(),
        );
        let log = vec![LogEntry::new(
            Term::new(1),
            LogIndex::new(1),
            EntryKind::Application(vec![1]),
        )];
        let mut core = RaftCore::from_recovered(
            node(1),
            recovered_with(config, log.clone(), Term::new(2)),
            timing(),
            5,
        )
        .unwrap();
        let request = AppendEntriesRequest {
            term: Term::new(2),
            leader_id: node(2),
            prev_log_index: LogIndex::new(3),
            prev_log_term: Term::new(1),
            entries: vec![LogEntry::new(
                Term::new(2),
                LogIndex::new(4),
                EntryKind::Application(vec![2]),
            )],
            leader_commit: LogIndex::ZERO,
        };

        core.handle_input(Input::Step(RaftMessage::AppendEntriesRequest(request)))
            .unwrap();
        let response = append_entries_response(&mut core);
        assert!(!response.success);
        assert_eq!(response.conflict_index, LogIndex::new(2));
        assert_eq!(core.stable_log(), log.as_slice());
    }

    #[test]
    fn heartbeat_commit_advancement_is_durable_before_apply_and_success() {
        let config = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], []).unwrap(),
        );
        let log = vec![LogEntry::new(
            Term::new(1),
            LogIndex::new(1),
            EntryKind::Application(vec![1]),
        )];
        let mut core = RaftCore::from_recovered(
            node(1),
            recovered_with(config, log, Term::new(2)),
            timing(),
            5,
        )
        .unwrap();

        core.handle_input(Input::Step(RaftMessage::AppendEntriesRequest(heartbeat(
            Term::new(2),
            node(2),
            LogIndex::new(1),
            Term::new(1),
            LogIndex::new(1),
        ))))
        .unwrap();
        assert_eq!(core.volatile_state().commit_index, LogIndex::ZERO);
        assert_eq!(core.volatile_state().leader_id, Some(node(2)));
        let effects = core.drain_effects().collect::<Vec<_>>();
        assert_eq!(effects.len(), 1);
        let Effect::Persist(batch) = &effects[0] else {
            panic!("expected commit persistence");
        };
        assert_eq!(
            batch.operations,
            vec![PersistenceOp::AdvanceCommit {
                to: LogIndex::new(1)
            }]
        );
        let token = batch.token;
        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        assert_eq!(core.volatile_state().commit_index, LogIndex::new(1));
        let completed = core.drain_effects().collect::<Vec<_>>();
        assert!(matches!(completed[0], Effect::Apply(_)));
        assert!(matches!(
            completed[1],
            Effect::Send {
                message: RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
                    success: true,
                    match_index,
                    ..
                }),
                ..
            } if match_index == LogIndex::new(1)
        ));
    }

    #[test]
    fn learner_accepts_matching_heartbeat_and_remains_non_voting() {
        let config = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], [node(4)]).unwrap(),
        );
        let mut core = RaftCore::from_recovered(
            node(4),
            recovered_with(config, Vec::new(), Term::new(1)),
            timing(),
            5,
        )
        .unwrap();

        core.handle_input(Input::Step(RaftMessage::AppendEntriesRequest(heartbeat(
            Term::new(1),
            node(2),
            LogIndex::ZERO,
            Term::ZERO,
            LogIndex::ZERO,
        ))))
        .unwrap();

        let response = append_entries_response(&mut core);
        assert!(response.success);
        assert_eq!(response.follower_id, node(4));
        assert_eq!(core.volatile_state().role, Role::Learner);
        assert_eq!(core.volatile_state().leader_id, Some(node(2)));
        assert!(!core.effective_configuration().contains_voter(node(4)));
    }

    #[test]
    fn foreign_higher_term_append_entries_request_cannot_raise_term() {
        let config = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], []).unwrap(),
        );
        let mut core = RaftCore::from_recovered(
            node(1),
            recovered_with(config, Vec::new(), Term::new(4)),
            timing(),
            5,
        )
        .unwrap();

        core.handle_input(Input::Step(RaftMessage::AppendEntriesRequest(heartbeat(
            Term::new(99),
            node(9),
            LogIndex::ZERO,
            Term::ZERO,
            LogIndex::ZERO,
        ))))
        .unwrap();

        assert_eq!(core.hard_state().current_term, Term::new(4));
        assert_eq!(core.in_flight_persistence(), None);
        let response = append_entries_response(&mut core);
        assert_eq!(response.term, Term::new(4));
        assert!(!response.success);
    }

    #[test]
    fn same_term_append_entries_response_updates_durable_progress_and_stages_commit() {
        let mut core = core(1);
        reach_three_voter_election_quorum(&mut core, Term::new(1));
        core.handle_input(Input::WinElection { term: Term::new(1) })
            .unwrap();
        let effects = core.drain_effects().collect::<Vec<_>>();
        let (token, _) = staged_single_append(&effects[1]);
        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        let _ = core.drain_effects().collect::<Vec<_>>();

        core.handle_input(Input::Step(RaftMessage::AppendEntriesResponse(
            AppendEntriesResponse {
                term: Term::new(1),
                follower_id: node(2),
                success: true,
                match_index: LogIndex::FIRST,
                conflict_term: None,
                conflict_index: LogIndex::ZERO,
            },
        )))
        .unwrap();

        assert_eq!(
            core.volatile_state().progress[&node(2)].match_index,
            LogIndex::FIRST
        );
        assert_eq!(
            core.volatile_state().progress[&node(2)].next_index,
            LogIndex::new(2)
        );
        assert!(core.volatile_state().progress[&node(2)].recently_active);
        let effects = core.drain_effects().collect::<Vec<_>>();
        assert!(effects.iter().any(|effect| matches!(
            effect,
            Effect::Persist(PersistenceBatch {
                operations,
                ..
            }) if operations == &vec![PersistenceOp::AdvanceCommit { to: LogIndex::FIRST }]
        )));
    }

    #[test]
    fn higher_term_append_entries_response_durably_fences_leader_and_clears_progress() {
        let mut core = core(1);
        reach_three_voter_election_quorum(&mut core, Term::new(1));
        core.handle_input(Input::WinElection { term: Term::new(1) })
            .unwrap();
        let effects = core.drain_effects().collect::<Vec<_>>();
        let (barrier_token, _) = staged_single_append(&effects[1]);
        core.handle_input(Input::PersistenceCompleted(barrier_token))
            .unwrap();
        let _ = core.drain_effects().collect::<Vec<_>>();
        assert_eq!(core.volatile_state().role, Role::Leader);

        core.handle_input(Input::Step(RaftMessage::AppendEntriesResponse(
            AppendEntriesResponse {
                term: Term::new(2),
                follower_id: node(2),
                success: false,
                match_index: LogIndex::ZERO,
                conflict_term: None,
                conflict_index: LogIndex::FIRST,
            },
        )))
        .unwrap();
        let persist = core.drain_effects().collect::<Vec<_>>();
        let (token, hard_state) = staged_hard_state(&persist[0]);
        assert_eq!(hard_state.current_term, Term::new(2));
        assert_eq!(core.volatile_state().role, Role::Leader);

        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        assert_eq!(core.hard_state().current_term, Term::new(2));
        assert_eq!(core.volatile_state().role, Role::Follower);
        assert!(core.volatile_state().progress.is_empty());
        assert_eq!(
            core.drain_effects().collect::<Vec<_>>(),
            vec![Effect::RoleChanged {
                role: Role::Follower,
                term: Term::new(2),
                leader: None,
            }]
        );
    }

    #[test]
    fn foreign_higher_term_append_entries_response_cannot_fence_leader() {
        let mut core = core(1);
        reach_three_voter_election_quorum(&mut core, Term::new(1));
        core.handle_input(Input::WinElection { term: Term::new(1) })
            .unwrap();
        let effects = core.drain_effects().collect::<Vec<_>>();
        let (token, _) = staged_single_append(&effects[1]);
        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        let _ = core.drain_effects().collect::<Vec<_>>();

        core.handle_input(Input::Step(RaftMessage::AppendEntriesResponse(
            AppendEntriesResponse {
                term: Term::new(99),
                follower_id: node(9),
                success: false,
                match_index: LogIndex::ZERO,
                conflict_term: None,
                conflict_index: LogIndex::FIRST,
            },
        )))
        .unwrap();

        assert_eq!(core.hard_state().current_term, Term::new(1));
        assert_eq!(core.volatile_state().role, Role::Leader);
        assert_eq!(core.in_flight_persistence(), None);
        assert!(core.drain_effects().next().is_none());
    }

    #[test]
    fn candidate_steps_down_before_rejecting_current_term_bad_prefix() {
        let config = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], []).unwrap(),
        );
        let log = vec![LogEntry::new(
            Term::new(1),
            LogIndex::new(1),
            EntryKind::Application(vec![1]),
        )];
        let mut core = RaftCore::from_recovered(
            node(1),
            recovered_with(config, log, Term::new(1)),
            timing(),
            5,
        )
        .unwrap();
        reach_pre_vote_quorum(&mut core, Term::new(2));
        core.handle_input(Input::BeginElection {
            prospective_term: Term::new(2),
        })
        .unwrap();
        let persist = core.drain_effects().collect::<Vec<_>>();
        let (token, _) = staged_hard_state(&persist[0]);
        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        let _ = core.drain_effects().collect::<Vec<_>>();
        assert_eq!(core.volatile_state().role, Role::Candidate);

        core.handle_input(Input::Step(RaftMessage::AppendEntriesRequest(heartbeat(
            Term::new(2),
            node(2),
            LogIndex::new(9),
            Term::new(1),
            LogIndex::ZERO,
        ))))
        .unwrap();

        assert_eq!(core.volatile_state().role, Role::Follower);
        assert_eq!(core.volatile_state().leader_id, Some(node(2)));
        assert!(core.volatile_state().election.is_none());
        let effects = core.drain_effects().collect::<Vec<_>>();
        assert_eq!(effects.len(), 2);
        assert!(matches!(
            effects[0],
            Effect::RoleChanged {
                role: Role::Follower,
                term,
                leader: Some(leader)
            } if term == Term::new(2) && leader == node(2)
        ));
        assert!(matches!(
            effects[1],
            Effect::Send {
                message: RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
                    success: false,
                    conflict_index,
                    ..
                }),
                ..
            } if conflict_index == LogIndex::new(2)
        ));
    }

    #[test]
    fn higher_term_bad_prefix_rejection_waits_for_term_persistence() {
        let config = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], []).unwrap(),
        );
        let mut core = RaftCore::from_recovered(
            node(1),
            recovered_with(config, Vec::new(), Term::new(4)),
            timing(),
            5,
        )
        .unwrap();

        core.handle_input(Input::Step(RaftMessage::AppendEntriesRequest(heartbeat(
            Term::new(5),
            node(2),
            LogIndex::new(3),
            Term::new(1),
            LogIndex::ZERO,
        ))))
        .unwrap();
        let effects = core.drain_effects().collect::<Vec<_>>();
        assert_eq!(effects.len(), 1);
        let (token, hard_state) = staged_hard_state(&effects[0]);
        assert_eq!(hard_state.current_term, Term::new(5));
        assert_eq!(core.hard_state().current_term, Term::new(4));
        assert_eq!(core.volatile_state().leader_id, None);

        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        assert_eq!(core.hard_state().current_term, Term::new(5));
        assert_eq!(core.volatile_state().leader_id, Some(node(2)));
        let response = append_entries_response(&mut core);
        assert_eq!(response.term, Term::new(5));
        assert!(!response.success);
        assert_eq!(response.conflict_index, LogIndex::FIRST);
    }

    #[test]
    fn higher_term_heartbeat_persistence_failure_sends_no_response() {
        let config = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], []).unwrap(),
        );
        let mut core = RaftCore::from_recovered(
            node(1),
            recovered_with(config, Vec::new(), Term::new(4)),
            timing(),
            5,
        )
        .unwrap();

        core.handle_input(Input::Step(RaftMessage::AppendEntriesRequest(heartbeat(
            Term::new(5),
            node(2),
            LogIndex::ZERO,
            Term::ZERO,
            LogIndex::ZERO,
        ))))
        .unwrap();
        let effects = core.drain_effects().collect::<Vec<_>>();
        let (token, _) = staged_hard_state(&effects[0]);

        core.handle_input(Input::PersistenceFailed {
            token,
            error: PersistenceFailure::new("simulated term fsync failure"),
        })
        .unwrap();
        assert!(core.is_stopped());
        assert_eq!(core.hard_state().current_term, Term::new(4));
        assert_eq!(core.volatile_state().leader_id, None);
        assert_eq!(
            core.drain_effects().collect::<Vec<_>>(),
            vec![Effect::Fatal("simulated term fsync failure".into())]
        );
    }

    #[test]
    fn leader_rejects_competing_same_term_heartbeat_without_losing_progress() {
        let mut core = core(1);
        reach_three_voter_election_quorum(&mut core, Term::new(1));
        core.handle_input(Input::WinElection { term: Term::new(1) })
            .unwrap();
        let effects = core.drain_effects().collect::<Vec<_>>();
        let (token, barrier) = staged_single_append(&effects[1]);
        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        let _ = core.drain_effects().collect::<Vec<_>>();
        let progress_before = core.volatile_state().progress.clone();

        core.handle_input(Input::Step(RaftMessage::AppendEntriesRequest(heartbeat(
            Term::new(1),
            node(2),
            barrier.index,
            barrier.term,
            LogIndex::ZERO,
        ))))
        .unwrap();

        let response = append_entries_response(&mut core);
        assert!(!response.success);
        assert_eq!(core.volatile_state().role, Role::Leader);
        assert_eq!(core.volatile_state().leader_id, Some(node(1)));
        assert_eq!(core.volatile_state().progress, progress_before);
    }

    #[test]
    fn prior_term_entry_commits_only_through_current_term_quorum_entry() {
        let config = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], []).unwrap(),
        );
        let prior = LogEntry::new(
            Term::new(1),
            LogIndex::FIRST,
            EntryKind::Application(b"prior".to_vec()),
        );
        let mut core = RaftCore::from_recovered(
            node(1),
            recovered_with(config, vec![prior.clone()], Term::new(1)),
            timing(),
            5,
        )
        .unwrap();

        reach_three_voter_election_quorum(&mut core, Term::new(2));
        core.handle_input(Input::WinElection { term: Term::new(2) })
            .unwrap();
        let effects = core.drain_effects().collect::<Vec<_>>();
        let (barrier_token, barrier) = staged_single_append(&effects[1]);
        assert_eq!(barrier.index, LogIndex::new(2));
        core.handle_input(Input::PersistenceCompleted(barrier_token))
            .unwrap();
        let _ = core.drain_effects().collect::<Vec<_>>();
        assert_eq!(core.volatile_state().commit_index, LogIndex::ZERO);

        core.handle_input(Input::Step(RaftMessage::AppendEntriesResponse(
            AppendEntriesResponse {
                term: Term::new(2),
                follower_id: node(2),
                success: true,
                match_index: LogIndex::new(2),
                conflict_term: None,
                conflict_index: LogIndex::ZERO,
            },
        )))
        .unwrap();
        let effects = core.drain_effects().collect::<Vec<_>>();
        let Effect::Persist(batch) = effects
            .iter()
            .find(|effect| matches!(effect, Effect::Persist(_)))
            .expect("current-term quorum must persist commit")
        else {
            unreachable!()
        };
        assert_eq!(
            batch.operations,
            vec![PersistenceOp::AdvanceCommit {
                to: LogIndex::new(2)
            }]
        );
        let token = batch.token;
        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        assert_eq!(core.volatile_state().commit_index, LogIndex::new(2));
        assert_eq!(core.hard_state().durable_commit_index, LogIndex::new(2));
        let completed = core.drain_effects().collect::<Vec<_>>();
        let applied = completed.iter().find_map(|effect| match effect {
            Effect::Apply(entries) => Some(entries),
            _ => None,
        });
        assert_eq!(applied.unwrap(), &vec![prior, barrier]);
    }

    #[test]
    fn leader_conflict_term_hint_skips_entire_conflicting_term() {
        let config = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], []).unwrap(),
        );
        let log = vec![
            LogEntry::new(
                Term::new(1),
                LogIndex::new(1),
                EntryKind::Application(vec![1]),
            ),
            LogEntry::new(
                Term::new(2),
                LogIndex::new(2),
                EntryKind::Application(vec![2]),
            ),
            LogEntry::new(
                Term::new(2),
                LogIndex::new(3),
                EntryKind::Application(vec![3]),
            ),
            LogEntry::new(
                Term::new(3),
                LogIndex::new(4),
                EntryKind::Application(vec![4]),
            ),
        ];
        let mut core = RaftCore::from_recovered(
            node(1),
            recovered_with(config, log, Term::new(3)),
            timing(),
            5,
        )
        .unwrap();
        reach_three_voter_election_quorum(&mut core, Term::new(4));
        core.handle_input(Input::WinElection { term: Term::new(4) })
            .unwrap();
        let effects = core.drain_effects().collect::<Vec<_>>();
        let (token, _) = staged_single_append(&effects[1]);
        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        let _ = core.drain_effects().collect::<Vec<_>>();

        core.handle_input(Input::Step(RaftMessage::AppendEntriesResponse(
            AppendEntriesResponse {
                term: Term::new(4),
                follower_id: node(2),
                success: false,
                match_index: LogIndex::ZERO,
                conflict_term: Some(Term::new(2)),
                conflict_index: LogIndex::new(2),
            },
        )))
        .unwrap();

        assert_eq!(
            core.volatile_state().progress[&node(2)].next_index,
            LogIndex::new(4)
        );
        let effects = core.drain_effects().collect::<Vec<_>>();
        assert!(matches!(
            effects.as_slice(),
            [Effect::Send {
                target,
                message: RaftMessage::AppendEntriesRequest(AppendEntriesRequest {
                    prev_log_index,
                    prev_log_term,
                    entries,
                    ..
                })
            }] if *target == node(2)
                && *prev_log_index == LogIndex::new(3)
                && *prev_log_term == Term::new(2)
                && entries.first().is_some_and(|entry| entry.index == LogIndex::new(4))
        ));
    }

    #[test]
    fn conflicting_committed_prefix_is_fatal_and_never_truncated() {
        let config = StableConfiguration::new([node(1), node(2), node(3)], []).unwrap();
        let snapshot =
            SnapshotMetadata::new(LogIndex::ZERO, Term::ZERO, config.clone(), SnapshotId::ZERO)
                .unwrap();
        let committed = LogEntry::new(
            Term::new(1),
            LogIndex::FIRST,
            EntryKind::Application(b"committed".to_vec()),
        );
        let recovered = RecoveredState {
            hard_state: HardState {
                current_term: Term::new(2),
                voted_for: None,
                durable_commit_index: LogIndex::FIRST,
            },
            snapshot,
            log: vec![committed.clone()],
            effective_configuration: EffectiveConfiguration::Stable(config),
            pending_configuration_index: None,
        };
        let mut core = RaftCore::from_recovered(node(1), recovered, timing(), 5).unwrap();
        let recovery = core.drain_effects().collect::<Vec<_>>();
        assert!(
            matches!(recovery.as_slice(), [Effect::Apply(entries)] if entries == &vec![committed.clone()])
        );
        core.handle_input(Input::AppliedThrough(LogIndex::FIRST))
            .unwrap();
        core.handle_input(Input::Step(RaftMessage::AppendEntriesRequest(
            AppendEntriesRequest {
                term: Term::new(2),
                leader_id: node(2),
                prev_log_index: LogIndex::ZERO,
                prev_log_term: Term::ZERO,
                entries: vec![LogEntry::new(
                    Term::new(2),
                    LogIndex::FIRST,
                    EntryKind::Application(b"conflict".to_vec()),
                )],
                leader_commit: LogIndex::FIRST,
            },
        )))
        .unwrap();

        assert!(core.is_stopped());
        assert_eq!(core.stable_log(), std::slice::from_ref(&committed));
        assert_eq!(core.in_flight_persistence(), None);
        assert!(matches!(
            core.drain_effects().collect::<Vec<_>>().as_slice(),
            [Effect::Fatal(reason)] if reason.contains("committed history")
        ));
    }

    #[test]
    fn application_backpressure_bounds_uncommitted_bytes_without_blocking_control() {
        let config = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], []).unwrap(),
        );
        let recovered = recovered_with(config, Vec::new(), Term::ZERO);
        let mut core = RaftCore::from_recovered_with_limits(
            node(1),
            recovered,
            timing(),
            5,
            RaftLimits::new(4).unwrap(),
        )
        .unwrap();
        reach_three_voter_election_quorum(&mut core, Term::new(1));
        core.handle_input(Input::WinElection { term: Term::new(1) })
            .unwrap();
        let effects = core.drain_effects().collect::<Vec<_>>();
        let (token, _) = staged_single_append(&effects[1]);
        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        let _ = core.drain_effects().collect::<Vec<_>>();

        core.handle_input(Input::Propose {
            id: ProposalId::new(100),
            payload: vec![1, 2, 3, 4],
        })
        .unwrap();
        let effects = core.drain_effects().collect::<Vec<_>>();
        let Effect::Persist(batch) = &effects[0] else {
            panic!("first proposal must persist");
        };
        let token = batch.token;
        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        let _ = core.drain_effects().collect::<Vec<_>>();

        core.handle_input(Input::Propose {
            id: ProposalId::new(101),
            payload: vec![5],
        })
        .unwrap();
        assert_eq!(
            core.drain_effects().collect::<Vec<_>>(),
            vec![Effect::ProposalResult {
                id: ProposalId::new(101),
                accepted: false,
                leader: Some(node(1)),
            }]
        );
    }

    #[test]
    fn joint_check_quorum_requires_recent_old_and_new_majorities() {
        let old = StableConfiguration::new([node(1), node(2), node(3)], []).unwrap();
        let new = StableConfiguration::new([node(3), node(4), node(5)], []).unwrap();
        let config = EffectiveConfiguration::Joint(JointConfiguration::new(old, new).unwrap());
        let mut core = RaftCore::from_recovered(
            node(1),
            recovered_with(config, Vec::new(), Term::new(3)),
            timing(),
            5,
        )
        .unwrap();
        core.volatile.role = Role::Leader;
        core.volatile.leader_id = Some(node(1));
        for member in core.volatile.effective_configuration.all_members() {
            core.volatile.progress.insert(
                member,
                Progress {
                    match_index: LogIndex::ZERO,
                    next_index: LogIndex::FIRST,
                    recently_active: member == node(1) || member == node(2),
                    mode: ReplicationMode::Probe,
                },
            );
        }
        core.volatile.election_elapsed = core.volatile.randomized_election_timeout - 1;
        core.handle_input(Input::Tick {
            reset_election_timeout: 6,
        })
        .unwrap();
        assert_eq!(core.volatile_state().role, Role::Follower);
        assert!(core.volatile_state().progress.is_empty());
        assert!(matches!(
            core.drain_effects().collect::<Vec<_>>().as_slice(),
            [Effect::RoleChanged {
                role: Role::Follower,
                ..
            }]
        ));
    }

    #[test]
    fn local_snapshot_persists_before_publishing_new_snapshot_boundary() {
        let config = StableConfiguration::new([node(1), node(2), node(3)], []).unwrap();
        let snapshot =
            SnapshotMetadata::new(LogIndex::ZERO, Term::ZERO, config.clone(), SnapshotId::ZERO)
                .unwrap();
        let log = vec![
            LogEntry::new(
                Term::new(1),
                LogIndex::new(1),
                EntryKind::Application(vec![1]),
            ),
            LogEntry::new(
                Term::new(2),
                LogIndex::new(2),
                EntryKind::Application(vec![2]),
            ),
            LogEntry::new(
                Term::new(2),
                LogIndex::new(3),
                EntryKind::Application(vec![3]),
            ),
        ];
        let recovered = RecoveredState {
            hard_state: HardState {
                current_term: Term::new(2),
                voted_for: None,
                durable_commit_index: LogIndex::new(3),
            },
            snapshot,
            log,
            effective_configuration: EffectiveConfiguration::Stable(config.clone()),
            pending_configuration_index: None,
        };
        let mut core = RaftCore::from_recovered(node(1), recovered, timing(), 5).unwrap();
        let replay = core.drain_effects().collect::<Vec<_>>();
        assert!(matches!(replay.as_slice(), [Effect::Apply(entries)] if entries.len() == 3));
        core.handle_input(Input::AppliedThrough(LogIndex::new(3)))
            .unwrap();
        core.handle_input(Input::RequestSnapshot(LogIndex::new(2)))
            .unwrap();
        assert_eq!(
            core.drain_effects().collect::<Vec<_>>(),
            vec![Effect::BuildSnapshot(LogIndex::new(2))]
        );

        let metadata = SnapshotMetadata::new(
            LogIndex::new(2),
            Term::new(2),
            config,
            SnapshotId::new([2; 16]),
        )
        .unwrap();
        core.handle_input(Input::SnapshotBuilt(metadata.clone()))
            .unwrap();
        assert_eq!(core.snapshot().last_included_index, LogIndex::ZERO);
        let effects = core.drain_effects().collect::<Vec<_>>();
        let Effect::Persist(batch) = &effects[0] else {
            panic!("snapshot must persist before publication");
        };
        assert!(matches!(
            batch.operations.as_slice(),
            [PersistenceOp::InstallSnapshot(_), PersistenceOp::DiscardPrefix { through }]
                if *through == LogIndex::new(2)
        ));
        let token = batch.token;
        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        assert_eq!(core.snapshot(), &metadata);
        assert_eq!(core.stable_log().len(), 1);
        assert_eq!(core.stable_log()[0].index, LogIndex::new(3));
    }

    #[test]
    fn install_snapshot_matching_boundary_retains_suffix_and_orders_success_after_durability() {
        let config = StableConfiguration::new([node(1), node(2), node(3)], []).unwrap();
        let snapshot =
            SnapshotMetadata::new(LogIndex::ZERO, Term::ZERO, config.clone(), SnapshotId::ZERO)
                .unwrap();
        let log = vec![
            LogEntry::new(
                Term::new(1),
                LogIndex::new(1),
                EntryKind::Application(vec![1]),
            ),
            LogEntry::new(
                Term::new(2),
                LogIndex::new(2),
                EntryKind::Application(vec![2]),
            ),
            LogEntry::new(
                Term::new(3),
                LogIndex::new(3),
                EntryKind::Application(vec![3]),
            ),
        ];
        let recovered = RecoveredState {
            hard_state: HardState {
                current_term: Term::new(4),
                voted_for: None,
                durable_commit_index: LogIndex::new(1),
            },
            snapshot,
            log,
            effective_configuration: EffectiveConfiguration::Stable(config.clone()),
            pending_configuration_index: None,
        };
        let mut core = RaftCore::from_recovered(node(1), recovered, timing(), 5).unwrap();
        let replay = core.drain_effects().collect::<Vec<_>>();
        assert!(matches!(replay.as_slice(), [Effect::Apply(entries)] if entries.len() == 1));
        core.handle_input(Input::AppliedThrough(LogIndex::new(1)))
            .unwrap();
        let incoming = SnapshotMetadata::new(
            LogIndex::new(2),
            Term::new(2),
            config,
            SnapshotId::new([3; 16]),
        )
        .unwrap();
        core.handle_input(Input::Step(RaftMessage::InstallSnapshotRequest(
            InstallSnapshotRequest {
                term: Term::new(4),
                leader_id: node(2),
                snapshot: incoming.clone(),
            },
        )))
        .unwrap();
        assert_eq!(core.snapshot().last_included_index, LogIndex::ZERO);
        let effects = core.drain_effects().collect::<Vec<_>>();
        let Effect::Persist(batch) = &effects[0] else {
            panic!("install snapshot must persist before success");
        };
        let token = batch.token;
        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        assert_eq!(core.snapshot(), &incoming);
        assert_eq!(core.stable_log().len(), 1);
        assert_eq!(core.stable_log()[0].index, LogIndex::new(3));
        let completed = core.drain_effects().collect::<Vec<_>>();
        assert!(matches!(
            completed[0],
            Effect::InstallSnapshotToStateMachine(_)
        ));
        assert!(matches!(
            completed[1],
            Effect::Send {
                message: RaftMessage::InstallSnapshotResponse(InstallSnapshotResponse {
                    success: true,
                    match_index,
                    ..
                }),
                ..
            } if match_index == LogIndex::new(2)
        ));
    }

    #[test]
    fn install_snapshot_conflicting_boundary_discards_suffix_and_failure_does_not_publish() {
        let config = StableConfiguration::new([node(1), node(2), node(3)], []).unwrap();
        let snapshot =
            SnapshotMetadata::new(LogIndex::ZERO, Term::ZERO, config.clone(), SnapshotId::ZERO)
                .unwrap();
        let log = vec![
            LogEntry::new(
                Term::new(1),
                LogIndex::new(1),
                EntryKind::Application(vec![1]),
            ),
            LogEntry::new(
                Term::new(2),
                LogIndex::new(2),
                EntryKind::Application(vec![2]),
            ),
            LogEntry::new(
                Term::new(3),
                LogIndex::new(3),
                EntryKind::Application(vec![3]),
            ),
        ];
        let recovered = RecoveredState {
            hard_state: HardState {
                current_term: Term::new(4),
                voted_for: None,
                durable_commit_index: LogIndex::new(1),
            },
            snapshot,
            log,
            effective_configuration: EffectiveConfiguration::Stable(config.clone()),
            pending_configuration_index: None,
        };
        let mut core = RaftCore::from_recovered(node(1), recovered, timing(), 5).unwrap();
        let replay = core.drain_effects().collect::<Vec<_>>();
        assert!(matches!(replay.as_slice(), [Effect::Apply(entries)] if entries.len() == 1));
        core.handle_input(Input::AppliedThrough(LogIndex::new(1)))
            .unwrap();
        let incoming = SnapshotMetadata::new(
            LogIndex::new(2),
            Term::new(3),
            config,
            SnapshotId::new([4; 16]),
        )
        .unwrap();
        core.handle_input(Input::Step(RaftMessage::InstallSnapshotRequest(
            InstallSnapshotRequest {
                term: Term::new(4),
                leader_id: node(2),
                snapshot: incoming,
            },
        )))
        .unwrap();
        let effects = core.drain_effects().collect::<Vec<_>>();
        let Effect::Persist(batch) = &effects[0] else {
            panic!("conflicting snapshot must persist replacement");
        };
        assert!(
            batch
                .operations
                .iter()
                .any(|operation| matches!(operation, PersistenceOp::TruncateSuffix { .. }))
        );
        let token = batch.token;
        core.handle_input(Input::PersistenceFailed {
            token,
            error: PersistenceFailure::new("snapshot fsync failure"),
        })
        .unwrap();
        assert!(core.is_stopped());
        assert_eq!(core.snapshot().last_included_index, LogIndex::ZERO);
        assert_eq!(core.stable_log().len(), 3);
        assert!(matches!(
            core.drain_effects().collect::<Vec<_>>().as_slice(),
            [Effect::Fatal(reason)] if reason == "snapshot fsync failure"
        ));
    }

    #[test]
    fn failed_leadership_transfer_times_out_and_resumes_proposals() {
        let mut core = core(1);
        reach_three_voter_election_quorum(&mut core, Term::new(1));
        core.handle_input(Input::WinElection { term: Term::new(1) })
            .unwrap();
        let effects = core.drain_effects().collect::<Vec<_>>();
        let (token, _) = staged_single_append(&effects[1]);
        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        let _ = core.drain_effects().collect::<Vec<_>>();

        for peer in [node(2), node(3)] {
            core.volatile
                .progress
                .get_mut(&peer)
                .unwrap()
                .recently_active = true;
        }
        core.handle_input(Input::TransferLeadership { target: node(2) })
            .unwrap();
        assert!(core.volatile_state().leadership_transfer.is_some());
        let _ = core.drain_effects().collect::<Vec<_>>();

        core.handle_input(Input::Propose {
            id: ProposalId::new(200),
            payload: b"paused".to_vec(),
        })
        .unwrap();
        assert!(matches!(
            core.drain_effects().collect::<Vec<_>>().as_slice(),
            [Effect::ProposalResult {
                accepted: false,
                ..
            }]
        ));

        let timeout = core.volatile_state().randomized_election_timeout;
        for _ in 0..timeout {
            core.handle_input(Input::Tick {
                reset_election_timeout: 6,
            })
            .unwrap();
            let _ = core.drain_effects().collect::<Vec<_>>();
        }
        assert_eq!(core.volatile_state().role, Role::Leader);
        assert!(core.volatile_state().leadership_transfer.is_none());

        core.handle_input(Input::Propose {
            id: ProposalId::new(201),
            payload: b"resumed".to_vec(),
        })
        .unwrap();
        assert!(matches!(
            core.drain_effects().collect::<Vec<_>>().as_slice(),
            [Effect::Persist(_)]
        ));
    }

    #[test]
    fn conflicting_uncommitted_suffix_truncates_and_appends_in_one_durable_batch() {
        let config = StableConfiguration::new([node(1), node(2), node(3)], []).unwrap();
        let snapshot =
            SnapshotMetadata::new(LogIndex::ZERO, Term::ZERO, config.clone(), SnapshotId::ZERO)
                .unwrap();
        let committed = LogEntry::new(
            Term::new(1),
            LogIndex::new(1),
            EntryKind::Application(vec![1]),
        );
        let stale = LogEntry::new(
            Term::new(1),
            LogIndex::new(2),
            EntryKind::Application(vec![2]),
        );
        let recovered = RecoveredState {
            hard_state: HardState {
                current_term: Term::new(2),
                voted_for: None,
                durable_commit_index: LogIndex::new(1),
            },
            snapshot,
            log: vec![committed.clone(), stale.clone()],
            effective_configuration: EffectiveConfiguration::Stable(config),
            pending_configuration_index: None,
        };
        let mut core = RaftCore::from_recovered(node(1), recovered, timing(), 5).unwrap();
        let replay = core.drain_effects().collect::<Vec<_>>();
        assert!(
            matches!(replay.as_slice(), [Effect::Apply(entries)] if entries == &vec![committed.clone()])
        );
        core.handle_input(Input::AppliedThrough(LogIndex::new(1)))
            .unwrap();

        let replacement = vec![
            LogEntry::new(
                Term::new(2),
                LogIndex::new(2),
                EntryKind::Application(vec![9]),
            ),
            LogEntry::new(
                Term::new(2),
                LogIndex::new(3),
                EntryKind::Application(vec![10]),
            ),
        ];
        core.handle_input(Input::Step(RaftMessage::AppendEntriesRequest(
            AppendEntriesRequest {
                term: Term::new(2),
                leader_id: node(2),
                prev_log_index: LogIndex::new(1),
                prev_log_term: Term::new(1),
                entries: replacement.clone(),
                leader_commit: LogIndex::new(1),
            },
        )))
        .unwrap();
        assert_eq!(core.stable_log(), &[committed.clone(), stale]);
        let effects = core.drain_effects().collect::<Vec<_>>();
        let Effect::Persist(batch) = &effects[0] else {
            panic!("conflicting suffix must be persisted as truncate+append");
        };
        assert!(matches!(
            batch.operations.as_slice(),
            [
                PersistenceOp::TruncateSuffix { from },
                PersistenceOp::Append(entries)
            ] if *from == LogIndex::new(2) && entries == &replacement
        ));
        let token = batch.token;
        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        assert_eq!(
            core.stable_log(),
            &[committed, replacement[0].clone(), replacement[1].clone()]
        );
        let response = append_entries_response(&mut core);
        assert!(response.success);
        assert_eq!(response.match_index, LogIndex::new(3));
    }

    #[test]
    fn learner_promotion_is_rejected_until_caught_up_through_commit() {
        let old = StableConfiguration::new([node(1), node(2), node(3)], [node(4)]).unwrap();
        let new = StableConfiguration::new([node(1), node(2), node(3), node(4)], []).unwrap();
        let config = EffectiveConfiguration::Stable(old.clone());
        let mut core = RaftCore::from_recovered(
            node(1),
            recovered_with(config, Vec::new(), Term::ZERO),
            timing(),
            5,
        )
        .unwrap();
        reach_three_voter_election_quorum(&mut core, Term::new(1));
        core.handle_input(Input::WinElection { term: Term::new(1) })
            .unwrap();
        let effects = core.drain_effects().collect::<Vec<_>>();
        let (barrier_token, _) = staged_single_append(&effects[1]);
        core.handle_input(Input::PersistenceCompleted(barrier_token))
            .unwrap();
        let _ = core.drain_effects().collect::<Vec<_>>();
        core.handle_input(Input::Step(RaftMessage::AppendEntriesResponse(
            AppendEntriesResponse {
                term: Term::new(1),
                follower_id: node(2),
                success: true,
                match_index: LogIndex::FIRST,
                conflict_term: None,
                conflict_index: LogIndex::ZERO,
            },
        )))
        .unwrap();
        let effects = core.drain_effects().collect::<Vec<_>>();
        let Effect::Persist(batch) = effects
            .iter()
            .find(|effect| matches!(effect, Effect::Persist(_)))
            .unwrap()
        else {
            unreachable!()
        };
        let token = batch.token;
        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        let _ = core.drain_effects().collect::<Vec<_>>();
        assert_eq!(core.volatile_state().commit_index, LogIndex::FIRST);
        assert_eq!(
            core.volatile_state().progress[&node(4)].match_index,
            LogIndex::ZERO
        );
        assert!(
            !core.configuration_change_is_allowed(&ConfigurationChange::EnterJoint {
                old: old.clone(),
                new: new.clone(),
            })
        );
        core.volatile
            .progress
            .get_mut(&node(4))
            .unwrap()
            .match_index = LogIndex::FIRST;
        assert!(
            core.configuration_change_is_allowed(&ConfigurationChange::EnterJoint { old, new })
        );
    }

    #[test]
    fn committed_stable_promotion_changes_local_learner_to_follower() {
        let old = StableConfiguration::new([node(1), node(2), node(3)], [node(4)]).unwrap();
        let new = StableConfiguration::new([node(1), node(2), node(3), node(4)], []).unwrap();
        let config = EffectiveConfiguration::Stable(old.clone());
        let mut core = RaftCore::from_recovered(
            node(4),
            recovered_with(config, Vec::new(), Term::new(1)),
            timing(),
            5,
        )
        .unwrap();
        assert_eq!(core.volatile_state().role, Role::Learner);

        core.handle_input(Input::Step(RaftMessage::AppendEntriesRequest(
            AppendEntriesRequest {
                term: Term::new(1),
                leader_id: node(1),
                prev_log_index: LogIndex::ZERO,
                prev_log_term: Term::ZERO,
                entries: vec![
                    LogEntry::new(
                        Term::new(1),
                        LogIndex::FIRST,
                        EntryKind::Configuration(ConfigurationChange::EnterJoint {
                            old: old.clone(),
                            new: new.clone(),
                        }),
                    ),
                    LogEntry::new(
                        Term::new(1),
                        LogIndex::new(2),
                        EntryKind::Configuration(ConfigurationChange::LeaveJoint {
                            new: new.clone(),
                        }),
                    ),
                ],
                leader_commit: LogIndex::new(2),
            },
        )))
        .unwrap();
        let effects = core.drain_effects().collect::<Vec<_>>();
        let Effect::Persist(batch) = &effects[0] else {
            panic!("promotion append must be persisted before publication");
        };
        let token = batch.token;
        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();

        assert_eq!(
            core.effective_configuration(),
            &EffectiveConfiguration::Stable(new)
        );
        assert_eq!(core.volatile_state().pending_configuration_index, None);
        assert_eq!(core.volatile_state().role, Role::Follower);
        assert_eq!(core.volatile_state().leader_id, Some(node(1)));
        assert!(core.drain_effects().any(|effect| matches!(
            effect,
            Effect::RoleChanged {
                role: Role::Follower,
                leader: Some(leader),
                ..
            } if leader == node(1)
        )));
    }

    #[test]
    fn membership_change_serialization_rejects_leave_before_joint_entry_commit() {
        let old = StableConfiguration::new([node(1), node(2), node(3)], []).unwrap();
        let new = StableConfiguration::new([node(1), node(2), node(3), node(4)], []).unwrap();
        let mut core = RaftCore::from_recovered(
            node(1),
            recovered_with(
                EffectiveConfiguration::Stable(old.clone()),
                Vec::new(),
                Term::new(2),
            ),
            timing(),
            5,
        )
        .unwrap();
        core.volatile.role = Role::Leader;
        core.volatile.leader_id = Some(node(1));
        core.volatile.effective_configuration = EffectiveConfiguration::Joint(
            JointConfiguration::new(old.clone(), new.clone()).unwrap(),
        );
        core.volatile.pending_configuration_index = Some(LogIndex::new(1));
        core.stable_log.push(LogEntry::new(
            Term::new(2),
            LogIndex::new(1),
            EntryKind::Configuration(ConfigurationChange::EnterJoint {
                old,
                new: new.clone(),
            }),
        ));
        core.volatile.commit_index = LogIndex::ZERO;
        assert!(
            !core.configuration_change_is_allowed(&ConfigurationChange::LeaveJoint {
                new: new.clone(),
            })
        );
        core.volatile.commit_index = LogIndex::new(1);
        assert!(core.configuration_change_is_allowed(&ConfigurationChange::LeaveJoint { new }));
    }

    #[test]
    fn stale_install_snapshot_is_acknowledged_without_backward_movement() {
        let config = StableConfiguration::new([node(1), node(2), node(3)], []).unwrap();
        let current = SnapshotMetadata::new(
            LogIndex::new(5),
            Term::new(2),
            config.clone(),
            SnapshotId::new([5; 16]),
        )
        .unwrap();
        let recovered = RecoveredState {
            hard_state: HardState {
                current_term: Term::new(3),
                voted_for: None,
                durable_commit_index: LogIndex::new(5),
            },
            snapshot: current.clone(),
            log: Vec::new(),
            effective_configuration: EffectiveConfiguration::Stable(config.clone()),
            pending_configuration_index: None,
        };
        let mut core = RaftCore::from_recovered(node(1), recovered, timing(), 5).unwrap();
        let stale = SnapshotMetadata::new(
            LogIndex::new(4),
            Term::new(2),
            config,
            SnapshotId::new([4; 16]),
        )
        .unwrap();
        core.handle_input(Input::Step(RaftMessage::InstallSnapshotRequest(
            InstallSnapshotRequest {
                term: Term::new(3),
                leader_id: node(2),
                snapshot: stale,
            },
        )))
        .unwrap();
        assert_eq!(core.snapshot(), &current);
        assert_eq!(core.in_flight_persistence(), None);
        assert!(matches!(
            core.drain_effects().collect::<Vec<_>>().as_slice(),
            [Effect::Send {
                message: RaftMessage::InstallSnapshotResponse(InstallSnapshotResponse {
                    success: true,
                    match_index,
                    ..
                }),
                ..
            }] if *match_index == LogIndex::new(5)
        ));
    }

    #[test]
    fn local_snapshot_is_rejected_while_joint_transition_is_unresolved() {
        let old = StableConfiguration::new([node(1), node(2), node(3)], []).unwrap();
        let new = StableConfiguration::new([node(1), node(2), node(3), node(4)], []).unwrap();
        let snapshot =
            SnapshotMetadata::new(LogIndex::ZERO, Term::ZERO, old.clone(), SnapshotId::ZERO)
                .unwrap();
        let entry = LogEntry::new(
            Term::new(1),
            LogIndex::FIRST,
            EntryKind::Configuration(ConfigurationChange::EnterJoint {
                old,
                new: new.clone(),
            }),
        );
        let recovered = RecoveredState::from_persisted(
            HardState {
                current_term: Term::new(1),
                voted_for: None,
                durable_commit_index: LogIndex::FIRST,
            },
            snapshot,
            vec![entry],
        )
        .unwrap();
        let mut core = RaftCore::from_recovered(node(1), recovered, timing(), 5).unwrap();
        let replay = core.drain_effects().collect::<Vec<_>>();
        assert!(matches!(replay.as_slice(), [Effect::Apply(_)]));
        core.handle_input(Input::AppliedThrough(LogIndex::FIRST))
            .unwrap();
        assert_eq!(
            core.handle_input(Input::RequestSnapshot(LogIndex::FIRST)),
            Err(RaftError::InvalidSnapshotState(
                "local snapshot requires stable resolved membership"
            ))
        );
        assert!(matches!(
            core.effective_configuration(),
            EffectiveConfiguration::Joint(_)
        ));
    }

    #[test]
    fn leader_switches_peer_to_snapshot_when_conflict_hint_falls_below_retained_floor() {
        let config = StableConfiguration::new([node(1), node(2), node(3)], []).unwrap();
        let snapshot = SnapshotMetadata::new(
            LogIndex::new(5),
            Term::new(2),
            config.clone(),
            SnapshotId::new([7; 16]),
        )
        .unwrap();
        let log = vec![LogEntry::new(
            Term::new(3),
            LogIndex::new(6),
            EntryKind::LeaderBarrier,
        )];
        let recovered = RecoveredState {
            hard_state: HardState {
                current_term: Term::new(3),
                voted_for: Some(node(1)),
                durable_commit_index: LogIndex::new(5),
            },
            snapshot: snapshot.clone(),
            log,
            effective_configuration: EffectiveConfiguration::Stable(config),
            pending_configuration_index: None,
        };
        let mut core = RaftCore::from_recovered(node(1), recovered, timing(), 5).unwrap();
        core.volatile.role = Role::Leader;
        core.volatile.leader_id = Some(node(1));
        for member in [node(1), node(2), node(3)] {
            core.volatile.progress.insert(
                member,
                Progress {
                    match_index: if member == node(1) {
                        LogIndex::new(6)
                    } else {
                        LogIndex::ZERO
                    },
                    next_index: LogIndex::new(7),
                    recently_active: member == node(1),
                    mode: if member == node(1) {
                        ReplicationMode::Replicate
                    } else {
                        ReplicationMode::Probe
                    },
                },
            );
        }
        core.handle_input(Input::Step(RaftMessage::AppendEntriesResponse(
            AppendEntriesResponse {
                term: Term::new(3),
                follower_id: node(2),
                success: false,
                match_index: LogIndex::ZERO,
                conflict_term: None,
                conflict_index: LogIndex::new(1),
            },
        )))
        .unwrap();
        assert_eq!(
            core.volatile_state().progress[&node(2)].mode,
            ReplicationMode::Snapshot
        );
        assert!(matches!(
            core.drain_effects().collect::<Vec<_>>().as_slice(),
            [Effect::Send {
                target,
                message: RaftMessage::InstallSnapshotRequest(InstallSnapshotRequest {
                    snapshot: sent,
                    ..
                })
            }] if *target == node(2) && sent == &snapshot
        ));
    }

    #[test]
    fn leadership_transfer_requires_eligible_voter_and_catchup_before_timeout_now() {
        let config = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], [node(4)]).unwrap(),
        );
        let mut core = RaftCore::from_recovered(
            node(1),
            recovered_with(config, Vec::new(), Term::ZERO),
            timing(),
            5,
        )
        .unwrap();
        reach_three_voter_election_quorum(&mut core, Term::new(1));
        core.handle_input(Input::WinElection { term: Term::new(1) })
            .unwrap();
        let effects = core.drain_effects().collect::<Vec<_>>();
        let (token, _) = staged_single_append(&effects[1]);
        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        let _ = core.drain_effects().collect::<Vec<_>>();

        assert_eq!(
            core.handle_input(Input::TransferLeadership { target: node(4) }),
            Err(RaftError::InvalidLeadershipTransfer(
                "leadership transfer target is not an eligible voter"
            ))
        );

        core.handle_input(Input::TransferLeadership { target: node(2) })
            .unwrap();
        assert!(
            core.volatile_state()
                .leadership_transfer
                .as_ref()
                .is_some_and(|transfer| !transfer.timeout_now_sent)
        );
        let effects = core.drain_effects().collect::<Vec<_>>();
        assert!(effects.iter().any(|effect| matches!(
            effect,
            Effect::Send {
                target,
                message: RaftMessage::AppendEntriesRequest(_)
            } if *target == node(2)
        )));
        assert!(!effects.iter().any(|effect| matches!(
            effect,
            Effect::Send {
                message: RaftMessage::TimeoutNow(_),
                ..
            }
        )));
    }

    #[test]
    fn recovery_surfaces_committed_suffix_for_ordered_replay() {
        let config = StableConfiguration::new([node(1), node(2), node(3)], []).unwrap();
        let snapshot =
            SnapshotMetadata::new(LogIndex::ZERO, Term::ZERO, config.clone(), SnapshotId::ZERO)
                .unwrap();
        let log = vec![
            LogEntry::new(
                Term::new(1),
                LogIndex::new(1),
                EntryKind::Application(vec![1]),
            ),
            LogEntry::new(
                Term::new(1),
                LogIndex::new(2),
                EntryKind::Application(vec![2]),
            ),
            LogEntry::new(
                Term::new(1),
                LogIndex::new(3),
                EntryKind::Application(vec![3]),
            ),
        ];
        let recovered = RecoveredState {
            hard_state: HardState {
                current_term: Term::new(1),
                voted_for: None,
                durable_commit_index: LogIndex::new(2),
            },
            snapshot,
            log,
            effective_configuration: EffectiveConfiguration::Stable(config),
            pending_configuration_index: None,
        };
        let mut core = RaftCore::from_recovered(node(1), recovered, timing(), 5).unwrap();
        assert_eq!(core.volatile_state().last_applied, LogIndex::ZERO);
        let effects = core.drain_effects().collect::<Vec<_>>();
        assert!(matches!(
            effects.as_slice(),
            [Effect::Apply(entries)]
                if entries.iter().map(|entry| entry.index.get()).collect::<Vec<_>>() == vec![1, 2]
        ));
        core.handle_input(Input::AppliedThrough(LogIndex::new(2)))
            .unwrap();
        assert_eq!(core.volatile_state().last_applied, LogIndex::new(2));
    }

    fn p8_v2_leader(max_batches: usize, max_bytes: usize) -> RaftCore {
        let config = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], []).unwrap(),
        );
        let recovered = recovered_with(config, Vec::new(), Term::ZERO);
        let mut core = RaftCore::from_recovered_with_limits(
            node(1),
            recovered,
            timing(),
            5,
            RaftLimits::with_persistence_queue(1024, max_batches, max_bytes).unwrap(),
        )
        .unwrap();
        reach_three_voter_election_quorum(&mut core, Term::new(1));
        core.handle_input(Input::WinElection { term: Term::new(1) })
            .unwrap();
        let effects = core.drain_effects().collect::<Vec<_>>();
        let (barrier_token, _) = staged_single_append(&effects[1]);
        core.handle_input(Input::PersistenceCompleted(barrier_token))
            .unwrap();
        let _ = core.drain_effects().collect::<Vec<_>>();
        core
    }

    #[test]
    fn p8_v2_append_tokens_queue_without_advancing_durable_frontier() {
        let mut core = p8_v2_leader(3, 1024);
        let durable_before = core.stable_log().len();
        let self_match_before = core.volatile_state().progress[&node(1)].match_index;

        core.handle_input(Input::Propose {
            id: ProposalId::new(700),
            payload: vec![1, 2],
        })
        .unwrap();
        let first_effects = core.drain_effects().collect::<Vec<_>>();
        let (first_token, first_entry) = staged_single_append(&first_effects[0]);
        assert_eq!(first_entry.index, self_match_before.checked_next().unwrap());
        assert_eq!(core.stable_log().len(), durable_before);
        assert_eq!(
            core.volatile_state().progress[&node(1)].match_index,
            self_match_before
        );

        core.handle_input(Input::Propose {
            id: ProposalId::new(701),
            payload: vec![3, 4],
        })
        .unwrap();
        let second_effects = core.drain_effects().collect::<Vec<_>>();
        let (second_token, second_entry) = staged_single_append(&second_effects[0]);
        assert_eq!(
            second_entry.index,
            first_entry.index.checked_next().unwrap()
        );
        assert_eq!(core.pending_persistence_len(), 2);
        assert_eq!(core.pending_persistence_bytes(), 4);
        assert_eq!(
            core.next_staged_log_index().unwrap(),
            second_entry.index.checked_next().unwrap()
        );
        assert_eq!(
            core.handle_input(Input::PersistenceCompleted(second_token)),
            Err(RaftError::UnexpectedPersistenceToken)
        );
        assert_eq!(core.stable_log().len(), durable_before);

        core.handle_input(Input::PersistenceCompleted(first_token))
            .unwrap();
        assert_eq!(core.stable_log().last().unwrap(), &first_entry);
        assert_eq!(core.pending_persistence_len(), 1);
        assert_eq!(
            core.volatile_state().progress[&node(1)].match_index,
            first_entry.index
        );
        let _ = core.drain_effects().collect::<Vec<_>>();

        core.handle_input(Input::PersistenceCompleted(second_token))
            .unwrap();
        assert_eq!(core.stable_log().last().unwrap(), &second_entry);
        assert_eq!(core.pending_persistence_len(), 0);
        assert_eq!(core.pending_persistence_bytes(), 0);
        assert_eq!(
            core.volatile_state().progress[&node(1)].match_index,
            second_entry.index
        );
    }

    #[test]
    fn p8_v2_queue_count_and_bytes_reject_one_over_before_staging() {
        let mut count_core = p8_v2_leader(2, 1024);
        for id in [710_u64, 711] {
            count_core
                .handle_input(Input::Propose {
                    id: ProposalId::new(id),
                    payload: vec![1],
                })
                .unwrap();
            let _ = count_core.drain_effects().collect::<Vec<_>>();
        }
        assert_eq!(count_core.pending_persistence_len(), 2);
        assert_eq!(
            count_core.handle_input(Input::Propose {
                id: ProposalId::new(712),
                payload: vec![1],
            }),
            Err(RaftError::PersistenceQueueFull)
        );
        assert_eq!(count_core.pending_persistence_len(), 2);

        let mut byte_core = p8_v2_leader(4, 3);
        byte_core
            .handle_input(Input::Propose {
                id: ProposalId::new(720),
                payload: vec![1, 2],
            })
            .unwrap();
        let _ = byte_core.drain_effects().collect::<Vec<_>>();
        assert_eq!(
            byte_core.handle_input(Input::Propose {
                id: ProposalId::new(721),
                payload: vec![3, 4],
            }),
            Err(RaftError::PersistenceQueueFull)
        );
        assert_eq!(byte_core.pending_persistence_len(), 1);
        assert_eq!(byte_core.pending_persistence_bytes(), 2);
    }

    #[test]
    fn p8_v2_non_append_inputs_remain_hard_barriers_until_queue_drains() {
        let mut core = p8_v2_leader(3, 1024);
        core.handle_input(Input::Propose {
            id: ProposalId::new(730),
            payload: vec![9],
        })
        .unwrap();
        let effects = core.drain_effects().collect::<Vec<_>>();
        let (token, _) = staged_single_append(&effects[0]);

        assert_eq!(
            core.handle_input(Input::Tick {
                reset_election_timeout: 6,
            }),
            Err(RaftError::PersistenceAlreadyInFlight)
        );
        assert_eq!(
            core.handle_input(Input::TransferLeadership { target: node(2) }),
            Err(RaftError::PersistenceAlreadyInFlight)
        );

        core.handle_input(Input::PersistenceCompleted(token))
            .unwrap();
        let _ = core.drain_effects().collect::<Vec<_>>();
        assert!(
            core.handle_input(Input::Tick {
                reset_election_timeout: 6,
            })
            .is_ok()
        );
    }
}
