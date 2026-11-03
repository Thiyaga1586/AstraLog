use std::collections::{BTreeMap, VecDeque};

use crate::{
    AppendEntriesRequest, AppendEntriesResponse, ConfigurationChange, Effect,
    EffectiveConfiguration, EntryKind, HardState, Input, InstallSnapshotRequest, LogEntry,
    LogIndex, NodeId, PersistenceBatch, PersistenceFailure, PersistenceOp, ProposalId, RaftCore,
    RaftMessage, RecoveredState, RequestVoteRequest, RequestVoteResponse, Role, SnapshotId,
    SnapshotMetadata, StableConfiguration, Term, TimingConfig,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PersistPolicy {
    Immediate,
    HoldBeforeApply,
    HoldAfterApply,
    FailNext,
}

#[derive(Debug, Clone)]
struct Envelope {
    sequence: u64,
    from: NodeId,
    to: NodeId,
    deliver_at: u64,
    message: RaftMessage,
}

#[derive(Debug)]
struct SimNode {
    core: Option<RaftCore>,
    durable: RecoveredState,
    timeout: u64,
    persist_policy: PersistPolicy,
    held_persistence: Option<(PersistenceBatch, bool)>,
}

#[derive(Debug, Clone, Copy)]
struct DeterministicRng(u64);

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, bound: usize) -> usize {
        if bound == 0 {
            0
        } else {
            (self.next_u64() as usize) % bound
        }
    }
}

struct Simulation {
    seed: u64,
    clock: u64,
    sequence: u64,
    rng: DeterministicRng,
    timing: TimingConfig,
    nodes: BTreeMap<NodeId, SimNode>,
    network: Vec<Envelope>,
    trace: VecDeque<String>,
}

impl Simulation {
    fn new(seed: u64, configuration: StableConfiguration) -> Self {
        let timing = TimingConfig::new(1, 5, 10).unwrap();
        let snapshot = SnapshotMetadata::new(
            LogIndex::ZERO,
            Term::ZERO,
            configuration.clone(),
            SnapshotId::ZERO,
        )
        .unwrap();
        let mut nodes = BTreeMap::new();
        for member in configuration.voters().chain(configuration.learners()) {
            let durable =
                RecoveredState::from_persisted(HardState::initial(), snapshot.clone(), Vec::new())
                    .unwrap();
            let timeout = 5 + member.get() % 4;
            let core = RaftCore::from_recovered(member, durable.clone(), timing, timeout).unwrap();
            nodes.insert(
                member,
                SimNode {
                    core: Some(core),
                    durable,
                    timeout,
                    persist_policy: PersistPolicy::Immediate,
                    held_persistence: None,
                },
            );
        }
        Self {
            seed,
            clock: 0,
            sequence: 0,
            rng: DeterministicRng::new(seed),
            timing,
            nodes,
            network: Vec::new(),
            trace: VecDeque::new(),
        }
    }

    fn record(&mut self, line: impl Into<String>) {
        if self.trace.len() == 96 {
            self.trace.pop_front();
        }
        self.trace.push_back(line.into());
    }

    fn input(&mut self, node: NodeId, input: Input) {
        self.record(format!(
            "t={} node={} input={input:?}",
            self.clock,
            node.get()
        ));
        let result = self
            .nodes
            .get_mut(&node)
            .and_then(|state| state.core.as_mut())
            .map(|core| core.handle_input(input));
        if let Some(Err(error)) = result {
            panic!(
                "simulation seed {} node {} input failed: {error}; trace={:?}",
                self.seed,
                node.get(),
                self.trace
            );
        }
        self.pump(node);
    }

    fn pump(&mut self, node: NodeId) {
        loop {
            let effects = match self
                .nodes
                .get_mut(&node)
                .and_then(|state| state.core.as_mut())
            {
                Some(core) => core.drain_effects().collect::<Vec<_>>(),
                None => return,
            };
            if effects.is_empty() {
                return;
            }

            let mut followups = Vec::new();
            for effect in effects {
                self.record(format!(
                    "t={} node={} effect={effect:?}",
                    self.clock,
                    node.get()
                ));
                match effect {
                    Effect::Persist(batch) => {
                        let policy = self.nodes[&node].persist_policy;
                        match policy {
                            PersistPolicy::Immediate => {
                                self.apply_persistence(node, &batch);
                                followups.push(Input::PersistenceCompleted(batch.token));
                            }
                            PersistPolicy::HoldBeforeApply => {
                                self.nodes.get_mut(&node).unwrap().held_persistence =
                                    Some((batch, false));
                            }
                            PersistPolicy::HoldAfterApply => {
                                self.apply_persistence(node, &batch);
                                self.nodes.get_mut(&node).unwrap().held_persistence =
                                    Some((batch, true));
                            }
                            PersistPolicy::FailNext => {
                                self.nodes.get_mut(&node).unwrap().persist_policy =
                                    PersistPolicy::Immediate;
                                followups.push(Input::PersistenceFailed {
                                    token: batch.token,
                                    error: PersistenceFailure::new("injected simulation failure"),
                                });
                            }
                        }
                    }
                    Effect::Send { target, message } => {
                        self.sequence += 1;
                        self.network.push(Envelope {
                            sequence: self.sequence,
                            from: node,
                            to: target,
                            deliver_at: self.clock,
                            message,
                        });
                    }
                    Effect::Apply(entries) => {
                        if let Some(last) = entries.last() {
                            followups.push(Input::AppliedThrough(last.index));
                        }
                    }
                    Effect::BuildSnapshot(index) => {
                        let state = &self.nodes[&node].durable;
                        let term = term_at(state, index).expect("snapshot term must be retained");
                        let config = match self.nodes[&node]
                            .core
                            .as_ref()
                            .unwrap()
                            .effective_configuration()
                        {
                            EffectiveConfiguration::Stable(config) => config.clone(),
                            EffectiveConfiguration::Joint(_) => {
                                panic!("snapshot requested during joint configuration")
                            }
                        };
                        let mut id = [0_u8; 16];
                        id[..8].copy_from_slice(&self.seed.to_le_bytes());
                        id[8..].copy_from_slice(&index.get().to_le_bytes());
                        followups.push(Input::SnapshotBuilt(
                            SnapshotMetadata::new(index, term, config, SnapshotId::new(id))
                                .unwrap(),
                        ));
                    }
                    Effect::InstallSnapshotToStateMachine(_) | Effect::RoleChanged { .. } => {}
                    Effect::ProposalResult { .. } => {}
                    Effect::PreVoteQuorumReached { prospective_term } => {
                        followups.push(Input::BeginElection { prospective_term });
                    }
                    Effect::ElectionQuorumReached { term } => {
                        followups.push(Input::WinElection { term });
                    }
                    Effect::Fatal(reason) => {
                        self.record(format!("node {} fatal: {reason}", node.get()));
                    }
                }
            }

            for followup in followups {
                let result = self
                    .nodes
                    .get_mut(&node)
                    .and_then(|state| state.core.as_mut())
                    .map(|core| core.handle_input(followup));
                if let Some(Err(error)) = result {
                    panic!(
                        "simulation seed {} node {} continuation failed: {error}; trace={:?}",
                        self.seed,
                        node.get(),
                        self.trace
                    );
                }
            }
        }
    }

    fn apply_persistence(&mut self, node: NodeId, batch: &PersistenceBatch) {
        let state = self.nodes.get_mut(&node).unwrap();
        let mut hard = state.durable.hard_state.clone();
        let mut snapshot = state.durable.snapshot.clone();
        let mut log = state.durable.log.clone();
        for operation in &batch.operations {
            match operation {
                PersistenceOp::UpdateHardState(value) => hard = value.clone(),
                PersistenceOp::TruncateSuffix { from } => {
                    log.retain(|entry| entry.index < *from);
                }
                PersistenceOp::Append(entries) => log.extend(entries.iter().cloned()),
                PersistenceOp::AdvanceCommit { to } => hard.durable_commit_index = *to,
                PersistenceOp::InstallSnapshot(value) => snapshot = value.clone(),
                PersistenceOp::DiscardPrefix { through } => {
                    log.retain(|entry| entry.index > *through);
                }
            }
        }
        state.durable =
            RecoveredState::from_persisted(hard, snapshot, log).unwrap_or_else(|error| {
                panic!(
                    "simulation seed {} persistence reconstruction failed on node {}: {error}",
                    self.seed,
                    node.get()
                )
            });
    }

    fn set_persist_policy(&mut self, node: NodeId, policy: PersistPolicy) {
        self.nodes.get_mut(&node).unwrap().persist_policy = policy;
    }

    fn complete_held_persistence(&mut self, node: NodeId) {
        let (batch, applied) = self
            .nodes
            .get_mut(&node)
            .unwrap()
            .held_persistence
            .take()
            .unwrap();
        if !applied {
            self.apply_persistence(node, &batch);
        }
        self.nodes.get_mut(&node).unwrap().persist_policy = PersistPolicy::Immediate;
        self.input(node, Input::PersistenceCompleted(batch.token));
    }

    fn crash(&mut self, node: NodeId) {
        self.record(format!("t={} crash={}", self.clock, node.get()));
        let state = self.nodes.get_mut(&node).unwrap();
        state.core = None;
        state.held_persistence = None;
        state.persist_policy = PersistPolicy::Immediate;
    }

    fn restart(&mut self, node: NodeId) {
        self.record(format!("t={} restart={}", self.clock, node.get()));
        let should_pump = {
            let state = self.nodes.get_mut(&node).unwrap();
            if state.core.is_none() {
                state.core = Some(
                    RaftCore::from_recovered(
                        node,
                        state.durable.clone(),
                        self.timing,
                        state.timeout,
                    )
                    .unwrap(),
                );
                true
            } else {
                false
            }
        };
        if should_pump {
            self.pump(node);
        }
    }

    fn tick_node(&mut self, node: NodeId) {
        self.clock += 1;
        let sample = 5 + self.rng.below(6) as u64;
        self.input(
            node,
            Input::Tick {
                reset_election_timeout: sample,
            },
        );
    }

    fn tick_all(&mut self) {
        let nodes = self.nodes.keys().copied().collect::<Vec<_>>();
        self.clock += 1;
        for node in nodes {
            if self.nodes[&node].core.is_some() {
                let sample = 5 + self.rng.below(6) as u64;
                self.input(
                    node,
                    Input::Tick {
                        reset_election_timeout: sample,
                    },
                );
            }
        }
    }

    fn deliver_next(&mut self) -> bool {
        let Some((index, _)) = self
            .network
            .iter()
            .enumerate()
            .filter(|(_, envelope)| envelope.deliver_at <= self.clock)
            .filter(|(_, envelope)| {
                self.nodes
                    .get(&envelope.to)
                    .is_some_and(|state| match &state.core {
                        None => true,
                        Some(core) if core.is_stopped() => true,
                        Some(core) => core.in_flight_persistence().is_none(),
                    })
            })
            .min_by_key(|(_, envelope)| (envelope.deliver_at, envelope.sequence))
        else {
            return false;
        };
        let envelope = self.network.remove(index);
        if self.nodes[&envelope.to]
            .core
            .as_ref()
            .is_some_and(|core| !core.is_stopped())
        {
            self.record(format!(
                "t={} deliver {}->{} {:?}",
                self.clock,
                envelope.from.get(),
                envelope.to.get(),
                envelope.message
            ));
            self.input(envelope.to, Input::Step(envelope.message));
        }
        true
    }

    fn deliver_matching(&mut self, predicate: impl Fn(&Envelope) -> bool) -> bool {
        let Some(index) = self.network.iter().position(|envelope| {
            envelope.deliver_at <= self.clock
                && predicate(envelope)
                && self
                    .nodes
                    .get(&envelope.to)
                    .is_some_and(|state| match &state.core {
                        None => true,
                        Some(core) if core.is_stopped() => true,
                        Some(core) => core.in_flight_persistence().is_none(),
                    })
        }) else {
            return false;
        };
        let envelope = self.network.remove(index);
        if self.nodes[&envelope.to]
            .core
            .as_ref()
            .is_some_and(|core| !core.is_stopped())
        {
            self.record(format!(
                "t={} deliver-matching {}->{} {:?}",
                self.clock,
                envelope.from.get(),
                envelope.to.get(),
                envelope.message
            ));
            self.input(envelope.to, Input::Step(envelope.message));
        }
        true
    }

    fn drain_network(&mut self, max: usize) {
        for _ in 0..max {
            if !self.deliver_next() {
                return;
            }
        }
        panic!(
            "simulation seed {} network did not quiesce; trace={:?}",
            self.seed, self.trace
        );
    }

    fn force_election(&mut self, candidate: NodeId) {
        let timeout = self.nodes[&candidate].timeout;
        for _ in 0..timeout {
            self.tick_node(candidate);
        }
        self.drain_network(512);
        assert_eq!(
            self.nodes[&candidate]
                .core
                .as_ref()
                .unwrap()
                .volatile_state()
                .role,
            Role::Leader,
            "seed {} failed to elect {}; trace={:?}",
            self.seed,
            candidate.get(),
            self.trace
        );
    }

    fn leader(&self) -> Option<NodeId> {
        self.nodes.iter().find_map(|(node, state)| {
            state
                .core
                .as_ref()
                .is_some_and(|core| core.volatile_state().role == Role::Leader)
                .then_some(*node)
        })
    }

    fn propose(&mut self, leader: NodeId, id: u64, payload: &[u8]) {
        self.input(
            leader,
            Input::Propose {
                id: ProposalId::new(id),
                payload: payload.to_vec(),
            },
        );
    }

    fn assert_safety(&self) {
        let mut leaders = BTreeMap::<Term, NodeId>::new();
        for (node, state) in &self.nodes {
            if let Some(core) = &state.core
                && core.volatile_state().role == Role::Leader
            {
                let term = core.hard_state().current_term;
                if let Some(previous) = leaders.insert(term, *node) {
                    panic!(
                        "seed {} election safety violated: term {:?}, leaders {:?}/{:?}; trace={:?}",
                        self.seed, term, previous, node, self.trace
                    );
                }
            }
            let last = state
                .durable
                .log
                .last()
                .map_or(state.durable.snapshot.last_included_index, |entry| {
                    entry.index
                });
            assert!(
                state.durable.hard_state.durable_commit_index <= last,
                "seed {} commit outran durable history on node {}; trace={:?}",
                self.seed,
                node.get(),
                self.trace
            );
        }

        let states = self
            .nodes
            .values()
            .map(|node| &node.durable)
            .collect::<Vec<_>>();
        for left in 0..states.len() {
            for right in (left + 1)..states.len() {
                let upto = std::cmp::min(
                    states[left].hard_state.durable_commit_index,
                    states[right].hard_state.durable_commit_index,
                );
                for index in 1..=upto.get() {
                    let index = LogIndex::new(index);
                    let left_entry = states[left].log.iter().find(|entry| entry.index == index);
                    let right_entry = states[right].log.iter().find(|entry| entry.index == index);
                    if let (Some(a), Some(b)) = (left_entry, right_entry) {
                        assert_eq!(
                            (a.term, &a.kind),
                            (b.term, &b.kind),
                            "seed {} committed-prefix mismatch at {:?}; trace={:?}",
                            self.seed,
                            index,
                            self.trace
                        );
                    }
                }
            }
        }
    }

    fn random_campaign(mut self, steps: usize) -> Vec<String> {
        let node_ids = self.nodes.keys().copied().collect::<Vec<_>>();
        for step in 0..steps {
            match self.rng.below(9) {
                0 | 1 => self.tick_all(),
                2 => {
                    let _ = self.deliver_next();
                }
                3 if !self.network.is_empty() => {
                    let index = self.rng.below(self.network.len());
                    let envelope = self.network[index].clone();
                    self.sequence += 1;
                    self.network.push(Envelope {
                        sequence: self.sequence,
                        ..envelope
                    });
                }
                4 if !self.network.is_empty() => {
                    let index = self.rng.below(self.network.len());
                    self.network.remove(index);
                }
                5 if !self.network.is_empty() => {
                    let index = self.rng.below(self.network.len());
                    self.network[index].deliver_at = self.clock + 1 + self.rng.below(4) as u64;
                }
                6 => {
                    let node = node_ids[self.rng.below(node_ids.len())];
                    if self.nodes[&node].core.is_some() {
                        self.crash(node);
                    } else {
                        self.restart(node);
                    }
                }
                7 => {
                    if let Some(leader) = self.leader() {
                        self.propose(leader, 10_000 + step as u64, &[step as u8]);
                    }
                }
                _ => {
                    let node = node_ids[self.rng.below(node_ids.len())];
                    if self.nodes[&node].core.is_some() {
                        self.tick_node(node);
                    }
                }
            }
            self.assert_safety();
        }
        self.trace.into_iter().collect()
    }
}

fn term_at(state: &RecoveredState, index: LogIndex) -> Option<Term> {
    if index == state.snapshot.last_included_index {
        return Some(state.snapshot.last_included_term);
    }
    state
        .log
        .iter()
        .find(|entry| entry.index == index)
        .map(|entry| entry.term)
}

fn node(value: u64) -> NodeId {
    NodeId::new(value).unwrap()
}

fn three_voters() -> StableConfiguration {
    StableConfiguration::new([node(1), node(2), node(3)], []).unwrap()
}

#[test]
fn scenario_01_single_node_election_and_commit() {
    let mut sim = Simulation::new(1, StableConfiguration::new([node(1)], []).unwrap());
    sim.force_election(node(1));
    let durable = &sim.nodes[&node(1)].durable;
    assert_eq!(durable.hard_state.durable_commit_index, LogIndex::FIRST);
    assert!(matches!(durable.log[0].kind, EntryKind::LeaderBarrier));
    sim.assert_safety();
}

#[test]
fn scenarios_02_07_three_node_election_replication_and_current_term_commit() {
    let mut sim = Simulation::new(2, three_voters());
    sim.force_election(node(1));
    let before = sim.nodes[&node(1)].durable.hard_state.durable_commit_index;
    sim.propose(node(1), 1, b"alpha");
    sim.drain_network(512);
    let leader = &sim.nodes[&node(1)].durable;
    assert!(leader.hard_state.durable_commit_index > before);
    assert!(leader.log.iter().any(|entry| {
        matches!(&entry.kind, EntryKind::Application(payload) if payload == b"alpha")
            && entry.index <= leader.hard_state.durable_commit_index
    }));
    sim.assert_safety();
}

#[test]
fn scenario_03_split_vote_retries_in_higher_term_and_succeeds() {
    let config = StableConfiguration::new([node(1), node(2), node(3), node(4)], []).unwrap();
    let mut sim = Simulation::new(3, config);

    let timeout_one = sim.nodes[&node(1)].timeout;
    for _ in 0..timeout_one {
        sim.tick_node(node(1));
    }
    let timeout_two = sim.nodes[&node(2)].timeout;
    for _ in 0..timeout_two {
        sim.tick_node(node(2));
    }

    for (candidate, voters) in [(node(1), [node(3), node(4)]), (node(2), [node(3), node(4)])] {
        for voter in voters {
            assert!(sim.deliver_matching(|envelope| {
                envelope.from == candidate
                    && envelope.to == voter
                    && matches!(envelope.message, RaftMessage::PreVoteRequest(_))
            }));
            assert!(sim.deliver_matching(|envelope| {
                envelope.from == voter
                    && envelope.to == candidate
                    && matches!(envelope.message, RaftMessage::PreVoteResponse(_))
            }));
        }
    }

    assert_eq!(
        sim.nodes[&node(1)]
            .core
            .as_ref()
            .unwrap()
            .volatile_state()
            .role,
        Role::Candidate
    );
    assert_eq!(
        sim.nodes[&node(2)]
            .core
            .as_ref()
            .unwrap()
            .volatile_state()
            .role,
        Role::Candidate
    );

    assert!(sim.deliver_matching(|envelope| {
        envelope.from == node(1)
            && envelope.to == node(3)
            && matches!(envelope.message, RaftMessage::RequestVoteRequest(_))
    }));
    assert!(sim.deliver_matching(|envelope| {
        envelope.from == node(3)
            && envelope.to == node(1)
            && matches!(envelope.message, RaftMessage::RequestVoteResponse(_))
    }));
    assert!(sim.deliver_matching(|envelope| {
        envelope.from == node(2)
            && envelope.to == node(4)
            && matches!(envelope.message, RaftMessage::RequestVoteRequest(_))
    }));
    assert!(sim.deliver_matching(|envelope| {
        envelope.from == node(4)
            && envelope.to == node(2)
            && matches!(envelope.message, RaftMessage::RequestVoteResponse(_))
    }));
    sim.network.clear();
    assert!(sim.leader().is_none());

    let retry_timeout = sim.nodes[&node(1)]
        .core
        .as_ref()
        .unwrap()
        .volatile_state()
        .randomized_election_timeout;
    for _ in 0..retry_timeout {
        sim.tick_node(node(1));
    }
    sim.drain_network(2048);
    assert_eq!(sim.leader(), Some(node(1)));
    assert_eq!(
        sim.nodes[&node(1)].durable.hard_state.current_term,
        Term::new(2)
    );
    sim.assert_safety();
}

#[test]
fn scenario_04_pre_vote_prevents_isolated_stale_node_term_disruption() {
    let mut sim = Simulation::new(4, three_voters());
    sim.force_election(node(1));
    let leader_term = sim.nodes[&node(1)].durable.hard_state.current_term;
    let stale_term = sim.nodes[&node(3)].durable.hard_state.current_term;

    let timeout = sim.nodes[&node(3)].timeout;
    for _ in 0..=(timeout * 2) {
        sim.tick_node(node(3));
        sim.network.retain(|envelope| envelope.from != node(3));
    }

    assert_eq!(
        sim.nodes[&node(3)].durable.hard_state.current_term,
        stale_term
    );
    assert_eq!(
        sim.nodes[&node(1)].durable.hard_state.current_term,
        leader_term
    );
    assert_eq!(sim.leader(), Some(node(1)));
    sim.assert_safety();
}

#[test]
fn scenario_05_one_durable_vote_per_term_survives_restart() {
    let mut sim = Simulation::new(5, three_voters());
    sim.input(
        node(2),
        Input::Step(RaftMessage::RequestVoteRequest(RequestVoteRequest {
            term: Term::new(1),
            candidate_id: node(1),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::ZERO,
        })),
    );
    assert_eq!(
        sim.nodes[&node(2)].durable.hard_state.voted_for,
        Some(node(1))
    );
    sim.network.clear();
    sim.crash(node(2));
    sim.restart(node(2));

    sim.input(
        node(2),
        Input::Step(RaftMessage::RequestVoteRequest(RequestVoteRequest {
            term: Term::new(1),
            candidate_id: node(3),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::ZERO,
        })),
    );
    assert!(sim.network.iter().any(|envelope| {
        matches!(
            envelope.message,
            RaftMessage::RequestVoteResponse(RequestVoteResponse {
                term,
                voter_id,
                vote_granted: false,
            }) if term == Term::new(1) && voter_id == node(2)
        )
    }));
    assert_eq!(
        sim.nodes[&node(2)].durable.hard_state.voted_for,
        Some(node(1))
    );
    sim.assert_safety();
}

#[test]
fn scenario_06_candidate_with_stale_log_cannot_win() {
    let config = three_voters();
    let snapshot =
        SnapshotMetadata::new(LogIndex::ZERO, Term::ZERO, config.clone(), SnapshotId::ZERO)
            .unwrap();
    let fresh = vec![LogEntry::new(
        Term::new(2),
        LogIndex::FIRST,
        EntryKind::Application(b"fresh".to_vec()),
    )];
    let mut sim = Simulation::new(6, config);
    for voter in [node(1), node(2)] {
        let durable = RecoveredState::from_persisted(
            HardState {
                current_term: Term::new(2),
                voted_for: None,
                durable_commit_index: LogIndex::ZERO,
            },
            snapshot.clone(),
            fresh.clone(),
        )
        .unwrap();
        let timeout = sim.nodes[&voter].timeout;
        sim.nodes.get_mut(&voter).unwrap().durable = durable.clone();
        sim.nodes.get_mut(&voter).unwrap().core =
            Some(RaftCore::from_recovered(voter, durable, sim.timing, timeout).unwrap());
    }

    for voter in [node(1), node(2)] {
        sim.input(
            voter,
            Input::Step(RaftMessage::RequestVoteRequest(RequestVoteRequest {
                term: Term::new(3),
                candidate_id: node(3),
                last_log_index: LogIndex::ZERO,
                last_log_term: Term::ZERO,
            })),
        );
    }
    let rejections = sim
        .network
        .iter()
        .filter(|envelope| {
            envelope.to == node(3)
                && matches!(
                    envelope.message,
                    RaftMessage::RequestVoteResponse(RequestVoteResponse {
                        term,
                        vote_granted: false,
                        ..
                    }) if term == Term::new(3)
                )
        })
        .count();
    assert_eq!(rejections, 2);
    assert_ne!(sim.leader(), Some(node(3)));
    sim.assert_safety();
}

#[test]
fn scenario_08_prior_term_entry_commits_only_through_current_term_entry() {
    let mut sim = Simulation::new(8, three_voters());
    sim.force_election(node(1));
    let first_term = sim.nodes[&node(1)].durable.hard_state.current_term;
    sim.propose(node(1), 80, b"prior-term");

    assert!(sim.deliver_matching(|envelope| {
        envelope.from == node(1)
            && envelope.to == node(2)
            && matches!(
                &envelope.message,
                RaftMessage::AppendEntriesRequest(AppendEntriesRequest { entries, .. })
                    if entries.iter().any(|entry| matches!(&entry.kind, EntryKind::Application(payload) if payload == b"prior-term"))
            )
    }));
    sim.network.clear();

    let prior_index = sim.nodes[&node(2)]
        .durable
        .log
        .iter()
        .find(|entry| matches!(&entry.kind, EntryKind::Application(payload) if payload == b"prior-term"))
        .unwrap()
        .index;
    assert!(prior_index > sim.nodes[&node(2)].durable.hard_state.durable_commit_index);
    sim.crash(node(1));
    sim.crash(node(3));
    sim.restart(node(3));

    sim.force_election(node(2));
    let second_term = sim.nodes[&node(2)].durable.hard_state.current_term;
    assert!(second_term > first_term);
    sim.drain_network(2048);

    let durable = &sim.nodes[&node(2)].durable;
    assert!(durable.hard_state.durable_commit_index >= prior_index);
    assert!(durable.log.iter().any(|entry| {
        entry.term == second_term
            && entry.index <= durable.hard_state.durable_commit_index
            && matches!(entry.kind, EntryKind::LeaderBarrier)
    }));
    sim.assert_safety();
}

#[test]
fn scenario_09_conflict_hint_skips_entire_conflicting_term() {
    let config = three_voters();
    let snapshot =
        SnapshotMetadata::new(LogIndex::ZERO, Term::ZERO, config.clone(), SnapshotId::ZERO)
            .unwrap();
    let leader_log = vec![
        LogEntry::new(
            Term::new(1),
            LogIndex::new(1),
            EntryKind::Application(vec![1]),
        ),
        LogEntry::new(
            Term::new(3),
            LogIndex::new(2),
            EntryKind::Application(vec![2]),
        ),
        LogEntry::new(
            Term::new(3),
            LogIndex::new(3),
            EntryKind::Application(vec![3]),
        ),
    ];
    let follower_log = vec![
        LogEntry::new(
            Term::new(1),
            LogIndex::new(1),
            EntryKind::Application(vec![1]),
        ),
        LogEntry::new(
            Term::new(2),
            LogIndex::new(2),
            EntryKind::Application(vec![9]),
        ),
        LogEntry::new(
            Term::new(2),
            LogIndex::new(3),
            EntryKind::Application(vec![9]),
        ),
    ];
    let mut sim = Simulation::new(9, config);
    for (member, log) in [
        (node(1), leader_log.clone()),
        (node(2), follower_log),
        (node(3), leader_log.clone()),
    ] {
        let durable = RecoveredState::from_persisted(
            HardState {
                current_term: Term::new(3),
                voted_for: None,
                durable_commit_index: LogIndex::FIRST,
            },
            snapshot.clone(),
            log,
        )
        .unwrap();
        let timeout = sim.nodes[&member].timeout;
        sim.nodes.get_mut(&member).unwrap().durable = durable.clone();
        sim.nodes.get_mut(&member).unwrap().core =
            Some(RaftCore::from_recovered(member, durable, sim.timing, timeout).unwrap());
    }

    sim.force_election(node(1));
    sim.network.clear();
    sim.input(
        node(1),
        Input::Step(RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
            term: sim.nodes[&node(1)].durable.hard_state.current_term,
            follower_id: node(2),
            success: false,
            match_index: LogIndex::ZERO,
            conflict_term: Some(Term::new(2)),
            conflict_index: LogIndex::new(2),
        })),
    );

    assert!(sim.network.iter().any(|envelope| {
        envelope.from == node(1)
            && envelope.to == node(2)
            && matches!(
                envelope.message,
                RaftMessage::AppendEntriesRequest(AppendEntriesRequest {
                    prev_log_index,
                    prev_log_term,
                    ..
                }) if prev_log_index == LogIndex::FIRST && prev_log_term == Term::new(1)
            )
    }));
    sim.assert_safety();
}

#[test]
fn scenario_10_committed_prefix_cannot_be_truncated() {
    let config = three_voters();
    let snapshot =
        SnapshotMetadata::new(LogIndex::ZERO, Term::ZERO, config.clone(), SnapshotId::ZERO)
            .unwrap();
    let original = LogEntry::new(
        Term::new(1),
        LogIndex::FIRST,
        EntryKind::Application(b"committed".to_vec()),
    );
    let durable = RecoveredState::from_persisted(
        HardState {
            current_term: Term::new(2),
            voted_for: None,
            durable_commit_index: LogIndex::FIRST,
        },
        snapshot,
        vec![original.clone()],
    )
    .unwrap();
    let mut sim = Simulation::new(10, config);
    let timeout = sim.nodes[&node(2)].timeout;
    sim.nodes.get_mut(&node(2)).unwrap().durable = durable.clone();
    sim.nodes.get_mut(&node(2)).unwrap().core =
        Some(RaftCore::from_recovered(node(2), durable, sim.timing, timeout).unwrap());
    sim.pump(node(2));

    sim.input(
        node(2),
        Input::Step(RaftMessage::AppendEntriesRequest(AppendEntriesRequest {
            term: Term::new(2),
            leader_id: node(1),
            prev_log_index: LogIndex::ZERO,
            prev_log_term: Term::ZERO,
            entries: vec![LogEntry::new(
                Term::new(2),
                LogIndex::FIRST,
                EntryKind::Application(b"conflict".to_vec()),
            )],
            leader_commit: LogIndex::FIRST,
        })),
    );

    assert!(sim.nodes[&node(2)].core.as_ref().unwrap().is_stopped());
    assert_eq!(sim.nodes[&node(2)].durable.log, vec![original]);
    assert_eq!(
        sim.nodes[&node(2)].durable.hard_state.durable_commit_index,
        LogIndex::FIRST
    );
}

#[test]
fn scenarios_11_12_minority_cannot_commit_and_stale_leader_steps_down() {
    let mut sim = Simulation::new(11, three_voters());
    sim.force_election(node(1));
    let committed = sim.nodes[&node(1)].durable.hard_state.durable_commit_index;
    sim.crash(node(2));
    sim.crash(node(3));
    sim.propose(node(1), 2, b"minority");
    sim.network.clear();
    assert_eq!(
        sim.nodes[&node(1)].durable.hard_state.durable_commit_index,
        committed
    );
    let timeout = sim.nodes[&node(1)]
        .core
        .as_ref()
        .unwrap()
        .volatile_state()
        .randomized_election_timeout;
    for _ in 0..=(timeout * 2 + 1) {
        sim.tick_node(node(1));
        sim.network.clear();
        if sim.nodes[&node(1)]
            .core
            .as_ref()
            .unwrap()
            .volatile_state()
            .role
            != Role::Leader
        {
            break;
        }
    }
    assert_ne!(
        sim.nodes[&node(1)]
            .core
            .as_ref()
            .unwrap()
            .volatile_state()
            .role,
        Role::Leader
    );
    sim.assert_safety();
}

#[test]
fn scenarios_13_14_crash_before_and_after_follower_append_persistence() {
    let mut sim = Simulation::new(13, three_voters());
    sim.force_election(node(1));

    sim.set_persist_policy(node(2), PersistPolicy::HoldBeforeApply);
    sim.propose(node(1), 3, b"before");
    while !sim.network.is_empty() {
        let _ = sim.deliver_next();
        if sim.nodes[&node(2)].held_persistence.is_some() {
            break;
        }
    }
    let before = sim.nodes[&node(2)].durable.log.len();
    sim.crash(node(2));
    sim.restart(node(2));
    assert_eq!(sim.nodes[&node(2)].durable.log.len(), before);

    sim.set_persist_policy(node(2), PersistPolicy::Immediate);
    sim.drain_network(512);
    sim.set_persist_policy(node(2), PersistPolicy::HoldAfterApply);
    sim.propose(node(1), 4, b"after");
    while !sim.network.is_empty() {
        let _ = sim.deliver_next();
        if sim.nodes[&node(2)].held_persistence.is_some() {
            break;
        }
    }
    let durable_after = sim.nodes[&node(2)].durable.log.len();
    sim.crash(node(2));
    sim.restart(node(2));
    assert_eq!(sim.nodes[&node(2)].durable.log.len(), durable_after);
    sim.assert_safety();
}

#[test]
fn scenario_15_crash_after_commit_fsync_before_completion_recovers_commit_boundary() {
    let mut sim = Simulation::new(15, three_voters());
    sim.force_election(node(1));
    let previous_commit = sim.nodes[&node(1)].durable.hard_state.durable_commit_index;
    sim.propose(node(1), 15, b"commit-crash");
    sim.set_persist_policy(node(1), PersistPolicy::HoldAfterApply);

    for _ in 0..512 {
        if sim.nodes[&node(1)].held_persistence.is_some() {
            break;
        }
        if !sim.deliver_next() {
            break;
        }
    }
    assert!(sim.nodes[&node(1)].held_persistence.is_some());
    let durable_commit = sim.nodes[&node(1)].durable.hard_state.durable_commit_index;
    assert!(durable_commit > previous_commit);
    assert!(
        sim.nodes[&node(1)]
            .core
            .as_ref()
            .unwrap()
            .volatile_state()
            .commit_index
            < durable_commit
    );

    sim.crash(node(1));
    sim.restart(node(1));
    assert_eq!(
        sim.nodes[&node(1)]
            .core
            .as_ref()
            .unwrap()
            .volatile_state()
            .commit_index,
        durable_commit
    );
    sim.assert_safety();
}

#[test]
fn scenario_16_duplicate_and_reordered_append_entries_is_safe() {
    let mut sim = Simulation::new(16, three_voters());
    sim.force_election(node(1));
    sim.propose(node(1), 5, b"duplicate");
    let copies = sim.network.clone();
    for envelope in copies.into_iter().rev() {
        sim.sequence += 1;
        sim.network.push(Envelope {
            sequence: sim.sequence,
            ..envelope
        });
    }
    sim.network.reverse();
    sim.drain_network(1024);
    sim.assert_safety();
}

#[test]
fn scenarios_17_18_19_learner_catchup_promotion_and_joint_quorum() {
    let old = StableConfiguration::new([node(1), node(2), node(3)], [node(4)]).unwrap();
    let mut sim = Simulation::new(17, old.clone());
    sim.crash(node(4));
    sim.force_election(node(1));
    let promoted = StableConfiguration::new([node(1), node(2), node(3), node(4)], []).unwrap();
    sim.input(
        node(1),
        Input::ProposeConfiguration {
            id: ProposalId::new(6),
            change: ConfigurationChange::EnterJoint {
                old: old.clone(),
                new: promoted.clone(),
            },
        },
    );
    assert!(matches!(
        sim.nodes[&node(1)]
            .core
            .as_ref()
            .unwrap()
            .effective_configuration(),
        EffectiveConfiguration::Stable(config) if config == &old
    ));

    sim.restart(node(4));
    sim.propose(node(1), 6, b"learner-catchup");
    sim.drain_network(1024);
    let commit = sim.nodes[&node(1)].durable.hard_state.durable_commit_index;
    assert!(
        sim.nodes[&node(4)]
            .durable
            .log
            .last()
            .is_some_and(|entry| entry.index >= commit)
    );
    assert_eq!(
        sim.nodes[&node(4)]
            .core
            .as_ref()
            .unwrap()
            .volatile_state()
            .role,
        Role::Learner
    );

    let new = promoted;
    sim.input(
        node(1),
        Input::ProposeConfiguration {
            id: ProposalId::new(7),
            change: ConfigurationChange::EnterJoint {
                old: old.clone(),
                new: new.clone(),
            },
        },
    );
    sim.drain_network(2048);
    assert!(matches!(
        sim.nodes[&node(1)]
            .core
            .as_ref()
            .unwrap()
            .effective_configuration(),
        EffectiveConfiguration::Joint(_)
    ));
    sim.input(
        node(1),
        Input::ProposeConfiguration {
            id: ProposalId::new(8),
            change: ConfigurationChange::LeaveJoint { new: new.clone() },
        },
    );
    sim.drain_network(2048);
    assert!(matches!(
        sim.nodes[&node(1)]
            .core
            .as_ref()
            .unwrap()
            .effective_configuration(),
        EffectiveConfiguration::Stable(config) if config == &new
    ));
    sim.assert_safety();
}

#[test]
fn scenario_20_leader_self_removal_steps_down_after_new_configuration_commits() {
    let old = three_voters();
    let mut sim = Simulation::new(20, old.clone());
    sim.force_election(node(1));
    let new = StableConfiguration::new([node(2), node(3)], []).unwrap();
    sim.input(
        node(1),
        Input::ProposeConfiguration {
            id: ProposalId::new(20),
            change: ConfigurationChange::EnterJoint {
                old: old.clone(),
                new: new.clone(),
            },
        },
    );
    sim.drain_network(2048);
    sim.input(
        node(1),
        Input::ProposeConfiguration {
            id: ProposalId::new(21),
            change: ConfigurationChange::LeaveJoint { new },
        },
    );
    sim.drain_network(2048);
    assert_ne!(
        sim.nodes[&node(1)]
            .core
            .as_ref()
            .unwrap()
            .volatile_state()
            .role,
        Role::Leader
    );
    sim.assert_safety();
}

#[test]
fn scenario_21_snapshot_install_with_matching_boundary_retains_suffix() {
    let config = three_voters();
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
    let durable = RecoveredState::from_persisted(
        HardState {
            current_term: Term::new(4),
            voted_for: None,
            durable_commit_index: LogIndex::new(2),
        },
        snapshot,
        log,
    )
    .unwrap();
    let mut sim = Simulation::new(21, config.clone());
    let timeout = sim.nodes[&node(2)].timeout;
    sim.nodes.get_mut(&node(2)).unwrap().durable = durable.clone();
    sim.nodes.get_mut(&node(2)).unwrap().core =
        Some(RaftCore::from_recovered(node(2), durable, sim.timing, timeout).unwrap());
    sim.pump(node(2));

    let incoming = SnapshotMetadata::new(
        LogIndex::new(2),
        Term::new(2),
        config,
        SnapshotId::new([21; 16]),
    )
    .unwrap();
    sim.input(
        node(2),
        Input::Step(RaftMessage::InstallSnapshotRequest(
            InstallSnapshotRequest {
                term: Term::new(4),
                leader_id: node(1),
                snapshot: incoming.clone(),
            },
        )),
    );

    assert_eq!(sim.nodes[&node(2)].durable.snapshot, incoming);
    assert_eq!(sim.nodes[&node(2)].durable.log.len(), 1);
    assert_eq!(sim.nodes[&node(2)].durable.log[0].index, LogIndex::new(3));
    assert_eq!(sim.nodes[&node(2)].durable.log[0].term, Term::new(3));
    sim.assert_safety();
}

#[test]
fn scenario_22_snapshot_install_with_conflicting_boundary_discards_suffix() {
    let config = three_voters();
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
    let durable = RecoveredState::from_persisted(
        HardState {
            current_term: Term::new(10),
            voted_for: None,
            durable_commit_index: LogIndex::FIRST,
        },
        snapshot,
        log,
    )
    .unwrap();
    let mut sim = Simulation::new(22, config.clone());
    let timeout = sim.nodes[&node(2)].timeout;
    sim.nodes.get_mut(&node(2)).unwrap().durable = durable.clone();
    sim.nodes.get_mut(&node(2)).unwrap().core =
        Some(RaftCore::from_recovered(node(2), durable, sim.timing, timeout).unwrap());
    sim.pump(node(2));

    let incoming = SnapshotMetadata::new(
        LogIndex::new(2),
        Term::new(9),
        config,
        SnapshotId::new([22; 16]),
    )
    .unwrap();
    sim.input(
        node(2),
        Input::Step(RaftMessage::InstallSnapshotRequest(
            InstallSnapshotRequest {
                term: Term::new(10),
                leader_id: node(1),
                snapshot: incoming.clone(),
            },
        )),
    );

    assert_eq!(sim.nodes[&node(2)].durable.snapshot, incoming);
    assert!(sim.nodes[&node(2)].durable.log.is_empty());
    assert_eq!(
        sim.nodes[&node(2)].durable.hard_state.durable_commit_index,
        LogIndex::new(2)
    );
    sim.assert_safety();
}

#[test]
fn scenario_24_leadership_transfer_uses_real_election() {
    let mut sim = Simulation::new(24, three_voters());
    sim.force_election(node(1));
    sim.input(node(1), Input::TransferLeadership { target: node(2) });
    sim.drain_network(2048);
    assert_eq!(sim.leader(), Some(node(2)));
    assert!(sim.nodes[&node(2)].durable.hard_state.current_term >= Term::new(2));
    sim.assert_safety();
}

#[test]
fn scenario_25_failed_leadership_transfer_resumes_ordinary_service() {
    let mut sim = Simulation::new(25, three_voters());
    sim.force_election(node(1));
    sim.crash(node(2));
    sim.input(node(1), Input::TransferLeadership { target: node(2) });

    let timeout = sim.nodes[&node(1)]
        .core
        .as_ref()
        .unwrap()
        .volatile_state()
        .randomized_election_timeout;
    for _ in 0..=timeout {
        sim.tick_node(node(1));
        while sim.deliver_matching(|envelope| envelope.to == node(3) || envelope.from == node(3)) {}
        sim.network.retain(|envelope| envelope.to != node(2));
    }

    let leader = sim.nodes[&node(1)].core.as_ref().unwrap();
    assert_eq!(leader.volatile_state().role, Role::Leader);
    assert!(leader.volatile_state().leadership_transfer.is_none());

    let before = sim.nodes[&node(1)].durable.log.len();
    sim.propose(node(1), 25, b"service-resumed");
    sim.drain_network(1024);
    assert!(sim.nodes[&node(1)].durable.log.len() > before);
    assert!(sim.nodes[&node(1)].durable.log.iter().any(|entry| {
        matches!(&entry.kind, EntryKind::Application(payload) if payload == b"service-resumed")
    }));
    sim.assert_safety();
}

#[test]
fn scenario_23_snapshot_install_crash_boundaries_preserve_only_durable_state() {
    let configuration = three_voters();
    let incoming = SnapshotMetadata::new(
        LogIndex::new(2),
        Term::new(1),
        configuration.clone(),
        SnapshotId::new([23; 16]),
    )
    .unwrap();

    let mut before = Simulation::new(23, configuration.clone());
    {
        let state = before.nodes.get_mut(&node(2)).unwrap();
        state.durable = RecoveredState::from_persisted(
            HardState {
                current_term: Term::new(1),
                voted_for: None,
                durable_commit_index: LogIndex::ZERO,
            },
            state.durable.snapshot.clone(),
            Vec::new(),
        )
        .unwrap();
        state.core = Some(
            RaftCore::from_recovered(node(2), state.durable.clone(), before.timing, state.timeout)
                .unwrap(),
        );
    }
    before.set_persist_policy(node(2), PersistPolicy::HoldBeforeApply);
    before.input(
        node(2),
        Input::Step(RaftMessage::InstallSnapshotRequest(
            crate::InstallSnapshotRequest {
                term: Term::new(1),
                leader_id: node(1),
                snapshot: incoming.clone(),
            },
        )),
    );
    assert!(before.nodes[&node(2)].held_persistence.is_some());
    assert_eq!(
        before.nodes[&node(2)].durable.snapshot.last_included_index,
        LogIndex::ZERO
    );
    before.crash(node(2));
    before.restart(node(2));
    assert_eq!(
        before.nodes[&node(2)].durable.snapshot.last_included_index,
        LogIndex::ZERO
    );

    let mut after = Simulation::new(230, configuration);
    {
        let state = after.nodes.get_mut(&node(2)).unwrap();
        state.durable = RecoveredState::from_persisted(
            HardState {
                current_term: Term::new(1),
                voted_for: None,
                durable_commit_index: LogIndex::ZERO,
            },
            state.durable.snapshot.clone(),
            Vec::new(),
        )
        .unwrap();
        state.core = Some(
            RaftCore::from_recovered(node(2), state.durable.clone(), after.timing, state.timeout)
                .unwrap(),
        );
    }
    after.set_persist_policy(node(2), PersistPolicy::HoldAfterApply);
    after.input(
        node(2),
        Input::Step(RaftMessage::InstallSnapshotRequest(
            crate::InstallSnapshotRequest {
                term: Term::new(1),
                leader_id: node(1),
                snapshot: incoming.clone(),
            },
        )),
    );
    assert!(after.nodes[&node(2)].held_persistence.is_some());
    assert_eq!(after.nodes[&node(2)].durable.snapshot, incoming);
    assert!(after.network.is_empty());
    after.crash(node(2));
    after.restart(node(2));
    assert_eq!(
        after.nodes[&node(2)].durable.snapshot.last_included_index,
        LogIndex::new(2)
    );
    assert_eq!(
        after.nodes[&node(2)]
            .core
            .as_ref()
            .unwrap()
            .snapshot()
            .last_included_index,
        LogIndex::new(2)
    );
}

#[test]
fn persistence_failure_is_injected_deterministically() {
    let mut sim = Simulation::new(99, three_voters());
    sim.set_persist_policy(node(1), PersistPolicy::FailNext);
    for _ in 0..sim.nodes[&node(1)].timeout {
        sim.tick_node(node(1));
    }
    sim.drain_network(128);
    assert!(sim.nodes[&node(1)].core.as_ref().unwrap().is_stopped());
}

#[test]
fn delayed_persistence_completion_is_reproducible() {
    let mut sim = Simulation::new(100, three_voters());
    sim.set_persist_policy(node(1), PersistPolicy::HoldBeforeApply);
    for _ in 0..sim.nodes[&node(1)].timeout {
        sim.tick_node(node(1));
    }
    sim.drain_network(128);
    assert!(sim.nodes[&node(1)].held_persistence.is_some());
    sim.complete_held_persistence(node(1));
    sim.drain_network(512);
    sim.assert_safety();
}

#[test]
fn seeded_campaign_is_exactly_reproducible() {
    let first = Simulation::new(0x0A57_A10C, three_voters()).random_campaign(250);
    let second = Simulation::new(0x0A57_A10C, three_voters()).random_campaign(250);
    assert_eq!(first, second);
}

#[test]
fn large_seeded_campaigns_preserve_safety() {
    for seed in 1..=64 {
        let _trace = Simulation::new(seed, three_voters()).random_campaign(300);
    }
}
