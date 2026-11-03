use std::collections::{BTreeSet, VecDeque};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ModelRole {
    Follower,
    Candidate,
    Leader,
    Learner,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ModelEntry {
    term: u8,
    value: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ModelNode {
    term: u8,
    durable_vote: Option<usize>,
    role: ModelRole,
    log: Vec<ModelEntry>,
    commit: usize,
    crashed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum ModelMessage {
    VoteRequest {
        from: usize,
        to: usize,
        term: u8,
        last_term: u8,
        last_index: usize,
    },
    VoteResponse {
        from: usize,
        to: usize,
        term: u8,
        granted: bool,
    },
    Append {
        from: usize,
        to: usize,
        term: u8,
        prev_index: usize,
        prev_term: u8,
        entry: Option<ModelEntry>,
        leader_commit: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ModelState {
    nodes: Vec<ModelNode>,
    messages: Vec<ModelMessage>,
    stable_voters: BTreeSet<usize>,
    joint_voters: Option<(BTreeSet<usize>, BTreeSet<usize>)>,
    learners: BTreeSet<usize>,
}

impl ModelState {
    fn initial() -> Self {
        Self {
            nodes: (0..3)
                .map(|_| ModelNode {
                    term: 0,
                    durable_vote: None,
                    role: ModelRole::Follower,
                    log: Vec::new(),
                    commit: 0,
                    crashed: false,
                })
                .collect(),
            messages: Vec::new(),
            stable_voters: [0, 1, 2].into_iter().collect(),
            joint_voters: None,
            learners: BTreeSet::new(),
        }
    }

    fn effective_voters(&self) -> BTreeSet<usize> {
        if let Some((old, new)) = &self.joint_voters {
            old.union(new).copied().collect()
        } else {
            self.stable_voters.clone()
        }
    }

    fn quorum(&self, members: &BTreeSet<usize>) -> bool {
        if let Some((old, new)) = &self.joint_voters {
            majority(old, members) && majority(new, members)
        } else {
            majority(&self.stable_voters, members)
        }
    }

    fn start_election(&self, candidate: usize) -> Option<Self> {
        let voters = self.effective_voters();
        if !voters.contains(&candidate)
            || self.learners.contains(&candidate)
            || self.nodes[candidate].crashed
            || self.nodes[candidate].term >= 3
        {
            return None;
        }
        let mut next = self.clone();
        let term = next.nodes[candidate].term + 1;
        next.nodes[candidate].term = term;
        next.nodes[candidate].durable_vote = Some(candidate);
        next.nodes[candidate].role = ModelRole::Candidate;
        let (last_index, last_term) = last_position(&next.nodes[candidate].log);
        for peer in voters {
            if peer != candidate {
                next.messages.push(ModelMessage::VoteRequest {
                    from: candidate,
                    to: peer,
                    term,
                    last_term,
                    last_index,
                });
            }
        }
        Some(next)
    }

    fn deliver(&self, message_index: usize) -> Self {
        let mut next = self.clone();
        let message = next.messages.remove(message_index);
        match message {
            ModelMessage::VoteRequest {
                from,
                to,
                term,
                last_term,
                last_index,
            } => {
                if next.nodes[to].crashed || !next.effective_voters().contains(&from) {
                    return next;
                }
                if term > next.nodes[to].term {
                    next.nodes[to].term = term;
                    next.nodes[to].durable_vote = None;
                    next.nodes[to].role = if next.learners.contains(&to) {
                        ModelRole::Learner
                    } else {
                        ModelRole::Follower
                    };
                }
                let (local_index, local_term) = last_position(&next.nodes[to].log);
                let fresh = last_term > local_term
                    || (last_term == local_term && last_index >= local_index);
                let granted = term == next.nodes[to].term
                    && next.effective_voters().contains(&to)
                    && !next.learners.contains(&to)
                    && fresh
                    && next.nodes[to].durable_vote.is_none_or(|vote| vote == from);
                if granted {
                    next.nodes[to].durable_vote = Some(from);
                }
                next.messages.push(ModelMessage::VoteResponse {
                    from: to,
                    to: from,
                    term: next.nodes[to].term,
                    granted,
                });
            }
            ModelMessage::VoteResponse {
                from,
                to,
                term,
                granted,
            } => {
                if next.nodes[to].crashed {
                    return next;
                }
                if term > next.nodes[to].term {
                    next.nodes[to].term = term;
                    next.nodes[to].durable_vote = None;
                    next.nodes[to].role = ModelRole::Follower;
                    return next;
                }
                if next.nodes[to].role != ModelRole::Candidate || term != next.nodes[to].term {
                    return next;
                }
                let mut votes = BTreeSet::from([to]);
                if granted {
                    votes.insert(from);
                }
                for pending in &next.messages {
                    if let ModelMessage::VoteResponse {
                        from,
                        to: response_to,
                        term: response_term,
                        granted: true,
                    } = pending
                        && *response_to == to
                        && *response_term == term
                    {
                        votes.insert(*from);
                    }
                }
                if next.quorum(&votes) {
                    next.nodes[to].role = ModelRole::Leader;
                }
            }
            ModelMessage::Append {
                from,
                to,
                term,
                prev_index,
                prev_term,
                entry,
                leader_commit,
            } => {
                if next.nodes[to].crashed || !next.effective_voters().contains(&from) {
                    return next;
                }
                if term > next.nodes[to].term {
                    next.nodes[to].term = term;
                    next.nodes[to].durable_vote = None;
                    next.nodes[to].role = if next.learners.contains(&to) {
                        ModelRole::Learner
                    } else {
                        ModelRole::Follower
                    };
                }
                if term != next.nodes[to].term {
                    return next;
                }
                let prefix_matches = if prev_index == 0 {
                    prev_term == 0
                } else {
                    next.nodes[to]
                        .log
                        .get(prev_index - 1)
                        .is_some_and(|value| value.term == prev_term)
                };
                if !prefix_matches {
                    return next;
                }
                if let Some(incoming) = entry {
                    let index = prev_index + 1;
                    if let Some(local) = next.nodes[to].log.get(index - 1) {
                        if local.term != incoming.term {
                            if index <= next.nodes[to].commit {
                                return next;
                            }
                            next.nodes[to].log.truncate(index - 1);
                            next.nodes[to].log.push(incoming);
                        }
                    } else {
                        next.nodes[to].log.push(incoming);
                    }
                }
                let matched = prev_index + usize::from(entry.is_some());
                next.nodes[to].commit = next.nodes[to].commit.max(leader_commit.min(matched));
            }
        }
        next
    }

    fn append_value(&self, leader: usize, value: u8) -> Option<Self> {
        if self.nodes[leader].crashed || self.nodes[leader].role != ModelRole::Leader {
            return None;
        }
        if self.nodes[leader].log.len() >= 2 {
            return None;
        }
        let mut next = self.clone();
        let entry = ModelEntry {
            term: next.nodes[leader].term,
            value,
        };
        let prev_index = next.nodes[leader].log.len();
        let prev_term = next.nodes[leader].log.last().map_or(0, |entry| entry.term);
        next.nodes[leader].log.push(entry);
        for peer in next.effective_voters() {
            if peer != leader {
                next.messages.push(ModelMessage::Append {
                    from: leader,
                    to: peer,
                    term: next.nodes[leader].term,
                    prev_index,
                    prev_term,
                    entry: Some(entry),
                    leader_commit: next.nodes[leader].commit,
                });
            }
        }
        Some(next)
    }

    fn maybe_commit(&self, leader: usize) -> Option<Self> {
        if self.nodes[leader].role != ModelRole::Leader || self.nodes[leader].crashed {
            return None;
        }
        let mut next = self.clone();
        let mut target = next.nodes[leader].commit;
        for index in (next.nodes[leader].commit + 1)..=next.nodes[leader].log.len() {
            if next.nodes[leader].log[index - 1].term != next.nodes[leader].term {
                continue;
            }
            let replicated = next
                .effective_voters()
                .into_iter()
                .filter(|node| {
                    next.nodes[*node].log.len() >= index
                        && next.nodes[*node].log[..index] == next.nodes[leader].log[..index]
                })
                .collect::<BTreeSet<_>>();
            if next.quorum(&replicated) {
                target = index;
            }
        }
        if target == next.nodes[leader].commit {
            return None;
        }
        next.nodes[leader].commit = target;
        Some(next)
    }

    fn crash_or_restart(&self, node: usize) -> Self {
        let mut next = self.clone();
        next.nodes[node].crashed = !next.nodes[node].crashed;
        if !next.nodes[node].crashed {
            next.nodes[node].role = if next.learners.contains(&node) {
                ModelRole::Learner
            } else {
                ModelRole::Follower
            };
        }
        next
    }

    fn enter_joint(&self, new_voters: BTreeSet<usize>) -> Option<Self> {
        if self.joint_voters.is_some() || new_voters.is_empty() {
            return None;
        }
        let committed_floor = self.nodes.iter().map(|node| node.commit).max().unwrap_or(0);
        for added in new_voters.difference(&self.stable_voters) {
            if !self.learners.contains(added) || self.nodes.get(*added)?.log.len() < committed_floor
            {
                return None;
            }
        }
        let mut next = self.clone();
        next.joint_voters = Some((self.stable_voters.clone(), new_voters));
        Some(next)
    }

    fn leave_joint(&self) -> Option<Self> {
        let (_, new) = self.joint_voters.clone()?;
        let mut next = self.clone();
        next.stable_voters = new.clone();
        next.joint_voters = None;
        next.learners.retain(|node| !new.contains(node));
        for (node, state) in next.nodes.iter_mut().enumerate() {
            if next.learners.contains(&node) {
                state.role = ModelRole::Learner;
            } else if state.role == ModelRole::Learner {
                state.role = ModelRole::Follower;
            }
        }
        Some(next)
    }

    fn assert_invariants(&self) {
        for term in 1..=3 {
            assert!(
                self.nodes
                    .iter()
                    .filter(|node| node.role == ModelRole::Leader && node.term == term)
                    .count()
                    <= 1,
                "R-01 election safety violated: {self:?}"
            );
        }

        for left in 0..self.nodes.len() {
            for right in (left + 1)..self.nodes.len() {
                let common = self.nodes[left].log.len().min(self.nodes[right].log.len());
                for index in 0..common {
                    if self.nodes[left].log[index].term == self.nodes[right].log[index].term {
                        assert_eq!(
                            self.nodes[left].log[..=index],
                            self.nodes[right].log[..=index],
                            "R-02 log matching violated: {self:?}"
                        );
                    }
                }

                let committed = self.nodes[left].commit.min(self.nodes[right].commit);
                if committed > 0 {
                    assert_eq!(
                        self.nodes[left].log[..committed],
                        self.nodes[right].log[..committed],
                        "R-04/R-07 committed-prefix safety violated: {self:?}"
                    );
                }
            }
        }

        for node in &self.nodes {
            assert!(
                node.commit <= node.log.len(),
                "R-06 commit durability violated"
            );
            if node.role == ModelRole::Learner {
                assert!(
                    self.learners
                        .iter()
                        .any(|learner| std::ptr::eq(node, &self.nodes[*learner])),
                    "R-11 learner authority violated"
                );
            }
        }

        for (leader_id, leader) in self.nodes.iter().enumerate() {
            if leader.role != ModelRole::Leader {
                continue;
            }
            for index in 0..leader.commit {
                let entry = leader.log[index];
                for candidate in &self.nodes {
                    if candidate.role == ModelRole::Leader && candidate.term > leader.term {
                        assert_eq!(
                            candidate.log.get(index),
                            Some(&entry),
                            "R-03 leader completeness violated: {self:?}"
                        );
                    }
                }
            }
            let _ = leader_id;
        }
    }

    fn successors(&self) -> Vec<Self> {
        let mut out = Vec::new();
        for node in 0..self.nodes.len() {
            if let Some(next) = self.start_election(node) {
                out.push(next);
            }
            out.push(self.crash_or_restart(node));
            if let Some(next) = self.append_value(node, 1) {
                out.push(next);
            }
            if let Some(next) = self.maybe_commit(node) {
                out.push(next);
            }
        }
        for message in 0..self.messages.len() {
            out.push(self.deliver(message));
            let mut dropped = self.clone();
            dropped.messages.remove(message);
            out.push(dropped);
            if self.messages.len() < 6 {
                let mut duplicated = self.clone();
                duplicated.messages.push(self.messages[message].clone());
                out.push(duplicated);
            }
        }
        out
    }
}

fn last_position(log: &[ModelEntry]) -> (usize, u8) {
    (log.len(), log.last().map_or(0, |entry| entry.term))
}

fn majority(voters: &BTreeSet<usize>, members: &BTreeSet<usize>) -> bool {
    voters.iter().filter(|node| members.contains(node)).count() > voters.len() / 2
}

#[test]
fn bounded_formal_state_space_checks_essential_raft_safety() {
    let initial = ModelState::initial();
    let mut seen = BTreeSet::new();
    let mut frontier = VecDeque::from([(initial, 0_usize)]);
    let mut explored = 0_usize;

    while let Some((state, depth)) = frontier.pop_front() {
        if !seen.insert(state.clone()) {
            continue;
        }
        state.assert_invariants();
        explored += 1;
        if depth >= 7 || explored >= 40_000 {
            continue;
        }
        for next in state.successors() {
            for node in 0..state.nodes.len() {
                assert!(
                    next.nodes[node].commit >= state.nodes[node].commit,
                    "R-06 commit durability moved backward: before={state:?} after={next:?}"
                );
            }
            frontier.push_back((next, depth + 1));
        }
    }
    assert!(
        explored >= 1_000,
        "formal exploration was unexpectedly shallow"
    );
}

#[test]
fn joint_quorum_intersection_is_exhaustively_checked() {
    let old = BTreeSet::from([0, 1, 2]);
    let new = BTreeSet::from([2, 3, 4]);
    let universe = [0, 1, 2, 3, 4];
    let mut joint_quorums = Vec::new();
    for mask in 0_u32..(1 << universe.len()) {
        let set = universe
            .iter()
            .enumerate()
            .filter_map(|(bit, node)| ((mask & (1 << bit)) != 0).then_some(*node))
            .collect::<BTreeSet<_>>();
        if majority(&old, &set) && majority(&new, &set) {
            joint_quorums.push(set);
        }
    }
    for left in &joint_quorums {
        for right in &joint_quorums {
            assert!(
                !left.is_disjoint(right),
                "R-10 joint quorum intersection violated: {left:?} vs {right:?}"
            );
        }
    }
}

#[test]
fn crash_restart_preserves_term_vote_log_and_commit_boundaries() {
    let mut state = ModelState::initial();
    state.nodes[0].term = 2;
    state.nodes[0].durable_vote = Some(0);
    state.nodes[0].log = vec![ModelEntry { term: 2, value: 9 }];
    state.nodes[0].commit = 1;
    let durable_before = (
        state.nodes[0].term,
        state.nodes[0].durable_vote,
        state.nodes[0].log.clone(),
        state.nodes[0].commit,
    );
    state = state.crash_or_restart(0);
    state = state.crash_or_restart(0);
    assert_eq!(
        durable_before,
        (
            state.nodes[0].term,
            state.nodes[0].durable_vote,
            state.nodes[0].log.clone(),
            state.nodes[0].commit,
        )
    );
}

#[test]
fn durable_vote_cannot_change_candidate_within_same_term() {
    let mut state = ModelState::initial();
    state.nodes[0].term = 2;
    state.nodes[0].durable_vote = Some(1);
    state.messages.push(ModelMessage::VoteRequest {
        from: 2,
        to: 0,
        term: 2,
        last_term: 0,
        last_index: 0,
    });

    let next = state.deliver(0);
    assert_eq!(next.nodes[0].term, 2);
    assert_eq!(next.nodes[0].durable_vote, Some(1));
    assert!(next.messages.iter().any(|message| matches!(
        message,
        ModelMessage::VoteResponse {
            from: 0,
            to: 2,
            term: 2,
            granted: false,
        }
    )));
}

#[test]
fn minority_and_learner_sets_never_form_commit_quorum() {
    let state = ModelState::initial();
    assert!(!state.quorum(&BTreeSet::from([0])));
    let mut learner_state = state.clone();
    learner_state.learners.insert(2);
    learner_state.stable_voters.remove(&2);
    assert!(!learner_state.quorum(&BTreeSet::from([0, 2])));
}

#[test]
fn higher_term_message_fences_candidate_in_bounded_model() {
    let mut state = ModelState::initial();
    state.nodes[0].term = 1;
    state.nodes[0].durable_vote = Some(0);
    state.nodes[0].role = ModelRole::Candidate;
    state.messages.push(ModelMessage::VoteResponse {
        from: 1,
        to: 0,
        term: 2,
        granted: false,
    });
    let next = state.deliver(0);
    assert_eq!(next.nodes[0].term, 2);
    assert_eq!(next.nodes[0].durable_vote, None);
    assert_eq!(next.nodes[0].role, ModelRole::Follower);
}

#[test]
fn joint_transition_and_learner_promotion_are_checked_in_model() {
    let mut state = ModelState::initial();
    state.nodes.push(ModelNode {
        term: 1,
        durable_vote: None,
        role: ModelRole::Learner,
        log: vec![ModelEntry { term: 1, value: 7 }],
        commit: 0,
        crashed: false,
    });
    state.learners.insert(3);
    for node in 0..3 {
        state.nodes[node].term = 1;
        state.nodes[node].log = vec![ModelEntry { term: 1, value: 7 }];
        state.nodes[node].commit = 1;
    }
    state.nodes[3].commit = 1;

    let new_voters = BTreeSet::from([0, 1, 2, 3]);
    let joint = state
        .enter_joint(new_voters.clone())
        .expect("caught-up learner must be promotable through joint consensus");
    assert_eq!(
        joint.joint_voters,
        Some((BTreeSet::from([0, 1, 2]), new_voters.clone()))
    );
    assert!(joint.quorum(&BTreeSet::from([0, 1, 3])));
    assert!(!joint.quorum(&BTreeSet::from([0, 3])));

    let stable = joint.leave_joint().unwrap();
    assert_eq!(stable.stable_voters, new_voters);
    assert!(stable.joint_voters.is_none());
    assert!(!stable.learners.contains(&3));
    assert_ne!(stable.nodes[3].role, ModelRole::Learner);

    let mut behind = state;
    behind.nodes[3].log.clear();
    assert!(behind.enter_joint(BTreeSet::from([0, 1, 2, 3])).is_none());
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PersistenceFrontierModel {
    staged: Vec<u8>,
    durable_prefix: usize,
    completed_prefix: usize,
    failed: bool,
}

impl PersistenceFrontierModel {
    fn with_staged(depth: usize) -> Self {
        Self {
            staged: (1..=depth).map(|value| value as u8).collect(),
            durable_prefix: 0,
            completed_prefix: 0,
            failed: false,
        }
    }

    fn prove_sync_prefix(&mut self, prefix: usize) -> bool {
        if self.failed || prefix < self.durable_prefix || prefix > self.staged.len() {
            return false;
        }
        self.durable_prefix = prefix;
        true
    }

    fn complete_next(&mut self) -> Option<u8> {
        if self.failed || self.completed_prefix >= self.durable_prefix {
            return None;
        }
        let value = self.staged[self.completed_prefix];
        self.completed_prefix += 1;
        Some(value)
    }

    fn complete_token(&mut self, token: usize) -> bool {
        if token != self.completed_prefix + 1 || token > self.durable_prefix {
            return false;
        }
        self.completed_prefix += 1;
        true
    }

    fn fail_unproven_suffix(&mut self) {
        self.failed = true;
    }

    fn hard_barrier_allowed(&self) -> bool {
        !self.failed && self.completed_prefix == self.staged.len()
    }

    fn recovered_prefix(&self) -> Vec<u8> {
        self.staged[..self.durable_prefix].to_vec()
    }
}

#[test]
fn persistence_frontier_v2_bounded_model_preserves_prefix_durability_and_completion() {
    for depth in 0..=3 {
        for durable_prefix in 0..=depth {
            let mut model = PersistenceFrontierModel::with_staged(depth);
            assert!(model.prove_sync_prefix(durable_prefix));
            assert_eq!(
                model.recovered_prefix(),
                model.staged[..durable_prefix].to_vec()
            );

            let expected = model.staged[..durable_prefix].to_vec();
            let mut completed = Vec::new();
            while let Some(value) = model.complete_next() {
                completed.push(value);
                assert!(model.completed_prefix <= model.durable_prefix);
            }
            assert_eq!(completed, expected);
            assert_eq!(model.completed_prefix, durable_prefix);
            assert_eq!(model.hard_barrier_allowed(), durable_prefix == depth);
        }
    }
}

#[test]
fn persistence_frontier_v2_bounded_model_rejects_out_of_order_completion_and_ambiguous_success() {
    for depth in 1..=3 {
        let mut model = PersistenceFrontierModel::with_staged(depth);
        assert!(model.prove_sync_prefix(depth));
        if depth >= 2 {
            assert!(!model.complete_token(2));
        }
        assert!(model.complete_token(1));
        if depth >= 3 {
            assert!(!model.complete_token(3));
        }

        let delivered_before_failure = model.completed_prefix;
        model.fail_unproven_suffix();
        assert!(model.complete_next().is_none());
        assert_eq!(model.completed_prefix, delivered_before_failure);
        assert!(!model.hard_barrier_allowed());
    }

    for depth in 1..=3 {
        let mut before_sync = PersistenceFrontierModel::with_staged(depth);
        before_sync.fail_unproven_suffix();
        assert!(before_sync.recovered_prefix().is_empty());
        assert!(before_sync.complete_next().is_none());

        let mut after_sync = PersistenceFrontierModel::with_staged(depth);
        assert!(after_sync.prove_sync_prefix(depth));
        after_sync.fail_unproven_suffix();
        assert_eq!(after_sync.recovered_prefix(), after_sync.staged);
        assert!(after_sync.complete_next().is_none());
    }
}
