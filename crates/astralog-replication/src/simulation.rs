use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use astralog_common::{PartitionId, StreamOffset, TopicId};
use astralog_raft::{EntryKind, NodeId, ProposalId, Role, StableConfiguration, TimingConfig};
use astralog_storage::{DataRecord, PartitionLogConfig};

use crate::{
    AckRequirement, CommandLimits, OutboundMessage, ProposalOutcome, ReplicaLimits,
    ReplicatedPartition,
};

#[derive(Debug, Clone)]
struct Envelope {
    sequence: u64,
    from: NodeId,
    to: NodeId,
    message: astralog_raft::RaftMessage,
}

struct DiskNode {
    directory: PathBuf,
    timeout: u64,
    replica: Option<ReplicatedPartition>,
}

struct DiskSimulation {
    root: PathBuf,
    topic_id: TopicId,
    partition_id: PartitionId,
    configuration: StableConfiguration,
    timing: TimingConfig,
    replica_limits: ReplicaLimits,
    storage_config: PartitionLogConfig,
    nodes: BTreeMap<NodeId, DiskNode>,
    network: Vec<Envelope>,
    blocked: BTreeSet<(NodeId, NodeId)>,
    sequence: u64,
    clock: i64,
}

impl DiskSimulation {
    fn new(voters: usize, label: &str) -> Self {
        Self::new_with_limits(voters, label, ReplicaLimits::default())
    }

    fn new_with_limits(voters: usize, label: &str, replica_limits: ReplicaLimits) -> Self {
        Self::new_with_storage(voters, label, replica_limits, PartitionLogConfig::default())
    }

    fn new_with_storage(
        voters: usize,
        label: &str,
        replica_limits: ReplicaLimits,
        storage_config: PartitionLogConfig,
    ) -> Self {
        assert!(matches!(voters, 1 | 3 | 5));
        let root = temp_dir(label);
        let topic_id = TopicId::new([0x55; 16]);
        let partition_id = PartitionId::new(0);
        let members = (1..=voters as u64).map(node).collect::<Vec<_>>();
        let configuration = StableConfiguration::new(members.iter().copied(), []).unwrap();
        let timing = TimingConfig::new(1, 5, 10).unwrap();
        let mut nodes = BTreeMap::new();
        for member in members {
            let directory = root.join(format!("node-{}", member.get()));
            fs::create_dir_all(&directory).unwrap();
            let timeout = 4 + member.get();
            let replica = open_replica(
                &directory,
                topic_id,
                partition_id,
                member,
                configuration.clone(),
                timing,
                timeout,
                replica_limits,
                storage_config,
                0,
            );
            nodes.insert(
                member,
                DiskNode {
                    directory,
                    timeout,
                    replica: Some(replica),
                },
            );
        }
        Self {
            root,
            topic_id,
            partition_id,
            configuration,
            timing,
            replica_limits,
            storage_config,
            nodes,
            network: Vec::new(),
            blocked: BTreeSet::new(),
            sequence: 0,
            clock: 0,
        }
    }

    fn tick_node(&mut self, member: NodeId) {
        self.clock += 1;
        let timeout = self.nodes[&member].timeout;
        if let Some(replica) = self.nodes.get_mut(&member).unwrap().replica.as_mut() {
            replica.tick(timeout, self.clock).unwrap();
            self.collect_outbound(member);
        }
    }

    fn tick_all(&mut self) {
        let members = self.nodes.keys().copied().collect::<Vec<_>>();
        for member in members {
            self.tick_node(member);
        }
    }

    fn collect_outbound(&mut self, from: NodeId) {
        let outgoing = self
            .nodes
            .get_mut(&from)
            .and_then(|node| node.replica.as_mut())
            .map(|replica| replica.drain_outbound().collect::<Vec<_>>())
            .unwrap_or_default();
        for OutboundMessage { target, message } in outgoing {
            self.sequence += 1;
            self.network.push(Envelope {
                sequence: self.sequence,
                from,
                to: target,
                message,
            });
        }
    }

    fn deliver_next(&mut self) -> bool {
        if self.network.is_empty() {
            return false;
        }
        let index = self
            .network
            .iter()
            .enumerate()
            .min_by_key(|(_, envelope)| envelope.sequence)
            .map(|(index, _)| index)
            .unwrap();
        let envelope = self.network.remove(index);
        if self.blocked.contains(&(envelope.from, envelope.to)) {
            return true;
        }
        let Some(replica) = self
            .nodes
            .get_mut(&envelope.to)
            .and_then(|node| node.replica.as_mut())
        else {
            return true;
        };
        if replica.is_fatal() {
            return true;
        }
        self.clock += 1;
        replica
            .step(envelope.from, envelope.message, self.clock)
            .unwrap();
        self.collect_outbound(envelope.to);
        true
    }

    fn drain_network(&mut self, limit: usize) {
        for _ in 0..limit {
            if !self.deliver_next() {
                return;
            }
        }
        panic!("P4 deterministic network did not quiesce");
    }

    fn force_election(&mut self, candidate: NodeId) {
        let timeout = self.nodes[&candidate].timeout;
        for _ in 0..timeout {
            self.tick_node(candidate);
        }
        self.drain_network(4096);
        assert_eq!(self.replica(candidate).role(), Role::Leader);
    }

    fn leader(&self) -> Option<NodeId> {
        self.nodes.iter().find_map(|(member, node)| {
            node.replica
                .as_ref()
                .is_some_and(|replica| replica.role() == Role::Leader)
                .then_some(*member)
        })
    }

    fn elect_any_live(&mut self, rounds: usize) -> NodeId {
        for _ in 0..rounds {
            self.tick_all();
            self.drain_network(8192);
            if let Some(leader) = self.leader() {
                return leader;
            }
        }
        panic!("P4 deterministic cluster failed to elect a live leader");
    }

    fn propose(
        &mut self,
        leader: NodeId,
        value: &[u8],
        ack: AckRequirement,
    ) -> (ProposalId, StreamOffset) {
        self.clock += 1;
        let receipt = self
            .nodes
            .get_mut(&leader)
            .unwrap()
            .replica
            .as_mut()
            .unwrap()
            .propose_records(vec![record(value)], ack, self.clock)
            .unwrap();
        self.collect_outbound(leader);
        (receipt.proposal_id, receipt.base_offset)
    }

    fn crash(&mut self, member: NodeId) {
        self.nodes.get_mut(&member).unwrap().replica = None;
        self.network
            .retain(|envelope| envelope.from != member && envelope.to != member);
    }

    fn restart(&mut self, member: NodeId) {
        let node = self.nodes.get_mut(&member).unwrap();
        assert!(node.replica.is_none());
        node.replica = Some(open_replica(
            &node.directory,
            self.topic_id,
            self.partition_id,
            member,
            self.configuration.clone(),
            self.timing,
            node.timeout,
            self.replica_limits,
            self.storage_config,
            self.clock,
        ));
        self.collect_outbound(member);
    }

    fn isolate(&mut self, member: NodeId) {
        for peer in self.nodes.keys().copied().collect::<Vec<_>>() {
            if peer != member {
                self.blocked.insert((member, peer));
                self.blocked.insert((peer, member));
            }
        }
    }

    fn heal(&mut self) {
        self.blocked.clear();
    }

    fn replica(&self, member: NodeId) -> &ReplicatedPartition {
        self.nodes[&member].replica.as_ref().unwrap()
    }

    fn replica_mut(&mut self, member: NodeId) -> &mut ReplicatedPartition {
        self.nodes
            .get_mut(&member)
            .unwrap()
            .replica
            .as_mut()
            .unwrap()
    }

    fn assert_committed_prefix_safety(&self) {
        let live = self
            .nodes
            .values()
            .filter_map(|node| node.replica.as_ref())
            .map(|replica| replica.recovered_state().unwrap())
            .collect::<Vec<_>>();
        for left in 0..live.len() {
            for right in left + 1..live.len() {
                let upto = std::cmp::min(
                    live[left].hard_state.durable_commit_index,
                    live[right].hard_state.durable_commit_index,
                );
                for raw in 1..=upto.get() {
                    let index = astralog_raft::LogIndex::new(raw);
                    let a = live[left].log.iter().find(|entry| entry.index == index);
                    let b = live[right].log.iter().find(|entry| entry.index == index);
                    if let (Some(a), Some(b)) = (a, b) {
                        assert_eq!((a.term, &a.kind), (b.term, &b.kind));
                    }
                }
            }
        }
    }

    fn cleanup(self) {
        fs::remove_dir_all(self.root).unwrap();
    }
}

#[allow(clippy::too_many_arguments)]
fn open_replica(
    directory: &Path,
    topic_id: TopicId,
    partition_id: PartitionId,
    local_node: NodeId,
    configuration: StableConfiguration,
    timing: TimingConfig,
    timeout: u64,
    replica_limits: ReplicaLimits,
    storage_config: PartitionLogConfig,
    now_ms: i64,
) -> ReplicatedPartition {
    ReplicatedPartition::open(
        directory,
        topic_id,
        partition_id,
        local_node,
        now_ms,
        storage_config,
        configuration,
        timing,
        timeout,
        CommandLimits::default(),
        replica_limits,
    )
    .unwrap()
}

fn temp_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "astralog-p4-disk-sim-{label}-{}-{nanos}",
        std::process::id()
    ));
    fs::create_dir_all(&path).unwrap();
    path
}

fn node(value: u64) -> NodeId {
    NodeId::new(value).unwrap()
}

fn record(value: &[u8]) -> DataRecord {
    DataRecord {
        timestamp_ms: 1,
        key: None,
        value: Some(value.to_vec()),
        headers: Vec::new(),
    }
}

#[test]
fn rf3_quorum_ack_waits_for_commit_and_committed_fetch_hides_local_tail() {
    let mut sim = DiskSimulation::new(3, "rf3-quorum");
    sim.force_election(node(1));
    let (proposal, _) = sim.propose(node(1), b"quorum", AckRequirement::Quorum);
    assert_eq!(
        sim.replica(node(1)).proposal_outcome(proposal),
        Some(ProposalOutcome::Pending)
    );
    assert!(
        sim.replica(node(1))
            .read_committed_records(StreamOffset::new(0), 10)
            .unwrap()
            .is_empty()
    );

    sim.drain_network(4096);
    assert_eq!(
        sim.replica(node(1)).proposal_outcome(proposal),
        Some(ProposalOutcome::QuorumCommitted)
    );
    assert_eq!(
        sim.replica(node(1))
            .read_committed_records(StreamOffset::new(0), 10)
            .unwrap()[0]
            .record
            .value
            .as_deref(),
        Some(b"quorum".as_slice())
    );
    sim.assert_committed_prefix_safety();
    sim.cleanup();
}

#[test]
fn minority_isolated_leader_cannot_quorum_commit() {
    let mut sim = DiskSimulation::new(3, "minority");
    sim.force_election(node(1));
    sim.isolate(node(1));
    let (proposal, _) = sim.propose(node(1), b"minority", AckRequirement::Quorum);
    sim.drain_network(512);
    assert_eq!(
        sim.replica(node(1)).proposal_outcome(proposal),
        Some(ProposalOutcome::Pending)
    );
    assert!(
        sim.replica(node(1))
            .read_committed_records(StreamOffset::new(0), 10)
            .unwrap()
            .is_empty()
    );
    for _ in 0..12 {
        sim.tick_all();
        sim.drain_network(1024);
    }
    assert_ne!(sim.replica(node(1)).role(), Role::Leader);
    assert_eq!(
        sim.replica(node(1)).proposal_outcome(proposal),
        Some(ProposalOutcome::LeadershipLost)
    );
    sim.assert_committed_prefix_safety();
    sim.cleanup();
}

#[test]
fn leader_only_acknowledged_tail_can_be_lost_after_failover() {
    let mut sim = DiskSimulation::new(3, "leader-ack-loss");
    sim.force_election(node(1));
    sim.isolate(node(1));
    let (proposal, _) = sim.propose(node(1), b"leader-only", AckRequirement::Leader);
    assert_eq!(
        sim.replica(node(1)).proposal_outcome(proposal),
        Some(ProposalOutcome::LocalDurable)
    );
    assert!(
        sim.replica(node(1))
            .read_committed_records(StreamOffset::new(0), 10)
            .unwrap()
            .is_empty()
    );
    sim.crash(node(1));
    sim.heal();
    let new_leader = sim.elect_any_live(20);
    assert_ne!(new_leader, node(1));
    sim.restart(node(1));
    for _ in 0..4 {
        sim.tick_node(new_leader);
        sim.drain_network(4096);
    }
    assert!(
        sim.replica(node(1))
            .recovered_state()
            .unwrap()
            .log
            .iter()
            .all(|entry| !matches!(
                &entry.kind,
                EntryKind::Application(payload) if payload.windows(b"leader-only".len()).any(|window| window == b"leader-only")
            ))
    );
    sim.assert_committed_prefix_safety();
    sim.cleanup();
}

#[test]
fn lagging_follower_rejoins_and_catches_up_from_real_p1_storage() {
    let mut sim = DiskSimulation::new(3, "lagging-rejoin");
    sim.force_election(node(1));
    sim.isolate(node(3));
    for value in [b"a".as_slice(), b"b".as_slice(), b"c".as_slice()] {
        let (proposal, _) = sim.propose(node(1), value, AckRequirement::Quorum);
        sim.drain_network(4096);
        assert_eq!(
            sim.replica(node(1)).proposal_outcome(proposal),
            Some(ProposalOutcome::QuorumCommitted)
        );
    }
    assert!(
        sim.replica(node(3))
            .read_committed_records(StreamOffset::new(0), 10)
            .unwrap()
            .is_empty()
    );
    sim.heal();
    for _ in 0..5 {
        sim.tick_node(node(1));
        sim.drain_network(4096);
    }
    let values = sim
        .replica(node(3))
        .read_committed_records(StreamOffset::new(0), 10)
        .unwrap();
    assert_eq!(values.len(), 3);
    sim.assert_committed_prefix_safety();
    sim.cleanup();
}

#[test]
fn p8f_slow_follower_sustained_writes_and_restart_catchup_stay_bounded() {
    let limits = ReplicaLimits {
        max_replication_inflight_messages_per_follower: 2,
        max_replication_inflight_bytes_per_follower: 4096,
        max_replication_catchup_bytes_per_message: 1024,
        ..ReplicaLimits::default()
    };
    let mut sim = DiskSimulation::new_with_limits(3, "p8f-slow-reconnect", limits);
    sim.force_election(node(1));
    sim.isolate(node(3));

    for ordinal in 0..48_u8 {
        let value = vec![ordinal; 32];
        let (proposal, _) = sim.propose(node(1), &value, AckRequirement::Quorum);
        sim.drain_network(4096);
        assert_eq!(
            sim.replica(node(1)).proposal_outcome(proposal),
            Some(ProposalOutcome::QuorumCommitted)
        );
        let (messages, bytes) = sim.replica(node(1)).replication_window_usage(node(3));
        assert!(messages <= limits.max_replication_inflight_messages_per_follower);
        assert!(bytes <= limits.max_replication_inflight_bytes_per_follower);
    }

    assert!(
        sim.replica(node(3))
            .read_committed_records(StreamOffset::new(0), 100)
            .unwrap()
            .is_empty()
    );
    sim.crash(node(3));
    sim.heal();
    sim.restart(node(3));

    for _ in 0..32 {
        sim.tick_node(node(1));
        sim.drain_network(8192);
        if sim
            .replica(node(3))
            .read_committed_records(StreamOffset::new(0), 100)
            .unwrap()
            .len()
            == 48
        {
            break;
        }
    }
    let recovered = sim
        .replica(node(3))
        .read_committed_records(StreamOffset::new(0), 100)
        .unwrap();
    assert_eq!(recovered.len(), 48);
    assert_eq!(
        recovered[47].record.value.as_deref(),
        Some(&vec![47; 32][..])
    );
    sim.assert_committed_prefix_safety();
    sim.cleanup();
}

#[test]
fn p8g_combined_producer_follower_consumer_pressure_preserves_control_progress() {
    let limits = ReplicaLimits {
        max_replication_inflight_messages_per_follower: 2,
        max_replication_inflight_bytes_per_follower: 4096,
        max_replication_catchup_bytes_per_message: 1024,
        ..ReplicaLimits::default()
    };
    let mut sim = DiskSimulation::new_with_limits(3, "p8g-combined-pressure", limits);
    sim.force_election(node(1));
    sim.isolate(node(3));

    for ordinal in 0..48_u8 {
        let (proposal, _) = sim.propose(node(1), &[ordinal; 64], AckRequirement::Quorum);
        sim.drain_network(4096);
        assert_eq!(
            sim.replica(node(1)).proposal_outcome(proposal),
            Some(ProposalOutcome::QuorumCommitted)
        );

        let (page, used_bytes) = sim
            .replica(node(1))
            .read_committed_records_bounded(StreamOffset::new(0), 1, 128)
            .unwrap();
        assert!(page.len() <= 1);
        assert!(used_bytes <= 128);

        let (messages, bytes) = sim.replica(node(1)).replication_window_usage(node(3));
        assert!(messages <= limits.max_replication_inflight_messages_per_follower);
        assert!(bytes <= limits.max_replication_inflight_bytes_per_follower);

        sim.tick_node(node(1));
        assert_eq!(sim.replica(node(1)).role(), Role::Leader);
        assert!(!sim.replica(node(1)).is_fatal());
    }

    sim.heal();
    for _ in 0..32 {
        sim.tick_node(node(1));
        sim.drain_network(8192);
        if sim
            .replica(node(3))
            .read_committed_records(StreamOffset::new(0), 100)
            .unwrap()
            .len()
            == 48
        {
            break;
        }
    }
    assert_eq!(
        sim.replica(node(3))
            .read_committed_records(StreamOffset::new(0), 100)
            .unwrap()
            .len(),
        48
    );
    sim.assert_committed_prefix_safety();
    sim.cleanup();
}

#[test]
fn rf5_quorum_commit_survives_two_replica_failures() {
    let mut sim = DiskSimulation::new(5, "rf5-minority-failure");
    sim.force_election(node(1));
    let (proposal, _) = sim.propose(node(1), b"rf5", AckRequirement::Quorum);
    sim.drain_network(8192);
    assert_eq!(
        sim.replica(node(1)).proposal_outcome(proposal),
        Some(ProposalOutcome::QuorumCommitted)
    );
    sim.crash(node(1));
    sim.crash(node(2));
    let new_leader = sim.elect_any_live(30);
    assert!(matches!(new_leader.get(), 3..=5));
    sim.drain_network(8192);
    let visible = sim
        .replica(new_leader)
        .read_committed_records(StreamOffset::new(0), 10)
        .unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].record.value.as_deref(), Some(b"rf5".as_slice()));
    sim.assert_committed_prefix_safety();
    sim.cleanup();
}

#[test]
fn leader_disk_failure_is_fatal_and_never_fabricates_quorum_success() {
    let mut sim = DiskSimulation::new(3, "leader-disk-failure");
    sim.force_election(node(1));
    sim.replica_mut(node(1))
        .inject_persistence_failure_once("leader fsync failed");

    let (proposal, _) = sim.propose(node(1), b"must-not-commit", AckRequirement::Quorum);
    assert!(sim.replica(node(1)).is_fatal());
    assert_eq!(
        sim.replica(node(1)).proposal_outcome(proposal),
        Some(ProposalOutcome::Pending)
    );
    assert!(
        sim.replica(node(1))
            .read_committed_records(StreamOffset::new(0), 10)
            .unwrap()
            .is_empty()
    );

    sim.crash(node(1));
    let new_leader = sim.elect_any_live(30);
    assert_ne!(new_leader, node(1));
    assert!(
        sim.replica(new_leader)
            .read_committed_records(StreamOffset::new(0), 10)
            .unwrap()
            .is_empty()
    );
    sim.assert_committed_prefix_safety();
    sim.cleanup();
}

#[test]
fn follower_disk_failure_cannot_ack_but_surviving_majority_commits() {
    let mut sim = DiskSimulation::new(3, "follower-disk-failure");
    sim.force_election(node(1));
    sim.replica_mut(node(3))
        .inject_persistence_failure_once("follower fsync failed");

    let (proposal, _) = sim.propose(node(1), b"majority-survives", AckRequirement::Quorum);
    sim.drain_network(4096);
    assert_eq!(
        sim.replica(node(1)).proposal_outcome(proposal),
        Some(ProposalOutcome::QuorumCommitted)
    );
    assert!(sim.replica(node(3)).is_fatal());
    assert_eq!(
        sim.replica(node(1))
            .read_committed_records(StreamOffset::new(0), 10)
            .unwrap()
            .len(),
        1
    );
    sim.assert_committed_prefix_safety();
    sim.cleanup();
}

#[test]
fn leader_crash_before_local_persistence_leaves_no_record_to_recover() {
    let mut sim = DiskSimulation::new(3, "leader-crash-before-persist");
    sim.force_election(node(1));
    sim.replica_mut(node(1))
        .hold_next_persistence_before_apply();

    let (proposal, _) = sim.propose(node(1), b"never-fsynced", AckRequirement::Quorum);
    assert_eq!(
        sim.replica(node(1)).proposal_outcome(proposal),
        Some(ProposalOutcome::Pending)
    );
    assert!(
        sim.replica(node(1))
            .recovered_state()
            .unwrap()
            .log
            .iter()
            .all(|entry| !matches!(&entry.kind, EntryKind::Application(_)))
    );

    sim.crash(node(1));
    let new_leader = sim.elect_any_live(30);
    assert!(
        sim.replica(new_leader)
            .read_committed_records(StreamOffset::new(0), 10)
            .unwrap()
            .is_empty()
    );
    sim.cleanup();
}

#[test]
fn durable_local_append_without_p3_completion_is_repaired_after_failover() {
    let mut sim = DiskSimulation::new(3, "leader-crash-after-fsync-before-complete");
    sim.force_election(node(1));
    sim.replica_mut(node(1))
        .hold_next_persistence_after_apply_before_completion();
    let (proposal, _) = sim.propose(node(1), b"durable-unacked", AckRequirement::Quorum);
    assert_eq!(
        sim.replica(node(1)).proposal_outcome(proposal),
        Some(ProposalOutcome::Pending)
    );
    assert!(
        sim.replica(node(1))
            .recovered_state()
            .unwrap()
            .log
            .iter()
            .any(|entry| matches!(&entry.kind, EntryKind::Application(_)))
    );

    sim.crash(node(1));
    let new_leader = sim.elect_any_live(30);
    let (replacement, _) = sim.propose(new_leader, b"replacement", AckRequirement::Quorum);
    sim.drain_network(4096);
    assert_eq!(
        sim.replica(new_leader).proposal_outcome(replacement),
        Some(ProposalOutcome::QuorumCommitted)
    );
    sim.restart(node(1));
    for _ in 0..6 {
        sim.tick_node(new_leader);
        sim.drain_network(4096);
    }
    let repaired = sim
        .replica(node(1))
        .read_committed_records(StreamOffset::new(0), 10)
        .unwrap();
    assert_eq!(repaired.len(), 1);
    assert_eq!(
        repaired[0].record.value.as_deref(),
        Some(b"replacement".as_slice())
    );
    sim.assert_committed_prefix_safety();
    sim.cleanup();
}

#[test]
fn committed_response_loss_and_retry_is_explicitly_non_idempotent_until_p7() {
    let mut sim = DiskSimulation::new(3, "lost-response-duplicate");
    sim.force_election(node(1));
    let old_term = sim.replica(node(1)).leader_epoch();
    let (first, _) = sim.propose(node(1), b"same-request", AckRequirement::Quorum);
    sim.drain_network(4096);
    assert_eq!(
        sim.replica(node(1)).proposal_outcome(first),
        Some(ProposalOutcome::QuorumCommitted)
    );

    // Model a lost producer response by never consuming the completed result,
    // then lose the leader and retry the same logical request on its successor.
    sim.crash(node(1));
    let new_leader = sim.elect_any_live(30);
    assert!(sim.replica(new_leader).leader_epoch() > old_term);
    let (retry, _) = sim.propose(new_leader, b"same-request", AckRequirement::Quorum);
    sim.drain_network(4096);
    assert_eq!(
        sim.replica(new_leader).proposal_outcome(retry),
        Some(ProposalOutcome::QuorumCommitted)
    );
    let visible = sim
        .replica(new_leader)
        .read_committed_records(StreamOffset::new(0), 10)
        .unwrap();
    assert_eq!(visible.len(), 2);
    assert!(
        visible
            .iter()
            .all(|record| { record.record.value.as_deref() == Some(b"same-request".as_slice()) })
    );
    sim.assert_committed_prefix_safety();
    sim.cleanup();
}

#[test]
fn stale_old_term_append_entries_after_failover_cannot_regress_new_leader() {
    let mut sim = DiskSimulation::new(3, "stale-internal-after-reconnect");
    sim.force_election(node(1));
    let old_term = sim.replica(node(1)).leader_epoch();
    let (proposal, _) = sim.propose(node(1), b"committed", AckRequirement::Quorum);
    sim.drain_network(4096);
    assert_eq!(
        sim.replica(node(1)).proposal_outcome(proposal),
        Some(ProposalOutcome::QuorumCommitted)
    );
    sim.crash(node(1));
    let new_leader = sim.elect_any_live(30);
    let new_term = sim.replica(new_leader).leader_epoch();
    let before = sim.replica(new_leader).recovered_state().unwrap();
    sim.clock += 1;
    let now = sim.clock;
    sim.replica_mut(new_leader)
        .step(
            node(1),
            astralog_raft::RaftMessage::AppendEntriesRequest(astralog_raft::AppendEntriesRequest {
                term: astralog_raft::Term::new(old_term),
                leader_id: node(1),
                prev_log_index: astralog_raft::LogIndex::ZERO,
                prev_log_term: astralog_raft::Term::ZERO,
                entries: Vec::new(),
                leader_commit: astralog_raft::LogIndex::ZERO,
            }),
            now,
        )
        .unwrap();
    assert_eq!(sim.replica(new_leader).role(), Role::Leader);
    assert_eq!(sim.replica(new_leader).leader_epoch(), new_term);
    assert_eq!(sim.replica(new_leader).recovered_state().unwrap(), before);
    sim.cleanup();
}

#[test]
fn list_offsets_latest_uses_committed_visibility_not_local_tail_or_control_entries() {
    let mut sim = DiskSimulation::new(3, "list-offsets-committed");
    sim.force_election(node(1));
    assert_eq!(sim.replica(node(1)).list_committed_offset(-1).unwrap().0, 0);
    sim.isolate(node(1));
    let (proposal, _) = sim.propose(node(1), b"local-only", AckRequirement::Leader);
    assert_eq!(
        sim.replica(node(1)).proposal_outcome(proposal),
        Some(ProposalOutcome::LocalDurable)
    );
    assert_eq!(sim.replica(node(1)).next_stream_offset().get(), 1);
    assert_eq!(sim.replica(node(1)).list_committed_offset(-1).unwrap().0, 0);
    sim.cleanup();
}

#[test]
fn bounded_outbox_backpressures_new_producers_without_killing_raft_control() {
    let limits = ReplicaLimits {
        max_outbound_messages: 1,
        max_outbound_bytes: 1024 * 1024,
        ..ReplicaLimits::default()
    };
    let mut sim = DiskSimulation::new_with_limits(3, "outbox-backpressure", limits);
    sim.force_election(node(1));

    sim.clock += 1;
    let first_now = sim.clock;
    let first = sim
        .replica_mut(node(1))
        .propose_records(vec![record(b"first")], AckRequirement::Leader, first_now)
        .unwrap();
    assert_eq!(
        sim.replica(node(1)).proposal_outcome(first.proposal_id),
        Some(ProposalOutcome::LocalDurable)
    );

    sim.clock += 1;
    let second_now = sim.clock;
    assert!(matches!(
        sim.replica_mut(node(1)).propose_records(
            vec![record(b"second")],
            AckRequirement::Leader,
            second_now,
        ),
        Err(crate::ReplicationError::Busy(_))
    ));

    // Draining the bounded outbox clears admission backpressure. Logical ticks
    // and Raft retry remain live rather than turning saturation into a fatal node.
    sim.collect_outbound(node(1));
    sim.drain_network(4096);
    sim.tick_node(node(1));
    sim.drain_network(4096);
    assert_eq!(sim.replica(node(1)).role(), Role::Leader);
    assert!(!sim.replica(node(1)).is_fatal());
    sim.cleanup();
}

#[test]
fn p9g_rf3_p3_naturally_requests_v2_snapshot_after_sparse_clean_and_follower_disk_loss() {
    use crate::{
        RebootstrapLimitsV2, decode_rebootstrap_package_v2, encode_rebootstrap_package_v2,
    };
    use astralog_raft::{LogIndex, RaftMessage};
    use astralog_storage::CompactionLimits;

    let mut sim = DiskSimulation::new_with_storage(
        3,
        "p9g-forced-p3-v2",
        ReplicaLimits::default(),
        PartitionLogConfig {
            max_segment_bytes: 320,
            max_segment_age_ms: None,
            index_interval_bytes: 1,
            limits: astralog_storage::StorageLimits::default(),
        },
    );
    sim.force_election(node(1));
    let keyed = |value: u8| DataRecord {
        timestamp_ms: 1,
        key: Some(b"single-key".to_vec()),
        value: Some(vec![value; 220]),
        headers: Vec::new(),
    };
    let leader = node(1);
    let now = sim.clock + 1;
    sim.replica_mut(leader)
        .propose_records(vec![keyed(b'a')], AckRequirement::Quorum, now)
        .unwrap();
    sim.collect_outbound(leader);
    sim.drain_network(8192);
    assert_eq!(
        sim.replica(node(3)).next_stream_offset(),
        StreamOffset::new(1)
    );
    sim.crash(node(3));

    let now = sim.clock + 1;
    sim.replica_mut(leader)
        .propose_records(vec![keyed(b'b')], AckRequirement::Quorum, now)
        .unwrap();
    sim.collect_outbound(leader);
    sim.drain_network(8192);
    let expected_commit = sim.replica(leader).durable_commit_index();
    let term = sim.replica(leader).leader_epoch();
    let snapshot_now = sim.clock + 1;
    assert!(
        sim.replica_mut(leader)
            .snapshot_committed_prefix_for_retention(term, snapshot_now)
            .unwrap()
            >= expected_commit
    );
    let planned = sim
        .replica_mut(leader)
        .plan_keyed_compaction(73, CompactionLimits::default())
        .unwrap();
    assert!(
        !planned.is_empty(),
        "source must contain a closed superseded keyed segment"
    );
    sim.replica_mut(leader)
        .execute_keyed_compaction(&planned, 73)
        .unwrap();
    assert!(sim.replica(leader).manifest_generation().is_some());
    assert_eq!(
        sim.replica(leader)
            .read_committed_records(StreamOffset::new(0), 10)
            .unwrap()
            .len(),
        1
    );

    let dead_dir = sim.nodes[&node(3)].directory.clone();
    fs::remove_dir_all(&dead_dir).unwrap();
    fs::create_dir_all(&dead_dir).unwrap();
    sim.restart(node(3));
    assert_eq!(sim.replica(node(3)).durable_commit_index(), LogIndex::ZERO);

    let mut v2_installations = 0_usize;
    for _ in 0..8192 {
        if sim.network.is_empty() {
            sim.tick_node(leader);
        }
        if sim.network.is_empty() {
            continue;
        }
        let envelope = sim.network.remove(0);
        if sim.blocked.contains(&(envelope.from, envelope.to))
            || sim.nodes[&envelope.to].replica.is_none()
        {
            continue;
        }
        sim.clock += 1;
        let now = sim.clock;
        if let RaftMessage::InstallSnapshotRequest(request) = &envelope.message {
            let limits = RebootstrapLimitsV2::default();
            let package = sim
                .replica(envelope.from)
                .build_rebootstrap_package_v2(&request.snapshot, limits)
                .unwrap();
            let bytes = encode_rebootstrap_package_v2(&package, limits).unwrap();
            assert!(bytes.starts_with(b"ASTRBTP2"));
            let decoded = decode_rebootstrap_package_v2(&bytes, limits).unwrap();
            sim.replica_mut(envelope.to)
                .step_with_rebootstrap_v2(envelope.from, envelope.message, decoded, now)
                .unwrap();
            v2_installations += 1;
        } else {
            sim.replica_mut(envelope.to)
                .step(envelope.from, envelope.message, now)
                .unwrap();
        }
        sim.collect_outbound(envelope.to);
        if v2_installations > 0 && sim.replica(node(3)).durable_commit_index() >= expected_commit {
            break;
        }
    }
    assert!(
        v2_installations > 0,
        "P3 must naturally request snapshot after empty follower cannot match retained prefix"
    );
    assert!(sim.replica(node(3)).manifest_generation().is_some());
    assert_eq!(sim.replica(node(3)).durable_commit_index(), expected_commit);
    let recovered = sim
        .replica(node(3))
        .read_committed_records(StreamOffset::new(0), 10)
        .unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(
        recovered[0].record.value.as_deref(),
        Some(vec![b'b'; 220].as_slice())
    );
    // A V2 installation must not leave the recovered follower stuck on a
    // historical generation: ordinary P3 V1 suffix replication and subsequent
    // replay must use the snapshot's next Raft index and next stream offset.
    sim.network.clear(); // Drop stale pre-install envelopes from the old P3 probe round.
    let now = sim.clock + 1;
    let suffix = sim
        .replica_mut(leader)
        .propose_records(
            vec![record(b"post-v2-v1-suffix")],
            AckRequirement::Quorum,
            now,
        )
        .unwrap();
    sim.collect_outbound(leader);
    sim.drain_network(8192);
    assert_eq!(
        sim.replica(leader).proposal_outcome(suffix.proposal_id),
        Some(ProposalOutcome::QuorumCommitted)
    );
    assert_eq!(
        sim.replica(node(3)).durable_commit_index(),
        sim.replica(leader).durable_commit_index()
    );
    let retained = sim
        .replica(node(3))
        .read_committed_records(StreamOffset::new(0), 10)
        .unwrap();
    assert_eq!(retained.len(), 2);
    assert_eq!(
        retained[0].record.value.as_deref(),
        Some(vec![b'b'; 220].as_slice())
    );
    assert_eq!(
        retained[1].record.value.as_deref(),
        Some(&b"post-v2-v1-suffix"[..])
    );

    sim.crash(node(3));
    sim.restart(node(3));
    assert!(sim.replica(node(3)).manifest_generation().is_some());
    assert_eq!(
        sim.replica(node(3))
            .read_committed_records(StreamOffset::new(0), 10,)
            .unwrap()
            .len(),
        2
    );
    sim.crash(leader);
    let successor = sim.elect_any_live(40);
    assert_ne!(successor, leader);
    let now = sim.clock + 1;
    let after_failover = sim
        .replica_mut(successor)
        .propose_records(
            vec![record(b"after-v2-failover")],
            AckRequirement::Quorum,
            now,
        )
        .unwrap();
    sim.collect_outbound(successor);
    sim.drain_network(8192);
    assert_eq!(
        sim.replica(successor)
            .proposal_outcome(after_failover.proposal_id),
        Some(ProposalOutcome::QuorumCommitted)
    );
    let visible = sim
        .replica(node(3))
        .read_committed_records(StreamOffset::new(0), 10)
        .unwrap();
    assert_eq!(visible.len(), 3);
    assert!(
        visible
            .iter()
            .all(|item| item.record.value.as_deref() != Some(vec![b'a'; 220].as_slice())),
        "obsolete keyed value may not resurrect after V2 rebootstrap and RF3 leadership change"
    );
    sim.cleanup();
}
/// P9-G transport identity alone is not sufficient: in a real P3 RF3 group,
/// an offline voter with no local clean proof blocks any positive safe floor,
/// even when the remaining majority commits ordinary data and elects a new term.
#[test]
fn p9g_rf3_missing_voter_progress_cannot_commit_safe_offset_across_failover() {
    use crate::{CleanProgressReport, CleanProgressTracker, VoterIdentity};

    let mut sim = DiskSimulation::new(3, "p9g-rf3-missing-clean-voter");
    let original_leader = node(1);
    sim.force_election(original_leader);
    let now = sim.clock + 1;
    let write = sim
        .replica_mut(original_leader)
        .propose_records(vec![record(b"quorum-data")], AckRequirement::Quorum, now)
        .unwrap();
    sim.collect_outbound(original_leader);
    sim.drain_network(4096);
    assert_eq!(
        sim.replica(original_leader)
            .proposal_outcome(write.proposal_id),
        Some(ProposalOutcome::QuorumCommitted)
    );
    let high = sim
        .replica(original_leader)
        .committed_stream_high_watermark()
        .unwrap();
    assert_eq!(high, StreamOffset::new(1));

    let term = sim.replica(original_leader).leader_epoch();
    let effective = sim
        .replica(original_leader)
        .membership_observation()
        .effective_configuration;
    let identities = (1..=3_u64)
        .map(|id| {
            (
                node(id),
                VoterIdentity {
                    broker_id: u32::try_from(id - 1).unwrap(),
                    incarnation: 9,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut reports = CleanProgressTracker::from_committed_assignment(
        sim.topic_id,
        sim.partition_id,
        term,
        7,
        &effective,
        &identities,
    )
    .unwrap();
    // These two reports are deliberately optimistic adversarial test inputs.
    // No test-created report supplies real local cleaning authority, and a
    // third eligible voter remains absent. A majority must NEVER suffice.
    for id in 1..=2_u64 {
        let broker_id = u32::try_from(id - 1).unwrap();
        let report = CleanProgressReport {
            topic_id: sim.topic_id,
            partition_id: sim.partition_id,
            term,
            leader_epoch: term,
            source_broker_id: broker_id,
            source_incarnation: 9,
            assignment_generation: 7,
            clean_compacted_offset: high,
        };
        assert!(reports.accept(node(id), broker_id, 9, report, high));
    }
    assert_eq!(reports.report_count(), 2);
    assert_eq!(reports.required_voter_count(), 3);
    assert_eq!(reports.candidate_safe_offset(), None);
    assert!(
        sim.replica_mut(original_leader)
            .propose_compaction_safe_from_reports(term, 7, &reports, now + 1)
            .unwrap()
            .is_none(),
        "all-voter progress, not majority progress, governs tombstone safety"
    );
    for id in 1..=3 {
        assert_eq!(
            sim.replica(node(id))
                .committed_compaction_safe_offset()
                .unwrap(),
            StreamOffset::new(0)
        );
    }

    sim.crash(node(3));
    let now = sim.clock + 1;
    let second = sim
        .replica_mut(original_leader)
        .propose_records(
            vec![record(b"quorum-while-voter-offline")],
            AckRequirement::Quorum,
            now,
        )
        .unwrap();
    sim.collect_outbound(original_leader);
    sim.drain_network(4096);
    assert_eq!(
        sim.replica(original_leader)
            .proposal_outcome(second.proposal_id),
        Some(ProposalOutcome::QuorumCommitted)
    );
    assert!(
        sim.replica_mut(original_leader)
            .propose_compaction_safe_from_reports(term, 7, &reports, now + 1)
            .unwrap()
            .is_none(),
        "a quorum data append cannot substitute for missing clean-progress proof"
    );
    sim.crash(original_leader);
    sim.restart(node(3));
    sim.network.clear();
    let successor = sim.elect_any_live(80);
    assert_ne!(successor, original_leader);
    assert!(sim.replica(successor).leader_epoch() > term);
    let successor_now = sim.clock + 1;
    assert!(
        sim.replica_mut(successor)
            .propose_compaction_safe_from_reports(term, 7, &reports, successor_now)
            .is_err(),
        "new term cannot reuse the old leader's volatile report authority"
    );
    for id in [node(2), node(3)] {
        assert_eq!(
            sim.replica(id).committed_compaction_safe_offset().unwrap(),
            StreamOffset::new(0),
            "missing voter must block unsafe floor through minority loss and election"
        );
    }
    sim.cleanup();
}
