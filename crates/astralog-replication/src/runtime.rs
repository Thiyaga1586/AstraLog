use std::collections::{BTreeMap, VecDeque};

use astralog_common::{
    PartitionId, ProducerAuthority, ProducerId, ProducerSequence, RaftIndex, StreamOffset, TopicId,
};
use astralog_raft::{
    ConfigurationChange, Effect, EffectiveConfiguration, EntryKind, Input, LogIndex, NodeId,
    PersistenceBatch, PersistenceFailure, PersistenceOp, PersistenceToken, ProposalId, RaftCore,
    RaftLimits, RaftMessage, RecoveredState, Role, SnapshotId, SnapshotMetadata,
    StableConfiguration, TimingConfig,
};
use astralog_storage::{
    CompactionLimits, CompactionOutcome, CompactionPlan, DataRecord, DeleteRetentionPlan,
    PartitionLogConfig, StoredRecord, TombstoneRemovalGate,
};

use crate::{
    ApplicationCommand, ApplicationCommandV2, ApplicationCompactionSafeCommandV1,
    ApplicationMaintenanceCommandV1, ApplicationProducerStateCheckpointCommandV1,
    CleanProgressTracker, CommandLimits, CommittedBatchV1, CommittedRetryV1, PartitionPersistence,
    PartitionProducerStateV1, ProducerAdmissionV1, ProducerBatchIdentityV1, ProducerStateError,
    RebootstrapLimitsV2, RebootstrapPackage, RebootstrapPackageV2, ReplicationError, Result,
    encode_application_command, encode_application_command_v2,
    encode_application_compaction_safe_command_v1, encode_application_maintenance_command_v1,
    encode_application_producer_state_checkpoint_command_v1,
    producer_digest::canonical_storage_record_set_digest,
};

pub const MAX_PRODUCER_CACHE_ENTRIES: usize = crate::MAX_PARTITION_PRODUCER_ENTRIES;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckRequirement {
    None,
    Leader,
    Quorum,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProposalOutcome {
    Pending,
    LocalDurable,
    QuorumCommitted,
    Rejected,
    LeadershipLost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProposalReceipt {
    pub proposal_id: ProposalId,
    pub raft_index: LogIndex,
    pub base_offset: StreamOffset,
    pub last_offset: StreamOffset,
    pub ack: AckRequirement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceProposalReceipt {
    pub proposal_id: ProposalId,
    pub raft_index: LogIndex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControllerAuthorityEvidence {
    NotChecked,
    Missing,
    Confirmed(ProducerAuthority),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdempotentProposalDisposition {
    Proposed(ProposalReceipt),
    Coalesced(ProposalReceipt),
    Duplicate(CommittedRetryV1),
}

#[derive(Debug)]
pub enum IdempotentProduceError {
    ControllerValidationRequired,
    UnknownProducer,
    ProducerFenced,
    OutOfOrderSequence,
    InvalidAckMode,
    InvalidTimestamp,
    NotLeader,
    StaleLeaderEpoch,
    Busy(&'static str),
    Replication(ReplicationError),
}

impl From<ReplicationError> for IdempotentProduceError {
    fn from(value: ReplicationError) -> Self {
        Self::Replication(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembershipObservation {
    pub role: Role,
    pub leader_id: Option<NodeId>,
    pub leader_epoch: u64,
    pub durable_commit_index: LogIndex,
    pub durable_last_index: LogIndex,
    pub effective_configuration: EffectiveConfiguration,
    pub pending_configuration_index: Option<LogIndex>,
    pub leader_match_indexes: BTreeMap<NodeId, LogIndex>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundMessage {
    pub target: NodeId,
    pub message: RaftMessage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicaLimits {
    pub max_outbound_messages: usize,
    pub max_outbound_bytes: usize,
    pub max_replication_inflight_messages_per_follower: usize,
    pub max_replication_inflight_bytes_per_follower: usize,
    pub max_replication_catchup_bytes_per_message: usize,
    pub max_pending_proposals: usize,
    pub max_pending_proposal_bytes: usize,
    pub max_pending_persistence_batches: usize,
    pub max_pending_persistence_bytes: usize,
    pub max_persistence_group_delay_ms: u64,
}

impl Default for ReplicaLimits {
    fn default() -> Self {
        Self {
            max_outbound_messages: 4096,
            max_outbound_bytes: 64 * 1024 * 1024,
            max_replication_inflight_messages_per_follower: 8,
            max_replication_inflight_bytes_per_follower: 48 * 1024 * 1024,
            max_replication_catchup_bytes_per_message: 24 * 1024 * 1024,
            max_pending_proposals: 4096,
            max_pending_proposal_bytes: 64 * 1024 * 1024,
            max_pending_persistence_batches: 1,
            max_pending_persistence_bytes: 64 * 1024 * 1024,
            max_persistence_group_delay_ms: 100,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PersistenceGroupMetrics {
    pub logical_tokens: u64,
    pub physical_syncs: u64,
}

#[derive(Debug, Default)]
struct PersistenceGroup {
    tokens: VecDeque<PersistenceToken>,
    logical_bytes: usize,
    opened_at_ms: Option<i64>,
}

#[derive(Debug)]
struct PendingProposal {
    receipt: ProposalReceipt,
    encoded_bytes: usize,
    outcome: ProposalOutcome,
}

#[derive(Debug, Clone, Copy)]
struct PendingIdempotent {
    batch: ProducerBatchIdentityV1,
    receipt: ProposalReceipt,
}

#[derive(Debug, Clone, Copy)]
struct InFlightReplicationRange {
    first_index: LogIndex,
    last_index: LogIndex,
    bytes: usize,
}

#[derive(Debug, Default)]
struct FollowerReplicationWindow {
    term: Option<astralog_raft::Term>,
    in_flight: VecDeque<InFlightReplicationRange>,
    in_flight_bytes: usize,
    sent_through: Option<LogIndex>,
    transport_incarnation: Option<u64>,
    inbound_connection_generation: Option<u64>,
}

impl FollowerReplicationWindow {
    fn reset_scheduling(&mut self) {
        self.term = None;
        self.in_flight.clear();
        self.in_flight_bytes = 0;
        self.sent_through = None;
    }
}

pub struct ReplicatedPartition {
    topic_id: TopicId,
    partition_id: PartitionId,
    local_node: NodeId,
    core: RaftCore,
    persistence: PartitionPersistence,
    command_limits: CommandLimits,
    limits: ReplicaLimits,
    next_proposal_id: u64,
    pending: BTreeMap<ProposalId, PendingProposal>,
    pending_configuration_results: BTreeMap<ProposalId, Option<bool>>,
    pending_bytes: usize,
    producer_cache: BTreeMap<ProducerId, PartitionProducerStateV1>,
    pending_idempotent: BTreeMap<ProducerId, PendingIdempotent>,
    outbound: VecDeque<OutboundMessage>,
    outbound_bytes: usize,
    outbound_data_messages: usize,
    outbound_data_bytes: usize,
    outbound_congested: bool,
    replication_windows: BTreeMap<NodeId, FollowerReplicationWindow>,
    replication_window_term: astralog_raft::Term,
    pending_rebootstrap: Option<RebootstrapPackage>,
    pending_rebootstrap_v2: Option<RebootstrapPackageV2>,
    // (manifest generation, committed safe bound, ticks since last attempt).
    // A failed bounded proof never grants authority, but may be transient
    // while the follower is rolling or installing a new retained generation.
    last_clean_revalidation: Option<(Option<u64>, u64, u32)>,
    // P9-G safe authority changes only when committed Raft history or its
    // manifest-selected retained representation changes. Do not re-scan the
    // full P1/P9 immutable inventory on every P3 tick/packet in legacy P6/P7
    // partitions whose committed safe boundary remains zero.
    cached_compaction_safe: Option<(u64, Option<u64>, StreamOffset)>,
    persistence_group: PersistenceGroup,
    persistence_group_metrics: PersistenceGroupMetrics,
    #[cfg(test)]
    persistence_hold: TestPersistenceHold,
    fatal: Option<String>,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TestPersistenceHold {
    None,
    BeforeApplyOnce,
    AfterApplyBeforeCompletionOnce,
}

impl ReplicatedPartition {
    #[allow(clippy::too_many_arguments)]
    pub fn open(
        directory: &std::path::Path,
        topic_id: TopicId,
        partition_id: PartitionId,
        local_node: NodeId,
        now_ms: i64,
        storage_config: PartitionLogConfig,
        configuration: StableConfiguration,
        timing: TimingConfig,
        randomized_election_timeout: u64,
        command_limits: CommandLimits,
        replica_limits: ReplicaLimits,
    ) -> Result<Self> {
        validate_replica_limits(replica_limits)?;
        let (persistence, recovered) = PartitionPersistence::open(
            directory,
            topic_id,
            partition_id,
            now_ms,
            storage_config,
            configuration,
            command_limits,
        )?;
        let raft_limits = RaftLimits::with_persistence_queue(
            replica_limits.max_pending_proposal_bytes,
            replica_limits.max_pending_persistence_batches,
            replica_limits.max_pending_persistence_bytes,
        )?
        .with_replication_send_bytes(replica_limits.max_replication_inflight_bytes_per_follower)?;
        let core = RaftCore::from_recovered_with_limits(
            local_node,
            recovered,
            timing,
            randomized_election_timeout,
            raft_limits,
        )?;
        let replication_window_term = core.hard_state().current_term;
        let mut runtime = Self {
            topic_id,
            partition_id,
            local_node,
            core,
            persistence,
            command_limits,
            limits: replica_limits,
            next_proposal_id: 1,
            pending: BTreeMap::new(),
            pending_configuration_results: BTreeMap::new(),
            pending_bytes: 0,
            // P7 producer state is a derived cache, not partition-startup
            // authority. Keep legacy/P6 actor startup independent of producer
            // history size; the first idempotent request for a missing
            // ProducerId reconstructs committed state on demand.
            producer_cache: BTreeMap::new(),
            pending_idempotent: BTreeMap::new(),
            outbound: VecDeque::new(),
            outbound_bytes: 0,
            outbound_data_messages: 0,
            outbound_data_bytes: 0,
            outbound_congested: false,
            replication_windows: BTreeMap::new(),
            replication_window_term,
            pending_rebootstrap: None,
            pending_rebootstrap_v2: None,
            last_clean_revalidation: None,
            cached_compaction_safe: None,
            persistence_group: PersistenceGroup::default(),
            persistence_group_metrics: PersistenceGroupMetrics::default(),
            #[cfg(test)]
            persistence_hold: TestPersistenceHold::None,
            fatal: None,
        };
        runtime.drive_effects(now_ms)?;
        Ok(runtime)
    }

    pub const fn topic_id(&self) -> TopicId {
        self.topic_id
    }

    pub const fn partition_id(&self) -> PartitionId {
        self.partition_id
    }

    pub const fn local_node(&self) -> NodeId {
        self.local_node
    }

    pub fn role(&self) -> Role {
        self.core.volatile_state().role
    }

    pub fn leader(&self) -> Option<NodeId> {
        self.core.volatile_state().leader_id
    }

    pub fn leader_epoch(&self) -> u64 {
        self.core.hard_state().current_term.get()
    }

    pub fn durable_commit_index(&self) -> LogIndex {
        self.core.hard_state().durable_commit_index
    }

    fn idempotent_leader_ready(&self) -> bool {
        if self.role() != Role::Leader {
            return false;
        }
        let hard_state = self.core.hard_state();
        let committed = hard_state.durable_commit_index;
        if committed == LogIndex::ZERO {
            return false;
        }
        let committed_term = if committed == self.core.snapshot().last_included_index {
            Some(self.core.snapshot().last_included_term)
        } else {
            self.core
                .stable_log()
                .iter()
                .find(|entry| entry.index == committed)
                .map(|entry| entry.term)
        };
        committed_term == Some(hard_state.current_term)
    }

    pub fn next_stream_offset(&self) -> StreamOffset {
        self.persistence.next_stream_offset()
    }

    pub fn committed_stream_high_watermark(&self) -> Result<StreamOffset> {
        self.persistence.committed_stream_high_watermark()
    }

    pub fn committed_log_start_offset(&self) -> Result<StreamOffset> {
        self.persistence.committed_log_start_offset()
    }

    pub fn committed_compaction_safe_offset(&self) -> Result<StreamOffset> {
        self.persistence.committed_compaction_safe_offset()
    }

    pub fn local_clean_compacted_offset(&self) -> StreamOffset {
        self.persistence.local_clean_compacted_offset()
    }

    /// Snapshot boundary used to bind bounded local cleaning verification.
    /// This is not a source of replicated compaction-safe authority.
    pub fn persisted_snapshot_floor_raft_index(&self) -> u64 {
        self.persistence.snapshot().last_included_index.get()
    }

    /// Re-verify the complete immutable prefix against the current manifest
    /// after a local manifest generation has changed. An old generation's
    /// sidecar is never inherited; only an independently checked current
    /// inventory may publish new local progress. This does not authorize
    /// tombstone removal or advance the replicated safe offset.
    pub fn revalidate_local_clean_progress_for_current_generation(&self) -> Result<StreamOffset> {
        self.persistence
            .revalidate_clean_progress_at_persisted_snapshot()
    }

    /// Replicated safe authority must never be exceeded by an unproven local
    /// retained generation. Rebootstrap is mandatory before any future
    /// leadership admission when this inequality holds.
    pub fn is_rebootstrap_required_for_compaction(&self) -> Result<bool> {
        Ok(crate::requires_retained_history_rebootstrap(
            self.persistence.local_clean_compacted_offset(),
            self.persistence.committed_compaction_safe_offset()?,
        ))
    }

    fn cached_committed_compaction_safe_offset(&mut self) -> Result<StreamOffset> {
        let observed_commit = self.persistence.log().committed_raft_index();
        let observed_generation = self.persistence.manifest_generation();
        if let Some((commit, generation, cached_safe)) = self.cached_compaction_safe
            && commit == observed_commit
            && generation == observed_generation
        {
            return Ok(cached_safe);
        }
        let safe = self.persistence.committed_compaction_safe_offset()?;
        self.cached_compaction_safe = Some((observed_commit, observed_generation, safe));
        Ok(safe)
    }

    fn cached_rebootstrap_required_for_compaction(&mut self) -> Result<bool> {
        let safe = self.cached_committed_compaction_safe_offset()?;
        if safe == StreamOffset::new(0) {
            return Ok(false);
        }
        Ok(crate::requires_retained_history_rebootstrap(
            self.persistence.local_clean_compacted_offset(),
            safe,
        ))
    }

    pub fn manifest_generation(&self) -> Option<u64> {
        self.persistence.manifest_generation()
    }

    pub fn plan_delete_retention(
        &mut self,
        now_ms: i64,
        retention_ms: Option<u64>,
        retention_bytes: Option<u64>,
        protected_log_start_ceiling: Option<StreamOffset>,
    ) -> Result<DeleteRetentionPlan> {
        self.persistence.plan_delete_retention(
            now_ms,
            retention_ms,
            retention_bytes,
            protected_log_start_ceiling,
        )
    }

    pub fn publish_delete_retention_generation(
        &mut self,
        expected_manifest_generation: u64,
    ) -> Result<usize> {
        self.persistence
            .publish_delete_retention_generation(expected_manifest_generation)
    }

    pub fn plan_keyed_compaction(
        &mut self,
        policy_epoch: u64,
        limits: CompactionLimits,
    ) -> Result<CompactionPlan> {
        self.persistence.plan_keyed_compaction(policy_epoch, limits)
    }

    pub fn plan_keyed_compaction_with_tombstone_gate(
        &mut self,
        policy_epoch: u64,
        limits: CompactionLimits,
        tombstone_removal: Option<TombstoneRemovalGate>,
    ) -> Result<CompactionPlan> {
        self.persistence.plan_keyed_compaction_with_tombstone_gate(
            policy_epoch,
            limits,
            tombstone_removal,
        )
    }

    pub fn execute_keyed_compaction(
        &mut self,
        plan: &CompactionPlan,
        current_policy_epoch: u64,
    ) -> Result<CompactionOutcome> {
        self.persistence
            .execute_keyed_compaction(plan, current_policy_epoch)
    }

    pub fn execute_keyed_compaction_with_publication_guard(
        &mut self,
        plan: &CompactionPlan,
        current_policy_epoch: u64,
        publication_metadata_epoch: &std::sync::RwLock<u64>,
    ) -> Result<CompactionOutcome> {
        self.persistence
            .execute_keyed_compaction_with_publication_guard(
                plan,
                current_policy_epoch,
                publication_metadata_epoch,
            )
    }

    pub fn snapshot_committed_prefix_for_retention(
        &mut self,
        expected_leader_epoch: u64,
        now_ms: i64,
    ) -> Result<LogIndex> {
        if self.role() != Role::Leader {
            return Err(ReplicationError::Invalid("local replica is not leader"));
        }
        if expected_leader_epoch == 0 || expected_leader_epoch != self.leader_epoch() {
            return Err(ReplicationError::Invalid(
                "P9 retention snapshot is fenced by leader epoch",
            ));
        }
        if !self.idempotent_leader_ready() {
            return Err(ReplicationError::Busy(
                "current-term leader barrier is not durably committed yet",
            ));
        }
        let committed = self.durable_commit_index();
        if committed == LogIndex::ZERO {
            return Ok(committed);
        }
        self.handle(Input::RequestSnapshot(committed), now_ms)?;
        Ok(committed)
    }

    pub fn set_data_compression(&mut self, compression: astralog_storage::Compression) {
        self.persistence.set_data_compression(compression);
    }

    pub fn recovered_state(&self) -> Result<RecoveredState> {
        self.persistence.recovered_state()
    }

    /// Persists rebuildable P7 producer state at the current committed
    /// boundary. This sidecar is derived acceleration only; committed history
    /// remains the source of truth.
    pub fn persist_producer_snapshot(&self) -> Result<()> {
        self.persistence.persist_producer_snapshot()
    }

    pub fn membership_observation(&self) -> MembershipObservation {
        let durable_last_index = self
            .core
            .stable_log()
            .last()
            .map_or(self.core.snapshot().last_included_index, |entry| {
                entry.index
            });
        let leader_match_indexes = if self.role() == Role::Leader {
            self.core
                .volatile_state()
                .progress
                .iter()
                .map(|(node, progress)| (*node, progress.match_index))
                .collect()
        } else {
            BTreeMap::new()
        };
        MembershipObservation {
            role: self.role(),
            leader_id: self.leader(),
            leader_epoch: self.leader_epoch(),
            durable_commit_index: self.durable_commit_index(),
            durable_last_index,
            effective_configuration: self.core.effective_configuration().clone(),
            pending_configuration_index: self.core.volatile_state().pending_configuration_index,
            leader_match_indexes,
        }
    }

    pub fn propose_configuration_change(
        &mut self,
        change: ConfigurationChange,
        now_ms: i64,
    ) -> Result<bool> {
        let proposal_id = ProposalId::new(self.next_proposal_id);
        self.next_proposal_id = self
            .next_proposal_id
            .checked_add(1)
            .ok_or(ReplicationError::Invalid("proposal id overflow"))?;
        self.pending_configuration_results.insert(proposal_id, None);
        if let Err(error) = self.handle(
            Input::ProposeConfiguration {
                id: proposal_id,
                change,
            },
            now_ms,
        ) {
            self.pending_configuration_results.remove(&proposal_id);
            return Err(error);
        }
        let result = self
            .pending_configuration_results
            .remove(&proposal_id)
            .flatten()
            .ok_or(ReplicationError::Invalid(
                "configuration proposal completed without a proposal result",
            ))?;
        Ok(result)
    }

    pub fn transfer_leadership(&mut self, target: NodeId, now_ms: i64) -> Result<()> {
        self.handle(Input::TransferLeadership { target }, now_ms)
    }

    pub fn is_fatal(&self) -> bool {
        self.fatal.is_some() || self.core.is_stopped()
    }

    pub fn fatal_reason(&self) -> Option<&str> {
        self.fatal.as_deref()
    }

    pub const fn persistence_group_metrics(&self) -> PersistenceGroupMetrics {
        self.persistence_group_metrics
    }

    pub fn pending_persistence_group_len(&self) -> usize {
        self.persistence_group.tokens.len()
    }

    pub fn pending_persistence_group_bytes(&self) -> usize {
        self.persistence_group.logical_bytes
    }

    pub fn flush_persistence(&mut self, now_ms: i64) -> Result<()> {
        self.flush_persistence_group()?;
        self.drive_effects(now_ms)?;
        Ok(())
    }

    pub fn tick(&mut self, reset_election_timeout: u64, now_ms: i64) -> Result<()> {
        if self.cached_rebootstrap_required_for_compaction()? {
            let safe = self.cached_committed_compaction_safe_offset()?.get();
            let observed = (self.persistence.manifest_generation(), safe);
            let should_revalidate = match self.last_clean_revalidation {
                Some((generation, bound, count))
                    if (generation, bound) == observed && count < 50 =>
                {
                    self.last_clean_revalidation = Some((generation, bound, count + 1));
                    false
                }
                _ => true,
            };
            if should_revalidate {
                self.last_clean_revalidation = Some((observed.0, observed.1, 0));
                // A harmless roll can invalidate the previous sidecar. Only
                // a fresh complete proof against this manifest may clear the
                // rebootstrap-required gate. Retry at most every 51 ticks if
                // proof remains unavailable: a failed first attempt must not
                // permanently strand an otherwise healthy follower.
                let _ = self
                    .persistence
                    .revalidate_clean_progress_at_persisted_snapshot();
            }
            if self.cached_rebootstrap_required_for_compaction()? {
                if self.role() == Role::Leader {
                    // A previously clean leader can lose its current-
                    // generation proof after a harmless roll or later
                    // committed safe-control observation. It is now
                    // ineligible to serve, but that is NOT a disk/Raft
                    // corruption verdict: killing its actor prevents a
                    // higher-term P3 leader from repairing it via snapshot.
                    // Suppress any queued old-leader outbound effects and
                    // wait for authenticated successor replication. Client
                    // proposals are independently fenced in handle().
                    let _ = self.drain_outbound().count();
                }
                // Do not advance local election timers or initiate campaigns;
                // continue to accept authenticated peer Append/InstallSnapshot
                // so the replica can rebootstrap under P3 authorization.
                return Ok(());
            }
        } else {
            self.last_clean_revalidation = None;
        }
        self.handle(
            Input::Tick {
                reset_election_timeout,
            },
            now_ms,
        )
    }

    pub fn step(&mut self, source: NodeId, message: RaftMessage, now_ms: i64) -> Result<()> {
        if message_sender(&message) != source {
            return Err(ReplicationError::Invalid(
                "transport source does not match Raft message identity",
            ));
        }
        if self.cached_rebootstrap_required_for_compaction()? {
            if self.role() == Role::Leader {
                // Do not flush previously queued heartbeats/appends from an
                // unsafe old leadership term while waiting to step down.
                let _ = self.drain_outbound().count();
            }
            let successor_replication = match &message {
                RaftMessage::AppendEntriesRequest(request) => {
                    self.role() != Role::Leader
                        || request.term > self.core.hard_state().current_term
                }
                RaftMessage::InstallSnapshotRequest(request) => {
                    self.role() != Role::Leader
                        || request.term > self.core.hard_state().current_term
                }
                _ => false,
            };
            if !successor_replication {
                // No election, response, stale/current-term replication, or
                // outbound leader authority may be exercised with unverified
                // retained history. A newer-term P3 Append/InstallSnapshot
                // is the only legitimate route to step down and rebootstrap.
                // Drop the input without killing the recoverable actor.
                return Ok(());
            }
        }
        self.observe_replication_response(&message);
        self.handle(Input::Step(message), now_ms)
    }

    pub fn step_with_rebootstrap(
        &mut self,
        source: NodeId,
        message: RaftMessage,
        package: RebootstrapPackage,
        now_ms: i64,
    ) -> Result<()> {
        if message_sender(&message) != source {
            return Err(ReplicationError::Invalid(
                "transport source does not match Raft message identity",
            ));
        }
        let RaftMessage::InstallSnapshotRequest(request) = &message else {
            return Err(ReplicationError::Invalid(
                "rebootstrap package requires InstallSnapshotRequest",
            ));
        };
        if request.snapshot != package.snapshot
            || package.topic_id != self.topic_id
            || package.partition_id != self.partition_id
        {
            return Err(ReplicationError::Invalid(
                "rebootstrap package does not match InstallSnapshot/group identity",
            ));
        }
        if self.pending_rebootstrap.is_some() || self.pending_rebootstrap_v2.is_some() {
            return Err(ReplicationError::Busy(
                "another rebootstrap package is already pending P3 authorization",
            ));
        }
        if request.snapshot.last_included_index
            <= self.persistence.hard_state().durable_commit_index
        {
            // P4 V1 and P9-G V2 must use the same committed-prefix fence.
            // A follower may have committed these bytes already while its
            // P3 snapshot *metadata* still trails. Replacing the files would
            // be unsafe, so let P3 process the snapshot using existing bytes
            // without attaching an obsolete rebootstrap package. Storage's
            // strict no-overwrite guard remains unchanged.
            return self.step(source, message, now_ms);
        }
        self.observe_replication_response(&message);
        self.pending_rebootstrap = Some(package);
        let result = self.handle(Input::Step(message), now_ms);
        // If P3 rejected/ignored the snapshot, no InstallSnapshot persistence
        // operation consumed the package. Discard it rather than carrying it
        // into an unrelated future snapshot.
        if self.pending_rebootstrap.is_some() {
            self.pending_rebootstrap = None;
        }
        result
    }

    /// P3 remains the sole authorizer of durable InstallSnapshot publication.
    /// A V2 package is only installed if that exact P3 persistence effect is
    /// emitted for this leader/term/snapshot. Rejected P3 requests discard
    /// their packages without persisting any retained files.
    pub fn step_with_rebootstrap_v2(
        &mut self,
        source: NodeId,
        message: RaftMessage,
        package: RebootstrapPackageV2,
        now_ms: i64,
    ) -> Result<()> {
        if message_sender(&message) != source {
            return Err(ReplicationError::Invalid(
                "P9-G V2 transport source identity differs from Raft sender",
            ));
        }
        let RaftMessage::InstallSnapshotRequest(request) = &message else {
            return Err(ReplicationError::Invalid(
                "P9-G V2 package requires P3 InstallSnapshotRequest",
            ));
        };
        if request.snapshot != package.snapshot
            || package.topic_id != self.topic_id
            || package.partition_id != self.partition_id
        {
            return Err(ReplicationError::Invalid(
                "P9-G V2 package differs from P3 snapshot/partition identity",
            ));
        }
        if self.pending_rebootstrap.is_some() || self.pending_rebootstrap_v2.is_some() {
            return Err(ReplicationError::Busy(
                "another P4/P9 rebootstrap is pending P3 authorization",
            ));
        }
        if request.snapshot.last_included_index
            <= self.persistence.hard_state().durable_commit_index
        {
            // The follower already holds this entire committed Raft prefix.
            // P3 may still need to advance its *snapshot metadata* because
            // the old local snapshot floor is lower. Installing a V2 file
            // package here would overwrite acknowledged history and must
            // remain forbidden by the storage boundary. Let P3 handle the
            // authenticated request against the existing local files instead:
            // it may persist metadata or reject the obsolete request, but no
            // V2 replacement is attached. This avoids converting an ordinary
            // catch-up race into a fatal replica/persistence error.
            return self.step(source, message, now_ms);
        }
        self.observe_replication_response(&message);
        self.pending_rebootstrap_v2 = Some(package);
        let result = self.handle(Input::Step(message), now_ms);
        self.pending_rebootstrap_v2 = None;
        result
    }

    pub fn build_rebootstrap_package_v2(
        &self,
        metadata: &SnapshotMetadata,
        limits: RebootstrapLimitsV2,
    ) -> Result<RebootstrapPackageV2> {
        self.persistence
            .build_rebootstrap_package_v2(metadata, limits)
    }

    pub fn build_rebootstrap_package(
        &self,
        metadata: &SnapshotMetadata,
    ) -> Result<RebootstrapPackage> {
        self.persistence.build_rebootstrap_package(metadata)
    }

    #[cfg(test)]
    pub(crate) fn inject_persistence_failure_once(&mut self, message: &'static str) {
        self.persistence.fail_next_execute(message);
    }

    #[cfg(test)]
    pub(crate) fn inject_group_sync_failure_once(&mut self, message: &'static str) {
        self.persistence.fail_next_group_sync(message);
    }

    #[cfg(test)]
    pub(crate) fn hold_next_persistence_before_apply(&mut self) {
        self.persistence_hold = TestPersistenceHold::BeforeApplyOnce;
    }

    #[cfg(test)]
    pub(crate) fn hold_next_persistence_after_apply_before_completion(&mut self) {
        self.persistence_hold = TestPersistenceHold::AfterApplyBeforeCompletionOnce;
    }

    pub fn producer_cache_len(&self) -> usize {
        self.producer_cache.len()
    }

    pub fn pending_idempotent_len(&self) -> usize {
        self.pending_idempotent.len()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn propose_idempotent_records(
        &mut self,
        expected_leader_epoch: u64,
        authority: ProducerAuthority,
        base_sequence: ProducerSequence,
        records: Vec<DataRecord>,
        ack: AckRequirement,
        controller_evidence: ControllerAuthorityEvidence,
        now_ms: i64,
    ) -> std::result::Result<IdempotentProposalDisposition, IdempotentProduceError> {
        if self.role() != Role::Leader {
            return Err(IdempotentProduceError::NotLeader);
        }
        if expected_leader_epoch == 0 || expected_leader_epoch != self.leader_epoch() {
            return Err(IdempotentProduceError::StaleLeaderEpoch);
        }
        if self
            .cached_rebootstrap_required_for_compaction()
            .map_err(IdempotentProduceError::Replication)?
        {
            // In particular, the exact-retry/Coalesced fast paths below must
            // not acknowledge a request from an unverified old leader.
            // This veto precedes all producer-cache admission decisions.
            return Err(IdempotentProduceError::Busy(
                "P9-G unverified retained history blocks producer authority",
            ));
        }
        if ack != AckRequirement::Quorum {
            return Err(IdempotentProduceError::InvalidAckMode);
        }
        if !self.idempotent_leader_ready() {
            return Err(IdempotentProduceError::Busy(
                "current-term leader barrier is not durably committed yet",
            ));
        }
        if self.outbound_congested {
            return Err(IdempotentProduceError::Busy(
                "Raft outbound queue is congested; rejecting new idempotent producer work",
            ));
        }
        if records.is_empty() {
            return Err(IdempotentProduceError::OutOfOrderSequence);
        }
        if records.iter().any(|record| record.timestamp_ms < 0) {
            return Err(IdempotentProduceError::InvalidTimestamp);
        }
        let record_count =
            u32::try_from(records.len()).map_err(|_| IdempotentProduceError::OutOfOrderSequence)?;
        let canonical_digest = canonical_storage_record_set_digest(&records)?;
        let candidate =
            ProducerBatchIdentityV1::new(authority, base_sequence, record_count, canonical_digest)
                .map_err(map_producer_state_error)?;

        if let Some(pending) = self.pending_idempotent.get(&authority.producer_id).copied() {
            if pending.batch == candidate {
                return Ok(IdempotentProposalDisposition::Coalesced(pending.receipt));
            }
            if pending.batch.authority.producer_epoch > authority.producer_epoch {
                return Err(IdempotentProduceError::ProducerFenced);
            }
            if pending.batch.authority == authority
                && pending.batch.base_sequence == candidate.base_sequence
                && pending.batch.last_sequence == candidate.last_sequence
                && pending.batch.record_count == candidate.record_count
            {
                return Err(IdempotentProduceError::OutOfOrderSequence);
            }
            return Err(IdempotentProduceError::Busy(
                "one unresolved idempotent batch is already pending for this producer-partition",
            ));
        }

        let committed_state = self.load_producer_state(authority.producer_id)?;
        let requires_controller = committed_state
            .as_ref()
            .is_none_or(|state| authority.producer_epoch > state.accepted_epoch());
        if requires_controller {
            validate_controller_evidence(authority, controller_evidence)?;
        }

        let admission = match committed_state.as_ref() {
            Some(state) => state
                .admit(authority, base_sequence, record_count, canonical_digest)
                .map_err(map_producer_state_error)?,
            None => ProducerAdmissionV1::Append(
                PartitionProducerStateV1::prepare_initial(
                    authority,
                    base_sequence,
                    record_count,
                    canonical_digest,
                )
                .map_err(map_producer_state_error)?,
            ),
        };
        if let ProducerAdmissionV1::Duplicate(retry) = admission {
            return Ok(IdempotentProposalDisposition::Duplicate(retry));
        }

        if self.pending.len() >= self.limits.max_pending_proposals {
            return Err(IdempotentProduceError::Replication(
                ReplicationError::LimitExceeded {
                    what: "pending proposal count",
                    actual: self.pending.len().saturating_add(1),
                    max: self.limits.max_pending_proposals,
                },
            ));
        }
        let base_offset = self.persistence.next_stream_offset();
        let last_offset = base_offset
            .checked_add(u64::from(record_count - 1))
            .ok_or(IdempotentProduceError::OutOfOrderSequence)?;
        let command = encode_application_command_v2(
            &ApplicationCommandV2 {
                base_stream_offset: base_offset,
                authority,
                base_sequence,
                records,
            },
            self.command_limits,
        )?;
        if self
            .pending_bytes
            .checked_add(command.len())
            .is_none_or(|bytes| bytes > self.limits.max_pending_proposal_bytes)
        {
            return Err(IdempotentProduceError::Replication(
                ReplicationError::LimitExceeded {
                    what: "pending proposal bytes",
                    actual: self.pending_bytes.saturating_add(command.len()),
                    max: self.limits.max_pending_proposal_bytes,
                },
            ));
        }
        let proposal_id = ProposalId::new(self.next_proposal_id);
        self.next_proposal_id = self
            .next_proposal_id
            .checked_add(1)
            .ok_or(ReplicationError::Invalid("proposal id overflow"))?;
        let raft_index = self
            .core
            .next_staged_log_index()
            .map_err(ReplicationError::from)?;
        let receipt = ProposalReceipt {
            proposal_id,
            raft_index,
            base_offset,
            last_offset,
            ack,
        };
        self.pending_bytes += command.len();
        self.pending.insert(
            proposal_id,
            PendingProposal {
                receipt,
                encoded_bytes: command.len(),
                outcome: ProposalOutcome::Pending,
            },
        );
        self.pending_idempotent.insert(
            authority.producer_id,
            PendingIdempotent {
                batch: candidate,
                receipt,
            },
        );
        if let Err(error) = self.handle(
            Input::Propose {
                id: proposal_id,
                payload: command,
            },
            now_ms,
        ) {
            self.pending_idempotent.remove(&authority.producer_id);
            self.remove_pending(proposal_id);
            return Err(error.into());
        }
        Ok(IdempotentProposalDisposition::Proposed(receipt))
    }

    pub fn propose_records(
        &mut self,
        records: Vec<DataRecord>,
        ack: AckRequirement,
        now_ms: i64,
    ) -> Result<ProposalReceipt> {
        if self.role() != Role::Leader {
            return Err(ReplicationError::Invalid("local replica is not leader"));
        }
        if self.outbound_congested {
            return Err(ReplicationError::Busy(
                "Raft outbound queue is congested; rejecting new producer work",
            ));
        }
        if records.is_empty() {
            return Err(ReplicationError::Invalid("produce batch is empty"));
        }
        if self.pending.len() >= self.limits.max_pending_proposals {
            return Err(ReplicationError::LimitExceeded {
                what: "pending proposal count",
                actual: self.pending.len().saturating_add(1),
                max: self.limits.max_pending_proposals,
            });
        }
        let base_offset = self.persistence.next_stream_offset();
        let record_count = u64::try_from(records.len())
            .map_err(|_| ReplicationError::Invalid("record count overflow"))?;
        let last_offset = base_offset
            .checked_add(record_count.saturating_sub(1))
            .ok_or(ReplicationError::Invalid("stream offset overflow"))?;
        let command = encode_application_command(
            &ApplicationCommand {
                base_stream_offset: base_offset,
                records,
            },
            self.command_limits,
        )?;
        if self
            .pending_bytes
            .checked_add(command.len())
            .is_none_or(|bytes| bytes > self.limits.max_pending_proposal_bytes)
        {
            return Err(ReplicationError::LimitExceeded {
                what: "pending proposal bytes",
                actual: self.pending_bytes.saturating_add(command.len()),
                max: self.limits.max_pending_proposal_bytes,
            });
        }
        let proposal_id = ProposalId::new(self.next_proposal_id);
        self.next_proposal_id = self
            .next_proposal_id
            .checked_add(1)
            .ok_or(ReplicationError::Invalid("proposal id overflow"))?;
        let raft_index = self.core.next_staged_log_index()?;
        let receipt = ProposalReceipt {
            proposal_id,
            raft_index,
            base_offset,
            last_offset,
            ack,
        };
        self.pending_bytes += command.len();
        self.pending.insert(
            proposal_id,
            PendingProposal {
                receipt,
                encoded_bytes: command.len(),
                outcome: ProposalOutcome::Pending,
            },
        );
        if let Err(error) = self.handle(
            Input::Propose {
                id: proposal_id,
                payload: command,
            },
            now_ms,
        ) {
            self.remove_pending(proposal_id);
            return Err(error);
        }
        Ok(receipt)
    }

    pub fn propose_log_start_offset(
        &mut self,
        expected_leader_epoch: u64,
        new_log_start_offset: StreamOffset,
        now_ms: i64,
    ) -> Result<MaintenanceProposalReceipt> {
        if self.role() != Role::Leader {
            return Err(ReplicationError::Invalid("local replica is not leader"));
        }
        if expected_leader_epoch == 0 || expected_leader_epoch != self.leader_epoch() {
            return Err(ReplicationError::Invalid(
                "P9 retention proposal is fenced by leader epoch",
            ));
        }
        if !self.idempotent_leader_ready() {
            return Err(ReplicationError::Busy(
                "current-term leader barrier is not durably committed yet",
            ));
        }
        let current = self.persistence.committed_log_start_offset()?;
        let high_watermark = self.persistence.committed_stream_high_watermark()?;
        if new_log_start_offset <= current {
            return Err(ReplicationError::Invalid(
                "P9 log-start proposal must strictly advance committed authority",
            ));
        }
        if new_log_start_offset > high_watermark {
            return Err(ReplicationError::Invalid(
                "P9 log-start proposal exceeds committed stream high watermark",
            ));
        }
        if self.pending.len() >= self.limits.max_pending_proposals {
            return Err(ReplicationError::LimitExceeded {
                what: "pending proposal count",
                actual: self.pending.len().saturating_add(1),
                max: self.limits.max_pending_proposals,
            });
        }
        let command = encode_application_maintenance_command_v1(ApplicationMaintenanceCommandV1 {
            new_log_start_offset,
        });
        if self
            .pending_bytes
            .checked_add(command.len())
            .is_none_or(|bytes| bytes > self.limits.max_pending_proposal_bytes)
        {
            return Err(ReplicationError::LimitExceeded {
                what: "pending proposal bytes",
                actual: self.pending_bytes.saturating_add(command.len()),
                max: self.limits.max_pending_proposal_bytes,
            });
        }
        let proposal_id = ProposalId::new(self.next_proposal_id);
        self.next_proposal_id = self
            .next_proposal_id
            .checked_add(1)
            .ok_or(ReplicationError::Invalid("proposal id overflow"))?;
        let raft_index = self.core.next_staged_log_index()?;
        let logical_offset = self.persistence.next_stream_offset();
        let receipt = ProposalReceipt {
            proposal_id,
            raft_index,
            base_offset: logical_offset,
            last_offset: logical_offset,
            ack: AckRequirement::Quorum,
        };
        self.pending_bytes += command.len();
        self.pending.insert(
            proposal_id,
            PendingProposal {
                receipt,
                encoded_bytes: command.len(),
                outcome: ProposalOutcome::Pending,
            },
        );
        if let Err(error) = self.handle(
            Input::Propose {
                id: proposal_id,
                payload: command,
            },
            now_ms,
        ) {
            self.remove_pending(proposal_id);
            return Err(error);
        }
        Ok(MaintenanceProposalReceipt {
            proposal_id,
            raft_index,
        })
    }

    pub fn propose_producer_state_checkpoint(
        &mut self,
        expected_leader_epoch: u64,
        now_ms: i64,
    ) -> Result<Option<MaintenanceProposalReceipt>> {
        if self.role() != Role::Leader {
            return Err(ReplicationError::Invalid("local replica is not leader"));
        }
        if expected_leader_epoch == 0 || expected_leader_epoch != self.leader_epoch() {
            return Err(ReplicationError::Invalid(
                "P9 producer checkpoint proposal is fenced by leader epoch",
            ));
        }
        if !self.idempotent_leader_ready() {
            return Err(ReplicationError::Busy(
                "current-term leader barrier is not durably committed yet",
            ));
        }
        let Some(checkpoint) = self.persistence.producer_checkpoint_candidate()? else {
            return Ok(None);
        };
        if self.pending.len() >= self.limits.max_pending_proposals {
            return Err(ReplicationError::LimitExceeded {
                what: "pending proposal count",
                actual: self.pending.len().saturating_add(1),
                max: self.limits.max_pending_proposals,
            });
        }
        let command = encode_application_producer_state_checkpoint_command_v1(
            &ApplicationProducerStateCheckpointCommandV1 { checkpoint },
            self.command_limits,
        )?;
        if self
            .pending_bytes
            .checked_add(command.len())
            .is_none_or(|bytes| bytes > self.limits.max_pending_proposal_bytes)
        {
            return Err(ReplicationError::LimitExceeded {
                what: "pending proposal bytes",
                actual: self.pending_bytes.saturating_add(command.len()),
                max: self.limits.max_pending_proposal_bytes,
            });
        }
        let proposal_id = ProposalId::new(self.next_proposal_id);
        self.next_proposal_id = self
            .next_proposal_id
            .checked_add(1)
            .ok_or(ReplicationError::Invalid("proposal id overflow"))?;
        let raft_index = self.core.next_staged_log_index()?;
        self.pending_bytes += command.len();
        self.pending.insert(
            proposal_id,
            PendingProposal {
                receipt: ProposalReceipt {
                    proposal_id,
                    raft_index,
                    base_offset: self.persistence.next_stream_offset(),
                    last_offset: self.persistence.next_stream_offset(),
                    ack: AckRequirement::Quorum,
                },
                encoded_bytes: command.len(),
                outcome: ProposalOutcome::Pending,
            },
        );
        if let Err(error) = self.handle(
            Input::Propose {
                id: proposal_id,
                payload: command,
            },
            now_ms,
        ) {
            self.remove_pending(proposal_id);
            return Err(error);
        }
        Ok(Some(MaintenanceProposalReceipt {
            proposal_id,
            raft_index,
        }))
    }

    /// Leader-side P9-G safety proposal. A caller cannot supply an arbitrary
    /// offset: evidence must cover every voter of the current effective P3
    /// configuration (including joint configurations). Broker/controller
    /// authority must independently verify that the supplied generation and
    /// broker incarnations are from its current committed P5 assignment.
    pub fn propose_compaction_safe_from_reports(
        &mut self,
        expected_leader_epoch: u64,
        committed_assignment_generation: u64,
        reports: &CleanProgressTracker,
        now_ms: i64,
    ) -> Result<Option<MaintenanceProposalReceipt>> {
        if self.role() != Role::Leader {
            return Err(ReplicationError::Invalid("local replica is not leader"));
        }
        if expected_leader_epoch == 0 || expected_leader_epoch != self.leader_epoch() {
            return Err(ReplicationError::Invalid(
                "P9 compaction-safe proposal is fenced by leader epoch",
            ));
        }
        if !self.idempotent_leader_ready() {
            return Err(ReplicationError::Busy(
                "current-term leader barrier is not durably committed yet",
            ));
        }
        if !reports.is_current_for(
            self.topic_id,
            self.partition_id,
            self.leader_epoch(),
            committed_assignment_generation,
            self.core.effective_configuration(),
        ) {
            return Err(ReplicationError::Invalid(
                "P9 progress evidence is stale for current effective voters/assignment",
            ));
        }
        let Some(candidate) = reports.candidate_safe_offset() else {
            return Ok(None);
        };
        let current = self.persistence.committed_compaction_safe_offset()?;
        if candidate <= current {
            return Ok(None);
        }
        if candidate > self.persistence.committed_stream_high_watermark()? {
            return Err(ReplicationError::Invalid(
                "P9 compaction-safe candidate exceeds committed stream high watermark",
            ));
        }
        if self.pending.len() >= self.limits.max_pending_proposals {
            return Err(ReplicationError::LimitExceeded {
                what: "pending proposal count",
                actual: self.pending.len().saturating_add(1),
                max: self.limits.max_pending_proposals,
            });
        }
        let command =
            encode_application_compaction_safe_command_v1(ApplicationCompactionSafeCommandV1 {
                new_compaction_safe_offset: candidate,
            });
        if self
            .pending_bytes
            .checked_add(command.len())
            .is_none_or(|bytes| bytes > self.limits.max_pending_proposal_bytes)
        {
            return Err(ReplicationError::LimitExceeded {
                what: "pending proposal bytes",
                actual: self.pending_bytes.saturating_add(command.len()),
                max: self.limits.max_pending_proposal_bytes,
            });
        }
        let proposal_id = ProposalId::new(self.next_proposal_id);
        self.next_proposal_id = self
            .next_proposal_id
            .checked_add(1)
            .ok_or(ReplicationError::Invalid("proposal id overflow"))?;
        let raft_index = self.core.next_staged_log_index()?;
        self.pending_bytes += command.len();
        self.pending.insert(
            proposal_id,
            PendingProposal {
                receipt: ProposalReceipt {
                    proposal_id,
                    raft_index,
                    base_offset: self.persistence.next_stream_offset(),
                    last_offset: self.persistence.next_stream_offset(),
                    ack: AckRequirement::Quorum,
                },
                encoded_bytes: command.len(),
                outcome: ProposalOutcome::Pending,
            },
        );
        if let Err(error) = self.handle(
            Input::Propose {
                id: proposal_id,
                payload: command,
            },
            now_ms,
        ) {
            self.remove_pending(proposal_id);
            return Err(error);
        }
        Ok(Some(MaintenanceProposalReceipt {
            proposal_id,
            raft_index,
        }))
    }

    pub fn discard_producer_snapshot(&self) -> Result<()> {
        self.persistence.discard_producer_snapshot()
    }

    pub fn proposal_outcome(&self, proposal_id: ProposalId) -> Option<ProposalOutcome> {
        self.pending
            .get(&proposal_id)
            .map(|pending| pending.outcome)
    }

    pub fn take_completed(&mut self, proposal_id: ProposalId) -> Option<ProposalOutcome> {
        let outcome = self.pending.get(&proposal_id)?.outcome;
        if outcome == ProposalOutcome::Pending {
            return None;
        }
        self.remove_pending(proposal_id);
        Some(outcome)
    }

    pub fn forget_proposal(&mut self, proposal_id: ProposalId) -> bool {
        let existed = self.pending.contains_key(&proposal_id);
        self.remove_pending(proposal_id);
        existed
    }

    pub fn drain_outbound(&mut self) -> impl Iterator<Item = OutboundMessage> + '_ {
        self.outbound_bytes = 0;
        self.outbound_data_messages = 0;
        self.outbound_data_bytes = 0;
        self.outbound_congested = false;
        self.outbound.drain(..)
    }

    pub fn replication_window_usage(&self, target: NodeId) -> (usize, usize) {
        self.replication_windows
            .get(&target)
            .map_or((0, 0), |window| {
                (window.in_flight.len(), window.in_flight_bytes)
            })
    }

    /// Observes the transport identity associated with an inbound peer frame.
    /// A newer incarnation/connection invalidates speculative send-window
    /// state; traffic from an older connection generation is ignored.
    pub fn observe_peer_transport(
        &mut self,
        source: NodeId,
        incarnation: u64,
        connection_generation: u64,
    ) -> Result<bool> {
        if incarnation == 0 || connection_generation == 0 {
            return Err(ReplicationError::Invalid(
                "peer transport incarnation/generation must be non-zero",
            ));
        }
        let window = self.replication_windows.entry(source).or_default();
        if let Some(known_incarnation) = window.transport_incarnation {
            if incarnation < known_incarnation {
                return Ok(false);
            }
            if incarnation > known_incarnation {
                window.reset_scheduling();
                window.transport_incarnation = Some(incarnation);
                window.inbound_connection_generation = Some(connection_generation);
                return Ok(true);
            }
        }
        if let Some(known_generation) = window.inbound_connection_generation {
            if connection_generation < known_generation {
                return Ok(false);
            }
            if connection_generation > known_generation {
                window.reset_scheduling();
            }
        }
        window.transport_incarnation = Some(incarnation);
        window.inbound_connection_generation = Some(connection_generation);
        Ok(true)
    }

    pub fn read_committed_records(
        &self,
        start: StreamOffset,
        max_records: usize,
    ) -> Result<Vec<StoredRecord>> {
        self.read_committed_records_bounded(start, max_records, usize::MAX)
            .map(|(records, _)| records)
    }

    pub fn read_committed_records_bounded(
        &self,
        start: StreamOffset,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<(Vec<StoredRecord>, usize)> {
        self.persistence
            .log()
            .read_committed_records_from_bounded(start, max_records, max_bytes)
            .map_err(ReplicationError::from)
    }

    pub fn list_committed_offset(&self, timestamp_selector: i64) -> Result<(u64, i64)> {
        Ok(self
            .persistence
            .log()
            .list_committed_offset(timestamp_selector)?)
    }

    fn handle(&mut self, input: Input, now_ms: i64) -> Result<()> {
        if self.is_fatal() {
            return Err(ReplicationError::Invalid("replica is fatal/non-serving"));
        }
        if matches!(
            &input,
            Input::Propose { .. }
                | Input::ProposeConfiguration { .. }
                | Input::RequestSnapshot(_)
                | Input::TransferLeadership { .. }
        ) && self.cached_rebootstrap_required_for_compaction()?
        {
            // An unsafe local leader is neither a durable write authority nor
            // a snapshot/configuration/transfer authority. Keep the actor
            // available for P3-authorized repair without acknowledging work.
            if self.role() == Role::Leader {
                let _ = self.drain_outbound().count();
            }
            return Err(ReplicationError::Invalid(
                "P9-G unverified retained history blocks local leader proposals",
            ));
        }
        let append_proposal = matches!(&input, Input::Propose { .. });
        if !self.persistence_group.tokens.is_empty()
            && (!append_proposal || self.persistence_group_due(now_ms))
        {
            self.flush_persistence_group()?;
            self.drive_effects(now_ms)?;
            if self.is_fatal() {
                return Err(ReplicationError::Invalid("replica is fatal/non-serving"));
            }
        }
        let previous_role = self.role();
        self.core.handle_input(input)?;
        self.drive_effects(now_ms)?;
        self.refresh_proposals(previous_role);
        Ok(())
    }

    fn persistence_group_due(&self, now_ms: i64) -> bool {
        let Some(opened_at_ms) = self.persistence_group.opened_at_ms else {
            return false;
        };
        let Ok(delay_ms) = i64::try_from(self.limits.max_persistence_group_delay_ms) else {
            return true;
        };
        now_ms.saturating_sub(opened_at_ms) >= delay_ms
    }

    fn stage_persistence_group_batch(
        &mut self,
        batch: &PersistenceBatch,
        logical_bytes: usize,
        now_ms: i64,
    ) -> Result<()> {
        let group_already_had_bytes = !self.persistence_group.tokens.is_empty();
        let syncs_before = self.persistence.physical_data_sync_count();
        let stage_result = self.persistence.stage_append_batch(batch, now_ms);
        let syncs_after = self.persistence.physical_data_sync_count();
        if group_already_had_bytes {
            self.record_persistence_sync_delta(syncs_before, syncs_after)?;
        }
        if let Err(error) = stage_result {
            let message = error.to_string();
            let failed_token = self
                .persistence_group
                .tokens
                .front()
                .copied()
                .unwrap_or(batch.token);
            self.persistence_group = PersistenceGroup::default();
            self.core.handle_input(Input::PersistenceFailed {
                token: failed_token,
                error: PersistenceFailure::new(message.clone()),
            })?;
            self.fatal = Some(message);
            return Ok(());
        }
        self.persistence_group.tokens.push_back(batch.token);
        self.persistence_group.logical_bytes = self
            .persistence_group
            .logical_bytes
            .checked_add(logical_bytes)
            .ok_or(ReplicationError::Invalid(
                "persistence group byte accounting overflow",
            ))?;
        self.persistence_group.opened_at_ms.get_or_insert(now_ms);
        self.persistence_group_metrics.logical_tokens = self
            .persistence_group_metrics
            .logical_tokens
            .checked_add(1)
            .ok_or(ReplicationError::Invalid(
                "persistence logical-token counter overflow",
            ))?;

        if self.persistence_group.tokens.len() >= self.limits.max_pending_persistence_batches
            || self.persistence_group.logical_bytes >= self.limits.max_pending_persistence_bytes
        {
            self.flush_persistence_group()?;
        }
        Ok(())
    }

    fn flush_persistence_group(&mut self) -> Result<()> {
        if self.persistence_group.tokens.is_empty() {
            return Ok(());
        }
        let first_token =
            self.persistence_group
                .tokens
                .front()
                .copied()
                .ok_or(ReplicationError::Invalid(
                    "persistence group lost its first token",
                ))?;
        let syncs_before = self.persistence.physical_data_sync_count();
        let flush_result = self.persistence.flush_staged_appends();
        let syncs_after = self.persistence.physical_data_sync_count();
        self.record_persistence_sync_delta(syncs_before, syncs_after)?;
        if let Err(error) = flush_result {
            let message = error.to_string();
            self.persistence_group = PersistenceGroup::default();
            self.core.handle_input(Input::PersistenceFailed {
                token: first_token,
                error: PersistenceFailure::new(message.clone()),
            })?;
            self.fatal = Some(message);
            return Ok(());
        }
        let tokens = std::mem::take(&mut self.persistence_group.tokens);
        self.persistence_group.logical_bytes = 0;
        self.persistence_group.opened_at_ms = None;

        #[cfg(test)]
        if self.persistence_hold == TestPersistenceHold::AfterApplyBeforeCompletionOnce {
            self.persistence_hold = TestPersistenceHold::None;
            return Ok(());
        }

        for token in tokens {
            self.core.handle_input(Input::PersistenceCompleted(token))?;
        }
        Ok(())
    }

    fn record_persistence_sync_delta(&mut self, before: u64, after: u64) -> Result<()> {
        let delta = after.checked_sub(before).ok_or(ReplicationError::Invalid(
            "partition physical-sync counter moved backwards",
        ))?;
        self.persistence_group_metrics.physical_syncs = self
            .persistence_group_metrics
            .physical_syncs
            .checked_add(delta)
            .ok_or(ReplicationError::Invalid(
                "persistence physical-sync counter overflow",
            ))?;
        Ok(())
    }

    fn drive_effects(&mut self, now_ms: i64) -> Result<()> {
        loop {
            self.synchronize_replication_window_term();
            let effects = self.core.drain_effects().collect::<Vec<_>>();
            if effects.is_empty() {
                break;
            }
            for effect in effects {
                match effect {
                    Effect::Persist(batch) => {
                        #[cfg(test)]
                        if self.persistence_hold == TestPersistenceHold::BeforeApplyOnce {
                            self.persistence_hold = TestPersistenceHold::None;
                            return Ok(());
                        }
                        if self.role() == Role::Leader
                            && let Some(logical_bytes) = groupable_append_bytes(&batch)?
                        {
                            self.stage_persistence_group_batch(&batch, logical_bytes, now_ms)?;
                            continue;
                        }
                        if !self.persistence_group.tokens.is_empty() {
                            return Err(ReplicationError::Invalid(
                                "hard persistence barrier reached before append group drained",
                            ));
                        }
                        let installs_snapshot = batch.operations.iter().any(|operation| {
                            matches!(operation, PersistenceOp::InstallSnapshot(_))
                        });
                        let package = installs_snapshot
                            .then(|| self.pending_rebootstrap.take())
                            .flatten();
                        let package_v2 = installs_snapshot
                            .then(|| self.pending_rebootstrap_v2.take())
                            .flatten();
                        match self.persistence.execute_with_rebootstrap_versioned(
                            &batch,
                            now_ms,
                            package.as_ref(),
                            package_v2.as_ref(),
                        ) {
                            Ok(()) => {
                                #[cfg(test)]
                                if self.persistence_hold
                                    == TestPersistenceHold::AfterApplyBeforeCompletionOnce
                                {
                                    self.persistence_hold = TestPersistenceHold::None;
                                    return Ok(());
                                }
                                self.core
                                    .handle_input(Input::PersistenceCompleted(batch.token))?;
                            }
                            Err(error) => {
                                let message = error.to_string();
                                self.core.handle_input(Input::PersistenceFailed {
                                    token: batch.token,
                                    error: PersistenceFailure::new(message.clone()),
                                })?;
                                self.fatal = Some(message);
                            }
                        }
                    }
                    Effect::Send { target, message } => self.enqueue_outbound(target, message)?,
                    Effect::Apply(entries) => {
                        if let Some(last) = entries.last() {
                            self.core.handle_input(Input::AppliedThrough(last.index))?;
                        }
                    }
                    Effect::BuildSnapshot(index) => self.finish_snapshot_build(index)?,
                    Effect::InstallSnapshotToStateMachine(_) | Effect::RoleChanged { .. } => {}
                    Effect::ProposalResult { id, accepted, .. } => {
                        if let Some(pending) = self.pending.get_mut(&id) {
                            pending.outcome = if accepted {
                                match pending.receipt.ack {
                                    AckRequirement::None | AckRequirement::Leader => {
                                        ProposalOutcome::LocalDurable
                                    }
                                    AckRequirement::Quorum => ProposalOutcome::Pending,
                                }
                            } else {
                                ProposalOutcome::Rejected
                            };
                        } else if let Some(result) = self.pending_configuration_results.get_mut(&id)
                        {
                            *result = Some(accepted);
                        }
                    }
                    Effect::PreVoteQuorumReached { prospective_term } => {
                        self.core
                            .handle_input(Input::BeginElection { prospective_term })?;
                    }
                    Effect::ElectionQuorumReached { term } => {
                        self.core.handle_input(Input::WinElection { term })?;
                    }
                    Effect::Fatal(reason) => self.fatal = Some(reason),
                }
            }
        }
        self.refresh_quorum_completions()?;
        Ok(())
    }

    fn synchronize_replication_window_term(&mut self) {
        let current = self.core.hard_state().current_term;
        if current == self.replication_window_term {
            return;
        }
        for window in self.replication_windows.values_mut() {
            window.reset_scheduling();
        }
        self.replication_window_term = current;
    }

    fn observe_replication_response(&mut self, message: &RaftMessage) {
        match message {
            RaftMessage::AppendEntriesResponse(response)
                if response.term == self.core.hard_state().current_term =>
            {
                let Some(window) = self.replication_windows.get_mut(&response.follower_id) else {
                    return;
                };
                if !response.success {
                    window.reset_scheduling();
                    return;
                }
                while window
                    .in_flight
                    .front()
                    .is_some_and(|range| range.last_index <= response.match_index)
                {
                    if let Some(range) = window.in_flight.pop_front() {
                        window.in_flight_bytes = window.in_flight_bytes.saturating_sub(range.bytes);
                    }
                }
            }
            RaftMessage::InstallSnapshotResponse(response)
                if response.term == self.core.hard_state().current_term =>
            {
                if let Some(window) = self.replication_windows.get_mut(&response.follower_id) {
                    window.reset_scheduling();
                }
            }
            _ => {}
        }
    }

    fn finish_snapshot_build(&mut self, index: LogIndex) -> Result<()> {
        let term = self
            .core
            .stable_log()
            .iter()
            .find(|entry| entry.index == index)
            .map(|entry| entry.term)
            .or_else(|| {
                (self.core.snapshot().last_included_index == index)
                    .then_some(self.core.snapshot().last_included_term)
            })
            .ok_or(ReplicationError::Invalid(
                "snapshot build index term is unavailable",
            ))?;
        let EffectiveConfiguration::Stable(config) = self.core.effective_configuration() else {
            return Err(ReplicationError::Invalid(
                "snapshot build requested during joint configuration",
            ));
        };
        let mut id = [0_u8; 16];
        id[..8].copy_from_slice(&index.get().to_le_bytes());
        id[8..].copy_from_slice(&term.get().to_le_bytes());
        self.core
            .handle_input(Input::SnapshotBuilt(SnapshotMetadata::new(
                index,
                term,
                config.clone(),
                SnapshotId::new(id),
            )?))?;
        Ok(())
    }

    fn enqueue_outbound(&mut self, target: NodeId, message: RaftMessage) -> Result<()> {
        match message {
            RaftMessage::AppendEntriesRequest(request) if !request.entries.is_empty() => {
                self.enqueue_replication_append(target, request)
            }
            RaftMessage::InstallSnapshotRequest(request) => {
                self.replication_windows
                    .entry(target)
                    .or_default()
                    .reset_scheduling();
                let _ = self.enqueue_raw_outbound(
                    target,
                    RaftMessage::InstallSnapshotRequest(request),
                    false,
                )?;
                Ok(())
            }
            message => {
                let _ = self.enqueue_raw_outbound(target, message, false)?;
                Ok(())
            }
        }
    }

    fn enqueue_replication_append(
        &mut self,
        target: NodeId,
        request: astralog_raft::AppendEntriesRequest,
    ) -> Result<()> {
        let Some(first) = request.entries.first() else {
            return Ok(());
        };
        {
            let window = self.replication_windows.entry(target).or_default();
            if window.term != Some(request.term) {
                window.reset_scheduling();
                window.term = Some(request.term);
            } else if window
                .in_flight
                .front()
                .is_some_and(|range| first.index <= range.first_index)
            {
                // Raft is retrying from the same/older durable progress point,
                // so the prior speculative send ranges can no longer be
                // relied upon to make transport progress.
                window.reset_scheduling();
                window.term = Some(request.term);
            }
        }

        let sent_through = self
            .replication_windows
            .get(&target)
            .and_then(|window| window.sent_through);
        let mut prev_log_index = request.prev_log_index;
        let mut prev_log_term = request.prev_log_term;
        let mut pending = VecDeque::new();
        for entry in request.entries {
            if sent_through.is_some_and(|sent| entry.index <= sent) {
                prev_log_index = entry.index;
                prev_log_term = entry.term;
                continue;
            }
            pending.push_back(entry);
        }

        while !pending.is_empty() {
            let (in_flight_messages, in_flight_bytes) = self.replication_window_usage(target);
            if in_flight_messages >= self.limits.max_replication_inflight_messages_per_follower
                || in_flight_bytes >= self.limits.max_replication_inflight_bytes_per_follower
            {
                break;
            }
            let available_bytes = self
                .limits
                .max_replication_inflight_bytes_per_follower
                .saturating_sub(in_flight_bytes);
            let message_limit = self
                .limits
                .max_replication_catchup_bytes_per_message
                .min(available_bytes);
            if message_limit <= 128 {
                break;
            }

            let mut entries = Vec::new();
            let mut estimated = 128_usize;
            while let Some(entry) = pending.front() {
                let entry_bytes = estimate_entry_bytes(entry);
                let candidate = estimated.saturating_add(entry_bytes);
                if candidate > self.limits.max_replication_catchup_bytes_per_message {
                    if entries.is_empty() {
                        return Err(ReplicationError::LimitExceeded {
                            what: "replication catch-up message bytes",
                            actual: candidate,
                            max: self.limits.max_replication_catchup_bytes_per_message,
                        });
                    }
                    break;
                }
                if candidate > message_limit {
                    break;
                }
                let entry = pending.pop_front().expect("front was present");
                estimated = candidate;
                entries.push(entry);
            }
            if entries.is_empty() {
                break;
            }

            let first_index = entries.first().expect("non-empty chunk").index;
            let last = entries.last().expect("non-empty chunk");
            let last_index = last.index;
            let last_term = last.term;
            let message = RaftMessage::AppendEntriesRequest(astralog_raft::AppendEntriesRequest {
                term: request.term,
                leader_id: request.leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: request.leader_commit,
            });
            if !self.enqueue_raw_outbound(target, message, true)? {
                break;
            }
            let window = self.replication_windows.entry(target).or_default();
            window.term = Some(request.term);
            window.in_flight.push_back(InFlightReplicationRange {
                first_index,
                last_index,
                bytes: estimated,
            });
            window.in_flight_bytes =
                window
                    .in_flight_bytes
                    .checked_add(estimated)
                    .ok_or(ReplicationError::Invalid(
                        "replication in-flight byte count overflow",
                    ))?;
            window.sent_through = Some(last_index);
            prev_log_index = last_index;
            prev_log_term = last_term;
        }
        Ok(())
    }

    fn enqueue_raw_outbound(
        &mut self,
        target: NodeId,
        message: RaftMessage,
        data: bool,
    ) -> Result<bool> {
        let estimated = estimate_message_bytes(&message);
        let reserved_messages = (self.limits.max_outbound_messages / 8).min(64);
        let reserved_bytes = (self.limits.max_outbound_bytes / 8).min(1024 * 1024);
        let data_message_limit = self
            .limits
            .max_outbound_messages
            .saturating_sub(reserved_messages);
        let data_byte_limit = self
            .limits
            .max_outbound_bytes
            .saturating_sub(reserved_bytes);
        let data_limit_hit = data
            && (self.outbound_data_messages >= data_message_limit
                || self
                    .outbound_data_bytes
                    .checked_add(estimated)
                    .is_none_or(|bytes| bytes > data_byte_limit));
        if data_limit_hit
            || self.outbound.len() >= self.limits.max_outbound_messages
            || self
                .outbound_bytes
                .checked_add(estimated)
                .is_none_or(|bytes| bytes > self.limits.max_outbound_bytes)
        {
            // Raft already tolerates packet loss. Treat a bounded local outbox
            // overflow as a dropped transport attempt, remember congestion so
            // new producer work is rejected, and let heartbeats/election retry
            // later after the owner drains the queue.
            self.outbound_congested = true;
            return Ok(false);
        }
        self.outbound_bytes += estimated;
        if data {
            self.outbound_data_messages += 1;
            self.outbound_data_bytes += estimated;
        }
        self.outbound.push_back(OutboundMessage { target, message });
        Ok(true)
    }

    fn refresh_quorum_completions(&mut self) -> Result<()> {
        let committed = self.core.hard_state().durable_commit_index;
        let completed = self
            .pending
            .iter()
            .filter_map(|(id, pending)| {
                (pending.receipt.ack == AckRequirement::Quorum
                    && pending.outcome == ProposalOutcome::Pending
                    && pending.receipt.raft_index <= committed)
                    .then_some(*id)
            })
            .collect::<Vec<_>>();
        for id in completed {
            if let Some(pending) = self.pending.get_mut(&id) {
                pending.outcome = ProposalOutcome::QuorumCommitted;
            }
            self.apply_committed_idempotent(id)?;
        }
        Ok(())
    }

    fn refresh_proposals(&mut self, previous_role: Role) {
        if previous_role == Role::Leader && self.role() != Role::Leader {
            for pending in self.pending.values_mut() {
                if pending.outcome == ProposalOutcome::Pending {
                    pending.outcome = ProposalOutcome::LeadershipLost;
                }
            }
        }
        self.cleanup_terminal_idempotent();
    }

    fn remove_pending(&mut self, id: ProposalId) {
        let producer_id = self
            .pending_idempotent
            .iter()
            .find_map(|(producer_id, pending)| {
                (pending.receipt.proposal_id == id).then_some(*producer_id)
            });
        if let Some(producer_id) = producer_id {
            self.pending_idempotent.remove(&producer_id);
        }
        if let Some(pending) = self.pending.remove(&id) {
            self.pending_bytes = self.pending_bytes.saturating_sub(pending.encoded_bytes);
        }
    }

    fn load_producer_state(
        &mut self,
        producer_id: ProducerId,
    ) -> Result<Option<PartitionProducerStateV1>> {
        if let Some(state) = self.producer_cache.get(&producer_id) {
            return Ok(Some(state.clone()));
        }
        let mut recovered = self.persistence.recover_committed_producer_states()?;
        let Some(state) = recovered.remove(&producer_id) else {
            return Ok(None);
        };
        self.insert_producer_state(producer_id, state.clone())?;
        Ok(Some(state))
    }

    fn insert_producer_state(
        &mut self,
        producer_id: ProducerId,
        state: PartitionProducerStateV1,
    ) -> Result<()> {
        if let std::collections::btree_map::Entry::Occupied(mut entry) =
            self.producer_cache.entry(producer_id)
        {
            entry.insert(state);
            return Ok(());
        }
        if self.producer_cache.len() >= MAX_PRODUCER_CACHE_ENTRIES {
            let evict = self
                .producer_cache
                .keys()
                .copied()
                .find(|candidate| !self.pending_idempotent.contains_key(candidate))
                .ok_or(ReplicationError::Busy(
                    "producer cache is full and every resident entry has pending authority",
                ))?;
            self.producer_cache.remove(&evict);
        }
        self.producer_cache.insert(producer_id, state);
        Ok(())
    }

    fn apply_committed_idempotent(&mut self, proposal_id: ProposalId) -> Result<()> {
        let pending = self
            .pending_idempotent
            .iter()
            .find_map(|(producer_id, pending)| {
                (pending.receipt.proposal_id == proposal_id).then_some((*producer_id, *pending))
            });
        let Some((producer_id, pending)) = pending else {
            return Ok(());
        };
        let committed = CommittedBatchV1::new(
            pending.batch.authority,
            pending.batch.base_sequence,
            pending.batch.record_count,
            pending.receipt.base_offset,
            RaftIndex::new(pending.receipt.raft_index.get()).ok_or(ReplicationError::Invalid(
                "committed idempotent proposal cannot use Raft index zero",
            ))?,
            pending.batch.canonical_digest,
        )
        .map_err(|error| {
            ReplicationError::InvalidOwned(format!(
                "committed idempotent proposal is invalid: {error:?}"
            ))
        })?;
        if let Some(state) = self.producer_cache.get_mut(&producer_id) {
            state.apply_committed(committed).map_err(|error| {
                ReplicationError::InvalidOwned(format!(
                    "committed idempotent proposal conflicts with producer state: {error:?}"
                ))
            })?;
        } else {
            let state =
                PartitionProducerStateV1::from_first_committed(committed).map_err(|error| {
                    ReplicationError::InvalidOwned(format!(
                        "first committed idempotent proposal is invalid: {error:?}"
                    ))
                })?;
            self.insert_producer_state(producer_id, state)?;
        }
        self.pending_idempotent.remove(&producer_id);
        Ok(())
    }

    fn cleanup_terminal_idempotent(&mut self) {
        let terminal = self
            .pending_idempotent
            .iter()
            .filter_map(|(producer_id, pending)| {
                let outcome = self
                    .pending
                    .get(&pending.receipt.proposal_id)
                    .map(|proposal| proposal.outcome);
                matches!(
                    outcome,
                    Some(ProposalOutcome::Rejected | ProposalOutcome::LeadershipLost) | None
                )
                .then_some(*producer_id)
            })
            .collect::<Vec<_>>();
        for producer_id in terminal {
            self.pending_idempotent.remove(&producer_id);
        }
    }
}

fn validate_controller_evidence(
    authority: ProducerAuthority,
    evidence: ControllerAuthorityEvidence,
) -> std::result::Result<(), IdempotentProduceError> {
    match evidence {
        ControllerAuthorityEvidence::NotChecked => {
            Err(IdempotentProduceError::ControllerValidationRequired)
        }
        ControllerAuthorityEvidence::Missing => Err(IdempotentProduceError::UnknownProducer),
        ControllerAuthorityEvidence::Confirmed(confirmed) if confirmed == authority => Ok(()),
        ControllerAuthorityEvidence::Confirmed(confirmed)
            if confirmed.producer_id == authority.producer_id =>
        {
            Err(IdempotentProduceError::ProducerFenced)
        }
        ControllerAuthorityEvidence::Confirmed(_) => Err(IdempotentProduceError::UnknownProducer),
    }
}

fn map_producer_state_error(error: ProducerStateError) -> IdempotentProduceError {
    match error {
        ProducerStateError::ProducerFenced => IdempotentProduceError::ProducerFenced,
        ProducerStateError::OutOfOrderSequence
        | ProducerStateError::InvalidProducer
        | ProducerStateError::InvalidBatch(_)
        | ProducerStateError::InvalidCommittedHistory(_) => {
            IdempotentProduceError::OutOfOrderSequence
        }
    }
}

fn validate_replica_limits(limits: ReplicaLimits) -> Result<()> {
    if limits.max_outbound_messages == 0
        || limits.max_outbound_bytes == 0
        || limits.max_replication_inflight_messages_per_follower == 0
        || limits.max_replication_inflight_bytes_per_follower == 0
        || limits.max_replication_catchup_bytes_per_message == 0
        || limits.max_pending_proposals == 0
        || limits.max_pending_proposal_bytes == 0
        || limits.max_pending_persistence_batches == 0
        || limits.max_pending_persistence_bytes == 0
        || limits.max_persistence_group_delay_ms == 0
    {
        return Err(ReplicationError::Invalid(
            "all replica resource limits must be non-zero",
        ));
    }
    if limits.max_replication_catchup_bytes_per_message
        > limits.max_replication_inflight_bytes_per_follower
    {
        return Err(ReplicationError::Invalid(
            "replication catch-up message bytes exceed follower in-flight byte window",
        ));
    }
    if limits.max_persistence_group_delay_ms > i64::MAX as u64 {
        return Err(ReplicationError::Invalid(
            "persistence group delay exceeds injected monotonic clock range",
        ));
    }
    Ok(())
}

fn groupable_append_bytes(batch: &PersistenceBatch) -> Result<Option<usize>> {
    let [PersistenceOp::Append(entries)] = batch.operations.as_slice() else {
        return Ok(None);
    };
    if entries.is_empty() {
        return Ok(None);
    }
    let mut bytes = 0_usize;
    for entry in entries {
        let EntryKind::Application(payload) = &entry.kind else {
            return Ok(None);
        };
        bytes = bytes
            .checked_add(payload.len())
            .ok_or(ReplicationError::Invalid(
                "persistence group logical-byte accounting overflow",
            ))?;
    }
    Ok(Some(bytes))
}

fn estimate_message_bytes(message: &RaftMessage) -> usize {
    match message {
        RaftMessage::AppendEntriesRequest(request) => request
            .entries
            .iter()
            .map(estimate_entry_bytes)
            .fold(128_usize, usize::saturating_add),
        _ => 256,
    }
}

fn estimate_entry_bytes(entry: &astralog_raft::LogEntry) -> usize {
    match &entry.kind {
        EntryKind::Application(payload) => 48_usize.saturating_add(payload.len()),
        EntryKind::LeaderBarrier => 48,
        EntryKind::Configuration(_) => 256,
    }
}

fn message_sender(message: &RaftMessage) -> NodeId {
    match message {
        RaftMessage::PreVoteRequest(value) => value.candidate_id,
        RaftMessage::PreVoteResponse(value) => value.voter_id,
        RaftMessage::RequestVoteRequest(value) => value.candidate_id,
        RaftMessage::RequestVoteResponse(value) => value.voter_id,
        RaftMessage::AppendEntriesRequest(value) => value.leader_id,
        RaftMessage::AppendEntriesResponse(value) => value.follower_id,
        RaftMessage::InstallSnapshotRequest(value) => value.leader_id,
        RaftMessage::InstallSnapshotResponse(value) => value.follower_id,
        RaftMessage::TimeoutNow(value) => value.leader_id,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use astralog_common::{RaftIndex, RaftTerm};
    use astralog_raft::{
        AppendEntriesRequest, InstallSnapshotRequest, LogEntry, PreVoteRequest, SnapshotId, Term,
    };
    use astralog_storage::{Compression, ControlEntry, Frame};

    use super::*;

    fn node(value: u64) -> NodeId {
        NodeId::new(value).unwrap()
    }

    fn temp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "astralog-p4-runtime-{label}-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn record(value: &[u8]) -> DataRecord {
        DataRecord {
            timestamp_ms: 1,
            key: None,
            value: Some(value.to_vec()),
            headers: Vec::new(),
        }
    }

    fn authority(id: u64, epoch: u32) -> ProducerAuthority {
        ProducerAuthority::new(
            ProducerId::new(id).unwrap(),
            astralog_common::ProducerEpoch::new(epoch).unwrap(),
        )
    }

    fn elect_single(replica: &mut ReplicatedPartition) {
        for _ in 0..5 {
            replica.tick(5, 0).unwrap();
        }
        assert_eq!(replica.role(), Role::Leader);
    }

    fn single(label: &str) -> (PathBuf, ReplicatedPartition) {
        let dir = temp_dir(label);
        let replica = ReplicatedPartition::open(
            &dir,
            TopicId::new([1; 16]),
            PartitionId::new(0),
            node(1),
            0,
            PartitionLogConfig::default(),
            StableConfiguration::new([node(1)], []).unwrap(),
            TimingConfig::new(1, 5, 10).unwrap(),
            5,
            CommandLimits::default(),
            ReplicaLimits::default(),
        )
        .unwrap();
        (dir, replica)
    }

    fn grouped_single(
        label: &str,
        max_batches: usize,
        max_bytes: usize,
        max_delay_ms: u64,
    ) -> (PathBuf, ReplicatedPartition) {
        let dir = temp_dir(label);
        let limits = ReplicaLimits {
            max_pending_persistence_batches: max_batches,
            max_pending_persistence_bytes: max_bytes,
            max_persistence_group_delay_ms: max_delay_ms,
            ..ReplicaLimits::default()
        };
        let mut replica = ReplicatedPartition::open(
            &dir,
            TopicId::new([0x81; 16]),
            PartitionId::new(0),
            node(1),
            0,
            PartitionLogConfig::default(),
            StableConfiguration::new([node(1)], []).unwrap(),
            TimingConfig::new(1, 5, 10).unwrap(),
            5,
            CommandLimits::default(),
            limits,
        )
        .unwrap();
        elect_single(&mut replica);
        (dir, replica)
    }

    #[test]
    fn single_replica_quorum_ack_is_committed_and_visible() {
        let (dir, mut replica) = single("single-quorum");
        for _ in 0..5 {
            replica.tick(5, 0).unwrap();
        }
        assert_eq!(replica.role(), Role::Leader);
        let receipt = replica
            .propose_records(vec![record(b"value")], AckRequirement::Quorum, 1)
            .unwrap();
        assert_eq!(
            replica.proposal_outcome(receipt.proposal_id),
            Some(ProposalOutcome::QuorumCommitted)
        );
        assert_eq!(replica.committed_stream_high_watermark().unwrap().get(), 1);
        assert_eq!(
            replica
                .read_committed_records(StreamOffset::new(0), 10)
                .unwrap()
                .len(),
            1
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p9d_rf1_log_start_commit_is_quorum_ordered_offset_free_and_restart_durable() {
        let (dir, mut replica) = single("p9d-rf1-log-start");
        elect_single(&mut replica);
        let produced = replica
            .propose_records(vec![record(b"expired")], AckRequirement::Quorum, 1)
            .unwrap();
        assert_eq!(
            replica.proposal_outcome(produced.proposal_id),
            Some(ProposalOutcome::QuorumCommitted)
        );
        assert_eq!(replica.next_stream_offset().get(), 1);
        assert_eq!(replica.committed_log_start_offset().unwrap().get(), 0);

        let epoch = replica.leader_epoch();
        assert!(
            replica
                .propose_log_start_offset(epoch.saturating_add(1), StreamOffset::new(1), 2)
                .is_err()
        );
        assert!(
            replica
                .propose_log_start_offset(epoch, StreamOffset::new(2), 2)
                .is_err()
        );
        let maintenance = replica
            .propose_log_start_offset(epoch, StreamOffset::new(1), 2)
            .unwrap();
        assert_eq!(
            replica.proposal_outcome(maintenance.proposal_id),
            Some(ProposalOutcome::QuorumCommitted)
        );
        assert_eq!(replica.committed_log_start_offset().unwrap().get(), 1);
        assert_eq!(
            replica.next_stream_offset().get(),
            1,
            "P9 maintenance consumes no stream offset"
        );
        assert!(
            replica
                .read_committed_records(StreamOffset::new(0), 10)
                .unwrap()
                .is_empty()
        );
        assert_eq!(replica.list_committed_offset(-2).unwrap(), (1, -1));
        drop(replica);

        let restarted = ReplicatedPartition::open(
            &dir,
            TopicId::new([1; 16]),
            PartitionId::new(0),
            node(1),
            3,
            PartitionLogConfig::default(),
            StableConfiguration::new([node(1)], []).unwrap(),
            TimingConfig::new(1, 5, 10).unwrap(),
            5,
            CommandLimits::default(),
            ReplicaLimits::default(),
        )
        .unwrap();
        assert_eq!(restarted.committed_log_start_offset().unwrap().get(), 1);
        assert_eq!(restarted.next_stream_offset().get(), 1);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p9g_rf1_safe_offset_requires_full_report_evidence_and_restarts_from_committed_control() {
        let (dir, mut replica) = single("p9g-rf1-safe-offset");
        elect_single(&mut replica);
        let leader_epoch = replica.leader_epoch();
        let receipt = replica
            .propose_records(vec![record(b"v")], AckRequirement::Quorum, 1)
            .unwrap();
        assert_eq!(
            replica.proposal_outcome(receipt.proposal_id),
            Some(ProposalOutcome::QuorumCommitted)
        );
        let high_watermark = replica.committed_stream_high_watermark().unwrap();
        assert_eq!(high_watermark, StreamOffset::new(1));
        assert_eq!(
            replica.committed_compaction_safe_offset().unwrap(),
            StreamOffset::new(0)
        );

        let effective = replica.membership_observation().effective_configuration;
        let voters = BTreeMap::from([(
            node(1),
            crate::VoterIdentity {
                broker_id: 0,
                incarnation: 3,
            },
        )]);
        let mut evidence = CleanProgressTracker::from_committed_assignment(
            replica.topic_id(),
            replica.partition_id(),
            leader_epoch,
            7,
            &effective,
            &voters,
        )
        .unwrap();
        assert!(
            replica
                .propose_compaction_safe_from_reports(leader_epoch, 7, &evidence, 2)
                .unwrap()
                .is_none(),
            "absent voter progress must not create safe authority"
        );
        let progress = crate::CleanProgressReport {
            topic_id: replica.topic_id(),
            partition_id: replica.partition_id(),
            term: leader_epoch,
            leader_epoch,
            source_broker_id: 0,
            source_incarnation: 3,
            assignment_generation: 7,
            clean_compacted_offset: high_watermark,
        };
        assert!(!evidence.accept(node(1), 0, 2, progress, high_watermark));
        assert!(!evidence.accept(node(1), 0, 3, progress, StreamOffset::new(0)));
        assert!(evidence.accept(node(1), 0, 3, progress, high_watermark));
        assert!(
            replica
                .propose_compaction_safe_from_reports(leader_epoch + 1, 7, &evidence, 3)
                .is_err()
        );
        assert!(
            replica
                .propose_compaction_safe_from_reports(leader_epoch, 8, &evidence, 3)
                .is_err()
        );
        let maintenance = replica
            .propose_compaction_safe_from_reports(leader_epoch, 7, &evidence, 3)
            .unwrap()
            .expect("complete current-voter evidence advances safe state");
        assert_eq!(
            replica.proposal_outcome(maintenance.proposal_id),
            Some(ProposalOutcome::QuorumCommitted)
        );
        assert_eq!(
            replica.committed_compaction_safe_offset().unwrap(),
            high_watermark
        );
        assert_eq!(
            replica.committed_stream_high_watermark().unwrap(),
            high_watermark
        );
        assert_eq!(
            replica.next_stream_offset(),
            high_watermark,
            "maintenance consumes no offset"
        );
        assert_eq!(
            replica.cached_committed_compaction_safe_offset().unwrap(),
            high_watermark,
            "P9-G committed control must invalidate the previous safe=0 Raft-index cache"
        );
        assert!(
            replica
                .cached_rebootstrap_required_for_compaction()
                .unwrap(),
            "fabricated report cannot replace independent local sidecar proof"
        );
        // P9-G liveness regression: a committed safe boundary can become
        // greater than this leader's newly verified local progress. Its
        // authority must fail closed without destroying the partition actor:
        // the next genuine higher-term P3 leader needs it alive to deliver
        // AppendEntries/InstallSnapshot recovery.
        let committed_before_fence = replica.durable_commit_index();
        replica
            .tick(5, 4)
            .expect("unsafe leader tick must not kill actor");
        assert!(!replica.is_fatal());
        assert_eq!(replica.durable_commit_index(), committed_before_fence);
        assert!(
            replica
                .propose_records(vec![record(b"must-not-append")], AckRequirement::Quorum, 5)
                .is_err(),
            "unverified retained history must not accept new leader writes"
        );
        let fenced_producer = authority(701, 1);
        assert!(matches!(
            replica.propose_idempotent_records(
                replica.leader_epoch(),
                fenced_producer,
                ProducerSequence::ZERO,
                vec![record(b"must-not-idempotently-ack")],
                AckRequirement::Quorum,
                ControllerAuthorityEvidence::Confirmed(fenced_producer),
                5,
            ),
            Err(IdempotentProduceError::Busy(_))
        ));
        assert_eq!(replica.durable_commit_index(), committed_before_fence);
        assert!(
            replica
                .plan_keyed_compaction(7, CompactionLimits::default())
                .is_ok(),
            "positive safe offset allows regular COW while tombstones still require a separate gate"
        );
        assert!(
            replica
                .plan_delete_retention(100, Some(1), None, None)
                .is_ok(),
            "retention planning is allowed; manifest publication preserves the committed P9-G safe-control anchor"
        );
        drop(replica);

        let mut restarted = ReplicatedPartition::open(
            &dir,
            TopicId::new([1; 16]),
            PartitionId::new(0),
            node(1),
            5,
            PartitionLogConfig::default(),
            StableConfiguration::new([node(1)], []).unwrap(),
            TimingConfig::new(1, 5, 10).unwrap(),
            5,
            CommandLimits::default(),
            ReplicaLimits::default(),
        )
        .unwrap();
        assert_eq!(
            restarted.committed_compaction_safe_offset().unwrap(),
            high_watermark
        );
        assert!(
            restarted.is_rebootstrap_required_for_compaction().unwrap(),
            "restart without locally proved safe retained history is not leadership eligible"
        );
        for tick in 0..20 {
            restarted.tick(5, 6 + tick).unwrap();
            assert_ne!(
                restarted.role(),
                Role::Leader,
                "behind-safe replica must not initiate a new local election"
            );
        }
        let generation = restarted.manifest_generation();
        assert_eq!(
            restarted.last_clean_revalidation,
            Some((generation, high_watermark.get(), 19)),
            "a failed full-prefix proof must not be retried on every fast Raft tick"
        );
        for tick in 20..=50 {
            restarted.tick(5, 6 + tick).unwrap();
            assert_ne!(restarted.role(), Role::Leader);
        }
        assert_eq!(
            restarted.last_clean_revalidation,
            Some((generation, high_watermark.get(), 50))
        );
        restarted.tick(5, 57).unwrap();
        assert_eq!(
            restarted.last_clean_revalidation,
            Some((generation, high_watermark.get(), 0)),
            "a failed same-generation verification must be periodically retried"
        );
        assert!(restarted.is_rebootstrap_required_for_compaction().unwrap());
        assert_ne!(
            restarted.role(),
            Role::Leader,
            "retry of unavailable clean proof must never weaken the election fence"
        );
        drop(restarted);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p9e_rf1_compaction_is_snapshot_fenced_and_fetch_continues_across_sparse_holes() {
        let dir = temp_dir("p9e-rf1-compaction");
        let storage_config = PartitionLogConfig {
            max_segment_bytes: 320,
            ..PartitionLogConfig::default()
        };
        let mut replica = ReplicatedPartition::open(
            &dir,
            TopicId::new([0xe9; 16]),
            PartitionId::new(0),
            node(1),
            0,
            storage_config,
            StableConfiguration::new([node(1)], []).unwrap(),
            TimingConfig::new(1, 5, 10).unwrap(),
            5,
            CommandLimits::default(),
            ReplicaLimits::default(),
        )
        .unwrap();
        elect_single(&mut replica);
        let keyed = |key: &[u8], value: &[u8], timestamp_ms: i64| DataRecord {
            timestamp_ms,
            key: Some(key.to_vec()),
            value: Some(value.to_vec()),
            headers: Vec::new(),
        };
        let first = replica
            .propose_records(
                vec![
                    keyed(b"K", b"old", 1),
                    keyed(b"X", b"stay", 2),
                    keyed(b"K", b"new", 3),
                ],
                AckRequirement::Quorum,
                1,
            )
            .unwrap();
        assert_eq!(
            replica.proposal_outcome(first.proposal_id),
            Some(ProposalOutcome::QuorumCommitted)
        );
        let second = replica
            .propose_records(vec![record(b"active")], AckRequirement::Quorum, 2)
            .unwrap();
        assert_eq!(
            replica.proposal_outcome(second.proposal_id),
            Some(ProposalOutcome::QuorumCommitted)
        );
        assert!(replica.manifest_generation().is_none());

        // Without a persisted P3 snapshot floor, no source may be selected.
        let before_snapshot = replica
            .plan_keyed_compaction(77, CompactionLimits::default())
            .unwrap();
        assert!(before_snapshot.is_empty());

        let epoch = replica.leader_epoch();
        let snapshot_floor = replica
            .snapshot_committed_prefix_for_retention(epoch, 3)
            .unwrap();
        assert!(snapshot_floor > LogIndex::ZERO);
        let before_high = replica.committed_stream_high_watermark().unwrap();
        let compact = replica
            .plan_keyed_compaction(77, CompactionLimits::default())
            .unwrap();
        assert_eq!(compact.source_file_names.len(), 1);
        assert!(
            replica.execute_keyed_compaction(&compact, 78).is_err(),
            "policy epoch drift must invalidate the cleaner plan"
        );
        let outcome = replica.execute_keyed_compaction(&compact, 77).unwrap();
        assert_eq!(outcome.replaced_segments, 1);
        assert_eq!(
            replica.committed_stream_high_watermark().unwrap(),
            before_high
        );
        let retained = replica
            .read_committed_records(StreamOffset::new(0), 10)
            .unwrap();
        assert_eq!(
            retained
                .iter()
                .map(|record| record.offset.get())
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(
            replica
                .read_committed_records(StreamOffset::new(0), 1)
                .unwrap()[0]
                .offset
                .get(),
            1,
            "Fetch must continue at the first surviving sparse offset"
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p9f_rf1_checkpoint_makes_producer_history_reclaimable_and_recovery_sidecar_independent() {
        let dir = temp_dir("p9f-rf1-producer-checkpoint");
        let storage_config = PartitionLogConfig {
            max_segment_bytes: 420,
            ..PartitionLogConfig::default()
        };
        let topic_id = TopicId::new([0xf9; 16]);
        let mut replica = ReplicatedPartition::open(
            &dir,
            topic_id,
            PartitionId::new(0),
            node(1),
            0,
            storage_config,
            StableConfiguration::new([node(1)], []).unwrap(),
            TimingConfig::new(1, 5, 10).unwrap(),
            5,
            CommandLimits::default(),
            ReplicaLimits::default(),
        )
        .unwrap();
        elect_single(&mut replica);
        let leader_epoch = replica.leader_epoch();
        let keyed = |value: u8| DataRecord {
            timestamp_ms: i64::from(value),
            key: Some(b"producer-key".to_vec()),
            value: Some(vec![value; 180]),
            headers: Vec::new(),
        };

        let epoch_one = authority(77, 1);
        let first = match replica
            .propose_idempotent_records(
                leader_epoch,
                epoch_one,
                ProducerSequence::ZERO,
                vec![keyed(1)],
                AckRequirement::Quorum,
                ControllerAuthorityEvidence::Confirmed(epoch_one),
                1,
            )
            .unwrap()
        {
            IdempotentProposalDisposition::Proposed(receipt) => receipt,
            other => panic!("unexpected epoch-one disposition: {other:?}"),
        };
        assert_eq!(
            replica.proposal_outcome(first.proposal_id),
            Some(ProposalOutcome::QuorumCommitted)
        );

        let epoch_two = authority(77, 2);
        let batches = [
            (0, vec![keyed(2), keyed(3)]),
            (2, vec![keyed(4)]),
            (3, vec![keyed(5)]),
            (4, vec![keyed(6)]),
            (5, vec![keyed(7)]),
            (6, vec![keyed(8)]),
        ];
        let mut latest_receipt = None;
        for (ordinal, (base_sequence, records)) in batches.into_iter().enumerate() {
            let disposition = replica
                .propose_idempotent_records(
                    leader_epoch,
                    epoch_two,
                    ProducerSequence::new(base_sequence),
                    records,
                    AckRequirement::Quorum,
                    if ordinal == 0 {
                        ControllerAuthorityEvidence::Confirmed(epoch_two)
                    } else {
                        ControllerAuthorityEvidence::NotChecked
                    },
                    i64::try_from(ordinal + 2).unwrap(),
                )
                .unwrap();
            let IdempotentProposalDisposition::Proposed(receipt) = disposition else {
                panic!("unexpected epoch-two disposition: {disposition:?}");
            };
            assert_eq!(
                replica.proposal_outcome(receipt.proposal_id),
                Some(ProposalOutcome::QuorumCommitted)
            );
            latest_receipt = Some(receipt);
        }
        let latest_receipt = latest_receipt.unwrap();

        // A newer non-idempotent value makes every producer-bearing keyed
        // source physically redundant, while producer authority must remain.
        let winner = replica
            .propose_records(vec![keyed(9)], AckRequirement::Quorum, 20)
            .unwrap();
        assert_eq!(
            replica.proposal_outcome(winner.proposal_id),
            Some(ProposalOutcome::QuorumCommitted)
        );
        let high_watermark = replica.committed_stream_high_watermark().unwrap();
        replica.persist_producer_snapshot().unwrap();
        assert!(replica.persistence.producer_snapshot_path().is_file());

        let duplicate_before = match replica
            .propose_idempotent_records(
                leader_epoch,
                epoch_two,
                ProducerSequence::new(6),
                vec![keyed(8)],
                AckRequirement::Quorum,
                ControllerAuthorityEvidence::NotChecked,
                21,
            )
            .unwrap()
        {
            IdempotentProposalDisposition::Duplicate(retry) => retry,
            other => panic!("latest retry did not dedupe before cleanup: {other:?}"),
        };
        assert_eq!(
            duplicate_before.raft_index.get(),
            latest_receipt.raft_index.get()
        );
        assert!(matches!(
            replica.propose_idempotent_records(
                leader_epoch,
                epoch_one,
                ProducerSequence::ZERO,
                vec![keyed(1)],
                AckRequirement::Quorum,
                ControllerAuthorityEvidence::NotChecked,
                21,
            ),
            Err(IdempotentProduceError::ProducerFenced)
        ));
        for (base, records) in [
            (6, vec![keyed(99)]),
            (5, vec![keyed(7), keyed(8)]),
            (8, vec![keyed(10)]),
            (0, vec![keyed(2), keyed(3)]),
        ] {
            assert!(matches!(
                replica.propose_idempotent_records(
                    leader_epoch,
                    epoch_two,
                    ProducerSequence::new(base),
                    records,
                    AckRequirement::Quorum,
                    ControllerAuthorityEvidence::NotChecked,
                    21,
                ),
                Err(IdempotentProduceError::OutOfOrderSequence)
            ));
        }

        // First snapshot makes producer source frames snapshot-safe, but no
        // checkpoint exists yet, so P9-F must still pin them.
        let first_floor = replica
            .snapshot_committed_prefix_for_retention(leader_epoch, 22)
            .unwrap();
        assert!(first_floor > LogIndex::ZERO);
        let before_checkpoint = replica
            .plan_keyed_compaction(91, CompactionLimits::default())
            .unwrap();
        assert!(
            before_checkpoint.is_empty(),
            "producer history is not reclaimable before checkpoint commit"
        );

        let checkpoint = replica
            .propose_producer_state_checkpoint(leader_epoch, 23)
            .unwrap()
            .expect("committed producer state requires a new checkpoint");
        assert_eq!(
            replica.proposal_outcome(checkpoint.proposal_id),
            Some(ProposalOutcome::QuorumCommitted)
        );
        assert_eq!(
            replica.committed_stream_high_watermark().unwrap(),
            high_watermark,
            "producer checkpoint consumes no stream offset"
        );
        assert!(
            replica
                .plan_keyed_compaction(91, CompactionLimits::default())
                .unwrap()
                .is_empty(),
            "checkpoint commit alone is insufficient until the checkpoint control is below the cleaning snapshot floor"
        );

        let second_floor = replica
            .snapshot_committed_prefix_for_retention(leader_epoch, 24)
            .unwrap();
        assert!(second_floor >= checkpoint.raft_index);
        let plan = replica
            .plan_keyed_compaction(91, CompactionLimits::default())
            .unwrap();
        assert!(
            !plan.source_file_names.is_empty(),
            "snapshot-safe checkpoint must unlock covered producer source cleanup"
        );
        let outcome = replica.execute_keyed_compaction(&plan, 91).unwrap();
        assert!(outcome.replaced_segments > 0);
        assert_eq!(
            replica.committed_stream_high_watermark().unwrap(),
            high_watermark
        );
        let recovered_before_restart = replica
            .persistence
            .recover_committed_producer_states()
            .unwrap();
        let expected_state = recovered_before_restart[&epoch_two.producer_id].clone();

        replica.discard_producer_snapshot().unwrap();
        assert!(!replica.persistence.producer_snapshot_path().exists());
        drop(replica);

        let mut restarted = ReplicatedPartition::open(
            &dir,
            topic_id,
            PartitionId::new(0),
            node(1),
            25,
            storage_config,
            StableConfiguration::new([node(1)], []).unwrap(),
            TimingConfig::new(1, 5, 10).unwrap(),
            5,
            CommandLimits::default(),
            ReplicaLimits::default(),
        )
        .unwrap();
        assert_eq!(
            restarted
                .persistence
                .recover_committed_producer_states()
                .unwrap()[&epoch_two.producer_id],
            expected_state
        );
        elect_single(&mut restarted);
        let restarted_epoch = restarted.leader_epoch();
        let duplicate_after = match restarted
            .propose_idempotent_records(
                restarted_epoch,
                epoch_two,
                ProducerSequence::new(6),
                vec![keyed(8)],
                AckRequirement::Quorum,
                ControllerAuthorityEvidence::NotChecked,
                26,
            )
            .unwrap()
        {
            IdempotentProposalDisposition::Duplicate(retry) => retry,
            other => panic!("latest retry did not dedupe after checkpoint recovery: {other:?}"),
        };
        assert_eq!(duplicate_after, duplicate_before);
        assert_eq!(
            restarted.committed_stream_high_watermark().unwrap(),
            high_watermark
        );
        assert!(matches!(
            restarted.propose_idempotent_records(
                restarted_epoch,
                epoch_one,
                ProducerSequence::ZERO,
                vec![keyed(1)],
                AckRequirement::Quorum,
                ControllerAuthorityEvidence::NotChecked,
                26,
            ),
            Err(IdempotentProduceError::ProducerFenced)
        ));
        for (base, records) in [
            (6, vec![keyed(99)]),
            (5, vec![keyed(7), keyed(8)]),
            (8, vec![keyed(10)]),
            (0, vec![keyed(2), keyed(3)]),
        ] {
            assert!(matches!(
                restarted.propose_idempotent_records(
                    restarted_epoch,
                    epoch_two,
                    ProducerSequence::new(base),
                    records,
                    AckRequirement::Quorum,
                    ControllerAuthorityEvidence::NotChecked,
                    26,
                ),
                Err(IdempotentProduceError::OutOfOrderSequence)
            ));
        }
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn follower_rejects_produce_and_source_identity_is_fenced() {
        let (dir, mut replica) = single("identity");
        assert!(
            replica
                .propose_records(vec![record(b"x")], AckRequirement::Leader, 0)
                .is_err()
        );
        let message = RaftMessage::PreVoteRequest(PreVoteRequest {
            prospective_term: Term::new(1),
            candidate_id: node(1),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::ZERO,
        });
        assert!(replica.step(node(2), message, 0).is_err());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p3_authorizes_rebootstrap_then_append_entries_continues_from_snapshot_boundary() {
        let dir = temp_dir("authorized-rebootstrap");
        let configuration = StableConfiguration::new([node(1), node(2), node(3)], []).unwrap();
        let mut replica = ReplicatedPartition::open(
            &dir,
            TopicId::new([2; 16]),
            PartitionId::new(0),
            node(2),
            0,
            PartitionLogConfig::default(),
            configuration.clone(),
            TimingConfig::new(1, 5, 10).unwrap(),
            6,
            CommandLimits::default(),
            ReplicaLimits::default(),
        )
        .unwrap();
        let snapshot = SnapshotMetadata::new(
            LogIndex::FIRST,
            Term::new(1),
            configuration,
            SnapshotId::new([0x77; 16]),
        )
        .unwrap();
        let package = RebootstrapPackage {
            topic_id: replica.topic_id(),
            partition_id: replica.partition_id(),
            snapshot: snapshot.clone(),
            frames: vec![Frame::Control(ControlEntry {
                raft_term: RaftTerm::new(1),
                raft_index: RaftIndex::FIRST,
                control_kind: crate::P4_CONTROL_LEADER_BARRIER,
                control_version: 1,
                body: Vec::new(),
            })],
        };

        replica
            .step_with_rebootstrap(
                node(1),
                RaftMessage::InstallSnapshotRequest(InstallSnapshotRequest {
                    term: Term::new(1),
                    leader_id: node(1),
                    snapshot: snapshot.clone(),
                }),
                package,
                1,
            )
            .unwrap();

        assert!(replica.pending_rebootstrap.is_none());
        let recovered = replica.recovered_state().unwrap();
        assert_eq!(recovered.snapshot, snapshot);
        assert_eq!(recovered.hard_state.durable_commit_index, LogIndex::FIRST);
        assert!(recovered.log.is_empty());

        let payload = encode_application_command(
            &ApplicationCommand {
                base_stream_offset: StreamOffset::new(0),
                records: vec![record(b"post-snapshot")],
            },
            CommandLimits::default(),
        )
        .unwrap();
        replica
            .step(
                node(1),
                RaftMessage::AppendEntriesRequest(AppendEntriesRequest {
                    term: Term::new(1),
                    leader_id: node(1),
                    prev_log_index: LogIndex::FIRST,
                    prev_log_term: Term::new(1),
                    entries: vec![LogEntry::new(
                        Term::new(1),
                        LogIndex::new(2),
                        EntryKind::Application(payload),
                    )],
                    leader_commit: LogIndex::new(2),
                }),
                2,
            )
            .unwrap();

        assert_eq!(replica.durable_commit_index(), LogIndex::new(2));
        let visible = replica
            .read_committed_records(StreamOffset::new(0), 10)
            .unwrap();
        assert_eq!(visible.len(), 1);
        assert_eq!(
            visible[0].record.value.as_deref(),
            Some(b"post-snapshot".as_slice())
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p9g_v2_install_snapshot_is_p3_authorized_and_followed_by_normal_v1_suffix() {
        let source_dir = temp_dir("p9g-v2-p3-source");
        let target_dir = temp_dir("p9g-v2-p3-target");
        let topic_id = TopicId::new([0xb7; 16]);
        let configuration = StableConfiguration::new([node(1), node(2), node(3)], []).unwrap();
        let (mut source, _) = PartitionPersistence::open(
            &source_dir,
            topic_id,
            PartitionId::new(0),
            0,
            PartitionLogConfig::default(),
            configuration.clone(),
            CommandLimits::default(),
        )
        .unwrap();
        source
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(1),
                    vec![
                        PersistenceOp::UpdateHardState(astralog_raft::HardState {
                            current_term: Term::new(1),
                            voted_for: Some(node(1)),
                            durable_commit_index: LogIndex::ZERO,
                        }),
                        PersistenceOp::Append(vec![LogEntry::new(
                            Term::new(1),
                            LogIndex::FIRST,
                            EntryKind::LeaderBarrier,
                        )]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::FIRST,
                        },
                    ],
                ),
                0,
            )
            .unwrap();
        let snapshot = SnapshotMetadata::new(
            LogIndex::FIRST,
            Term::new(1),
            configuration.clone(),
            SnapshotId::new([0xb7; 16]),
        )
        .unwrap();
        source
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(2),
                    vec![PersistenceOp::InstallSnapshot(snapshot.clone())],
                ),
                1,
            )
            .unwrap();
        source.log_mut().roll(2).unwrap();
        source.log_mut().adopt_manifest_inventory().unwrap();
        let package = source
            .build_rebootstrap_package_v2(&snapshot, RebootstrapLimitsV2::default())
            .unwrap();
        // A follower can already have the entire snapshot index committed in
        // its own V1 history while its P3 snapshot floor is still behind. An
        // incoming V2 package at that boundary must neither replace its
        // committed bytes nor turn an ordinary P3 snapshot advancement into a
        // fatal persistence error. This occurred during live P9-G RF4 joint
        // membership and permanently removed a voter from the election.
        let ahead_dir = temp_dir("p9g-v2-committed-prefix-no-replacement");
        let mut ahead = ReplicatedPartition::open(
            &ahead_dir,
            topic_id,
            PartitionId::new(0),
            node(2),
            0,
            PartitionLogConfig::default(),
            configuration.clone(),
            TimingConfig::new(1, 5, 10).unwrap(),
            6,
            CommandLimits::default(),
            ReplicaLimits::default(),
        )
        .unwrap();
        ahead
            .step(
                node(1),
                RaftMessage::AppendEntriesRequest(AppendEntriesRequest {
                    term: Term::new(1),
                    leader_id: node(1),
                    prev_log_index: LogIndex::ZERO,
                    prev_log_term: Term::ZERO,
                    entries: vec![LogEntry::new(
                        Term::new(1),
                        LogIndex::FIRST,
                        EntryKind::LeaderBarrier,
                    )],
                    leader_commit: LogIndex::FIRST,
                }),
                2,
            )
            .unwrap();
        let ahead_payload = encode_application_command(
            &ApplicationCommand {
                base_stream_offset: StreamOffset::new(0),
                records: vec![record(b"existing-committed-suffix")],
            },
            CommandLimits::default(),
        )
        .unwrap();
        ahead
            .step(
                node(1),
                RaftMessage::AppendEntriesRequest(AppendEntriesRequest {
                    term: Term::new(1),
                    leader_id: node(1),
                    prev_log_index: LogIndex::FIRST,
                    prev_log_term: Term::new(1),
                    entries: vec![LogEntry::new(
                        Term::new(1),
                        LogIndex::new(2),
                        EntryKind::Application(ahead_payload),
                    )],
                    leader_commit: LogIndex::new(2),
                }),
                3,
            )
            .unwrap();
        assert_eq!(ahead.durable_commit_index(), LogIndex::new(2));
        assert_eq!(
            ahead
                .recovered_state()
                .unwrap()
                .snapshot
                .last_included_index,
            LogIndex::ZERO
        );
        ahead
            .step_with_rebootstrap_v2(
                node(1),
                RaftMessage::InstallSnapshotRequest(InstallSnapshotRequest {
                    term: Term::new(1),
                    leader_id: node(1),
                    snapshot: snapshot.clone(),
                }),
                package.clone(),
                4,
            )
            .expect(
                "already-committed V2 snapshot must use existing P3 history without fatal swap",
            );
        assert!(!ahead.is_fatal());
        assert_eq!(ahead.durable_commit_index(), LogIndex::new(2));
        assert_eq!(ahead.recovered_state().unwrap().snapshot, snapshot);
        assert!(
            ahead.manifest_generation().is_none(),
            "V2 must not overwrite already-committed V1 history"
        );
        assert!(ahead.pending_rebootstrap_v2.is_none());
        assert_eq!(
            ahead
                .read_committed_records(StreamOffset::new(0), 10)
                .unwrap()[0]
                .record
                .value
                .as_deref(),
            Some(&b"existing-committed-suffix"[..]),
        );
        drop(ahead);
        let reopened = ReplicatedPartition::open(
            &ahead_dir,
            topic_id,
            PartitionId::new(0),
            node(2),
            5,
            PartitionLogConfig::default(),
            configuration.clone(),
            TimingConfig::new(1, 5, 10).unwrap(),
            6,
            CommandLimits::default(),
            ReplicaLimits::default(),
        )
        .unwrap();
        assert!(!reopened.is_fatal());
        assert_eq!(reopened.durable_commit_index(), LogIndex::new(2));
        assert_eq!(reopened.recovered_state().unwrap().snapshot, snapshot);
        assert_eq!(
            reopened
                .read_committed_records(StreamOffset::new(0), 10)
                .unwrap()[0]
                .record
                .value
                .as_deref(),
            Some(&b"existing-committed-suffix"[..]),
        );
        drop(reopened);
        fs::remove_dir_all(ahead_dir).unwrap();
        let mut follower = ReplicatedPartition::open(
            &target_dir,
            topic_id,
            PartitionId::new(0),
            node(2),
            0,
            PartitionLogConfig::default(),
            configuration.clone(),
            TimingConfig::new(1, 5, 10).unwrap(),
            6,
            CommandLimits::default(),
            ReplicaLimits::default(),
        )
        .unwrap();
        let request = RaftMessage::InstallSnapshotRequest(InstallSnapshotRequest {
            term: Term::new(1),
            leader_id: node(1),
            snapshot: snapshot.clone(),
        });
        // Exercise real RF3 peer TCP framing, identity/incarnation handshake,
        // ASTRBTP2 canonical codec and exact P3 InstallSnapshot authorization
        // together; this is not a shortcut that injects a decoded package
        // directly into the follower without passing the wire boundary.
        let receive_v2 = {
            use crate::{
                GroupKey, PeerDescriptor, PeerTransportConfig, TransportLimits,
                decode_rebootstrap_package_v2, encode_rebootstrap_package_v2, start_peer_transport,
            };
            use std::net::Ipv4Addr;
            use std::time::Duration;
            let addresses = std::array::from_fn::<_, 3, _>(|_| {
                let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
                let address = listener.local_addr().unwrap();
                drop(listener);
                address
            });
            let identities = (0_u32..3)
                .map(|id| PeerDescriptor {
                    broker_id: id,
                    incarnation: u64::from(id) + 31,
                    address: addresses[id as usize],
                })
                .collect::<Vec<_>>();
            let group = GroupKey {
                topic_id,
                partition: 0,
            };
            let build_config = |id: usize| PeerTransportConfig {
                local_broker_id: id as u32,
                local_incarnation: identities[id].incarnation,
                bind_addr: addresses[id],
                peers: identities
                    .iter()
                    .filter(|peer| peer.broker_id != id as u32)
                    .map(|peer| (peer.broker_id, *peer))
                    .collect(),
                allowed_groups: [group].into_iter().collect(),
                limits: TransportLimits::default(),
                max_inbound_frames: 16,
                max_outbound_frames_per_peer: 16,
                max_outbound_bytes_per_peer: 1024 * 1024,
                reconnect_min_delay: Duration::from_millis(5),
                reconnect_max_delay: Duration::from_millis(20),
            };
            let encoded =
                encode_rebootstrap_package_v2(&package, RebootstrapLimitsV2::default()).unwrap();
            assert!(encoded.starts_with(b"ASTRBTP2"));
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let frame = rt.block_on(async {
                let (sender, _rx_sender) = start_peer_transport(build_config(0)).await.unwrap();
                let (target, mut rx_target) = start_peer_transport(build_config(1)).await.unwrap();
                let (third, _rx_third) = start_peer_transport(build_config(2)).await.unwrap();
                sender
                    .sender()
                    .try_send_rebootstrap(1, group, request.clone(), encoded)
                    .unwrap();
                let arrived = tokio::time::timeout(Duration::from_secs(3), rx_target.recv())
                    .await
                    .unwrap()
                    .unwrap();
                assert!(arrived.connection_generation > 0);
                assert_eq!(arrived.frame.source_broker_id, 0);
                assert_eq!(arrived.frame.source_incarnation, 31);
                assert_eq!(arrived.frame.topic_id, topic_id);
                assert_eq!(arrived.frame.partition, 0);
                sender.shutdown().await;
                target.shutdown().await;
                third.shutdown().await;
                arrived.frame
            });
            let decoded = decode_rebootstrap_package_v2(
                frame.rebootstrap.as_ref().unwrap(),
                RebootstrapLimitsV2::default(),
            )
            .unwrap();
            (frame.message.unwrap(), decoded)
        };
        assert_eq!(receive_v2.1, package);
        follower
            .step_with_rebootstrap_v2(node(1), receive_v2.0, receive_v2.1, 3)
            .unwrap();
        assert!(follower.pending_rebootstrap_v2.is_none());
        assert_eq!(follower.recovered_state().unwrap().snapshot, snapshot);
        assert_eq!(follower.durable_commit_index(), LogIndex::FIRST);
        assert!(follower.manifest_generation().is_some());

        // A stale P3 request is a no-op; V2 retained files cannot publish a
        // second, unauthorized authoritative generation after installation.
        let before = follower.recovered_state().unwrap();
        follower
            .step_with_rebootstrap_v2(node(1), request, package, 4)
            .unwrap();
        assert_eq!(follower.recovered_state().unwrap(), before);
        assert!(follower.pending_rebootstrap_v2.is_none());

        let payload = encode_application_command(
            &ApplicationCommand {
                base_stream_offset: StreamOffset::new(0),
                records: vec![record(b"v2-then-live-v1")],
            },
            CommandLimits::default(),
        )
        .unwrap();
        follower
            .step(
                node(1),
                RaftMessage::AppendEntriesRequest(AppendEntriesRequest {
                    term: Term::new(1),
                    leader_id: node(1),
                    prev_log_index: LogIndex::FIRST,
                    prev_log_term: Term::new(1),
                    entries: vec![LogEntry::new(
                        Term::new(1),
                        LogIndex::new(2),
                        EntryKind::Application(payload),
                    )],
                    leader_commit: LogIndex::new(2),
                }),
                5,
            )
            .unwrap();
        assert_eq!(follower.durable_commit_index(), LogIndex::new(2));
        assert_eq!(
            follower
                .read_committed_records(StreamOffset::new(0), 10)
                .unwrap()[0]
                .record
                .value
                .as_deref(),
            Some(&b"v2-then-live-v1"[..])
        );
        drop(follower);
        let restarted = ReplicatedPartition::open(
            &target_dir,
            topic_id,
            PartitionId::new(0),
            node(2),
            6,
            PartitionLogConfig::default(),
            configuration,
            TimingConfig::new(1, 5, 10).unwrap(),
            6,
            CommandLimits::default(),
            ReplicaLimits::default(),
        )
        .unwrap();
        assert_eq!(restarted.durable_commit_index(), LogIndex::new(2));
        assert_eq!(
            restarted
                .read_committed_records(StreamOffset::new(0), 10)
                .unwrap()[0]
                .record
                .value
                .as_deref(),
            Some(&b"v2-then-live-v1"[..])
        );
        drop(restarted);
        drop(source);
        fs::remove_dir_all(source_dir).unwrap();
        fs::remove_dir_all(target_dir).unwrap();
    }

    #[test]
    fn v1_snapshot_of_already_committed_prefix_preserves_local_bytes() {
        // A V1 package is allowed when the leader has contiguous history,
        // even in a P9-G-enabled cluster. If this follower already committed
        // the same prefix, P3 may update snapshot metadata, but storage must
        // never replace committed data. This reproduced a fatal RF3 follower.
        let dir = temp_dir("v1-snapshot-committed-prefix");
        let configuration = StableConfiguration::new([node(1), node(2), node(3)], []).unwrap();
        let topic_id = TopicId::new([0x8a; 16]);
        let mut replica = ReplicatedPartition::open(
            &dir,
            topic_id,
            PartitionId::new(0),
            node(2),
            0,
            PartitionLogConfig::default(),
            configuration.clone(),
            TimingConfig::new(1, 5, 10).unwrap(),
            6,
            CommandLimits::default(),
            ReplicaLimits::default(),
        )
        .unwrap();
        let payload = encode_application_command(
            &ApplicationCommand {
                base_stream_offset: StreamOffset::new(0),
                records: vec![record(b"already-committed-v1-data")],
            },
            CommandLimits::default(),
        )
        .unwrap();
        replica
            .step(
                node(1),
                RaftMessage::AppendEntriesRequest(AppendEntriesRequest {
                    term: Term::new(1),
                    leader_id: node(1),
                    prev_log_index: LogIndex::ZERO,
                    prev_log_term: Term::ZERO,
                    entries: vec![
                        LogEntry::new(Term::new(1), LogIndex::FIRST, EntryKind::LeaderBarrier),
                        LogEntry::new(
                            Term::new(1),
                            LogIndex::new(2),
                            EntryKind::Application(payload),
                        ),
                    ],
                    leader_commit: LogIndex::new(2),
                }),
                1,
            )
            .unwrap();
        assert_eq!(replica.durable_commit_index(), LogIndex::new(2));
        let snapshot = SnapshotMetadata::new(
            LogIndex::FIRST,
            Term::new(1),
            configuration.clone(),
            SnapshotId::new([0x8b; 16]),
        )
        .unwrap();
        let package = RebootstrapPackage {
            topic_id,
            partition_id: PartitionId::new(0),
            snapshot: snapshot.clone(),
            frames: vec![Frame::Control(ControlEntry {
                raft_term: RaftTerm::new(1),
                raft_index: RaftIndex::FIRST,
                control_kind: crate::P4_CONTROL_LEADER_BARRIER,
                control_version: 1,
                body: Vec::new(),
            })],
        };
        replica
            .step_with_rebootstrap(
                node(1),
                RaftMessage::InstallSnapshotRequest(InstallSnapshotRequest {
                    term: Term::new(1),
                    leader_id: node(1),
                    snapshot: snapshot.clone(),
                }),
                package,
                2,
            )
            .expect("V1 snapshot must not replace an already committed prefix");
        assert!(!replica.is_fatal());
        assert_eq!(replica.durable_commit_index(), LogIndex::new(2));
        assert_eq!(replica.recovered_state().unwrap().snapshot, snapshot);
        assert!(replica.pending_rebootstrap.is_none());
        assert_eq!(
            replica
                .read_committed_records(StreamOffset::new(0), 10)
                .unwrap()[0]
                .record
                .value
                .as_deref(),
            Some(&b"already-committed-v1-data"[..]),
        );
        drop(replica);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn stale_rebootstrap_package_is_discarded_when_p3_does_not_install_it() {
        let dir = temp_dir("stale-rebootstrap");
        let configuration = StableConfiguration::new([node(1), node(2), node(3)], []).unwrap();
        let mut replica = ReplicatedPartition::open(
            &dir,
            TopicId::new([3; 16]),
            PartitionId::new(0),
            node(2),
            0,
            PartitionLogConfig::default(),
            configuration.clone(),
            TimingConfig::new(1, 5, 10).unwrap(),
            6,
            CommandLimits::default(),
            ReplicaLimits::default(),
        )
        .unwrap();
        let snapshot = SnapshotMetadata::new(
            LogIndex::FIRST,
            Term::new(1),
            configuration,
            SnapshotId::new([0x78; 16]),
        )
        .unwrap();
        let package = RebootstrapPackage {
            topic_id: replica.topic_id(),
            partition_id: replica.partition_id(),
            snapshot: snapshot.clone(),
            frames: vec![Frame::Control(ControlEntry {
                raft_term: RaftTerm::new(1),
                raft_index: RaftIndex::FIRST,
                control_kind: crate::P4_CONTROL_LEADER_BARRIER,
                control_version: 1,
                body: Vec::new(),
            })],
        };
        replica
            .step_with_rebootstrap(
                node(1),
                RaftMessage::InstallSnapshotRequest(InstallSnapshotRequest {
                    term: Term::new(1),
                    leader_id: node(1),
                    snapshot: snapshot.clone(),
                }),
                package.clone(),
                1,
            )
            .unwrap();
        let before = replica.recovered_state().unwrap();

        replica
            .step_with_rebootstrap(
                node(1),
                RaftMessage::InstallSnapshotRequest(InstallSnapshotRequest {
                    term: Term::new(1),
                    leader_id: node(1),
                    snapshot,
                }),
                package,
                2,
            )
            .unwrap();
        assert!(replica.pending_rebootstrap.is_none());
        assert_eq!(replica.recovered_state().unwrap(), before);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn pending_proposal_count_bound_rejects_without_disturbing_raft() {
        let dir = temp_dir("pending-count-bound");
        let limits = ReplicaLimits {
            max_pending_proposals: 1,
            ..ReplicaLimits::default()
        };
        let mut replica = ReplicatedPartition::open(
            &dir,
            TopicId::new([4; 16]),
            PartitionId::new(0),
            node(1),
            0,
            PartitionLogConfig::default(),
            StableConfiguration::new([node(1)], []).unwrap(),
            TimingConfig::new(1, 5, 10).unwrap(),
            5,
            CommandLimits::default(),
            limits,
        )
        .unwrap();
        for _ in 0..5 {
            replica.tick(5, 0).unwrap();
        }
        let first = replica
            .propose_records(vec![record(b"first")], AckRequirement::Leader, 1)
            .unwrap();
        assert_eq!(
            replica.proposal_outcome(first.proposal_id),
            Some(ProposalOutcome::LocalDurable)
        );
        assert!(matches!(
            replica.propose_records(vec![record(b"second")], AckRequirement::Leader, 2),
            Err(ReplicationError::LimitExceeded {
                what: "pending proposal count",
                ..
            })
        ));
        assert_eq!(
            replica.take_completed(first.proposal_id),
            Some(ProposalOutcome::LocalDurable)
        );
        assert!(
            replica
                .propose_records(vec![record(b"third")], AckRequirement::Leader, 3)
                .is_ok()
        );
        assert_eq!(replica.role(), Role::Leader);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn pending_proposal_byte_bound_rejects_application_but_control_ticks_continue() {
        let dir = temp_dir("pending-byte-bound");
        let limits = ReplicaLimits {
            max_pending_proposal_bytes: 1,
            ..ReplicaLimits::default()
        };
        let mut replica = ReplicatedPartition::open(
            &dir,
            TopicId::new([5; 16]),
            PartitionId::new(0),
            node(1),
            0,
            PartitionLogConfig::default(),
            StableConfiguration::new([node(1)], []).unwrap(),
            TimingConfig::new(1, 5, 10).unwrap(),
            5,
            CommandLimits::default(),
            limits,
        )
        .unwrap();
        for _ in 0..5 {
            replica.tick(5, 0).unwrap();
        }
        assert_eq!(replica.role(), Role::Leader);
        assert!(matches!(
            replica.propose_records(vec![record(b"too-large")], AckRequirement::Quorum, 1),
            Err(ReplicationError::LimitExceeded {
                what: "pending proposal bytes",
                ..
            })
        ));
        replica.tick(5, 2).unwrap();
        assert_eq!(replica.role(), Role::Leader);
        assert!(!replica.is_fatal());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p7_idempotent_admission_requires_controller_commits_only_and_dedupes() {
        let (dir, mut replica) = single("p7-admission");
        let epoch_one = authority(7, 1);
        assert!(matches!(
            replica.propose_idempotent_records(
                1,
                epoch_one,
                ProducerSequence::ZERO,
                vec![record(b"follower")],
                AckRequirement::Quorum,
                ControllerAuthorityEvidence::Confirmed(epoch_one),
                0,
            ),
            Err(IdempotentProduceError::NotLeader)
        ));
        elect_single(&mut replica);
        let leader_epoch = replica.leader_epoch();
        assert!(matches!(
            replica.propose_idempotent_records(
                leader_epoch.saturating_sub(1),
                epoch_one,
                ProducerSequence::ZERO,
                vec![record(b"stale-term")],
                AckRequirement::Quorum,
                ControllerAuthorityEvidence::Confirmed(epoch_one),
                1,
            ),
            Err(IdempotentProduceError::StaleLeaderEpoch)
        ));

        assert!(matches!(
            replica.propose_idempotent_records(
                leader_epoch,
                epoch_one,
                ProducerSequence::ZERO,
                vec![record(b"a")],
                AckRequirement::Quorum,
                ControllerAuthorityEvidence::NotChecked,
                1,
            ),
            Err(IdempotentProduceError::ControllerValidationRequired)
        ));
        assert_eq!(replica.pending_idempotent_len(), 0);
        assert_eq!(replica.producer_cache_len(), 0);
        assert_eq!(replica.next_proposal_id, 1);

        assert!(matches!(
            replica.propose_idempotent_records(
                leader_epoch,
                epoch_one,
                ProducerSequence::ZERO,
                vec![record(b"a")],
                AckRequirement::Quorum,
                ControllerAuthorityEvidence::Missing,
                1,
            ),
            Err(IdempotentProduceError::UnknownProducer)
        ));
        assert!(matches!(
            replica.propose_idempotent_records(
                leader_epoch,
                epoch_one,
                ProducerSequence::ZERO,
                vec![record(b"a")],
                AckRequirement::Leader,
                ControllerAuthorityEvidence::Confirmed(epoch_one),
                1,
            ),
            Err(IdempotentProduceError::InvalidAckMode)
        ));
        assert!(matches!(
            replica.propose_idempotent_records(
                leader_epoch,
                epoch_one,
                ProducerSequence::ZERO,
                vec![record(b"a")],
                AckRequirement::None,
                ControllerAuthorityEvidence::Confirmed(epoch_one),
                1,
            ),
            Err(IdempotentProduceError::InvalidAckMode)
        ));
        let mut hidden_timestamp = record(b"a");
        hidden_timestamp.timestamp_ms = -1;
        assert!(matches!(
            replica.propose_idempotent_records(
                leader_epoch,
                epoch_one,
                ProducerSequence::ZERO,
                vec![hidden_timestamp],
                AckRequirement::Quorum,
                ControllerAuthorityEvidence::Confirmed(epoch_one),
                1,
            ),
            Err(IdempotentProduceError::InvalidTimestamp)
        ));

        let first = match replica
            .propose_idempotent_records(
                leader_epoch,
                epoch_one,
                ProducerSequence::ZERO,
                vec![record(b"a")],
                AckRequirement::Quorum,
                ControllerAuthorityEvidence::Confirmed(epoch_one),
                1,
            )
            .unwrap()
        {
            IdempotentProposalDisposition::Proposed(receipt) => receipt,
            other => panic!("unexpected first P7 disposition: {other:?}"),
        };
        assert_eq!(
            replica.proposal_outcome(first.proposal_id),
            Some(ProposalOutcome::QuorumCommitted)
        );
        assert_eq!(replica.pending_idempotent_len(), 0);
        assert_eq!(replica.producer_cache_len(), 1);
        assert_eq!(
            replica.producer_cache[&epoch_one.producer_id].next_sequence(),
            ProducerSequence::new(1)
        );
        let next_proposal = replica.next_proposal_id;
        let high_watermark = replica.committed_stream_high_watermark().unwrap();

        let duplicate = replica
            .propose_idempotent_records(
                leader_epoch,
                epoch_one,
                ProducerSequence::ZERO,
                vec![record(b"a")],
                AckRequirement::Quorum,
                ControllerAuthorityEvidence::NotChecked,
                2,
            )
            .unwrap();
        assert!(matches!(
            duplicate,
            IdempotentProposalDisposition::Duplicate(retry)
                if retry.base_stream_offset == first.base_offset
                    && retry.last_stream_offset == first.last_offset
        ));
        assert_eq!(replica.next_proposal_id, next_proposal);
        assert_eq!(
            replica.committed_stream_high_watermark().unwrap(),
            high_watermark
        );

        let second = replica
            .propose_idempotent_records(
                leader_epoch,
                epoch_one,
                ProducerSequence::new(1),
                vec![record(b"b")],
                AckRequirement::Quorum,
                ControllerAuthorityEvidence::NotChecked,
                3,
            )
            .unwrap();
        assert!(matches!(second, IdempotentProposalDisposition::Proposed(_)));
        assert_eq!(
            replica.producer_cache[&epoch_one.producer_id].next_sequence(),
            ProducerSequence::new(2)
        );

        let epoch_two = authority(7, 2);
        assert!(matches!(
            replica.propose_idempotent_records(
                leader_epoch,
                epoch_two,
                ProducerSequence::ZERO,
                vec![record(b"epoch-two")],
                AckRequirement::Quorum,
                ControllerAuthorityEvidence::NotChecked,
                4,
            ),
            Err(IdempotentProduceError::ControllerValidationRequired)
        ));
        assert!(matches!(
            replica.propose_idempotent_records(
                leader_epoch,
                epoch_two,
                ProducerSequence::ZERO,
                vec![record(b"epoch-two")],
                AckRequirement::Quorum,
                ControllerAuthorityEvidence::Confirmed(epoch_two),
                4,
            ),
            Ok(IdempotentProposalDisposition::Proposed(_))
        ));
        assert!(matches!(
            replica.propose_idempotent_records(
                leader_epoch,
                epoch_one,
                ProducerSequence::new(2),
                vec![record(b"stale")],
                AckRequirement::Quorum,
                ControllerAuthorityEvidence::NotChecked,
                5,
            ),
            Err(IdempotentProduceError::ProducerFenced)
        ));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p7_pending_duplicate_coalesces_conflict_fails_and_restart_rolls_back_tentative_fence() {
        let dir = temp_dir("p7-pending");
        let mut replica = ReplicatedPartition::open(
            &dir,
            TopicId::new([9; 16]),
            PartitionId::new(0),
            node(1),
            0,
            PartitionLogConfig::default(),
            StableConfiguration::new([node(1)], []).unwrap(),
            TimingConfig::new(1, 5, 10).unwrap(),
            5,
            CommandLimits::default(),
            ReplicaLimits::default(),
        )
        .unwrap();
        elect_single(&mut replica);
        let epoch_one = authority(11, 1);
        let leader_epoch = replica.leader_epoch();
        assert!(matches!(
            replica.propose_idempotent_records(
                leader_epoch,
                epoch_one,
                ProducerSequence::ZERO,
                vec![record(b"committed")],
                AckRequirement::Quorum,
                ControllerAuthorityEvidence::Confirmed(epoch_one),
                1,
            ),
            Ok(IdempotentProposalDisposition::Proposed(_))
        ));

        let epoch_two = authority(11, 2);
        replica.hold_next_persistence_before_apply();
        let pending = match replica
            .propose_idempotent_records(
                leader_epoch,
                epoch_two,
                ProducerSequence::ZERO,
                vec![record(b"pending")],
                AckRequirement::Quorum,
                ControllerAuthorityEvidence::Confirmed(epoch_two),
                2,
            )
            .unwrap()
        {
            IdempotentProposalDisposition::Proposed(receipt) => receipt,
            other => panic!("unexpected pending disposition: {other:?}"),
        };
        assert_eq!(replica.pending_idempotent_len(), 1);
        let pending_bytes = replica.pending_bytes;
        assert_eq!(
            replica.producer_cache[&epoch_one.producer_id].accepted_epoch(),
            epoch_one.producer_epoch
        );
        assert!(matches!(
            replica.propose_idempotent_records(
                leader_epoch,
                epoch_two,
                ProducerSequence::ZERO,
                vec![record(b"pending")],
                AckRequirement::Quorum,
                ControllerAuthorityEvidence::NotChecked,
                3,
            ),
            Ok(IdempotentProposalDisposition::Coalesced(receipt))
                if receipt.proposal_id == pending.proposal_id
        ));
        assert_eq!(replica.pending_bytes, pending_bytes);
        assert!(matches!(
            replica.propose_idempotent_records(
                leader_epoch,
                epoch_two,
                ProducerSequence::ZERO,
                vec![record(b"different")],
                AckRequirement::Quorum,
                ControllerAuthorityEvidence::NotChecked,
                3,
            ),
            Err(IdempotentProduceError::OutOfOrderSequence)
        ));
        assert!(matches!(
            replica.propose_idempotent_records(
                leader_epoch,
                epoch_two,
                ProducerSequence::new(1),
                vec![record(b"next")],
                AckRequirement::Quorum,
                ControllerAuthorityEvidence::NotChecked,
                3,
            ),
            Err(IdempotentProduceError::Busy(_))
        ));
        assert!(matches!(
            replica.propose_idempotent_records(
                leader_epoch,
                epoch_one,
                ProducerSequence::new(1),
                vec![record(b"old")],
                AckRequirement::Quorum,
                ControllerAuthorityEvidence::NotChecked,
                3,
            ),
            Err(IdempotentProduceError::ProducerFenced)
        ));
        drop(replica);

        let mut restarted = ReplicatedPartition::open(
            &dir,
            TopicId::new([9; 16]),
            PartitionId::new(0),
            node(1),
            4,
            PartitionLogConfig::default(),
            StableConfiguration::new([node(1)], []).unwrap(),
            TimingConfig::new(1, 5, 10).unwrap(),
            5,
            CommandLimits::default(),
            ReplicaLimits::default(),
        )
        .unwrap();
        assert_eq!(restarted.pending_idempotent_len(), 0);
        assert_eq!(restarted.producer_cache_len(), 0);
        elect_single(&mut restarted);
        let restarted_term = restarted.leader_epoch();
        let resumed = restarted.propose_idempotent_records(
            restarted_term,
            epoch_one,
            ProducerSequence::new(1),
            vec![record(b"old-continues")],
            AckRequirement::Quorum,
            ControllerAuthorityEvidence::NotChecked,
            5,
        );
        assert!(
            matches!(resumed, Ok(IdempotentProposalDisposition::Proposed(_))),
            "unexpected lazy-recovery admission: {resumed:?}"
        );
        assert_eq!(
            restarted.producer_cache[&epoch_one.producer_id].accepted_epoch(),
            epoch_one.producer_epoch
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p7_producer_cache_is_bounded_and_deterministically_evicts_reconstructable_state() {
        let (dir, mut replica) = single("p7-cache-bound");
        for value in 1..=MAX_PRODUCER_CACHE_ENTRIES {
            let producer_id = ProducerId::new(u64::try_from(value).unwrap()).unwrap();
            let producer_authority = ProducerAuthority::new(
                producer_id,
                astralog_common::ProducerEpoch::new(1).unwrap(),
            );
            let committed = CommittedBatchV1::new(
                producer_authority,
                ProducerSequence::ZERO,
                1,
                StreamOffset::new(u64::try_from(value - 1).unwrap()),
                RaftIndex::new(u64::try_from(value).unwrap()).unwrap(),
                [u8::try_from(value % 251).unwrap(); 32],
            )
            .unwrap();
            replica
                .insert_producer_state(
                    producer_id,
                    PartitionProducerStateV1::from_first_committed(committed).unwrap(),
                )
                .unwrap();
        }
        assert_eq!(replica.producer_cache_len(), MAX_PRODUCER_CACHE_ENTRIES);

        let protected = ProducerId::new(1).unwrap();
        replica.pending_idempotent.insert(
            protected,
            PendingIdempotent {
                batch: ProducerBatchIdentityV1::new(
                    authority(1, 1),
                    ProducerSequence::new(1),
                    1,
                    [9; 32],
                )
                .unwrap(),
                receipt: ProposalReceipt {
                    proposal_id: ProposalId::new(1),
                    raft_index: LogIndex::FIRST,
                    base_offset: StreamOffset::new(0),
                    last_offset: StreamOffset::new(0),
                    ack: AckRequirement::Quorum,
                },
            },
        );
        let added = ProducerId::new(4097).unwrap();
        let committed = CommittedBatchV1::new(
            authority(4097, 1),
            ProducerSequence::ZERO,
            1,
            StreamOffset::new(4096),
            RaftIndex::new(4097).unwrap(),
            [7; 32],
        )
        .unwrap();
        replica
            .insert_producer_state(
                added,
                PartitionProducerStateV1::from_first_committed(committed).unwrap(),
            )
            .unwrap();
        assert_eq!(replica.producer_cache_len(), MAX_PRODUCER_CACHE_ENTRIES);
        assert!(replica.producer_cache.contains_key(&protected));
        assert!(
            !replica
                .producer_cache
                .contains_key(&ProducerId::new(2).unwrap())
        );
        assert!(replica.producer_cache.contains_key(&added));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p7_snapshot_reconstructs_evicted_cache_entry_with_same_retry_decision() {
        let (dir, mut replica) = single("p7-snapshot-cache-reconstruct");
        elect_single(&mut replica);
        let producer = authority(44, 1);
        let leader_epoch = replica.leader_epoch();
        let receipt = match replica
            .propose_idempotent_records(
                leader_epoch,
                producer,
                ProducerSequence::ZERO,
                vec![record(b"snapshot-retry")],
                AckRequirement::Quorum,
                ControllerAuthorityEvidence::Confirmed(producer),
                1,
            )
            .unwrap()
        {
            IdempotentProposalDisposition::Proposed(receipt) => receipt,
            other => panic!("unexpected initial snapshot proposal disposition: {other:?}"),
        };
        replica.persistence.persist_producer_snapshot().unwrap();
        let before = replica.producer_cache[&producer.producer_id].clone();
        replica.producer_cache.remove(&producer.producer_id);
        assert!(!replica.producer_cache.contains_key(&producer.producer_id));

        let recovered = replica
            .load_producer_state(producer.producer_id)
            .unwrap()
            .unwrap();
        assert_eq!(recovered, before);
        let digest = canonical_storage_record_set_digest(&[record(b"snapshot-retry")]).unwrap();
        assert_eq!(
            recovered
                .admit(producer, ProducerSequence::ZERO, 1, digest)
                .unwrap(),
            ProducerAdmissionV1::Duplicate(CommittedRetryV1 {
                base_stream_offset: receipt.base_offset,
                last_stream_offset: receipt.last_offset,
                raft_index: RaftIndex::new(receipt.raft_index.get()).unwrap(),
            })
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p7_idempotent_pressure_does_not_block_raft_control_ticks() {
        let dir = temp_dir("p7-control-priority");
        let limits = ReplicaLimits {
            max_pending_proposals: 1,
            ..ReplicaLimits::default()
        };
        let mut replica = ReplicatedPartition::open(
            &dir,
            TopicId::new([10; 16]),
            PartitionId::new(0),
            node(1),
            0,
            PartitionLogConfig::default(),
            StableConfiguration::new([node(1)], []).unwrap(),
            TimingConfig::new(1, 5, 10).unwrap(),
            5,
            CommandLimits::default(),
            limits,
        )
        .unwrap();
        elect_single(&mut replica);
        let first = authority(21, 1);
        let term = replica.leader_epoch();
        assert!(matches!(
            replica.propose_idempotent_records(
                term,
                first,
                ProducerSequence::ZERO,
                vec![record(b"first")],
                AckRequirement::Quorum,
                ControllerAuthorityEvidence::Confirmed(first),
                1,
            ),
            Ok(IdempotentProposalDisposition::Proposed(_))
        ));
        let second = authority(22, 1);
        assert!(matches!(
            replica.propose_idempotent_records(
                term,
                second,
                ProducerSequence::ZERO,
                vec![record(b"second")],
                AckRequirement::Quorum,
                ControllerAuthorityEvidence::Confirmed(second),
                2,
            ),
            Err(IdempotentProduceError::Replication(
                ReplicationError::LimitExceeded {
                    what: "pending proposal count",
                    ..
                }
            ))
        ));
        replica.tick(5, 3).unwrap();
        assert_eq!(replica.role(), Role::Leader);
        assert!(!replica.is_fatal());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p7j_pending_duplicate_coalescing_campaign_is_bounded_and_conflict_safe() {
        let dir = temp_dir("p7j-pending-campaign");
        let mut replica = ReplicatedPartition::open(
            &dir,
            TopicId::new([0x31; 16]),
            PartitionId::new(0),
            node(1),
            0,
            PartitionLogConfig::default(),
            StableConfiguration::new([node(1)], []).unwrap(),
            TimingConfig::new(1, 5, 10).unwrap(),
            5,
            CommandLimits::default(),
            ReplicaLimits::default(),
        )
        .unwrap();
        elect_single(&mut replica);
        let producer = authority(31, 1);
        let term = replica.leader_epoch();
        replica.hold_next_persistence_before_apply();
        let first = match replica
            .propose_idempotent_records(
                term,
                producer,
                ProducerSequence::ZERO,
                vec![record(b"same")],
                AckRequirement::Quorum,
                ControllerAuthorityEvidence::Confirmed(producer),
                1,
            )
            .unwrap()
        {
            IdempotentProposalDisposition::Proposed(receipt) => receipt,
            other => panic!("unexpected first disposition: {other:?}"),
        };
        let pending_bytes = replica.pending_bytes;
        let next_proposal_id = replica.next_proposal_id;

        for attempt in 0..64_i64 {
            let duplicate = replica
                .propose_idempotent_records(
                    term,
                    producer,
                    ProducerSequence::ZERO,
                    vec![record(b"same")],
                    AckRequirement::Quorum,
                    ControllerAuthorityEvidence::NotChecked,
                    2 + attempt,
                )
                .unwrap();
            assert!(matches!(
                duplicate,
                IdempotentProposalDisposition::Coalesced(receipt)
                    if receipt.proposal_id == first.proposal_id
            ));
            assert_eq!(replica.pending_idempotent_len(), 1);
            assert_eq!(replica.pending_bytes, pending_bytes);
            assert_eq!(replica.next_proposal_id, next_proposal_id);

            assert!(matches!(
                replica.propose_idempotent_records(
                    term,
                    producer,
                    ProducerSequence::ZERO,
                    vec![record(b"different")],
                    AckRequirement::Quorum,
                    ControllerAuthorityEvidence::NotChecked,
                    100 + attempt,
                ),
                Err(IdempotentProduceError::OutOfOrderSequence)
            ));
            assert!(matches!(
                replica.propose_idempotent_records(
                    term,
                    producer,
                    ProducerSequence::new(1),
                    vec![record(b"next")],
                    AckRequirement::Quorum,
                    ControllerAuthorityEvidence::NotChecked,
                    200 + attempt,
                ),
                Err(IdempotentProduceError::Busy(_))
            ));
        }
        assert_eq!(replica.pending_idempotent_len(), 1);
        assert_eq!(replica.pending_bytes, pending_bytes);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p7j_retry_storm_remains_bounded_while_raft_control_progresses() {
        let dir = temp_dir("p7j-retry-storm");
        let limits = ReplicaLimits {
            max_pending_proposals: 1,
            max_pending_proposal_bytes: 4 * 1024,
            ..ReplicaLimits::default()
        };
        let mut replica = ReplicatedPartition::open(
            &dir,
            TopicId::new([0x32; 16]),
            PartitionId::new(0),
            node(1),
            0,
            PartitionLogConfig::default(),
            StableConfiguration::new([node(1)], []).unwrap(),
            TimingConfig::new(1, 5, 10).unwrap(),
            5,
            CommandLimits::default(),
            limits,
        )
        .unwrap();
        elect_single(&mut replica);
        let producer = authority(32, 1);
        let term = replica.leader_epoch();
        let first = replica
            .propose_idempotent_records(
                term,
                producer,
                ProducerSequence::ZERO,
                vec![record(b"storm")],
                AckRequirement::Quorum,
                ControllerAuthorityEvidence::Confirmed(producer),
                1,
            )
            .unwrap();
        assert!(matches!(first, IdempotentProposalDisposition::Proposed(_)));
        let baseline_cache = replica.producer_cache_len();

        for tick in 2..=258_i64 {
            let contender = authority(u64::try_from(tick).unwrap() + 10_000, 1);
            let pressure = replica.propose_idempotent_records(
                term,
                contender,
                ProducerSequence::ZERO,
                vec![record(b"storm-contender")],
                AckRequirement::Quorum,
                ControllerAuthorityEvidence::Confirmed(contender),
                tick,
            );
            assert!(matches!(
                pressure,
                Err(IdempotentProduceError::Replication(
                    ReplicationError::LimitExceeded {
                        what: "pending proposal count",
                        ..
                    }
                ))
            ));
            assert_eq!(replica.producer_cache_len(), baseline_cache);
            replica.tick(5, tick).unwrap();
            assert_eq!(replica.role(), Role::Leader);
            assert!(!replica.is_fatal());
        }
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p8e_group_commit_amortizes_two_tokens_into_one_physical_sync() {
        let (dir, mut replica) = grouped_single("p8e-amortize", 2, 1024 * 1024, 1000);
        let first = replica
            .propose_records(vec![record(b"first")], AckRequirement::Leader, 10)
            .unwrap();
        assert_eq!(
            replica.proposal_outcome(first.proposal_id),
            Some(ProposalOutcome::Pending)
        );
        assert_eq!(replica.pending_persistence_group_len(), 1);
        assert_eq!(
            replica.persistence_group_metrics(),
            PersistenceGroupMetrics {
                logical_tokens: 1,
                physical_syncs: 0,
            }
        );

        let second = replica
            .propose_records(vec![record(b"second")], AckRequirement::Leader, 11)
            .unwrap();
        assert_eq!(second.raft_index, first.raft_index.checked_next().unwrap());
        assert_eq!(
            replica.proposal_outcome(first.proposal_id),
            Some(ProposalOutcome::LocalDurable)
        );
        assert_eq!(
            replica.proposal_outcome(second.proposal_id),
            Some(ProposalOutcome::LocalDurable)
        );
        assert_eq!(replica.pending_persistence_group_len(), 0);
        assert_eq!(
            replica.persistence_group_metrics(),
            PersistenceGroupMetrics {
                logical_tokens: 2,
                physical_syncs: 1,
            }
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p8e_injected_deadline_flushes_existing_group_before_next_append() {
        let (dir, mut replica) = grouped_single("p8e-deadline", 8, 1024 * 1024, 10);
        let first = replica
            .propose_records(vec![record(b"first")], AckRequirement::Leader, 100)
            .unwrap();
        assert_eq!(
            replica.proposal_outcome(first.proposal_id),
            Some(ProposalOutcome::Pending)
        );
        let second = replica
            .propose_records(vec![record(b"second")], AckRequirement::Leader, 109)
            .unwrap();
        assert_eq!(
            replica.proposal_outcome(second.proposal_id),
            Some(ProposalOutcome::Pending)
        );
        assert_eq!(replica.pending_persistence_group_len(), 2);

        let third = replica
            .propose_records(vec![record(b"third")], AckRequirement::Leader, 110)
            .unwrap();
        assert_eq!(
            replica.proposal_outcome(first.proposal_id),
            Some(ProposalOutcome::LocalDurable)
        );
        assert_eq!(
            replica.proposal_outcome(second.proposal_id),
            Some(ProposalOutcome::LocalDurable)
        );
        assert_eq!(
            replica.proposal_outcome(third.proposal_id),
            Some(ProposalOutcome::Pending)
        );
        assert_eq!(replica.pending_persistence_group_len(), 1);
        assert_eq!(
            replica.persistence_group_metrics(),
            PersistenceGroupMetrics {
                logical_tokens: 3,
                physical_syncs: 1,
            }
        );
        replica.flush_persistence(111).unwrap();
        assert_eq!(
            replica.proposal_outcome(third.proposal_id),
            Some(ProposalOutcome::LocalDurable)
        );
        assert_eq!(replica.persistence_group_metrics().physical_syncs, 2);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p8e_ambiguous_group_sync_never_fabricates_success() {
        let (dir, mut replica) = grouped_single("p8e-sync-failure", 2, 1024 * 1024, 1000);
        let first = replica
            .propose_records(vec![record(b"first")], AckRequirement::Leader, 1)
            .unwrap();
        replica.inject_group_sync_failure_once("ambiguous fsync");
        let second = replica
            .propose_records(vec![record(b"second")], AckRequirement::Leader, 2)
            .unwrap();

        assert!(replica.is_fatal());
        assert_ne!(
            replica.proposal_outcome(first.proposal_id),
            Some(ProposalOutcome::LocalDurable)
        );
        assert_ne!(
            replica.proposal_outcome(second.proposal_id),
            Some(ProposalOutcome::LocalDurable)
        );
        assert_eq!(replica.persistence_group_metrics().physical_syncs, 0);
        assert_eq!(replica.pending_persistence_group_len(), 0);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p8e_crash_boundaries_distinguish_staged_from_synced_uncompleted_history() {
        let (staged_dir, mut staged) =
            grouped_single("p8e-crash-before-sync", 4, 1024 * 1024, 1000);
        let staged_receipt = staged
            .propose_records(vec![record(b"staged")], AckRequirement::Leader, 1)
            .unwrap();
        assert_eq!(
            staged.proposal_outcome(staged_receipt.proposal_id),
            Some(ProposalOutcome::Pending)
        );
        drop(staged);
        let reopened_staged = ReplicatedPartition::open(
            &staged_dir,
            TopicId::new([0x81; 16]),
            PartitionId::new(0),
            node(1),
            2,
            PartitionLogConfig::default(),
            StableConfiguration::new([node(1)], []).unwrap(),
            TimingConfig::new(1, 5, 10).unwrap(),
            5,
            CommandLimits::default(),
            ReplicaLimits::default(),
        )
        .unwrap();
        assert!(
            reopened_staged
                .read_committed_records(StreamOffset::new(0), 10)
                .unwrap()
                .is_empty()
        );
        fs::remove_dir_all(staged_dir).unwrap();

        let (synced_dir, mut synced) = grouped_single("p8e-crash-after-sync", 2, 1024 * 1024, 1000);
        let first = synced
            .propose_records(vec![record(b"one")], AckRequirement::Leader, 1)
            .unwrap();
        synced.hold_next_persistence_after_apply_before_completion();
        let second = synced
            .propose_records(vec![record(b"two")], AckRequirement::Leader, 2)
            .unwrap();
        assert_eq!(synced.persistence_group_metrics().physical_syncs, 1);
        assert_eq!(
            synced.proposal_outcome(first.proposal_id),
            Some(ProposalOutcome::Pending)
        );
        assert_eq!(
            synced.proposal_outcome(second.proposal_id),
            Some(ProposalOutcome::Pending)
        );
        drop(synced);
        let reopened_synced = ReplicatedPartition::open(
            &synced_dir,
            TopicId::new([0x81; 16]),
            PartitionId::new(0),
            node(1),
            3,
            PartitionLogConfig::default(),
            StableConfiguration::new([node(1)], []).unwrap(),
            TimingConfig::new(1, 5, 10).unwrap(),
            5,
            CommandLimits::default(),
            ReplicaLimits::default(),
        )
        .unwrap();
        assert!(
            reopened_synced
                .read_committed_records(StreamOffset::new(0), 10)
                .unwrap()
                .is_empty()
        );
        assert!(
            reopened_synced
                .recovered_state()
                .unwrap()
                .log
                .iter()
                .filter(|entry| matches!(entry.kind, EntryKind::Application(_)))
                .count()
                >= 2
        );
        fs::remove_dir_all(synced_dir).unwrap();
    }

    #[test]
    fn p8e_mixed_compression_group_preserves_logical_order_and_offsets() {
        let (dir, mut replica) = grouped_single("p8e-mixed-codecs", 3, 1024 * 1024, 1000);
        replica.set_data_compression(Compression::None);
        let first = replica
            .propose_records(vec![record(&vec![b'N'; 4096])], AckRequirement::Leader, 1)
            .unwrap();
        replica.set_data_compression(Compression::Lz4);
        let second = replica
            .propose_records(vec![record(&vec![b'L'; 4096])], AckRequirement::Leader, 2)
            .unwrap();
        replica.set_data_compression(Compression::Zstd);
        let third = replica
            .propose_records(vec![record(&vec![b'Z'; 4096])], AckRequirement::Leader, 3)
            .unwrap();

        assert_eq!(
            (
                first.base_offset.get(),
                second.base_offset.get(),
                third.base_offset.get()
            ),
            (0, 1, 2)
        );
        assert_eq!(
            replica.persistence_group_metrics(),
            PersistenceGroupMetrics {
                logical_tokens: 3,
                physical_syncs: 1,
            }
        );
        let codecs = replica
            .persistence
            .log()
            .read_frames()
            .unwrap()
            .into_iter()
            .filter_map(|frame| match frame {
                Frame::Data(batch) => Some(batch.compression),
                Frame::Control(_) => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            codecs,
            vec![Compression::None, Compression::Lz4, Compression::Zstd]
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p8e_grouped_execution_matches_sequential_reference() {
        let (sequential_dir, mut sequential) =
            grouped_single("p8e-sequential-reference", 1, 1024 * 1024, 1000);
        let (grouped_dir, mut grouped) =
            grouped_single("p8e-grouped-reference", 3, 1024 * 1024, 1000);

        for (ordinal, value) in [b"alpha".as_slice(), b"beta".as_slice(), b"gamma".as_slice()]
            .into_iter()
            .enumerate()
        {
            let now_ms = 10 + i64::try_from(ordinal).unwrap();
            let sequential_receipt = sequential
                .propose_records(vec![record(value)], AckRequirement::Leader, now_ms)
                .unwrap();
            assert_eq!(
                sequential.proposal_outcome(sequential_receipt.proposal_id),
                Some(ProposalOutcome::LocalDurable)
            );
            let grouped_receipt = grouped
                .propose_records(vec![record(value)], AckRequirement::Leader, now_ms)
                .unwrap();
            if ordinal < 2 {
                assert_eq!(
                    grouped.proposal_outcome(grouped_receipt.proposal_id),
                    Some(ProposalOutcome::Pending)
                );
            }
        }
        grouped.flush_persistence(20).unwrap();

        assert_eq!(
            grouped.recovered_state().unwrap(),
            sequential.recovered_state().unwrap()
        );
        assert_eq!(
            grouped.persistence.log().read_frames().unwrap(),
            sequential.persistence.log().read_frames().unwrap()
        );
        assert_eq!(
            grouped.next_stream_offset(),
            sequential.next_stream_offset()
        );
        assert_eq!(
            grouped.persistence_group_metrics(),
            PersistenceGroupMetrics {
                logical_tokens: 3,
                physical_syncs: 1,
            }
        );
        assert_eq!(
            sequential.persistence_group_metrics(),
            PersistenceGroupMetrics {
                logical_tokens: 3,
                physical_syncs: 3,
            }
        );

        fs::remove_dir_all(sequential_dir).unwrap();
        fs::remove_dir_all(grouped_dir).unwrap();
    }

    #[test]
    fn p8e_only_application_append_batches_are_groupable() {
        let token = PersistenceToken::new(1);
        let application = LogEntry::new(
            Term::new(1),
            LogIndex::FIRST,
            EntryKind::Application(vec![1, 2, 3]),
        );
        assert_eq!(
            groupable_append_bytes(&PersistenceBatch::new(
                token,
                vec![PersistenceOp::Append(vec![application.clone()])],
            ))
            .unwrap(),
            Some(3)
        );

        let barrier = LogEntry::new(Term::new(1), LogIndex::FIRST, EntryKind::LeaderBarrier);
        let snapshot = SnapshotMetadata::new(
            LogIndex::FIRST,
            Term::new(1),
            StableConfiguration::new([node(1)], []).unwrap(),
            SnapshotId::new([0x55; 16]),
        )
        .unwrap();
        let hard_state = astralog_raft::HardState {
            current_term: Term::new(1),
            voted_for: Some(node(1)),
            durable_commit_index: LogIndex::ZERO,
        };
        for operations in [
            vec![PersistenceOp::UpdateHardState(hard_state)],
            vec![PersistenceOp::TruncateSuffix {
                from: LogIndex::FIRST,
            }],
            vec![PersistenceOp::Append(vec![barrier])],
            vec![PersistenceOp::AdvanceCommit {
                to: LogIndex::FIRST,
            }],
            vec![PersistenceOp::InstallSnapshot(snapshot)],
            vec![PersistenceOp::DiscardPrefix {
                through: LogIndex::FIRST,
            }],
            vec![
                PersistenceOp::Append(vec![application]),
                PersistenceOp::AdvanceCommit {
                    to: LogIndex::FIRST,
                },
            ],
        ] {
            assert_eq!(
                groupable_append_bytes(&PersistenceBatch::new(token, operations)).unwrap(),
                None
            );
        }
    }

    #[test]
    fn p8e_group_commit_preserves_leader_and_quorum_ack_boundaries() {
        let (dir, mut replica) = grouped_single("p8e-ack-boundaries", 2, 1024 * 1024, 1000);
        let leader = replica
            .propose_records(vec![record(b"leader")], AckRequirement::Leader, 1)
            .unwrap();
        assert_eq!(
            replica.proposal_outcome(leader.proposal_id),
            Some(ProposalOutcome::Pending)
        );

        let quorum = replica
            .propose_records(vec![record(b"quorum")], AckRequirement::Quorum, 2)
            .unwrap();
        assert_eq!(
            replica.proposal_outcome(leader.proposal_id),
            Some(ProposalOutcome::LocalDurable)
        );
        assert_eq!(
            replica.proposal_outcome(quorum.proposal_id),
            Some(ProposalOutcome::QuorumCommitted)
        );
        assert_eq!(replica.durable_commit_index(), quorum.raft_index);
        assert_eq!(
            replica.persistence_group_metrics(),
            PersistenceGroupMetrics {
                logical_tokens: 2,
                physical_syncs: 1,
            }
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p8e_advance_commit_waits_for_group_sync() {
        let (dir, mut replica) = grouped_single("p8e-commit-behind-sync", 4, 1024 * 1024, 1000);
        let durable_before = replica.durable_commit_index();
        let receipt = replica
            .propose_records(vec![record(b"commit")], AckRequirement::Quorum, 1)
            .unwrap();
        assert_eq!(
            replica.proposal_outcome(receipt.proposal_id),
            Some(ProposalOutcome::Pending)
        );
        assert_eq!(replica.durable_commit_index(), durable_before);
        assert_eq!(replica.persistence_group_metrics().physical_syncs, 0);

        replica.flush_persistence(2).unwrap();
        assert_eq!(
            replica.proposal_outcome(receipt.proposal_id),
            Some(ProposalOutcome::QuorumCommitted)
        );
        assert_eq!(replica.durable_commit_index(), receipt.raft_index);
        assert_eq!(replica.persistence_group_metrics().physical_syncs, 1);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p8e_snapshot_request_drains_open_group_before_snapshot_barrier() {
        let (dir, mut replica) = grouped_single("p8e-snapshot-barrier", 4, 1024 * 1024, 1000);
        let receipt = replica
            .propose_records(vec![record(b"snapshot")], AckRequirement::Quorum, 1)
            .unwrap();
        assert_eq!(replica.pending_persistence_group_len(), 1);
        assert_eq!(
            replica.proposal_outcome(receipt.proposal_id),
            Some(ProposalOutcome::Pending)
        );

        replica
            .handle(Input::RequestSnapshot(receipt.raft_index), 2)
            .unwrap();
        assert_eq!(replica.pending_persistence_group_len(), 0);
        assert_eq!(
            replica.proposal_outcome(receipt.proposal_id),
            Some(ProposalOutcome::QuorumCommitted)
        );
        assert_eq!(
            replica.core.snapshot().last_included_index,
            receipt.raft_index
        );
        assert!(replica.persistence_group_metrics().physical_syncs >= 1);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p8e_sustained_append_staging_does_not_starve_control_ticks() {
        let (dir, mut replica) = grouped_single("p8e-control-progress", 8, 1024 * 1024, 1000);
        let mut now_ms = 100_i64;
        for ordinal in 0..64_u8 {
            replica
                .propose_records(vec![record(&[ordinal])], AckRequirement::Leader, now_ms)
                .unwrap();
            now_ms += 1;
            if ordinal % 4 == 3 {
                replica.tick(5, now_ms).unwrap();
                assert_eq!(replica.role(), Role::Leader);
                assert!(!replica.is_fatal());
                now_ms += 1;
            }
        }
        replica.flush_persistence(now_ms).unwrap();
        let metrics = replica.persistence_group_metrics();
        assert_eq!(metrics.logical_tokens, 64);
        assert!(metrics.physical_syncs > 0);
        assert!(metrics.physical_syncs < metrics.logical_tokens);
        assert_eq!(replica.role(), Role::Leader);
        assert!(!replica.is_fatal());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p8e_physical_sync_counter_includes_segment_roll_syncs() {
        let dir = temp_dir("p8e-roll-sync-counter");
        let limits = ReplicaLimits {
            max_pending_persistence_batches: 2,
            max_pending_persistence_bytes: 1024 * 1024,
            max_persistence_group_delay_ms: 1000,
            ..ReplicaLimits::default()
        };
        let storage_config = PartitionLogConfig {
            max_segment_bytes: 512,
            ..PartitionLogConfig::default()
        };
        let mut replica = ReplicatedPartition::open(
            &dir,
            TopicId::new([0x82; 16]),
            PartitionId::new(0),
            node(1),
            0,
            storage_config,
            StableConfiguration::new([node(1)], []).unwrap(),
            TimingConfig::new(1, 5, 10).unwrap(),
            5,
            CommandLimits::default(),
            limits,
        )
        .unwrap();
        elect_single(&mut replica);

        replica
            .propose_records(vec![record(&vec![b'A'; 2048])], AckRequirement::Leader, 1)
            .unwrap();
        assert_eq!(replica.persistence_group_metrics().physical_syncs, 0);
        replica
            .propose_records(vec![record(&vec![b'B'; 2048])], AckRequirement::Leader, 2)
            .unwrap();

        assert_eq!(
            replica.persistence_group_metrics(),
            PersistenceGroupMetrics {
                logical_tokens: 2,
                physical_syncs: 2,
            }
        );
        assert_eq!(replica.pending_persistence_group_len(), 0);
        fs::remove_dir_all(dir).unwrap();
    }

    fn p8f_test_replica(label: &str, limits: ReplicaLimits) -> (PathBuf, ReplicatedPartition) {
        let dir = temp_dir(label);
        let replica = ReplicatedPartition::open(
            &dir,
            TopicId::new([0x91; 16]),
            PartitionId::new(0),
            node(1),
            0,
            PartitionLogConfig::default(),
            StableConfiguration::new([node(1)], []).unwrap(),
            TimingConfig::new(1, 5, 10).unwrap(),
            5,
            CommandLimits::default(),
            limits,
        )
        .unwrap();
        (dir, replica)
    }

    fn p8f_append_request(term: Term, first_index: u64, count: u64) -> AppendEntriesRequest {
        let entries = (first_index..first_index + count)
            .map(|index| {
                LogEntry::new(
                    term,
                    LogIndex::new(index),
                    EntryKind::Application(vec![0xA5; 64]),
                )
            })
            .collect::<Vec<_>>();
        let prev_log_index = LogIndex::new(first_index.saturating_sub(1));
        AppendEntriesRequest {
            term,
            leader_id: node(1),
            prev_log_index,
            prev_log_term: term,
            entries,
            leader_commit: LogIndex::ZERO,
        }
    }

    #[test]
    fn p8f_per_follower_replication_windows_bound_count_bytes_and_catchup_chunks() {
        let limits = ReplicaLimits {
            max_outbound_messages: 32,
            max_outbound_bytes: 16 * 1024,
            max_replication_inflight_messages_per_follower: 2,
            max_replication_inflight_bytes_per_follower: 500,
            max_replication_catchup_bytes_per_message: 300,
            ..ReplicaLimits::default()
        };
        let (dir, mut replica) = p8f_test_replica("p8f-window-bounds", limits);
        let term = replica.core.hard_state().current_term;
        let target = node(2);

        replica
            .enqueue_replication_append(target, p8f_append_request(term, 1, 6))
            .unwrap();
        let usage = replica.replication_window_usage(target);
        assert_eq!(usage.0, 2);
        assert!(usage.1 <= limits.max_replication_inflight_bytes_per_follower);

        let queued = replica.drain_outbound().collect::<Vec<_>>();
        assert_eq!(queued.len(), 2);
        for outbound in &queued {
            let RaftMessage::AppendEntriesRequest(request) = &outbound.message else {
                panic!("P8-F data window queued a non-AppendEntries message");
            };
            assert_eq!(request.entries.len(), 1);
            assert!(
                estimate_message_bytes(&outbound.message)
                    <= limits.max_replication_catchup_bytes_per_message
            );
        }

        replica.observe_replication_response(&RaftMessage::AppendEntriesResponse(
            astralog_raft::AppendEntriesResponse {
                term,
                follower_id: target,
                success: true,
                match_index: LogIndex::new(1),
                conflict_term: None,
                conflict_index: LogIndex::new(2),
            },
        ));
        assert_eq!(replica.replication_window_usage(target).0, 1);

        replica
            .enqueue_replication_append(target, p8f_append_request(term, 2, 5))
            .unwrap();
        assert_eq!(replica.replication_window_usage(target).0, 2);
        assert!(
            replica.replication_window_usage(target).1
                <= limits.max_replication_inflight_bytes_per_follower
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p8f_window_resets_on_term_connection_incarnation_and_snapshot_boundaries() {
        let limits = ReplicaLimits {
            max_replication_inflight_messages_per_follower: 4,
            max_replication_inflight_bytes_per_follower: 4096,
            max_replication_catchup_bytes_per_message: 512,
            ..ReplicaLimits::default()
        };
        let (dir, mut replica) = p8f_test_replica("p8f-window-resets", limits);
        let term = replica.core.hard_state().current_term;
        let target = node(2);
        replica
            .enqueue_replication_append(target, p8f_append_request(term, 1, 2))
            .unwrap();
        assert!(replica.replication_window_usage(target).0 > 0);

        assert!(replica.observe_peer_transport(target, 7, 1).unwrap());
        assert!(replica.observe_peer_transport(target, 7, 2).unwrap());
        assert_eq!(replica.replication_window_usage(target), (0, 0));
        assert!(!replica.observe_peer_transport(target, 7, 1).unwrap());

        replica
            .enqueue_replication_append(target, p8f_append_request(term, 1, 2))
            .unwrap();
        assert!(replica.observe_peer_transport(target, 8, 3).unwrap());
        assert_eq!(replica.replication_window_usage(target), (0, 0));
        assert!(!replica.observe_peer_transport(target, 7, 4).unwrap());

        replica
            .enqueue_replication_append(target, p8f_append_request(term, 1, 2))
            .unwrap();
        replica.replication_window_term = Term::new(term.get().saturating_add(1));
        replica.synchronize_replication_window_term();
        assert_eq!(replica.replication_window_usage(target), (0, 0));

        replica
            .enqueue_replication_append(target, p8f_append_request(term, 1, 2))
            .unwrap();
        let snapshot = SnapshotMetadata::new(
            LogIndex::ZERO,
            Term::ZERO,
            StableConfiguration::new([node(1)], []).unwrap(),
            SnapshotId::new([0x91; 16]),
        )
        .unwrap();
        replica
            .enqueue_outbound(
                target,
                RaftMessage::InstallSnapshotRequest(InstallSnapshotRequest {
                    term,
                    leader_id: node(1),
                    snapshot,
                }),
            )
            .unwrap();
        assert_eq!(replica.replication_window_usage(target), (0, 0));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p8f_reserved_control_capacity_survives_saturated_data_outbox() {
        let limits = ReplicaLimits {
            max_outbound_messages: 8,
            max_outbound_bytes: 16 * 1024,
            ..ReplicaLimits::default()
        };
        let (dir, mut replica) = p8f_test_replica("p8f-control-reserve", limits);
        let term = replica.core.hard_state().current_term;
        let target = node(2);
        let data = RaftMessage::AppendEntriesRequest(p8f_append_request(term, 1, 1));
        for _ in 0..7 {
            assert!(
                replica
                    .enqueue_raw_outbound(target, data.clone(), true)
                    .unwrap()
            );
        }
        assert!(!replica.enqueue_raw_outbound(target, data, true).unwrap());
        let heartbeat = RaftMessage::AppendEntriesRequest(AppendEntriesRequest {
            term,
            leader_id: node(1),
            prev_log_index: LogIndex::ZERO,
            prev_log_term: Term::ZERO,
            entries: Vec::new(),
            leader_commit: LogIndex::ZERO,
        });
        assert!(
            replica
                .enqueue_raw_outbound(target, heartbeat, false)
                .unwrap()
        );
        assert_eq!(replica.outbound.len(), 8);
        fs::remove_dir_all(dir).unwrap();
    }
}
