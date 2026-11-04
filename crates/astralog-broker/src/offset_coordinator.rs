use std::{
    fmt,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use astralog_common::StreamOffset;
use astralog_groups::{
    GroupRecordV1, InternalRecordV1, OFFSETS_INTERNAL_TOPIC_NAME, OffsetApplyOutcome,
    OffsetGroupFenceV1, OffsetOwnerFenceV1, OffsetStateError, OffsetStateMachine, OffsetValueV1,
    P6_INTERNAL_TOPIC_PARTITIONS, P6_INTERNAL_TOPIC_REPLICATION_FACTOR,
    encode_offset_group_fence_key, encode_offset_owner_fence_key, encode_offset_value_key,
    group_shard,
};
use astralog_protocol::{
    PublicError,
    api::{
        CommitOffsetV0, GroupId, MAX_GROUP_ID_LEN, MAX_GROUP_RECORD_BYTES, MAX_OFFSET_METADATA_LEN,
        MAX_OFFSETS_PER_REQUEST, MemberAuthorityV0, TopicPartitionV0,
    },
};
use astralog_raft::Role;
use astralog_replication::AckRequirement;
use astralog_storage::DataRecord;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    task::JoinHandle,
};

use crate::{
    coordinator_queue::{self, Queued},
    replication::{ReplicationClient, RoutingError},
};

const RECOVERY_REQUEST_BYTES: usize = MAX_GROUP_ID_LEN + 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffsetCoordinatorRuntimeConfig {
    pub max_control_requests: usize,
    pub max_mutation_requests: usize,
    pub max_pending_bytes: usize,
    pub replay_batch_records: usize,
}

impl Default for OffsetCoordinatorRuntimeConfig {
    fn default() -> Self {
        Self {
            max_control_requests: 64,
            max_mutation_requests: 256,
            max_pending_bytes: 64 * 1024 * 1024,
            replay_batch_records: 256,
        }
    }
}

impl OffsetCoordinatorRuntimeConfig {
    pub fn validate(self) -> Result<Self, OffsetCoordinatorError> {
        if self.max_control_requests == 0
            || self.max_mutation_requests == 0
            || self.replay_batch_records == 0
        {
            return Err(OffsetCoordinatorError::InvalidConfig(
                "offset coordinator request/replay bounds must be positive",
            ));
        }
        let maximum_offset_batch_bytes = MAX_OFFSETS_PER_REQUEST
            .checked_mul(MAX_OFFSET_METADATA_LEN.saturating_add(128))
            .ok_or(OffsetCoordinatorError::InvalidConfig(
                "offset coordinator maximum request bound overflow",
            ))?;
        let minimum_bytes = MAX_GROUP_RECORD_BYTES
            .checked_add(maximum_offset_batch_bytes)
            .and_then(|value| value.checked_add(MAX_GROUP_ID_LEN))
            .and_then(|value| value.checked_add(512))
            .ok_or(OffsetCoordinatorError::InvalidConfig(
                "offset coordinator byte bound overflow",
            ))?;
        if self.max_pending_bytes < minimum_bytes || self.max_pending_bytes > u32::MAX as usize {
            return Err(OffsetCoordinatorError::InvalidConfig(
                "offset coordinator byte bound must fit one maximum P6 record and u32 semaphore permits",
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OffsetCoordinatorError {
    InvalidConfig(&'static str),
    NotReady(&'static str),
    NotLeader {
        leader_broker_id: u32,
        leader_epoch: u64,
    },
    MutationAmbiguous {
        error: PublicError,
        leader_broker_id: u32,
        leader_epoch: u64,
    },
    Validation(PublicError),
    Busy,
    Corrupt(&'static str),
    State(String),
    Replication(PublicError),
    Closed,
}

impl fmt::Display for OffsetCoordinatorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => {
                write!(f, "invalid offset coordinator config: {message}")
            }
            Self::NotReady(message) => write!(f, "offset coordinator not ready: {message}"),
            Self::NotLeader {
                leader_broker_id,
                leader_epoch,
            } => write!(
                f,
                "not offsets leader: leader broker {leader_broker_id}, epoch {leader_epoch}"
            ),
            Self::MutationAmbiguous {
                error,
                leader_broker_id,
                leader_epoch,
            } => write!(
                f,
                "offset mutation outcome is ambiguous after proposal ({error:?}); observed leader broker {leader_broker_id}, epoch {leader_epoch}"
            ),
            Self::Validation(error) => write!(f, "offset validation failed: {error:?}"),
            Self::Busy => f.write_str("offset coordinator bounded work capacity is saturated"),
            Self::Corrupt(message) => write!(f, "corrupt committed offsets history: {message}"),
            Self::State(message) => write!(f, "offset state error: {message}"),
            Self::Replication(error) => write!(f, "offset replication error: {error:?}"),
            Self::Closed => f.write_str("offset coordinator runtime is closed"),
        }
    }
}

impl std::error::Error for OffsetCoordinatorError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFenceCommitOutcome {
    pub appended: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitOutcome {
    pub offsets_commit_index: u64,
    pub values: Vec<OffsetValueV1>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchOutcome {
    pub values: Vec<(TopicPartitionV0, Option<OffsetValueV1>)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffsetShardObservation {
    pub shard: u32,
    pub offsets_topic_id: astralog_common::TopicId,
    pub leader_broker_id: u32,
    pub leader_epoch: u64,
}

#[derive(Clone)]
pub(crate) struct OffsetCoordinatorClient {
    control_tx: mpsc::Sender<OffsetCommand>,
    mutation_tx: mpsc::Sender<OffsetCommand>,
    bytes: Arc<Semaphore>,
}

pub(crate) struct OffsetCoordinatorRuntime {
    client: OffsetCoordinatorClient,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

enum OffsetCommand {
    Observe {
        group_id: GroupId,
        _permit: OwnedSemaphorePermit,
        response: oneshot::Sender<Result<OffsetShardObservation, OffsetCoordinatorError>>,
    },
    Recover {
        group_id: GroupId,
        expected_epoch: u64,
        _permit: OwnedSemaphorePermit,
        response: oneshot::Sender<Result<OffsetStateMachine, OffsetCoordinatorError>>,
    },
    CommitGroupFence {
        expected_epoch: u64,
        fence: OffsetGroupFenceV1,
        encoded: Vec<u8>,
        _permit: OwnedSemaphorePermit,
        response: oneshot::Sender<Result<OffsetFenceCommitOutcome, OffsetCoordinatorError>>,
    },
    CommitOwnerFence {
        expected_epoch: u64,
        fence: OffsetOwnerFenceV1,
        encoded: Vec<u8>,
        _permit: OwnedSemaphorePermit,
        response: oneshot::Sender<Result<OffsetFenceCommitOutcome, OffsetCoordinatorError>>,
    },
    CommitOffsets {
        expected_epoch: u64,
        group: GroupRecordV1,
        authority: MemberAuthorityV0,
        offsets: Vec<CommitOffsetV0>,
        _permit: OwnedSemaphorePermit,
        response: oneshot::Sender<Result<OffsetCommitOutcome, OffsetCoordinatorError>>,
    },
    FetchOffsets {
        group_id: GroupId,
        expected_epoch: u64,
        partitions: Vec<TopicPartitionV0>,
        _permit: OwnedSemaphorePermit,
        response: oneshot::Sender<Result<OffsetFetchOutcome, OffsetCoordinatorError>>,
    },
}

impl OffsetCoordinatorRuntime {
    pub(crate) fn start(
        replication: ReplicationClient,
        config: OffsetCoordinatorRuntimeConfig,
    ) -> Result<Self, OffsetCoordinatorError> {
        let config = config.validate()?;
        let (control_tx, mut control_rx) = mpsc::channel(config.max_control_requests);
        let (mutation_tx, mut mutation_rx) = mpsc::channel(config.max_mutation_requests);
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let bytes = Arc::new(Semaphore::new(config.max_pending_bytes));
        let worker = OffsetWorker {
            replication,
            replay_batch_records: config.replay_batch_records,
        };
        let task = tokio::spawn(async move {
            let mut consecutive_controls = 0_u8;
            while let Some(command) = coordinator_queue::next(
                &mut shutdown_rx,
                &mut control_rx,
                &mut mutation_rx,
                &mut consecutive_controls,
            )
            .await
            {
                match command {
                    Queued::Control(command) | Queued::Mutation(command) => {
                        worker.handle(command).await
                    }
                }
            }
        });
        Ok(Self {
            client: OffsetCoordinatorClient {
                control_tx,
                mutation_tx,
                bytes,
            },
            shutdown: Some(shutdown_tx),
            task,
        })
    }

    pub(crate) fn client(&self) -> OffsetCoordinatorClient {
        self.client.clone()
    }

    pub(crate) async fn shutdown(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let _ = self.task.await;
    }
}

impl OffsetCoordinatorClient {
    pub(crate) async fn observe(
        &self,
        group_id: GroupId,
    ) -> Result<OffsetShardObservation, OffsetCoordinatorError> {
        let permit = self.try_bytes(RECOVERY_REQUEST_BYTES)?;
        let (response, receiver) = oneshot::channel();
        self.control_tx
            .try_send(OffsetCommand::Observe {
                group_id,
                _permit: permit,
                response,
            })
            .map_err(|_| OffsetCoordinatorError::Busy)?;
        receiver.await.map_err(|_| OffsetCoordinatorError::Closed)?
    }

    pub(crate) async fn recover(
        &self,
        group_id: GroupId,
        expected_epoch: u64,
    ) -> Result<OffsetStateMachine, OffsetCoordinatorError> {
        let permit = self.try_bytes(RECOVERY_REQUEST_BYTES)?;
        let (response, receiver) = oneshot::channel();
        self.control_tx
            .try_send(OffsetCommand::Recover {
                group_id,
                expected_epoch,
                _permit: permit,
                response,
            })
            .map_err(|_| OffsetCoordinatorError::Busy)?;
        receiver.await.map_err(|_| OffsetCoordinatorError::Closed)?
    }

    pub(crate) async fn commit_group_fence(
        &self,
        expected_epoch: u64,
        fence: OffsetGroupFenceV1,
    ) -> Result<OffsetFenceCommitOutcome, OffsetCoordinatorError> {
        let encoded = InternalRecordV1::OffsetGroupFence(fence.clone())
            .encode()
            .map_err(|error| OffsetCoordinatorError::State(error.to_string()))?;
        let key = encode_offset_group_fence_key(&fence.group_id)
            .map_err(|error| OffsetCoordinatorError::State(error.to_string()))?;
        let permit = self.try_bytes(encoded.len().saturating_add(key.len()).saturating_add(256))?;
        let (response, receiver) = oneshot::channel();
        self.mutation_tx
            .try_send(OffsetCommand::CommitGroupFence {
                expected_epoch,
                fence,
                encoded,
                _permit: permit,
                response,
            })
            .map_err(|_| OffsetCoordinatorError::Busy)?;
        receiver.await.map_err(|_| OffsetCoordinatorError::Closed)?
    }

    pub(crate) async fn commit_owner_fence(
        &self,
        expected_epoch: u64,
        fence: OffsetOwnerFenceV1,
    ) -> Result<OffsetFenceCommitOutcome, OffsetCoordinatorError> {
        let encoded = InternalRecordV1::OffsetOwnerFence(fence.clone())
            .encode()
            .map_err(|error| OffsetCoordinatorError::State(error.to_string()))?;
        let key = encode_offset_owner_fence_key(&fence.group_id, fence.topic_partition)
            .map_err(|error| OffsetCoordinatorError::State(error.to_string()))?;
        let permit = self.try_bytes(encoded.len().saturating_add(key.len()).saturating_add(256))?;
        let (response, receiver) = oneshot::channel();
        self.mutation_tx
            .try_send(OffsetCommand::CommitOwnerFence {
                expected_epoch,
                fence,
                encoded,
                _permit: permit,
                response,
            })
            .map_err(|_| OffsetCoordinatorError::Busy)?;
        receiver.await.map_err(|_| OffsetCoordinatorError::Closed)?
    }

    pub(crate) async fn commit_offsets(
        &self,
        expected_epoch: u64,
        group: GroupRecordV1,
        authority: MemberAuthorityV0,
        offsets: Vec<CommitOffsetV0>,
    ) -> Result<OffsetCommitOutcome, OffsetCoordinatorError> {
        validate_offset_request_shape(&offsets)?;
        let group_bytes = InternalRecordV1::GroupState(group.clone())
            .encode()
            .map_err(|error| OffsetCoordinatorError::State(error.to_string()))?;
        let variable = offsets.iter().try_fold(0_usize, |total, offset| {
            total
                .checked_add(offset.metadata.len())
                .and_then(|value| value.checked_add(128))
        });
        let cost = group_bytes
            .len()
            .checked_add(variable.ok_or(OffsetCoordinatorError::Busy)?)
            .and_then(|value| value.checked_add(512))
            .ok_or(OffsetCoordinatorError::Busy)?;
        let permit = self.try_bytes(cost)?;
        let (response, receiver) = oneshot::channel();
        self.mutation_tx
            .try_send(OffsetCommand::CommitOffsets {
                expected_epoch,
                group,
                authority,
                offsets,
                _permit: permit,
                response,
            })
            .map_err(|_| OffsetCoordinatorError::Busy)?;
        receiver.await.map_err(|_| OffsetCoordinatorError::Closed)?
    }

    pub(crate) async fn fetch_offsets(
        &self,
        group_id: GroupId,
        expected_epoch: u64,
        partitions: Vec<TopicPartitionV0>,
    ) -> Result<OffsetFetchOutcome, OffsetCoordinatorError> {
        validate_fetch_partitions(&partitions)?;
        let cost = RECOVERY_REQUEST_BYTES
            .checked_add(partitions.len().saturating_mul(32))
            .ok_or(OffsetCoordinatorError::Busy)?;
        let permit = self.try_bytes(cost)?;
        let (response, receiver) = oneshot::channel();
        self.control_tx
            .try_send(OffsetCommand::FetchOffsets {
                group_id,
                expected_epoch,
                partitions,
                _permit: permit,
                response,
            })
            .map_err(|_| OffsetCoordinatorError::Busy)?;
        receiver.await.map_err(|_| OffsetCoordinatorError::Closed)?
    }

    fn try_bytes(&self, bytes: usize) -> Result<OwnedSemaphorePermit, OffsetCoordinatorError> {
        let permits = u32::try_from(bytes).map_err(|_| OffsetCoordinatorError::Busy)?;
        self.bytes
            .clone()
            .try_acquire_many_owned(permits)
            .map_err(|_| OffsetCoordinatorError::Busy)
    }
}

struct OffsetWorker {
    replication: ReplicationClient,
    replay_batch_records: usize,
}

impl OffsetWorker {
    async fn handle(&self, command: OffsetCommand) {
        match command {
            OffsetCommand::Observe {
                group_id, response, ..
            } => {
                let _ = response.send(self.observe(&group_id).await);
            }
            OffsetCommand::Recover {
                group_id,
                expected_epoch,
                response,
                ..
            } => {
                let _ = response.send(self.recover(&group_id, expected_epoch).await);
            }
            OffsetCommand::CommitGroupFence {
                expected_epoch,
                fence,
                encoded,
                response,
                ..
            } => {
                let _ = response.send(
                    self.commit_group_fence(expected_epoch, fence, encoded)
                        .await,
                );
            }
            OffsetCommand::CommitOwnerFence {
                expected_epoch,
                fence,
                encoded,
                response,
                ..
            } => {
                let _ = response.send(
                    self.commit_owner_fence(expected_epoch, fence, encoded)
                        .await,
                );
            }
            OffsetCommand::CommitOffsets {
                expected_epoch,
                group,
                authority,
                offsets,
                response,
                ..
            } => {
                let _ = response.send(
                    self.commit_offsets(expected_epoch, group, authority, offsets)
                        .await,
                );
            }
            OffsetCommand::FetchOffsets {
                group_id,
                expected_epoch,
                partitions,
                response,
                ..
            } => {
                let _ = response.send(
                    self.fetch_offsets(&group_id, expected_epoch, partitions)
                        .await,
                );
            }
        }
    }

    async fn recover(
        &self,
        group_id: &GroupId,
        expected_epoch: u64,
    ) -> Result<OffsetStateMachine, OffsetCoordinatorError> {
        let (topic_id, shard) = self.require_local_leader(group_id, expected_epoch).await?;
        self.recover_machine(group_id, topic_id, shard, expected_epoch)
            .await
    }

    async fn observe(
        &self,
        group_id: &GroupId,
    ) -> Result<OffsetShardObservation, OffsetCoordinatorError> {
        let shard = group_shard(group_id)
            .map_err(|_| OffsetCoordinatorError::NotReady("offset shard mapping failed"))?;
        let topology = self.replication.topology_snapshot();
        let topic = topology.topic_by_name(OFFSETS_INTERNAL_TOPIC_NAME).ok_or(
            OffsetCoordinatorError::NotReady(
                "__astralog_offsets is absent from local replicated topology",
            ),
        )?;
        if topic.partition_count() != P6_INTERNAL_TOPIC_PARTITIONS
            || topic.replication_factor() != Some(P6_INTERNAL_TOPIC_REPLICATION_FACTOR)
        {
            return Err(OffsetCoordinatorError::NotReady(
                "__astralog_offsets topology differs from frozen P6 shape",
            ));
        }
        let status = self.replication.status(topic.topic_id, shard).await.ok_or(
            OffsetCoordinatorError::NotReady("local offsets partition actor is unavailable"),
        )?;
        Ok(OffsetShardObservation {
            shard,
            offsets_topic_id: topic.topic_id,
            leader_broker_id: status.leader_broker_id.unwrap_or(u32::MAX),
            leader_epoch: status.leader_epoch,
        })
    }

    async fn commit_group_fence(
        &self,
        expected_epoch: u64,
        fence: OffsetGroupFenceV1,
        encoded: Vec<u8>,
    ) -> Result<OffsetFenceCommitOutcome, OffsetCoordinatorError> {
        let (topic_id, shard) = self
            .require_local_leader(&fence.group_id, expected_epoch)
            .await?;
        let mut state = self
            .recover_machine(&fence.group_id, topic_id, shard, expected_epoch)
            .await?;
        match state.apply_committed_group_fence(fence.clone()) {
            Ok(OffsetApplyOutcome::Idempotent) => {
                return Ok(OffsetFenceCommitOutcome { appended: false });
            }
            Ok(OffsetApplyOutcome::Applied) => {}
            Err(error) => return Err(map_state_error(error)),
        }
        let key = encode_offset_group_fence_key(&fence.group_id)
            .map_err(|error| OffsetCoordinatorError::State(error.to_string()))?;
        self.replication
            .produce(
                topic_id,
                shard,
                Some(expected_epoch),
                vec![data_record(key, encoded)],
                AckRequirement::Quorum,
            )
            .await
            .map_err(map_mutation_routing_error)?;
        let committed = self
            .recover_machine(&fence.group_id, topic_id, shard, expected_epoch)
            .await
            .map_err(map_post_commit_error)?;
        if committed.group_fence() != Some(&fence) {
            return Err(OffsetCoordinatorError::Corrupt(
                "quorum-committed group fence did not reconstruct as current fence",
            ));
        }
        Ok(OffsetFenceCommitOutcome { appended: true })
    }

    async fn commit_owner_fence(
        &self,
        expected_epoch: u64,
        fence: OffsetOwnerFenceV1,
        encoded: Vec<u8>,
    ) -> Result<OffsetFenceCommitOutcome, OffsetCoordinatorError> {
        let (topic_id, shard) = self
            .require_local_leader(&fence.group_id, expected_epoch)
            .await?;
        let mut state = self
            .recover_machine(&fence.group_id, topic_id, shard, expected_epoch)
            .await?;
        match state.apply_committed_owner_fence(fence.clone()) {
            Ok(OffsetApplyOutcome::Idempotent) => {
                return Ok(OffsetFenceCommitOutcome { appended: false });
            }
            Ok(OffsetApplyOutcome::Applied) => {}
            Err(error) => return Err(map_state_error(error)),
        }
        let key = encode_offset_owner_fence_key(&fence.group_id, fence.topic_partition)
            .map_err(|error| OffsetCoordinatorError::State(error.to_string()))?;
        self.replication
            .produce(
                topic_id,
                shard,
                Some(expected_epoch),
                vec![data_record(key, encoded)],
                AckRequirement::Quorum,
            )
            .await
            .map_err(map_mutation_routing_error)?;
        let committed = self
            .recover_machine(&fence.group_id, topic_id, shard, expected_epoch)
            .await
            .map_err(map_post_commit_error)?;
        if committed.owner_fence(fence.topic_partition) != Some(&fence) {
            return Err(OffsetCoordinatorError::Corrupt(
                "quorum-committed owner fence did not reconstruct as current fence",
            ));
        }
        Ok(OffsetFenceCommitOutcome { appended: true })
    }

    async fn commit_offsets(
        &self,
        expected_epoch: u64,
        group: GroupRecordV1,
        authority: MemberAuthorityV0,
        offsets: Vec<CommitOffsetV0>,
    ) -> Result<OffsetCommitOutcome, OffsetCoordinatorError> {
        let (topic_id, shard) = self
            .require_local_leader(&group.group_id, expected_epoch)
            .await?;
        let state = self
            .recover_machine(&group.group_id, topic_id, shard, expected_epoch)
            .await?;
        let commit_timestamp_ms = server_commit_timestamp_ms();
        let values = state
            .prepare_commit(&group, &authority, &offsets, commit_timestamp_ms)
            .map_err(map_state_error)?;
        let mut records = Vec::with_capacity(values.len());
        for value in &values {
            let key = encode_offset_value_key(&value.group_id, value.topic_partition)
                .map_err(|error| OffsetCoordinatorError::State(error.to_string()))?;
            let encoded = InternalRecordV1::OffsetValue(value.clone())
                .encode()
                .map_err(|error| OffsetCoordinatorError::State(error.to_string()))?;
            records.push(data_record(key, encoded));
        }
        let result = self
            .replication
            .produce(
                topic_id,
                shard,
                Some(expected_epoch),
                records,
                AckRequirement::Quorum,
            )
            .await
            .map_err(map_mutation_routing_error)?;
        let committed = self
            .recover_machine(&group.group_id, topic_id, shard, expected_epoch)
            .await
            .map_err(map_post_commit_error)?;
        for value in &values {
            if committed.value(value.topic_partition) != Some(value) {
                return Err(OffsetCoordinatorError::Corrupt(
                    "quorum-committed offset batch did not reconstruct atomically",
                ));
            }
        }
        Ok(OffsetCommitOutcome {
            offsets_commit_index: result.raft_index,
            values,
        })
    }

    async fn fetch_offsets(
        &self,
        group_id: &GroupId,
        expected_epoch: u64,
        partitions: Vec<TopicPartitionV0>,
    ) -> Result<OffsetFetchOutcome, OffsetCoordinatorError> {
        validate_fetch_partitions(&partitions)?;
        let (topic_id, shard) = self.require_local_leader(group_id, expected_epoch).await?;
        let state = self
            .recover_machine(group_id, topic_id, shard, expected_epoch)
            .await?;
        Ok(OffsetFetchOutcome {
            values: partitions
                .into_iter()
                .map(|partition| (partition, state.value(partition).cloned()))
                .collect(),
        })
    }

    async fn recover_machine(
        &self,
        group_id: &GroupId,
        topic_id: astralog_common::TopicId,
        shard: u32,
        expected_epoch: u64,
    ) -> Result<OffsetStateMachine, OffsetCoordinatorError> {
        let mut state = OffsetStateMachine::new(group_id.clone());
        let mut start = StreamOffset::new(0);
        loop {
            let page = self
                .replication
                .fetch(
                    topic_id,
                    shard,
                    Some(expected_epoch),
                    start,
                    self.replay_batch_records,
                    usize::MAX,
                )
                .await
                .map_err(map_read_routing_error)?;
            if page.records.is_empty() {
                break;
            }
            let mut next = None;
            for stored in page.records {
                next = stored.offset.checked_add(1);
                apply_replayed_offset_record(&mut state, &stored.record)?;
            }
            let Some(next) = next else { break };
            start = next;
        }
        Ok(state)
    }

    async fn require_local_leader(
        &self,
        group_id: &GroupId,
        expected_epoch: u64,
    ) -> Result<(astralog_common::TopicId, u32), OffsetCoordinatorError> {
        let shard = group_shard(group_id)
            .map_err(|_| OffsetCoordinatorError::NotReady("offset shard mapping failed"))?;
        let topology = self.replication.topology_snapshot();
        let topic = topology.topic_by_name(OFFSETS_INTERNAL_TOPIC_NAME).ok_or(
            OffsetCoordinatorError::NotReady(
                "__astralog_offsets is absent from local replicated topology",
            ),
        )?;
        if topic.partition_count() != P6_INTERNAL_TOPIC_PARTITIONS
            || topic.replication_factor() != Some(P6_INTERNAL_TOPIC_REPLICATION_FACTOR)
        {
            return Err(OffsetCoordinatorError::NotReady(
                "__astralog_offsets topology differs from frozen P6 shape",
            ));
        }
        let status = self.replication.status(topic.topic_id, shard).await.ok_or(
            OffsetCoordinatorError::NotReady("local offsets partition actor is unavailable"),
        )?;
        if status.role != Role::Leader || status.leader_epoch != expected_epoch {
            return Err(OffsetCoordinatorError::NotLeader {
                leader_broker_id: status.leader_broker_id.unwrap_or(u32::MAX),
                leader_epoch: status.leader_epoch,
            });
        }
        if !status.current_term_committed {
            return Err(OffsetCoordinatorError::NotReady(
                "current-term offsets leader barrier is not durably committed",
            ));
        }
        Ok((topic.topic_id, shard))
    }
}

fn data_record(key: Vec<u8>, value: Vec<u8>) -> DataRecord {
    DataRecord {
        timestamp_ms: 0,
        key: Some(key),
        value: Some(value),
        headers: Vec::new(),
    }
}

fn server_commit_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(i64::MAX)
}

fn validate_offset_request_shape(offsets: &[CommitOffsetV0]) -> Result<(), OffsetCoordinatorError> {
    if offsets.is_empty()
        || offsets.len() > MAX_OFFSETS_PER_REQUEST
        || offsets.iter().any(|offset| {
            offset.metadata.len() > MAX_OFFSET_METADATA_LEN
                || offset.metadata.as_bytes().contains(&0)
        })
    {
        return Err(OffsetCoordinatorError::Validation(
            PublicError::InvalidOffsetCommit,
        ));
    }
    let mut previous = None;
    for offset in offsets {
        let current = TopicPartitionV0 {
            topic_id: offset.topic_id,
            partition: offset.partition,
        };
        if previous.is_some_and(|value| value >= current) {
            return Err(OffsetCoordinatorError::Validation(
                PublicError::InvalidOffsetCommit,
            ));
        }
        previous = Some(current);
    }
    Ok(())
}

fn validate_fetch_partitions(
    partitions: &[TopicPartitionV0],
) -> Result<(), OffsetCoordinatorError> {
    if partitions.is_empty()
        || partitions.len() > MAX_OFFSETS_PER_REQUEST
        || partitions.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(OffsetCoordinatorError::Validation(
            PublicError::InvalidRequest,
        ));
    }
    Ok(())
}

fn apply_replayed_offset_record(
    state: &mut OffsetStateMachine,
    record: &DataRecord,
) -> Result<(), OffsetCoordinatorError> {
    if !offset_key_belongs_to_group(record.key.as_deref(), state.group_id()) {
        return Ok(());
    }
    let value = record
        .value
        .as_deref()
        .ok_or(OffsetCoordinatorError::Corrupt(
            "offset record is a tombstone before P9 compaction",
        ))?;
    let decoded = InternalRecordV1::decode(value)
        .map_err(|_| OffsetCoordinatorError::Corrupt("offset value is not canonical"))?;
    match decoded {
        InternalRecordV1::GroupState(_) => Err(OffsetCoordinatorError::Corrupt(
            "offsets topic contains group-state record kind",
        )),
        InternalRecordV1::OffsetGroupFence(fence) => {
            if fence.group_id != *state.group_id() {
                return Err(OffsetCoordinatorError::Corrupt(
                    "group-fence key/value group identity mismatch",
                ));
            }
            let expected = encode_offset_group_fence_key(&fence.group_id)
                .map_err(|_| OffsetCoordinatorError::Corrupt("invalid group-fence key"))?;
            if record.key.as_deref() != Some(expected.as_slice()) {
                return Err(OffsetCoordinatorError::Corrupt(
                    "group-fence logical key does not match value",
                ));
            }
            state
                .apply_committed_group_fence(fence)
                .map_err(map_corrupt_state)?;
            Ok(())
        }
        InternalRecordV1::OffsetOwnerFence(fence) => {
            if fence.group_id != *state.group_id() {
                return Err(OffsetCoordinatorError::Corrupt(
                    "owner-fence key/value group identity mismatch",
                ));
            }
            let expected = encode_offset_owner_fence_key(&fence.group_id, fence.topic_partition)
                .map_err(|_| OffsetCoordinatorError::Corrupt("invalid owner-fence key"))?;
            if record.key.as_deref() != Some(expected.as_slice()) {
                return Err(OffsetCoordinatorError::Corrupt(
                    "owner-fence logical key does not match value",
                ));
            }
            state
                .apply_committed_owner_fence(fence)
                .map_err(map_corrupt_state)?;
            Ok(())
        }
        InternalRecordV1::OffsetValue(value) => {
            if value.group_id != *state.group_id() {
                return Err(OffsetCoordinatorError::Corrupt(
                    "offset-value key/value group identity mismatch",
                ));
            }
            let expected = encode_offset_value_key(&value.group_id, value.topic_partition)
                .map_err(|_| OffsetCoordinatorError::Corrupt("invalid offset-value key"))?;
            if record.key.as_deref() != Some(expected.as_slice()) {
                return Err(OffsetCoordinatorError::Corrupt(
                    "offset logical key does not match value",
                ));
            }
            state
                .apply_committed_value(value)
                .map_err(map_corrupt_state)?;
            Ok(())
        }
    }
}

fn offset_key_belongs_to_group(key: Option<&[u8]>, group_id: &GroupId) -> bool {
    let Some(key) = key else {
        return false;
    };
    let Ok(group_len) = u16::try_from(group_id.as_bytes().len()) else {
        return false;
    };
    let mut prefix = Vec::with_capacity(3 + group_id.as_bytes().len());
    let Some(kind) = key.first().copied() else {
        return false;
    };
    if !matches!(kind, 1..=3) {
        return false;
    }
    prefix.push(kind);
    prefix.extend_from_slice(&group_len.to_le_bytes());
    prefix.extend_from_slice(group_id.as_bytes());
    if !key.starts_with(&prefix) {
        return false;
    }
    match kind {
        1 => key.len() == prefix.len(),
        2 | 3 => key.len() == prefix.len() + 20,
        _ => false,
    }
}

fn map_state_error(error: OffsetStateError) -> OffsetCoordinatorError {
    if let Some(public) = error.public_error() {
        OffsetCoordinatorError::Validation(public)
    } else {
        OffsetCoordinatorError::State(error.to_string())
    }
}

fn map_corrupt_state(_error: OffsetStateError) -> OffsetCoordinatorError {
    OffsetCoordinatorError::Corrupt("committed offset history violates canonical fence/state rules")
}

fn map_read_routing_error(error: RoutingError) -> OffsetCoordinatorError {
    match error.error {
        PublicError::NotLeader | PublicError::StaleEpoch => OffsetCoordinatorError::NotLeader {
            leader_broker_id: error.leader_broker_id,
            leader_epoch: error.leader_epoch,
        },
        PublicError::BrokerBusy => OffsetCoordinatorError::Busy,
        other => OffsetCoordinatorError::Replication(other),
    }
}

fn map_mutation_routing_error(error: RoutingError) -> OffsetCoordinatorError {
    OffsetCoordinatorError::MutationAmbiguous {
        error: error.error,
        leader_broker_id: error.leader_broker_id,
        leader_epoch: error.leader_epoch,
    }
}

fn map_post_commit_error(error: OffsetCoordinatorError) -> OffsetCoordinatorError {
    match error {
        OffsetCoordinatorError::NotLeader {
            leader_broker_id,
            leader_epoch,
        } => OffsetCoordinatorError::MutationAmbiguous {
            error: PublicError::NotLeader,
            leader_broker_id,
            leader_epoch,
        },
        OffsetCoordinatorError::Busy => OffsetCoordinatorError::MutationAmbiguous {
            error: PublicError::BrokerBusy,
            leader_broker_id: u32::MAX,
            leader_epoch: u64::MAX,
        },
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use astralog_protocol::api::{GroupEpoch, GroupStateVersion, MemberEpoch, MemberId};

    use super::*;

    #[test]
    fn offset_runtime_bounds_are_finite() {
        let defaults = OffsetCoordinatorRuntimeConfig::default()
            .validate()
            .unwrap();
        assert!(defaults.max_control_requests > 0);
        assert!(defaults.max_mutation_requests > 0);
        assert!(defaults.replay_batch_records > 0);
        assert!(defaults.max_pending_bytes > MAX_GROUP_RECORD_BYTES);
        let mut invalid = defaults;
        invalid.max_control_requests = 0;
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn replay_ignores_other_group_garbage_but_rejects_matching_tombstone() {
        let group_id = GroupId::new("target").unwrap();
        let other_group = GroupId::new("other").unwrap();
        let mut state = OffsetStateMachine::new(group_id.clone());

        let unrelated = DataRecord {
            timestamp_ms: 0,
            key: Some(encode_offset_group_fence_key(&other_group).unwrap()),
            value: None,
            headers: Vec::new(),
        };
        apply_replayed_offset_record(&mut state, &unrelated).unwrap();

        let matching_tombstone = DataRecord {
            timestamp_ms: 0,
            key: Some(encode_offset_group_fence_key(&group_id).unwrap()),
            value: None,
            headers: Vec::new(),
        };
        assert!(matches!(
            apply_replayed_offset_record(&mut state, &matching_tombstone),
            Err(OffsetCoordinatorError::Corrupt(_))
        ));

        let wrong_group_fence = OffsetGroupFenceV1 {
            group_id: other_group.clone(),
            group_epoch: GroupEpoch::new(1).unwrap(),
            source_group_state_version: GroupStateVersion::new(1).unwrap(),
            owners: std::collections::BTreeMap::new(),
        };
        let mismatched_identity = DataRecord {
            timestamp_ms: 0,
            key: Some(encode_offset_group_fence_key(&group_id).unwrap()),
            value: Some(
                InternalRecordV1::OffsetGroupFence(wrong_group_fence)
                    .encode()
                    .unwrap(),
            ),
            headers: Vec::new(),
        };
        assert!(matches!(
            apply_replayed_offset_record(&mut state, &mismatched_identity),
            Err(OffsetCoordinatorError::Corrupt(_))
        ));

        let owner = MemberId::new([1; 16]).unwrap();
        let partition = TopicPartitionV0 {
            topic_id: astralog_common::TopicId::new([2; 16]),
            partition: 0,
        };
        let fence = OffsetGroupFenceV1 {
            group_id: group_id.clone(),
            group_epoch: GroupEpoch::new(1).unwrap(),
            source_group_state_version: GroupStateVersion::new(1).unwrap(),
            owners: std::collections::BTreeMap::from([(
                partition,
                (owner, MemberEpoch::new(1).unwrap()),
            )]),
        };
        let mismatched_key = DataRecord {
            timestamp_ms: 0,
            key: Some(encode_offset_group_fence_key(&group_id).unwrap()),
            value: Some(InternalRecordV1::OffsetGroupFence(fence).encode().unwrap()),
            headers: Vec::new(),
        };
        apply_replayed_offset_record(&mut state, &mismatched_key).unwrap();
        assert!(state.group_fence().is_some());
    }
}
