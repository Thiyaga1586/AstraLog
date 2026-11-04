use std::{fmt, sync::Arc};

use astralog_common::StreamOffset;
use astralog_groups::{
    GROUPS_INTERNAL_TOPIC_NAME, GroupApplyOutcome, GroupPhase, GroupRecordV1, GroupStateMachine,
    InternalRecordV1, P6_INTERNAL_TOPIC_PARTITIONS, P6_INTERNAL_TOPIC_REPLICATION_FACTOR,
    encode_group_state_key, group_shard,
};
use astralog_protocol::{
    PublicError,
    api::{GroupId, MAX_GROUP_ID_LEN, MAX_GROUP_RECORD_BYTES},
};
use astralog_raft::Role;
use astralog_replication::AckRequirement;
use astralog_storage::DataRecord;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    task::JoinHandle,
};

use crate::replication::{ReplicationClient, RoutingError};

const RECOVERY_REQUEST_BYTES: usize = MAX_GROUP_ID_LEN + 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupCoordinatorRuntimeConfig {
    pub max_control_requests: usize,
    pub max_mutation_requests: usize,
    pub max_pending_bytes: usize,
    pub replay_batch_records: usize,
}

impl Default for GroupCoordinatorRuntimeConfig {
    fn default() -> Self {
        Self {
            max_control_requests: 64,
            max_mutation_requests: 256,
            max_pending_bytes: 64 * 1024 * 1024,
            replay_batch_records: 256,
        }
    }
}

impl GroupCoordinatorRuntimeConfig {
    pub fn validate(self) -> Result<Self, GroupCoordinatorError> {
        if self.max_control_requests == 0
            || self.max_mutation_requests == 0
            || self.replay_batch_records == 0
        {
            return Err(GroupCoordinatorError::InvalidConfig(
                "coordinator request/replay bounds must be positive",
            ));
        }
        let minimum_bytes = MAX_GROUP_RECORD_BYTES
            .checked_add(MAX_GROUP_ID_LEN)
            .and_then(|value| value.checked_add(256))
            .ok_or(GroupCoordinatorError::InvalidConfig(
                "coordinator byte bound overflow",
            ))?;
        if self.max_pending_bytes < minimum_bytes || self.max_pending_bytes > u32::MAX as usize {
            return Err(GroupCoordinatorError::InvalidConfig(
                "coordinator byte bound must fit one maximum P6 record and u32 semaphore permits",
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupCoordinatorError {
    InvalidConfig(&'static str),
    NotReady(&'static str),
    NotCoordinator {
        leader_broker_id: u32,
        leader_epoch: u64,
    },
    MutationAmbiguous {
        error: PublicError,
        leader_broker_id: u32,
        leader_epoch: u64,
    },
    Busy,
    Corrupt(&'static str),
    State(String),
    Replication(PublicError),
    Closed,
}

impl fmt::Display for GroupCoordinatorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => {
                write!(f, "invalid group coordinator config: {message}")
            }
            Self::NotReady(message) => write!(f, "group coordinator not ready: {message}"),
            Self::NotCoordinator {
                leader_broker_id,
                leader_epoch,
            } => write!(
                f,
                "not group coordinator: leader broker {leader_broker_id}, epoch {leader_epoch}"
            ),
            Self::MutationAmbiguous {
                error,
                leader_broker_id,
                leader_epoch,
            } => write!(
                f,
                "group mutation outcome is ambiguous after proposal ({error:?}); observed leader broker {leader_broker_id}, epoch {leader_epoch}"
            ),
            Self::Busy => f.write_str("group coordinator bounded work capacity is saturated"),
            Self::Corrupt(message) => write!(f, "corrupt committed group history: {message}"),
            Self::State(message) => write!(f, "group state error: {message}"),
            Self::Replication(error) => write!(f, "group replication error: {error:?}"),
            Self::Closed => f.write_str("group coordinator runtime is closed"),
        }
    }
}

impl std::error::Error for GroupCoordinatorError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupCommitOutcome {
    pub record: GroupRecordV1,
    pub coordinator_epoch: u64,
    pub appended: bool,
}

#[derive(Clone)]
pub(crate) struct GroupCoordinatorClient {
    control_tx: mpsc::Sender<CoordinatorCommand>,
    mutation_tx: mpsc::Sender<CoordinatorCommand>,
    bytes: Arc<Semaphore>,
}

pub(crate) struct GroupCoordinatorRuntime {
    client: GroupCoordinatorClient,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

enum CoordinatorCommand {
    Recover {
        group_id: GroupId,
        expected_epoch: u64,
        _permit: OwnedSemaphorePermit,
        response: oneshot::Sender<Result<Option<GroupRecordV1>, GroupCoordinatorError>>,
    },
    RecoverHistory {
        group_id: GroupId,
        expected_epoch: u64,
        _permit: OwnedSemaphorePermit,
        response: oneshot::Sender<Result<Vec<GroupRecordV1>, GroupCoordinatorError>>,
    },
    Commit {
        expected_epoch: u64,
        candidate: GroupRecordV1,
        encoded: Vec<u8>,
        _permit: OwnedSemaphorePermit,
        response: oneshot::Sender<Result<GroupCommitOutcome, GroupCoordinatorError>>,
    },
}

impl GroupCoordinatorRuntime {
    pub(crate) fn start(
        replication: ReplicationClient,
        config: GroupCoordinatorRuntimeConfig,
    ) -> Result<Self, GroupCoordinatorError> {
        let config = config.validate()?;
        let (control_tx, mut control_rx) = mpsc::channel(config.max_control_requests);
        let (mutation_tx, mut mutation_rx) = mpsc::channel(config.max_mutation_requests);
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let bytes = Arc::new(Semaphore::new(config.max_pending_bytes));
        let worker = CoordinatorWorker {
            replication,
            replay_batch_records: config.replay_batch_records,
        };
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = &mut shutdown_rx => break,
                    command = control_rx.recv() => {
                        let Some(command) = command else { break };
                        worker.handle(command).await;
                    }
                    command = mutation_rx.recv() => {
                        let Some(command) = command else { break };
                        worker.handle(command).await;
                    }
                }
            }
        });
        let client = GroupCoordinatorClient {
            control_tx,
            mutation_tx,
            bytes,
        };
        Ok(Self {
            client,
            shutdown: Some(shutdown_tx),
            task,
        })
    }

    pub(crate) fn client(&self) -> GroupCoordinatorClient {
        self.client.clone()
    }

    pub(crate) async fn shutdown(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let _ = self.task.await;
    }
}

impl GroupCoordinatorClient {
    pub(crate) async fn recover(
        &self,
        group_id: GroupId,
        expected_epoch: u64,
    ) -> Result<Option<GroupRecordV1>, GroupCoordinatorError> {
        let permit = self.try_bytes(RECOVERY_REQUEST_BYTES)?;
        let (response, receiver) = oneshot::channel();
        self.control_tx
            .try_send(CoordinatorCommand::Recover {
                group_id,
                expected_epoch,
                _permit: permit,
                response,
            })
            .map_err(|_| GroupCoordinatorError::Busy)?;
        receiver.await.map_err(|_| GroupCoordinatorError::Closed)?
    }

    pub(crate) async fn recover_history(
        &self,
        group_id: GroupId,
        expected_epoch: u64,
    ) -> Result<Vec<GroupRecordV1>, GroupCoordinatorError> {
        let permit = self.try_bytes(RECOVERY_REQUEST_BYTES)?;
        let (response, receiver) = oneshot::channel();
        self.control_tx
            .try_send(CoordinatorCommand::RecoverHistory {
                group_id,
                expected_epoch,
                _permit: permit,
                response,
            })
            .map_err(|_| GroupCoordinatorError::Busy)?;
        receiver.await.map_err(|_| GroupCoordinatorError::Closed)?
    }

    pub(crate) async fn commit(
        &self,
        expected_epoch: u64,
        candidate: GroupRecordV1,
    ) -> Result<GroupCommitOutcome, GroupCoordinatorError> {
        let encoded = InternalRecordV1::GroupState(candidate.clone())
            .encode()
            .map_err(|error| GroupCoordinatorError::State(error.to_string()))?;
        let key_len = encode_group_state_key(&candidate.group_id).len();
        let cost = encoded
            .len()
            .checked_add(key_len)
            .and_then(|value| value.checked_add(256))
            .ok_or(GroupCoordinatorError::Busy)?;
        let permit = self.try_bytes(cost)?;
        let (response, receiver) = oneshot::channel();
        self.mutation_tx
            .try_send(CoordinatorCommand::Commit {
                expected_epoch,
                candidate,
                encoded,
                _permit: permit,
                response,
            })
            .map_err(|_| GroupCoordinatorError::Busy)?;
        receiver.await.map_err(|_| GroupCoordinatorError::Closed)?
    }

    fn try_bytes(&self, bytes: usize) -> Result<OwnedSemaphorePermit, GroupCoordinatorError> {
        let permits = u32::try_from(bytes).map_err(|_| GroupCoordinatorError::Busy)?;
        self.bytes
            .clone()
            .try_acquire_many_owned(permits)
            .map_err(|_| GroupCoordinatorError::Busy)
    }
}

struct CoordinatorWorker {
    replication: ReplicationClient,
    replay_batch_records: usize,
}

impl CoordinatorWorker {
    async fn handle(&self, command: CoordinatorCommand) {
        match command {
            CoordinatorCommand::Recover {
                group_id,
                expected_epoch,
                response,
                ..
            } => {
                let _ = response.send(self.recover(&group_id, expected_epoch).await);
            }
            CoordinatorCommand::RecoverHistory {
                group_id,
                expected_epoch,
                response,
                ..
            } => {
                let _ = response.send(self.recover_history(&group_id, expected_epoch).await);
            }
            CoordinatorCommand::Commit {
                expected_epoch,
                candidate,
                encoded,
                response,
                ..
            } => {
                let _ = response.send(self.commit(expected_epoch, candidate, encoded).await);
            }
        }
    }

    async fn commit(
        &self,
        expected_epoch: u64,
        candidate: GroupRecordV1,
        encoded: Vec<u8>,
    ) -> Result<GroupCommitOutcome, GroupCoordinatorError> {
        let (topic_id, shard) = self
            .require_local_leader(&candidate.group_id, expected_epoch)
            .await?;
        let mut state = self
            .recover_machine(&candidate.group_id, topic_id, shard, expected_epoch)
            .await?;
        match state.apply_committed(candidate.clone()) {
            Ok(GroupApplyOutcome::Idempotent) => {
                return Ok(GroupCommitOutcome {
                    record: candidate,
                    coordinator_epoch: expected_epoch,
                    appended: false,
                });
            }
            Ok(GroupApplyOutcome::Applied) => {}
            Err(error) => return Err(GroupCoordinatorError::State(error.to_string())),
        }

        let key = encode_group_state_key(&candidate.group_id);
        let record = DataRecord {
            timestamp_ms: 0,
            key: Some(key),
            value: Some(encoded),
            headers: Vec::new(),
        };
        self.replication
            .produce(
                topic_id,
                shard,
                Some(expected_epoch),
                vec![record],
                AckRequirement::Quorum,
            )
            .await
            .map_err(map_mutation_routing_error)?;

        let committed = self
            .recover_machine(&candidate.group_id, topic_id, shard, expected_epoch)
            .await
            .map_err(map_post_commit_error)?;
        if committed.current() != Some(&candidate) {
            return Err(GroupCoordinatorError::Corrupt(
                "quorum-committed mutation did not reconstruct as current group state",
            ));
        }

        Ok(GroupCommitOutcome {
            record: candidate,
            coordinator_epoch: expected_epoch,
            appended: true,
        })
    }

    async fn recover(
        &self,
        group_id: &GroupId,
        expected_epoch: u64,
    ) -> Result<Option<GroupRecordV1>, GroupCoordinatorError> {
        let (topic_id, shard) = self.require_local_leader(group_id, expected_epoch).await?;
        let state = self
            .recover_machine(group_id, topic_id, shard, expected_epoch)
            .await?;
        Ok(state.current().cloned())
    }

    async fn recover_history(
        &self,
        group_id: &GroupId,
        expected_epoch: u64,
    ) -> Result<Vec<GroupRecordV1>, GroupCoordinatorError> {
        let (topic_id, shard) = self.require_local_leader(group_id, expected_epoch).await?;
        let key = encode_group_state_key(group_id);
        let mut state = GroupStateMachine::new(group_id.clone());
        let mut history = Vec::new();
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
                if stored.record.key.as_deref() != Some(key.as_slice()) {
                    continue;
                }
                let value =
                    stored
                        .record
                        .value
                        .as_deref()
                        .ok_or(GroupCoordinatorError::Corrupt(
                            "group-state record is a tombstone",
                        ))?;
                let decoded = InternalRecordV1::decode(value).map_err(|_| {
                    GroupCoordinatorError::Corrupt("group-state value is not canonical")
                })?;
                let InternalRecordV1::GroupState(record) = decoded else {
                    return Err(GroupCoordinatorError::Corrupt(
                        "group-state key contains non-group record kind",
                    ));
                };
                state.apply_committed(record.clone()).map_err(|_| {
                    GroupCoordinatorError::Corrupt(
                        "committed group history violates canonical state-machine sequence",
                    )
                })?;
                if record.phase == GroupPhase::PreparingEpochFence {
                    history.clear();
                }
                history.push(record);
            }
            let Some(next) = next else { break };
            start = next;
        }
        Ok(history)
    }

    async fn recover_machine(
        &self,
        group_id: &GroupId,
        topic_id: astralog_common::TopicId,
        shard: u32,
        expected_epoch: u64,
    ) -> Result<GroupStateMachine, GroupCoordinatorError> {
        let key = encode_group_state_key(group_id);
        let mut state = GroupStateMachine::new(group_id.clone());
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
                apply_replayed_group_record(&mut state, &key, &stored.record)?;
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
    ) -> Result<(astralog_common::TopicId, u32), GroupCoordinatorError> {
        let shard = group_shard(group_id)
            .map_err(|_| GroupCoordinatorError::NotReady("group shard mapping failed"))?;
        let topology = self.replication.topology_snapshot();
        let topic = topology.topic_by_name(GROUPS_INTERNAL_TOPIC_NAME).ok_or(
            GroupCoordinatorError::NotReady(
                "__astralog_groups is absent from local replicated topology",
            ),
        )?;
        if topic.partition_count() != P6_INTERNAL_TOPIC_PARTITIONS
            || topic.replication_factor() != Some(P6_INTERNAL_TOPIC_REPLICATION_FACTOR)
        {
            return Err(GroupCoordinatorError::NotReady(
                "__astralog_groups topology differs from frozen P6 shape",
            ));
        }
        let status = self.replication.status(topic.topic_id, shard).await.ok_or(
            GroupCoordinatorError::NotReady(
                "local group-coordinator partition actor is unavailable",
            ),
        )?;
        if status.role != Role::Leader || status.leader_epoch != expected_epoch {
            return Err(GroupCoordinatorError::NotCoordinator {
                leader_broker_id: status.leader_broker_id.unwrap_or(u32::MAX),
                leader_epoch: status.leader_epoch,
            });
        }
        if !status.current_term_committed {
            return Err(GroupCoordinatorError::NotReady(
                "current-term group leader barrier is not durably committed",
            ));
        }
        Ok((topic.topic_id, shard))
    }
}

fn apply_replayed_group_record(
    state: &mut GroupStateMachine,
    expected_key: &[u8],
    record: &DataRecord,
) -> Result<(), GroupCoordinatorError> {
    if record.key.as_deref() != Some(expected_key) {
        return Ok(());
    }
    let value = record
        .value
        .as_deref()
        .ok_or(GroupCoordinatorError::Corrupt(
            "group-state record is a tombstone",
        ))?;
    let decoded = InternalRecordV1::decode(value)
        .map_err(|_| GroupCoordinatorError::Corrupt("group-state value is not canonical"))?;
    let InternalRecordV1::GroupState(record) = decoded else {
        return Err(GroupCoordinatorError::Corrupt(
            "group-state key contains non-group record kind",
        ));
    };
    state.apply_committed(record).map_err(|_| {
        GroupCoordinatorError::Corrupt(
            "committed group history violates canonical state-machine sequence",
        )
    })?;
    Ok(())
}

fn map_read_routing_error(error: RoutingError) -> GroupCoordinatorError {
    match error.error {
        PublicError::NotLeader | PublicError::StaleEpoch => GroupCoordinatorError::NotCoordinator {
            leader_broker_id: error.leader_broker_id,
            leader_epoch: error.leader_epoch,
        },
        PublicError::BrokerBusy => GroupCoordinatorError::Busy,
        other => GroupCoordinatorError::Replication(other),
    }
}

fn map_mutation_routing_error(error: RoutingError) -> GroupCoordinatorError {
    GroupCoordinatorError::MutationAmbiguous {
        error: error.error,
        leader_broker_id: error.leader_broker_id,
        leader_epoch: error.leader_epoch,
    }
}

fn map_post_commit_error(error: GroupCoordinatorError) -> GroupCoordinatorError {
    match error {
        GroupCoordinatorError::NotCoordinator {
            leader_broker_id,
            leader_epoch,
        } => GroupCoordinatorError::MutationAmbiguous {
            error: PublicError::NotLeader,
            leader_broker_id,
            leader_epoch,
        },
        GroupCoordinatorError::Busy => GroupCoordinatorError::MutationAmbiguous {
            error: PublicError::BrokerBusy,
            leader_broker_id: u32::MAX,
            leader_epoch: u64::MAX,
        },
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use astralog_common::TopicId;
    use astralog_groups::{GroupCommand, GroupMutationPlan};
    use astralog_protocol::api::MemberId;

    use super::*;

    fn test_group_record(group_id: GroupId) -> GroupRecordV1 {
        let member_id = MemberId::new([1; 16]).unwrap();
        let mut machine = GroupStateMachine::new(group_id);
        machine
            .apply(GroupCommand::AdmitMember {
                expected_group_epoch: 0,
                allocated_member_id: member_id,
                instance_id: None,
                session_timeout_ms: 30_000,
                subscriptions: vec![TopicId::new([7; 16])],
                plan: GroupMutationPlan {
                    target_assignments: BTreeMap::new(),
                },
            })
            .unwrap()
    }

    #[test]
    fn coordinator_runtime_bounds_are_finite_and_support_one_maximum_record() {
        let defaults = GroupCoordinatorRuntimeConfig::default().validate().unwrap();
        assert!(defaults.max_control_requests > 0);
        assert!(defaults.max_mutation_requests > 0);
        assert!(defaults.replay_batch_records > 0);
        assert!(defaults.max_pending_bytes > MAX_GROUP_RECORD_BYTES);

        let mut invalid = defaults;
        invalid.max_mutation_requests = 0;
        assert!(invalid.validate().is_err());
        let mut too_small = defaults;
        too_small.max_pending_bytes = MAX_GROUP_RECORD_BYTES;
        assert!(too_small.validate().is_err());
    }

    #[test]
    fn committed_history_replay_rejects_tombstones_malformed_values_conflicts_and_gaps() {
        let group_id = GroupId::new("history-replay").unwrap();
        let key = encode_group_state_key(&group_id);
        let first = test_group_record(group_id.clone());
        let encoded = InternalRecordV1::GroupState(first.clone())
            .encode()
            .unwrap();
        let mut state = GroupStateMachine::new(group_id.clone());
        let valid = DataRecord {
            timestamp_ms: 0,
            key: Some(key.clone()),
            value: Some(encoded),
            headers: Vec::new(),
        };
        apply_replayed_group_record(&mut state, &key, &valid).unwrap();
        assert_eq!(state.current(), Some(&first));

        let tombstone = DataRecord {
            timestamp_ms: 0,
            key: Some(key.clone()),
            value: None,
            headers: Vec::new(),
        };
        assert!(matches!(
            apply_replayed_group_record(
                &mut GroupStateMachine::new(group_id.clone()),
                &key,
                &tombstone
            ),
            Err(GroupCoordinatorError::Corrupt(_))
        ));

        let malformed = DataRecord {
            timestamp_ms: 0,
            key: Some(key.clone()),
            value: Some(vec![0xff, 0x00, 0x01]),
            headers: Vec::new(),
        };
        assert!(matches!(
            apply_replayed_group_record(
                &mut GroupStateMachine::new(group_id.clone()),
                &key,
                &malformed
            ),
            Err(GroupCoordinatorError::Corrupt(_))
        ));

        let mut conflicting = first.clone();
        conflicting
            .members
            .values_mut()
            .next()
            .unwrap()
            .session_timeout_ms = 45_000;
        let conflict = DataRecord {
            timestamp_ms: 0,
            key: Some(key.clone()),
            value: Some(InternalRecordV1::GroupState(conflicting).encode().unwrap()),
            headers: Vec::new(),
        };
        assert!(matches!(
            apply_replayed_group_record(&mut state, &key, &conflict),
            Err(GroupCoordinatorError::Corrupt(_))
        ));

        let mut gap = first.clone();
        gap.state_version = astralog_protocol::api::GroupStateVersion::new(3).unwrap();
        let gap_record = DataRecord {
            timestamp_ms: 0,
            key: Some(key.clone()),
            value: Some(InternalRecordV1::GroupState(gap).encode().unwrap()),
            headers: Vec::new(),
        };
        assert!(matches!(
            apply_replayed_group_record(&mut state, &key, &gap_record),
            Err(GroupCoordinatorError::Corrupt(_))
        ));

        let unrelated = DataRecord {
            timestamp_ms: 0,
            key: Some(vec![9, 9, 9]),
            value: None,
            headers: Vec::new(),
        };
        apply_replayed_group_record(&mut state, &key, &unrelated).unwrap();
        assert_eq!(state.current(), Some(&first));
    }

    #[tokio::test]
    async fn coordinator_admission_is_fail_fast_bounded_by_count_and_bytes() {
        let group_id = GroupId::new("bounded-admission").unwrap();
        let record = test_group_record(group_id.clone());
        let (control_tx, _control_rx) = mpsc::channel(1);
        let (mutation_tx, _mutation_rx) = mpsc::channel(1);
        let bytes = Arc::new(Semaphore::new(RECOVERY_REQUEST_BYTES * 4));
        let client = GroupCoordinatorClient {
            control_tx,
            mutation_tx,
            bytes: bytes.clone(),
        };

        let first_permit = bytes.clone().try_acquire_owned().unwrap();
        let (response, _receiver) = oneshot::channel();
        client
            .control_tx
            .try_send(CoordinatorCommand::Recover {
                group_id: group_id.clone(),
                expected_epoch: 1,
                _permit: first_permit,
                response,
            })
            .unwrap();
        assert_eq!(
            client.recover(group_id.clone(), 1).await.unwrap_err(),
            GroupCoordinatorError::Busy
        );

        let encoded = InternalRecordV1::GroupState(record.clone())
            .encode()
            .unwrap();
        let mutation_permit = bytes.clone().try_acquire_owned().unwrap();
        let (response, _receiver) = oneshot::channel();
        client
            .mutation_tx
            .try_send(CoordinatorCommand::Commit {
                expected_epoch: 1,
                candidate: record.clone(),
                encoded,
                _permit: mutation_permit,
                response,
            })
            .unwrap();
        assert_eq!(
            client.commit(1, record).await.unwrap_err(),
            GroupCoordinatorError::Busy
        );

        let (control_tx, _control_rx) = mpsc::channel(1);
        let (mutation_tx, _mutation_rx) = mpsc::channel(1);
        let byte_starved = GroupCoordinatorClient {
            control_tx,
            mutation_tx,
            bytes: Arc::new(Semaphore::new(RECOVERY_REQUEST_BYTES - 1)),
        };
        assert_eq!(
            byte_starved.recover(group_id, 1).await.unwrap_err(),
            GroupCoordinatorError::Busy
        );
    }
}
