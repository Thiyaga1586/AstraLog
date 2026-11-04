use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Instant,
};

use astralog_common::StreamOffset;
use astralog_controller::{BrokerAuthority, BrokerLifecycle, ControllerState, TopicLifecycle};
use astralog_groups::{
    CooperativeApplyOutcome, CooperativeError, CooperativeStateMachine, GROUPS_INTERNAL_TOPIC_NAME,
    GroupCommand, GroupMutationPlan, GroupPhase, GroupRecordV1, GroupStateError, GroupStateMachine,
    InternalRecordV1, OFFSETS_INTERNAL_TOPIC_NAME, OffsetGroupFenceV1, OffsetOwnerFenceV1,
    P6_INTERNAL_TOPIC_REPLICATION_FACTOR, P6_INTERNAL_TOPICS, SessionError, SessionTracker,
    encode_group_state_key, group_shard, heartbeat_interval_ms, prepare_static_replacement,
    uniform_sticky_assign,
};
use astralog_protocol::{
    ClientHello, FrameDecoder, FrameKind, HelloStatus, MAX_WIRE_FRAME_LEN_V1, PublicError, Reader,
    ServerHello, UNKNOWN_BROKER_ID, UNKNOWN_EPOCH, WIRE_VERSION, WireFrame, Writer,
    api::{
        BodyCodec, CommitOffsetsRequestV0, CommitOffsetsResponseV0, CoordinatorHintV0,
        FetchOffsetResultV0, FetchOffsetValueV0, FetchOffsetsRequestV0, FetchOffsetsResponseV0,
        GroupEpoch, GroupId, HeartbeatRequestV0, HeartbeatResponseV0, JoinSubscribeRequestV0,
        JoinSubscribeResponseV0, LeaveRequestV0, LeaveResponseV0, MAX_GROUP_ID_LEN,
        MemberAuthorityV0, MemberEpoch, MemberId, RevokeAckRequestV0, TopicPartitionV0,
    },
    encode_frame,
};
use astralog_raft::Role;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
    time,
};

use crate::{
    controller::{DynamicBrokerControl, GroupCoordinatorObservation},
    group_coordinator::{GroupCoordinatorClient, GroupCoordinatorError},
    offset_coordinator::{OffsetCoordinatorClient, OffsetCoordinatorError},
    replication::{PartitionStatus, ReplicationClient},
};

pub(crate) const PRIVATE_P6_OFFSET_API: u16 = 65_000;
const PRIVATE_MAX_BODY: usize = 16 * 1024 * 1024;
const PRIVATE_OP_GROUP_FENCE: u8 = 1;
const PRIVATE_OP_OWNER_FENCE: u8 = 2;
const PRIVATE_OP_COMMIT_OFFSETS: u8 = 3;
const PRIVATE_OP_FETCH_OFFSETS: u8 = 4;
const PRIVATE_OP_RECOVER_FENCES: u8 = 5;
const MAX_TRACKED_SESSION_GROUPS: usize = 1_024;
const MAX_LEADERSHIP_DISCOVERY_GROUPS: usize = MAX_TRACKED_SESSION_GROUPS * 4;
const LEADERSHIP_DISCOVERY_INTERVAL_MS: u64 = 1_000;
const LEADERSHIP_REPLAY_BATCH_RECORDS: usize = 256;
const MAX_LEADERSHIP_DISCOVERY_TASKS: usize = 8;
const LEADERSHIP_DISCOVERY_IO_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
const PRIVATE_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
const PRIVATE_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Debug, Clone)]
pub(crate) struct P6Failure {
    pub error: PublicError,
    pub hint: CoordinatorHintV0,
}

impl P6Failure {
    fn new(error: PublicError, hint: CoordinatorHintV0) -> Self {
        Self { error, hint }
    }

    fn unknown(error: PublicError) -> Self {
        Self::new(error, CoordinatorHintV0::UNKNOWN)
    }
}

#[derive(Clone)]
pub(crate) struct P6Service {
    local_broker_id: u32,
    dynamic: Arc<DynamicBrokerControl>,
    replication: ReplicationClient,
    groups: GroupCoordinatorClient,
    offsets: OffsetCoordinatorClient,
    sessions: Arc<Mutex<BTreeMap<GroupId, SessionEntry>>>,
    initialized_group_shards: Arc<Mutex<BTreeMap<u32, u64>>>,
    next_leadership_scan_ms: Arc<Mutex<u64>>,
    started_at: Instant,
}

struct SessionEntry {
    coordinator_epoch: u64,
    tracker: SessionTracker,
}

struct CoordinatorContext {
    observation: GroupCoordinatorObservation,
    group: Option<GroupRecordV1>,
}

#[derive(Debug)]
struct PrivateOffsetReply {
    leader_broker_id: u32,
    payload: Vec<u8>,
}

#[derive(Debug)]
struct CurrentFences {
    group: Option<OffsetGroupFenceV1>,
    owners: Vec<OffsetOwnerFenceV1>,
}

impl P6Service {
    pub(crate) fn new(
        local_broker_id: u32,
        dynamic: Arc<DynamicBrokerControl>,
        replication: ReplicationClient,
        groups: GroupCoordinatorClient,
        offsets: OffsetCoordinatorClient,
    ) -> Self {
        Self {
            local_broker_id,
            dynamic,
            replication,
            groups,
            offsets,
            sessions: Arc::new(Mutex::new(BTreeMap::new())),
            initialized_group_shards: Arc::new(Mutex::new(BTreeMap::new())),
            next_leadership_scan_ms: Arc::new(Mutex::new(0)),
            started_at: Instant::now(),
        }
    }

    pub(crate) fn ready(&self) -> bool {
        let Ok(state) = self.dynamic.state_snapshot() else {
            return false;
        };
        if state.active_feature_level() < 2 || validate_internal_topics(&state).is_err() {
            return false;
        }
        let topology = self.replication.topology_snapshot();
        P6_INTERNAL_TOPICS.iter().all(|descriptor| {
            topology
                .topic_by_name(descriptor.name)
                .is_some_and(|topic| {
                    topic.partition_count() == descriptor.partition_count
                        && topic.replication_factor() == Some(descriptor.replication_factor)
                })
        })
    }

    pub(crate) async fn join(
        &self,
        request: JoinSubscribeRequestV0,
    ) -> Result<JoinSubscribeResponseV0, P6Failure> {
        let mut context = self.require_local_coordinator(&request.group_id).await?;
        if let Some(group) = context.group.clone()
            && group.phase == GroupPhase::PreparingEpochFence
        {
            self.finish_prepared(&context.observation, group).await?;
            context = self.require_local_coordinator(&request.group_id).await?;
        }
        let current = context.group.clone();
        if current
            .as_ref()
            .is_some_and(|group| group.phase == GroupPhase::Revoking)
        {
            return Err(self.failure_with_group(
                PublicError::RebalanceInProgress,
                &context.observation,
                current.as_ref(),
                member_id_from_bytes(request.member_id).ok(),
            ));
        }

        if request.member_id != [0; 16] {
            let member_id = MemberId::new(request.member_id)
                .map_err(|_| P6Failure::unknown(PublicError::MalformedRequest))?;
            let Some(group) = current.as_ref() else {
                return Err(self.failure_with_group(
                    PublicError::UnknownMember,
                    &context.observation,
                    None,
                    Some(member_id),
                ));
            };
            let member = group.members.get(&member_id).ok_or_else(|| {
                self.failure_with_group(
                    PublicError::UnknownMember,
                    &context.observation,
                    Some(group),
                    Some(member_id),
                )
            })?;
            if member.instance_id.is_none()
                && request.instance_id.is_none()
                && member.member_epoch.get() == request.expected_member_epoch
                && group.active_group_epoch == request.expected_group_epoch
                && member.subscriptions == request.subscriptions
                && member.session_timeout_ms == request.session_timeout_ms
            {
                self.track_joined_member(
                    context.observation.hint.coordinator_epoch,
                    group,
                    member_id,
                )
                .await?;
                return self.member_join_response(&context.observation, group, member_id);
            }
        }

        let state = self.dynamic.state_snapshot().map_err(|_| {
            self.failure_with_group(
                PublicError::BrokerNotAvailable,
                &context.observation,
                current.as_ref(),
                member_id_from_bytes(request.member_id).ok(),
            )
        })?;
        let mut machine = match current.clone() {
            Some(record) => GroupStateMachine::from_snapshot(record.clone()).map_err(|error| {
                self.group_state_failure(error, &context.observation, Some(&record), None)
            })?,
            None => GroupStateMachine::new(request.group_id.clone()),
        };

        let (member_id, command) = if request.member_id == [0; 16] {
            let allocated = allocate_member_id(current.as_ref())
                .map_err(|_| P6Failure::unknown(PublicError::UnknownServerError))?;
            let mut members = current
                .as_ref()
                .map(|group| group.members.clone())
                .unwrap_or_default();
            members.insert(
                allocated,
                astralog_groups::MemberRecordV1 {
                    member_id: allocated,
                    instance_id: request.instance_id.clone(),
                    member_epoch: MemberEpoch::new(1)
                        .map_err(|_| P6Failure::unknown(PublicError::UnknownServerError))?,
                    session_timeout_ms: request.session_timeout_ms,
                    subscriptions: request.subscriptions.clone(),
                },
            );
            let current_assignments = current
                .as_ref()
                .map(|group| &group.active_assignments)
                .cloned()
                .unwrap_or_default();
            let counts = topic_partition_counts(&state, &members).map_err(|error| {
                self.failure_with_group(error, &context.observation, current.as_ref(), None)
            })?;
            let target = uniform_sticky_assign(&members, &current_assignments, &counts)
                .map_err(|_| {
                    self.failure_with_group(
                        PublicError::InvalidRequest,
                        &context.observation,
                        current.as_ref(),
                        None,
                    )
                })?
                .assignments;
            (
                allocated,
                GroupCommand::AdmitMember {
                    expected_group_epoch: request.expected_group_epoch,
                    allocated_member_id: allocated,
                    instance_id: request.instance_id.clone(),
                    session_timeout_ms: request.session_timeout_ms,
                    subscriptions: request.subscriptions.clone(),
                    plan: GroupMutationPlan {
                        target_assignments: target,
                    },
                },
            )
        } else {
            let member_id = MemberId::new(request.member_id)
                .map_err(|_| P6Failure::unknown(PublicError::MalformedRequest))?;
            let group = current.as_ref().expect("member existence checked above");
            let member = &group.members[&member_id];
            let expected_member_epoch = MemberEpoch::new(request.expected_member_epoch)
                .map_err(|_| P6Failure::unknown(PublicError::MalformedRequest))?;
            if member.instance_id.is_some() {
                if request.instance_id != member.instance_id {
                    return Err(self.failure_with_group(
                        PublicError::UnknownMember,
                        &context.observation,
                        Some(group),
                        Some(member_id),
                    ));
                }
                let counts = topic_partition_counts_for_subscriptions(
                    &state,
                    group
                        .members
                        .values()
                        .flat_map(|m| m.subscriptions.iter())
                        .copied()
                        .chain(request.subscriptions.iter().copied()),
                )
                .map_err(|error| {
                    self.failure_with_group(
                        error,
                        &context.observation,
                        Some(group),
                        Some(member_id),
                    )
                })?;
                let command = prepare_static_replacement(
                    group,
                    member_id,
                    expected_member_epoch,
                    request.session_timeout_ms,
                    request.subscriptions.clone(),
                    &counts,
                )
                .map_err(|error| {
                    self.session_failure(error, &context.observation, group, member_id)
                })?;
                (member_id, command)
            } else {
                if request.instance_id.is_some()
                    || member.session_timeout_ms != request.session_timeout_ms
                {
                    return Err(self.failure_with_group(
                        PublicError::InvalidRequest,
                        &context.observation,
                        Some(group),
                        Some(member_id),
                    ));
                }
                let mut members = group.members.clone();
                let updated = members.get_mut(&member_id).expect("member checked");
                updated.subscriptions = request.subscriptions.clone();
                let counts = topic_partition_counts(&state, &members).map_err(|error| {
                    self.failure_with_group(
                        error,
                        &context.observation,
                        Some(group),
                        Some(member_id),
                    )
                })?;
                let target = uniform_sticky_assign(&members, &group.active_assignments, &counts)
                    .map_err(|_| {
                        self.failure_with_group(
                            PublicError::InvalidRequest,
                            &context.observation,
                            Some(group),
                            Some(member_id),
                        )
                    })?
                    .assignments;
                (
                    member_id,
                    GroupCommand::UpdateSubscription {
                        expected_group_epoch: GroupEpoch::new(request.expected_group_epoch)
                            .map_err(|_| P6Failure::unknown(PublicError::MalformedRequest))?,
                        member_id,
                        expected_member_epoch,
                        subscriptions: request.subscriptions.clone(),
                        plan: GroupMutationPlan {
                            target_assignments: target,
                        },
                    },
                )
            }
        };

        let prepared = machine.apply(command).map_err(|error| {
            self.group_state_failure(
                error,
                &context.observation,
                current.as_ref(),
                Some(member_id),
            )
        })?;
        let committed = self
            .groups
            .commit(context.observation.hint.coordinator_epoch, prepared)
            .await
            .map_err(|error| {
                self.group_runtime_failure(
                    error,
                    &context.observation,
                    current.as_ref(),
                    Some(member_id),
                )
            })?
            .record;
        // The initial group mutation is already quorum committed at this point.
        // Any subsequent failure while fencing/activating/tracking the prepared
        // epoch makes the public JoinSubscribe outcome ambiguous: replaying the
        // original request can otherwise observe the committed higher epoch and
        // incorrectly surface GroupEpochStale as a terminal fresh mutation.
        //
        // Preserve the frozen P6 rule used by the coordinator commit path:
        // once durable mutation authority exists, callers must reconcile from
        // committed history rather than blindly retrying the mutation.
        let final_group = self
            .finish_prepared(&context.observation, committed.clone())
            .await
            .map_err(|_| {
                self.failure_with_group(
                    PublicError::RequestTimeout,
                    &context.observation,
                    Some(&committed),
                    Some(member_id),
                )
            })?;
        self.track_joined_member(
            context.observation.hint.coordinator_epoch,
            &final_group,
            member_id,
        )
        .await
        .map_err(|_| {
            self.failure_with_group(
                PublicError::RequestTimeout,
                &context.observation,
                Some(&final_group),
                Some(member_id),
            )
        })?;
        self.member_join_response(&context.observation, &final_group, member_id)
    }

    pub(crate) async fn maintenance_once(&self) {
        if !self.ready() {
            return;
        }
        let group_ids = self
            .sessions
            .lock()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let mut tasks = tokio::task::JoinSet::new();
        for group_id in group_ids {
            let service = self.clone();
            tasks.spawn(async move {
                let _ = service.maintain_group(group_id).await;
            });
        }
        while tasks.join_next().await.is_some() {
            // The session map is hard-bounded by MAX_TRACKED_SESSION_GROUPS, so
            // maintenance creates at most that many in-flight tasks. The ticker
            // awaits this whole bounded set before starting the next round, which
            // prevents per-group overlap while removing cross-group head-of-line
            // blocking from slow fence/routing work.
        }
    }

    /// Discover previously committed members after a P3 leadership change.
    /// Run independently of session maintenance so a slow or large history
    /// replay cannot starve expiry of already tracked groups.
    pub(crate) async fn discover_leadership_once(&self) {
        if self.ready() {
            let _ = self.initialize_new_group_leadership().await;
        }
    }

    async fn initialize_new_group_leadership(&self) -> Result<(), P6Failure> {
        let now = self.now_ms();
        {
            let mut next = self.next_leadership_scan_ms.lock().await;
            if now < *next {
                return Ok(());
            }
            *next = now.saturating_add(LEADERSHIP_DISCOVERY_INTERVAL_MS);
        }

        let topology = self.replication.topology_snapshot();
        let Some(groups_topic) = topology.topic_by_name(GROUPS_INTERNAL_TOPIC_NAME) else {
            return Ok(());
        };
        let groups_topic_id = groups_topic.topic_id;
        let local_groups = self.replication.local_groups();
        let mut first_error = None;
        let mut tasks = tokio::task::JoinSet::new();
        for local in local_groups
            .into_iter()
            .filter(|group| group.topic_id == groups_topic_id)
        {
            // Discover up to eight independent partition leaders concurrently.
            // Sequentially fetching all 64 group-shard histories can starve the
            // maintenance ticker and delay session expiration on a loaded host.
            // Every shard still checks authoritative P3 leadership before
            // installing fresh grace; bounded concurrency changes no authority
            // or committed-history requirements.
            let service = self.clone();
            // The scan is in its own supervisor, separately from expiry
            // maintenance. Bound each actor read rather than the entire replay:
            // a legitimate large shard must not be cancelled every three
            // seconds and starve forever.
            tasks.spawn(async move {
                service
                    .initialize_group_shard_leadership(groups_topic_id, local.partition)
                    .await
            });
            if tasks.len() >= MAX_LEADERSHIP_DISCOVERY_TASKS
                && let Some(result) = tasks.join_next().await
            {
                match result {
                    Ok(Err(error)) if first_error.is_none() => first_error = Some(error),
                    Err(_) if first_error.is_none() => {
                        first_error = Some(P6Failure::unknown(PublicError::UnknownServerError));
                    }
                    _ => {}
                }
            }
        }
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Err(error)) if first_error.is_none() => first_error = Some(error),
                Err(_) if first_error.is_none() => {
                    first_error = Some(P6Failure::unknown(PublicError::UnknownServerError));
                }
                _ => {}
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    async fn initialize_group_shard_leadership(
        &self,
        groups_topic_id: astralog_common::TopicId,
        shard: u32,
    ) -> Result<(), P6Failure> {
        let Some(status) = time::timeout(
            LEADERSHIP_DISCOVERY_IO_TIMEOUT,
            self.replication.status(groups_topic_id, shard),
        )
        .await
        .map_err(|_| P6Failure::unknown(PublicError::RequestTimeout))?
        else {
            return Ok(());
        };
        if !committed_local_group_leadership(status, self.local_broker_id, None) {
            return Ok(());
        }
        if self
            .initialized_group_shards
            .lock()
            .await
            .get(&shard)
            .copied()
            == Some(status.leader_epoch)
        {
            return Ok(());
        }
        let groups = self
            .recover_active_groups_on_shard(groups_topic_id, shard, status.leader_epoch)
            .await?;
        // Recheck *after* replay as well. An in-flight leadership transition
        // must not install an incomplete/stale tracker or mark this term as
        // initialized. The next scan will retry once the barrier commits.
        let Some(after_replay) = time::timeout(
            LEADERSHIP_DISCOVERY_IO_TIMEOUT,
            self.replication.status(groups_topic_id, shard),
        )
        .await
        .map_err(|_| P6Failure::unknown(PublicError::RequestTimeout))?
        else {
            return Ok(());
        };
        if !committed_local_group_leadership(
            after_replay,
            self.local_broker_id,
            Some(status.leader_epoch),
        ) {
            return Ok(());
        }
        self.install_new_leader_grace(shard, status.leader_epoch, groups)
            .await?;
        self.initialized_group_shards
            .lock()
            .await
            .insert(shard, status.leader_epoch);
        Ok(())
    }

    async fn recover_active_groups_on_shard(
        &self,
        topic_id: astralog_common::TopicId,
        shard: u32,
        leader_epoch: u64,
    ) -> Result<BTreeMap<GroupId, GroupRecordV1>, P6Failure> {
        let mut states = BTreeMap::<GroupId, GroupStateMachine>::new();
        let mut start = StreamOffset::new(0);
        loop {
            let page = time::timeout(
                LEADERSHIP_DISCOVERY_IO_TIMEOUT,
                self.replication.fetch(
                    topic_id,
                    shard,
                    Some(leader_epoch),
                    start,
                    LEADERSHIP_REPLAY_BATCH_RECORDS,
                    usize::MAX,
                ),
            )
            .await
            .map_err(|_| P6Failure::unknown(PublicError::RequestTimeout))?
            .map_err(|error| P6Failure::unknown(error.error))?;
            if page.records.is_empty() {
                break;
            }
            let mut next = None;
            for stored in page.records {
                next = stored.offset.checked_add(1);
                let value = stored
                    .record
                    .value
                    .as_deref()
                    .ok_or_else(|| P6Failure::unknown(PublicError::UnknownServerError))?;
                let InternalRecordV1::GroupState(record) = InternalRecordV1::decode(value)
                    .map_err(|_| P6Failure::unknown(PublicError::UnknownServerError))?
                else {
                    return Err(P6Failure::unknown(PublicError::UnknownServerError));
                };
                if stored.record.key.as_deref()
                    != Some(encode_group_state_key(&record.group_id).as_slice())
                    || group_shard(&record.group_id)
                        .map_err(|_| P6Failure::unknown(PublicError::UnknownServerError))?
                        != shard
                {
                    return Err(P6Failure::unknown(PublicError::UnknownServerError));
                }
                if !states.contains_key(&record.group_id)
                    && states.len() >= MAX_LEADERSHIP_DISCOVERY_GROUPS
                {
                    return Err(P6Failure::unknown(PublicError::BrokerBusy));
                }
                let group_id = record.group_id.clone();
                let state = states
                    .entry(group_id.clone())
                    .or_insert_with(|| GroupStateMachine::new(group_id));
                state
                    .apply_committed(record)
                    .map_err(|_| P6Failure::unknown(PublicError::UnknownServerError))?;
            }
            let Some(next) = next else { break };
            start = next;
        }
        let active = states
            .into_iter()
            .filter_map(|(group_id, state)| {
                state
                    .current()
                    .filter(|record| !record.members.is_empty())
                    .cloned()
                    .map(|record| (group_id, record))
            })
            .collect::<BTreeMap<_, _>>();
        if active.len() > MAX_TRACKED_SESSION_GROUPS {
            return Err(P6Failure::unknown(PublicError::BrokerBusy));
        }
        Ok(active)
    }

    async fn install_new_leader_grace(
        &self,
        shard: u32,
        leader_epoch: u64,
        groups: BTreeMap<GroupId, GroupRecordV1>,
    ) -> Result<(), P6Failure> {
        if groups.len() > MAX_TRACKED_SESSION_GROUPS {
            return Err(P6Failure::unknown(PublicError::BrokerBusy));
        }
        let now = self.now_ms();
        let mut sessions = self.sessions.lock().await;
        let stale = sessions
            .keys()
            .filter(|group_id| group_shard(group_id).ok() == Some(shard))
            .filter(|group_id| !groups.contains_key(*group_id))
            .cloned()
            .collect::<Vec<_>>();
        let retained = sessions.len().saturating_sub(stale.len());
        let incoming_new = groups
            .keys()
            .filter(|group_id| !sessions.contains_key(*group_id))
            .count();
        if retained.saturating_add(incoming_new) > MAX_TRACKED_SESSION_GROUPS {
            return Err(P6Failure::unknown(PublicError::BrokerBusy));
        }
        for group_id in stale {
            sessions.remove(&group_id);
        }
        for (group_id, group) in groups {
            if let Some(existing) = sessions.get_mut(&group_id)
                && existing.coordinator_epoch == leader_epoch
            {
                // A public Join/Heartbeat can establish an in-term session
                // while the discovery replay is pending. Never grant that
                // same member a second full grace merely because discovery
                // completed later in the term.
                existing
                    .tracker
                    .reconcile_committed_group(now, &group)
                    .map_err(|_| P6Failure::unknown(PublicError::UnknownServerError))?;
                continue;
            }
            let tracker = SessionTracker::new_leader(now, &group)
                .map_err(|_| P6Failure::unknown(PublicError::UnknownServerError))?;
            sessions.insert(
                group_id,
                SessionEntry {
                    coordinator_epoch: leader_epoch,
                    tracker,
                },
            );
        }
        Ok(())
    }

    pub(crate) async fn heartbeat(
        &self,
        request: HeartbeatRequestV0,
    ) -> Result<HeartbeatResponseV0, P6Failure> {
        let context = self
            .require_local_coordinator(&request.authority.group_id)
            .await?;
        let group = context.group.as_ref().ok_or_else(|| {
            self.failure_with_group(
                PublicError::UnknownMember,
                &context.observation,
                None,
                Some(request.authority.member_id),
            )
        })?;
        self.refresh_session_heartbeat(
            context.observation.hint.coordinator_epoch,
            group,
            &request.authority,
        )
        .await?;
        self.member_state_response(&context.observation, group, request.authority.member_id)
    }

    pub(crate) async fn revoke_ack(
        &self,
        request: RevokeAckRequestV0,
    ) -> Result<HeartbeatResponseV0, P6Failure> {
        let context = self
            .require_local_coordinator(&request.authority.group_id)
            .await?;
        let group = context.group.as_ref().ok_or_else(|| {
            self.failure_with_group(
                PublicError::UnknownMember,
                &context.observation,
                None,
                Some(request.authority.member_id),
            )
        })?;
        let history = self
            .groups
            .recover_history(
                request.authority.group_id.clone(),
                context.observation.hint.coordinator_epoch,
            )
            .await
            .map_err(|error| {
                self.group_runtime_failure(
                    error,
                    &context.observation,
                    Some(group),
                    Some(request.authority.member_id),
                )
            })?;
        let fences = self
            .route_recover_fences(&request.authority.group_id)
            .await
            .map_err(|error| {
                self.failure_with_group(
                    error,
                    &context.observation,
                    Some(group),
                    Some(request.authority.member_id),
                )
            })?;
        let group_fences = fences.group.into_iter().collect::<Vec<_>>();
        let mut cooperative = CooperativeStateMachine::recover_workflow_suffix(
            &history,
            &group_fences,
            &fences.owners,
        )
        .map_err(|error| {
            self.cooperative_failure(
                error,
                &context.observation,
                Some(group),
                Some(request.authority.member_id),
            )
        })?;
        let outcome = cooperative
            .revoke_ack(&request.authority, &request.revoked)
            .map_err(|error| {
                self.cooperative_failure(
                    error,
                    &context.observation,
                    Some(group),
                    Some(request.authority.member_id),
                )
            })?;
        if outcome == CooperativeApplyOutcome::Applied {
            self.groups
                .commit(
                    context.observation.hint.coordinator_epoch,
                    cooperative.current().clone(),
                )
                .await
                .map_err(|error| {
                    self.group_runtime_failure(
                        error,
                        &context.observation,
                        Some(group),
                        Some(request.authority.member_id),
                    )
                })?;
        }
        let final_group = self
            .finish_released(&context.observation, cooperative)
            .await?;
        self.member_state_response(
            &context.observation,
            &final_group,
            request.authority.member_id,
        )
    }

    pub(crate) async fn leave(
        &self,
        request: LeaveRequestV0,
    ) -> Result<LeaveResponseV0, P6Failure> {
        let context = self
            .require_local_coordinator(&request.authority.group_id)
            .await?;
        let group = context.group.as_ref().ok_or_else(|| {
            self.failure_with_group(
                PublicError::UnknownMember,
                &context.observation,
                None,
                Some(request.authority.member_id),
            )
        })?;
        if group.phase != GroupPhase::Stable {
            return Err(self.failure_with_group(
                PublicError::RebalanceInProgress,
                &context.observation,
                Some(group),
                Some(request.authority.member_id),
            ));
        }
        let state = self.dynamic.state_snapshot().map_err(|_| {
            self.failure_with_group(
                PublicError::BrokerNotAvailable,
                &context.observation,
                Some(group),
                Some(request.authority.member_id),
            )
        })?;
        validate_authority(group, &request.authority).map_err(|error| {
            self.group_state_failure(
                error,
                &context.observation,
                Some(group),
                Some(request.authority.member_id),
            )
        })?;
        let mut members = group.members.clone();
        members.remove(&request.authority.member_id);
        let counts = topic_partition_counts(&state, &members).map_err(|error| {
            self.failure_with_group(
                error,
                &context.observation,
                Some(group),
                Some(request.authority.member_id),
            )
        })?;
        let target = if members.is_empty() {
            BTreeMap::new()
        } else {
            uniform_sticky_assign(&members, &group.active_assignments, &counts)
                .map_err(|_| {
                    self.failure_with_group(
                        PublicError::InvalidRequest,
                        &context.observation,
                        Some(group),
                        Some(request.authority.member_id),
                    )
                })?
                .assignments
        };
        let mut machine = GroupStateMachine::from_snapshot(group.clone()).map_err(|error| {
            self.group_state_failure(
                error,
                &context.observation,
                Some(group),
                Some(request.authority.member_id),
            )
        })?;
        let prepared = machine
            .apply(GroupCommand::Leave {
                expected_group_epoch: request.authority.group_epoch,
                member_id: request.authority.member_id,
                expected_member_epoch: request.authority.member_epoch,
                plan: GroupMutationPlan {
                    target_assignments: target,
                },
            })
            .map_err(|error| {
                self.group_state_failure(
                    error,
                    &context.observation,
                    Some(group),
                    Some(request.authority.member_id),
                )
            })?;
        let prepared = self
            .groups
            .commit(context.observation.hint.coordinator_epoch, prepared)
            .await
            .map_err(|error| {
                self.group_runtime_failure(
                    error,
                    &context.observation,
                    Some(group),
                    Some(request.authority.member_id),
                )
            })?
            .record;
        let final_group = self.finish_prepared(&context.observation, prepared).await?;
        self.sessions
            .lock()
            .await
            .remove(&request.authority.group_id);
        Ok(LeaveResponseV0 {
            hint: self.hint(
                &context.observation,
                Some(&final_group),
                Some(request.authority.member_id),
            ),
            tags: Vec::new(),
        })
    }

    pub(crate) async fn commit_offsets(
        &self,
        request: CommitOffsetsRequestV0,
    ) -> Result<CommitOffsetsResponseV0, P6Failure> {
        let context = self
            .require_local_coordinator(&request.authority.group_id)
            .await?;
        let group = context.group.as_ref().ok_or_else(|| {
            self.failure_with_group(
                PublicError::UnknownMember,
                &context.observation,
                None,
                Some(request.authority.member_id),
            )
        })?;
        let commit_index = self
            .route_commit_offsets(group, &request)
            .await
            .map_err(|error| {
                self.failure_with_group(
                    error,
                    &context.observation,
                    Some(group),
                    Some(request.authority.member_id),
                )
            })?;
        Ok(CommitOffsetsResponseV0 {
            hint: self.hint(
                &context.observation,
                Some(group),
                Some(request.authority.member_id),
            ),
            offsets_commit_index: commit_index,
            tags: Vec::new(),
        })
    }

    pub(crate) async fn fetch_offsets(
        &self,
        request: FetchOffsetsRequestV0,
    ) -> Result<FetchOffsetsResponseV0, P6Failure> {
        let context = self.require_local_coordinator(&request.group_id).await?;
        let results = self.route_fetch_offsets(&request).await.map_err(|error| {
            self.failure_with_group(error, &context.observation, context.group.as_ref(), None)
        })?;
        Ok(FetchOffsetsResponseV0 {
            hint: self.hint(&context.observation, context.group.as_ref(), None),
            results,
            tags: Vec::new(),
        })
    }

    pub(crate) async fn handle_private_offset(&self, frame: &WireFrame) -> WireFrame {
        let mut reader = Reader::new(&frame.body);
        let decoded = decode_private_request(&mut reader);
        let (operation, authority, payload) = match decoded {
            Ok(value) if reader.is_empty() => value,
            _ => {
                return private_error_response(
                    frame,
                    PublicError::MalformedRequest,
                    UNKNOWN_BROKER_ID,
                    UNKNOWN_EPOCH,
                );
            }
        };
        let state = match self.dynamic.state_snapshot() {
            Ok(state) => state,
            Err(_) => {
                return private_error_response(
                    frame,
                    PublicError::BrokerNotAvailable,
                    UNKNOWN_BROKER_ID,
                    UNKNOWN_EPOCH,
                );
            }
        };
        if state.validate_broker_authority(authority).is_err() {
            return private_error_response(
                frame,
                PublicError::BrokerNotAvailable,
                UNKNOWN_BROKER_ID,
                UNKNOWN_EPOCH,
            );
        }
        let group_id = match private_payload_group(operation, &payload) {
            Ok(group_id) => group_id,
            Err(error) => {
                return private_error_response(frame, error, UNKNOWN_BROKER_ID, UNKNOWN_EPOCH);
            }
        };
        let observation = match self.offsets.observe(group_id.clone()).await {
            Ok(observation) => observation,
            Err(error) => {
                let (public, leader, epoch) = offset_error(error);
                return private_error_response(frame, public, leader, epoch);
            }
        };
        if observation.leader_broker_id != self.local_broker_id {
            return private_error_response(
                frame,
                PublicError::NotLeader,
                observation.leader_broker_id,
                observation.leader_epoch,
            );
        }
        let result = self
            .execute_private_offset(operation, &payload, observation.leader_epoch)
            .await;
        match result {
            Ok(payload) => private_success_response(
                frame,
                observation.leader_broker_id,
                observation.leader_epoch,
                payload,
            ),
            Err(error) => {
                let (public, leader, epoch) = offset_error(error);
                private_error_response(frame, public, leader, epoch)
            }
        }
    }

    async fn execute_private_offset(
        &self,
        operation: u8,
        payload: &[u8],
        epoch: u64,
    ) -> Result<Vec<u8>, OffsetCoordinatorError> {
        match operation {
            PRIVATE_OP_GROUP_FENCE => {
                let decoded = InternalRecordV1::decode(payload)
                    .map_err(|_| OffsetCoordinatorError::Validation(PublicError::InvalidRequest))?;
                let InternalRecordV1::OffsetGroupFence(fence) = decoded else {
                    return Err(OffsetCoordinatorError::Validation(
                        PublicError::InvalidRequest,
                    ));
                };
                self.offsets.commit_group_fence(epoch, fence).await?;
                Ok(Vec::new())
            }
            PRIVATE_OP_OWNER_FENCE => {
                let decoded = InternalRecordV1::decode(payload)
                    .map_err(|_| OffsetCoordinatorError::Validation(PublicError::InvalidRequest))?;
                let InternalRecordV1::OffsetOwnerFence(fence) = decoded else {
                    return Err(OffsetCoordinatorError::Validation(
                        PublicError::InvalidRequest,
                    ));
                };
                self.offsets.commit_owner_fence(epoch, fence).await?;
                Ok(Vec::new())
            }
            PRIVATE_OP_COMMIT_OFFSETS => {
                let mut reader = Reader::new(payload);
                let group_bytes = reader
                    .read_bytes32(PRIVATE_MAX_BODY, "private_group_record")
                    .map_err(|_| OffsetCoordinatorError::Validation(PublicError::InvalidRequest))?;
                let request_bytes = reader
                    .read_bytes32(PRIVATE_MAX_BODY, "private_commit_offsets")
                    .map_err(|_| OffsetCoordinatorError::Validation(PublicError::InvalidRequest))?;
                if !reader.is_empty() {
                    return Err(OffsetCoordinatorError::Validation(
                        PublicError::InvalidRequest,
                    ));
                }
                let decoded = InternalRecordV1::decode(&group_bytes)
                    .map_err(|_| OffsetCoordinatorError::Validation(PublicError::InvalidRequest))?;
                let InternalRecordV1::GroupState(group) = decoded else {
                    return Err(OffsetCoordinatorError::Validation(
                        PublicError::InvalidRequest,
                    ));
                };
                let request = CommitOffsetsRequestV0::decode_body(&request_bytes)
                    .map_err(|_| OffsetCoordinatorError::Validation(PublicError::InvalidRequest))?;
                let outcome = self
                    .offsets
                    .commit_offsets(epoch, group, request.authority, request.offsets)
                    .await?;
                let mut writer = Writer::new();
                writer.put_u64(outcome.offsets_commit_index);
                Ok(writer.into_inner())
            }
            PRIVATE_OP_FETCH_OFFSETS => {
                let request = FetchOffsetsRequestV0::decode_body(payload)
                    .map_err(|_| OffsetCoordinatorError::Validation(PublicError::InvalidRequest))?;
                let outcome = self
                    .offsets
                    .fetch_offsets(request.group_id, epoch, request.partitions)
                    .await?;
                let results = outcome
                    .values
                    .into_iter()
                    .map(|(partition, value)| FetchOffsetResultV0 {
                        topic_id: partition.topic_id,
                        partition: partition.partition,
                        value: value.map(|value| FetchOffsetValueV0 {
                            next_offset: value.next_offset,
                            commit_timestamp_ms: value.commit_timestamp_ms,
                            group_epoch: value.group_epoch,
                            member_id: value.member_id,
                            member_epoch: value.member_epoch,
                            metadata: value.metadata,
                        }),
                    })
                    .collect();
                FetchOffsetsResponseV0 {
                    hint: CoordinatorHintV0::UNKNOWN,
                    results,
                    tags: Vec::new(),
                }
                .encode_body()
                .map_err(|_| OffsetCoordinatorError::Validation(PublicError::InvalidRequest))
            }
            PRIVATE_OP_RECOVER_FENCES => {
                let mut reader = Reader::new(payload);
                let group_id =
                    GroupId::new(reader.read_string16(MAX_GROUP_ID_LEN, "group_id").map_err(
                        |_| OffsetCoordinatorError::Validation(PublicError::InvalidRequest),
                    )?)
                    .map_err(|_| OffsetCoordinatorError::Validation(PublicError::InvalidRequest))?;
                if !reader.is_empty() {
                    return Err(OffsetCoordinatorError::Validation(
                        PublicError::InvalidRequest,
                    ));
                }
                let state = self.offsets.recover(group_id, epoch).await?;
                encode_current_fences(&state)
                    .map_err(|_| OffsetCoordinatorError::Validation(PublicError::InvalidRequest))
            }
            _ => Err(OffsetCoordinatorError::Validation(
                PublicError::InvalidRequest,
            )),
        }
    }

    async fn require_local_coordinator(
        &self,
        group_id: &GroupId,
    ) -> Result<CoordinatorContext, P6Failure> {
        if !self.ready() {
            return Err(P6Failure::unknown(PublicError::UnsupportedFeature));
        }
        let observation = self
            .dynamic
            .observe_group_coordinator(group_id)
            .await
            .map_err(|_| P6Failure::unknown(PublicError::NotLeader))?;
        if observation.hint.coordinator_broker_id != self.local_broker_id {
            return Err(P6Failure::new(PublicError::NotLeader, observation.hint));
        }
        self.dynamic
            .validate_group_coordinator_authority(group_id, observation.hint.coordinator_epoch)
            .await
            .map_err(|_| P6Failure::new(PublicError::NotLeader, observation.hint))?;
        let group = self
            .groups
            .recover(group_id.clone(), observation.hint.coordinator_epoch)
            .await
            .map_err(|error| self.group_runtime_failure(error, &observation, None, None))?;
        Ok(CoordinatorContext { observation, group })
    }

    async fn finish_prepared(
        &self,
        observation: &GroupCoordinatorObservation,
        prepared: GroupRecordV1,
    ) -> Result<GroupRecordV1, P6Failure> {
        if prepared.phase != GroupPhase::PreparingEpochFence {
            return Ok(prepared);
        }
        let mut cooperative = CooperativeStateMachine::from_group_record(prepared.clone())
            .map_err(|error| self.cooperative_failure(error, observation, Some(&prepared), None))?;
        let fence = cooperative
            .required_group_fence()
            .map_err(|error| self.cooperative_failure(error, observation, Some(&prepared), None))?;
        self.route_group_fence(&fence)
            .await
            .map_err(|error| self.failure_with_group(error, observation, Some(&prepared), None))?;
        cooperative
            .apply_committed_group_fence(fence)
            .map_err(|error| self.cooperative_failure(error, observation, Some(&prepared), None))?;
        let activated = cooperative
            .activate_prepared_epoch()
            .map_err(|error| self.cooperative_failure(error, observation, Some(&prepared), None))?;
        let committed = self
            .groups
            .commit(observation.hint.coordinator_epoch, activated)
            .await
            .map_err(|error| self.group_runtime_failure(error, observation, Some(&prepared), None))?
            .record;
        if cooperative.current() != &committed {
            return Err(self.failure_with_group(
                PublicError::UnknownServerError,
                observation,
                Some(&committed),
                None,
            ));
        }
        self.finish_released(observation, cooperative).await
    }

    async fn finish_released(
        &self,
        observation: &GroupCoordinatorObservation,
        mut cooperative: CooperativeStateMachine,
    ) -> Result<GroupRecordV1, P6Failure> {
        if cooperative.current().phase != GroupPhase::Revoking {
            return Ok(cooperative.current().clone());
        }
        let fences = cooperative.required_owner_fences().map_err(|error| {
            self.cooperative_failure(error, observation, Some(cooperative.current()), None)
        })?;
        for fence in fences {
            self.route_owner_fence(&fence).await.map_err(|error| {
                self.failure_with_group(error, observation, Some(cooperative.current()), None)
            })?;
            cooperative
                .apply_committed_owner_fence(fence)
                .map_err(|error| {
                    self.cooperative_failure(error, observation, Some(cooperative.current()), None)
                })?;
        }
        match cooperative.activate_fenced_targets() {
            Ok(candidate) => {
                let committed = self
                    .groups
                    .commit(observation.hint.coordinator_epoch, candidate)
                    .await
                    .map_err(|error| {
                        self.group_runtime_failure(
                            error,
                            observation,
                            Some(cooperative.current()),
                            None,
                        )
                    })?
                    .record;
                Ok(committed)
            }
            Err(CooperativeError::NoReadyActivation) => Ok(cooperative.current().clone()),
            Err(error) => {
                Err(self.cooperative_failure(error, observation, Some(cooperative.current()), None))
            }
        }
    }

    async fn maintain_group(&self, group_id: GroupId) -> Result<(), P6Failure> {
        let mut context = match self.require_local_coordinator(&group_id).await {
            Ok(context) => context,
            Err(error) if error.error == PublicError::NotLeader => {
                // A broker-level coordinator observation can fail transiently while
                // the local P3 actor is still the authoritative leader. Discarding
                // volatile session state on that transient would incorrectly grant
                // another full grace interval when the same term is rediscovered.
                // Only concrete local P3 evidence that leadership moved elsewhere
                // is allowed to discard this leader-local tracker. If status itself
                // is temporarily unavailable, preserve the bounded volatile state
                // and retry on a later maintenance tick; it cannot authorize any
                // mutation because all public/durable paths still pass the normal
                // coordinator authority gate.
                let mut leadership_moved = false;
                if let Ok(shard) = group_shard(&group_id) {
                    let topology = self.replication.topology_snapshot();
                    if let Some(topic) = topology.topic_by_name(GROUPS_INTERNAL_TOPIC_NAME)
                        && let Some(status) = self.replication.status(topic.topic_id, shard).await
                    {
                        leadership_moved = status.role != Role::Leader
                            || status.leader_broker_id != Some(self.local_broker_id)
                            || status.leader_epoch == 0;
                    }
                }
                if leadership_moved {
                    self.sessions.lock().await.remove(&group_id);
                }
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let Some(mut group) = context.group.clone() else {
            self.sessions.lock().await.remove(&group_id);
            return Ok(());
        };

        if group.phase == GroupPhase::PreparingEpochFence {
            group = self.finish_prepared(&context.observation, group).await?;
            context = self.require_local_coordinator(&group_id).await?;
        }
        if group.phase == GroupPhase::Revoking {
            group = self
                .resume_released_workflow(&context.observation, &group)
                .await?;
        }
        self.reconcile_session_tracker(context.observation.hint.coordinator_epoch, &group)
            .await?;
        if group.phase != GroupPhase::Stable {
            return Ok(());
        }

        let state = self.dynamic.state_snapshot().map_err(|_| {
            self.failure_with_group(
                PublicError::BrokerNotAvailable,
                &context.observation,
                Some(&group),
                None,
            )
        })?;
        let counts = topic_partition_counts(&state, &group.members).map_err(|error| {
            self.failure_with_group(error, &context.observation, Some(&group), None)
        })?;
        let now = self.now_ms();
        let timeout_command = {
            let mut sessions = self.sessions.lock().await;
            let Some(entry) = sessions.get_mut(&group_id) else {
                return Ok(());
            };
            if entry.coordinator_epoch != context.observation.hint.coordinator_epoch {
                entry.coordinator_epoch = context.observation.hint.coordinator_epoch;
                entry.tracker = SessionTracker::new_leader(now, &group)
                    .map_err(|_| P6Failure::unknown(PublicError::UnknownServerError))?;
            } else {
                entry
                    .tracker
                    .reconcile_committed_group(now, &group)
                    .map_err(|_| P6Failure::unknown(PublicError::UnknownServerError))?;
            }
            let candidate = entry
                .tracker
                .expired_members(now, &group)
                .map_err(|_| P6Failure::unknown(PublicError::UnknownServerError))?
                .into_iter()
                .next();
            if let Some(candidate) = candidate {
                Some(
                    entry
                        .tracker
                        .prepare_timeout_removal(&group, &candidate, &counts)
                        .map_err(|error| {
                            self.session_failure(
                                error,
                                &context.observation,
                                &group,
                                candidate.authority.member_id,
                            )
                        })?,
                )
            } else {
                None
            }
        };

        if let Some(command) = timeout_command {
            let final_group = self
                .commit_maintenance_command(&context.observation, &group, command)
                .await?;
            self.reconcile_session_tracker(
                context.observation.hint.coordinator_epoch,
                &final_group,
            )
            .await?;
            return Ok(());
        }

        let target = uniform_sticky_assign(&group.members, &group.active_assignments, &counts)
            .map_err(|_| {
                self.failure_with_group(
                    PublicError::InvalidRequest,
                    &context.observation,
                    Some(&group),
                    None,
                )
            })?
            .assignments;
        if target != group.active_assignments {
            let expected_group_epoch = GroupEpoch::new(group.active_group_epoch)
                .map_err(|_| P6Failure::unknown(PublicError::UnknownServerError))?;
            let final_group = self
                .commit_maintenance_command(
                    &context.observation,
                    &group,
                    GroupCommand::RebalanceAssignments {
                        expected_group_epoch,
                        plan: GroupMutationPlan {
                            target_assignments: target,
                        },
                    },
                )
                .await?;
            self.reconcile_session_tracker(
                context.observation.hint.coordinator_epoch,
                &final_group,
            )
            .await?;
        }
        Ok(())
    }

    async fn resume_released_workflow(
        &self,
        observation: &GroupCoordinatorObservation,
        group: &GroupRecordV1,
    ) -> Result<GroupRecordV1, P6Failure> {
        let history = self
            .groups
            .recover_history(group.group_id.clone(), observation.hint.coordinator_epoch)
            .await
            .map_err(|error| self.group_runtime_failure(error, observation, Some(group), None))?;
        let fences = self
            .route_recover_fences(&group.group_id)
            .await
            .map_err(|error| self.failure_with_group(error, observation, Some(group), None))?;
        let group_fences = fences.group.into_iter().collect::<Vec<_>>();
        let cooperative = CooperativeStateMachine::recover_workflow_suffix(
            &history,
            &group_fences,
            &fences.owners,
        )
        .map_err(|error| self.cooperative_failure(error, observation, Some(group), None))?;
        self.finish_released(observation, cooperative).await
    }

    async fn commit_maintenance_command(
        &self,
        observation: &GroupCoordinatorObservation,
        current: &GroupRecordV1,
        command: GroupCommand,
    ) -> Result<GroupRecordV1, P6Failure> {
        let mut machine = GroupStateMachine::from_snapshot(current.clone())
            .map_err(|error| self.group_state_failure(error, observation, Some(current), None))?;
        let prepared = machine
            .apply(command)
            .map_err(|error| self.group_state_failure(error, observation, Some(current), None))?;
        let committed = self
            .groups
            .commit(observation.hint.coordinator_epoch, prepared)
            .await
            .map_err(|error| self.group_runtime_failure(error, observation, Some(current), None))?
            .record;
        self.finish_prepared(observation, committed).await
    }

    async fn reconcile_session_tracker(
        &self,
        coordinator_epoch: u64,
        group: &GroupRecordV1,
    ) -> Result<(), P6Failure> {
        let now = self.now_ms();
        let mut sessions = self.sessions.lock().await;
        let Some(entry) = sessions.get_mut(&group.group_id) else {
            return Ok(());
        };
        if entry.coordinator_epoch != coordinator_epoch {
            entry.coordinator_epoch = coordinator_epoch;
            entry.tracker = SessionTracker::new_leader(now, group)
                .map_err(|_| P6Failure::unknown(PublicError::UnknownServerError))?;
        } else {
            entry
                .tracker
                .reconcile_committed_group(now, group)
                .map_err(|_| P6Failure::unknown(PublicError::UnknownServerError))?;
        }
        if group.members.is_empty() {
            sessions.remove(&group.group_id);
        }
        Ok(())
    }

    async fn track_joined_member(
        &self,
        coordinator_epoch: u64,
        group: &GroupRecordV1,
        member_id: MemberId,
    ) -> Result<(), P6Failure> {
        let member = group
            .members
            .get(&member_id)
            .ok_or_else(|| P6Failure::unknown(PublicError::UnknownMember))?;
        let group_epoch = GroupEpoch::new(group.active_group_epoch)
            .map_err(|_| P6Failure::unknown(PublicError::UnknownServerError))?;
        let authority = MemberAuthorityV0 {
            group_id: group.group_id.clone(),
            member_id,
            group_epoch,
            member_epoch: member.member_epoch,
        };
        self.refresh_session_heartbeat(coordinator_epoch, group, &authority)
            .await
    }

    async fn refresh_session_heartbeat(
        &self,
        coordinator_epoch: u64,
        group: &GroupRecordV1,
        authority: &MemberAuthorityV0,
    ) -> Result<(), P6Failure> {
        let now = self.now_ms();
        let mut sessions = self.sessions.lock().await;
        if !sessions.contains_key(&group.group_id) && sessions.len() >= MAX_TRACKED_SESSION_GROUPS {
            return Err(P6Failure::unknown(PublicError::BrokerBusy));
        }
        let entry = match sessions.entry(group.group_id.clone()) {
            std::collections::btree_map::Entry::Vacant(entry) => entry.insert(SessionEntry {
                coordinator_epoch,
                tracker: SessionTracker::new_leader(now, group)
                    .map_err(|_| P6Failure::unknown(PublicError::UnknownServerError))?,
            }),
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                if entry.get().coordinator_epoch != coordinator_epoch {
                    entry.insert(SessionEntry {
                        coordinator_epoch,
                        tracker: SessionTracker::new_leader(now, group)
                            .map_err(|_| P6Failure::unknown(PublicError::UnknownServerError))?,
                    });
                } else {
                    entry
                        .get_mut()
                        .tracker
                        .reconcile_committed_group(now, group)
                        .map_err(|error| {
                            self.session_failure(
                                error,
                                &unknown_observation(coordinator_epoch, self.local_broker_id),
                                group,
                                authority.member_id,
                            )
                        })?;
                }
                entry.into_mut()
            }
        };
        entry
            .tracker
            .heartbeat(now, group, authority)
            .map_err(|error| {
                self.session_failure(
                    error,
                    &unknown_observation(coordinator_epoch, self.local_broker_id),
                    group,
                    authority.member_id,
                )
            })
    }

    async fn route_group_fence(&self, fence: &OffsetGroupFenceV1) -> Result<(), PublicError> {
        let payload = InternalRecordV1::OffsetGroupFence(fence.clone())
            .encode()
            .map_err(|_| PublicError::InvalidRequest)?;
        self.route_private_offset(&fence.group_id, PRIVATE_OP_GROUP_FENCE, payload, true, true)
            .await
            .map(|_| ())
    }

    async fn route_owner_fence(&self, fence: &OffsetOwnerFenceV1) -> Result<(), PublicError> {
        let payload = InternalRecordV1::OffsetOwnerFence(fence.clone())
            .encode()
            .map_err(|_| PublicError::InvalidRequest)?;
        self.route_private_offset(&fence.group_id, PRIVATE_OP_OWNER_FENCE, payload, true, true)
            .await
            .map(|_| ())
    }

    async fn route_commit_offsets(
        &self,
        group: &GroupRecordV1,
        request: &CommitOffsetsRequestV0,
    ) -> Result<u64, PublicError> {
        let group_bytes = InternalRecordV1::GroupState(group.clone())
            .encode()
            .map_err(|_| PublicError::InvalidRequest)?;
        let request_bytes = request
            .encode_body()
            .map_err(|_| PublicError::InvalidRequest)?;
        let mut writer = Writer::new();
        writer
            .put_bytes32(&group_bytes, PRIVATE_MAX_BODY, "private_group_record")
            .map_err(|_| PublicError::InvalidRequest)?;
        writer
            .put_bytes32(&request_bytes, PRIVATE_MAX_BODY, "private_commit_offsets")
            .map_err(|_| PublicError::InvalidRequest)?;
        let reply = self
            .route_private_offset(
                &group.group_id,
                PRIVATE_OP_COMMIT_OFFSETS,
                writer.into_inner(),
                true,
                false,
            )
            .await?;
        let mut reader = Reader::new(&reply.payload);
        let index = reader
            .read_u64()
            .map_err(|_| PublicError::UnknownServerError)?;
        if !reader.is_empty() {
            return Err(PublicError::UnknownServerError);
        }
        Ok(index)
    }

    async fn route_fetch_offsets(
        &self,
        request: &FetchOffsetsRequestV0,
    ) -> Result<Vec<FetchOffsetResultV0>, PublicError> {
        let payload = request
            .encode_body()
            .map_err(|_| PublicError::InvalidRequest)?;
        let reply = self
            .route_private_offset(
                &request.group_id,
                PRIVATE_OP_FETCH_OFFSETS,
                payload,
                false,
                true,
            )
            .await?;
        FetchOffsetsResponseV0::decode_body(&reply.payload)
            .map(|response| response.results)
            .map_err(|_| PublicError::UnknownServerError)
    }

    async fn route_recover_fences(&self, group_id: &GroupId) -> Result<CurrentFences, PublicError> {
        let mut writer = Writer::new();
        writer
            .put_string16(group_id.as_str(), MAX_GROUP_ID_LEN, "group_id")
            .map_err(|_| PublicError::InvalidRequest)?;
        let reply = self
            .route_private_offset(
                group_id,
                PRIVATE_OP_RECOVER_FENCES,
                writer.into_inner(),
                false,
                true,
            )
            .await?;
        decode_current_fences(&reply.payload).map_err(|_| PublicError::UnknownServerError)
    }

    async fn route_private_offset(
        &self,
        group_id: &GroupId,
        operation: u8,
        payload: Vec<u8>,
        mutation: bool,
        idempotent_mutation: bool,
    ) -> Result<PrivateOffsetReply, PublicError> {
        let state = self
            .dynamic
            .state_snapshot()
            .map_err(|_| PublicError::BrokerNotAvailable)?;
        let offsets = state
            .topic_by_name(OFFSETS_INTERNAL_TOPIC_NAME)
            .ok_or(PublicError::UnsupportedFeature)?;
        let shard =
            astralog_groups::group_shard(group_id).map_err(|_| PublicError::InvalidRequest)?;
        let assignment = state
            .assignments()
            .get(&(offsets.topic_id, shard))
            .ok_or(PublicError::BrokerNotAvailable)?;
        let authority = local_broker_authority(&state, self.local_broker_id)?;
        let mut candidates = assignment.voters.clone();
        candidates.extend(assignment.learners.iter().copied());
        candidates.sort_unstable();
        candidates.dedup();
        let mut preferred = None;
        let mut attempts = 0_usize;
        while attempts < candidates.len().saturating_add(2).min(5) {
            let broker_id = preferred
                .take()
                .or_else(|| {
                    candidates
                        .get(attempts.min(candidates.len().saturating_sub(1)))
                        .copied()
                })
                .ok_or(PublicError::BrokerNotAvailable)?;
            attempts += 1;
            let broker = state
                .broker(broker_id)
                .ok_or(PublicError::BrokerNotAvailable)?;
            let request_body = encode_private_request(operation, authority, &payload)
                .map_err(|_| PublicError::InvalidRequest)?;
            match send_private_request(
                broker.client_host.as_str(),
                broker.client_port,
                request_body,
            )
            .await
            {
                Ok((PublicError::None, reply)) => return Ok(reply),
                Ok((PublicError::NotLeader, reply)) => {
                    if reply.leader_broker_id != UNKNOWN_BROKER_ID
                        && reply.leader_broker_id != broker_id
                        && candidates.contains(&reply.leader_broker_id)
                    {
                        preferred = Some(reply.leader_broker_id);
                    }
                }
                Ok((PublicError::BrokerBusy, _)) => continue,
                Ok((PublicError::RequestTimeout, _)) if mutation && !idempotent_mutation => {
                    return Err(PublicError::RequestTimeout);
                }
                Ok((error, _)) => return Err(error),
                Err(sent) if sent && mutation && !idempotent_mutation => {
                    return Err(PublicError::RequestTimeout);
                }
                Err(_) => continue,
            }
        }
        Err(PublicError::BrokerNotAvailable)
    }

    fn member_join_response(
        &self,
        observation: &GroupCoordinatorObservation,
        group: &GroupRecordV1,
        member_id: MemberId,
    ) -> Result<JoinSubscribeResponseV0, P6Failure> {
        let member = group.members.get(&member_id).ok_or_else(|| {
            self.failure_with_group(
                PublicError::UnknownMember,
                observation,
                Some(group),
                Some(member_id),
            )
        })?;
        Ok(JoinSubscribeResponseV0 {
            hint: self.hint(observation, Some(group), Some(member_id)),
            member_id: member_id.as_bytes(),
            group_phase: group.phase as u8,
            heartbeat_interval_ms: heartbeat_interval_ms(member.session_timeout_ms)
                .map_err(|_| P6Failure::unknown(PublicError::UnknownServerError))?,
            active_assignments: assignments_for(group, member_id),
            revocations: revocations_for(group, member_id),
            tags: Vec::new(),
        })
    }

    fn member_state_response(
        &self,
        observation: &GroupCoordinatorObservation,
        group: &GroupRecordV1,
        member_id: MemberId,
    ) -> Result<HeartbeatResponseV0, P6Failure> {
        let member = group.members.get(&member_id).ok_or_else(|| {
            self.failure_with_group(
                PublicError::UnknownMember,
                observation,
                Some(group),
                Some(member_id),
            )
        })?;
        Ok(HeartbeatResponseV0 {
            hint: self.hint(observation, Some(group), Some(member_id)),
            group_phase: group.phase as u8,
            heartbeat_interval_ms: heartbeat_interval_ms(member.session_timeout_ms)
                .map_err(|_| P6Failure::unknown(PublicError::UnknownServerError))?,
            active_assignments: assignments_for(group, member_id),
            revocations: revocations_for(group, member_id),
            tags: Vec::new(),
        })
    }

    fn hint(
        &self,
        observation: &GroupCoordinatorObservation,
        group: Option<&GroupRecordV1>,
        member_id: Option<MemberId>,
    ) -> CoordinatorHintV0 {
        CoordinatorHintV0 {
            coordinator_broker_id: observation.hint.coordinator_broker_id,
            coordinator_epoch: observation.hint.coordinator_epoch,
            current_group_epoch: group.map(|group| group.active_group_epoch).unwrap_or(0),
            current_member_epoch: member_id
                .and_then(|member_id| group.and_then(|group| group.members.get(&member_id)))
                .map(|member| member.member_epoch.get())
                .unwrap_or(0),
        }
    }

    fn failure_with_group(
        &self,
        error: PublicError,
        observation: &GroupCoordinatorObservation,
        group: Option<&GroupRecordV1>,
        member_id: Option<MemberId>,
    ) -> P6Failure {
        P6Failure::new(error, self.hint(observation, group, member_id))
    }

    fn group_state_failure(
        &self,
        error: GroupStateError,
        observation: &GroupCoordinatorObservation,
        group: Option<&GroupRecordV1>,
        member_id: Option<MemberId>,
    ) -> P6Failure {
        let public = error.public_error().unwrap_or(PublicError::InvalidRequest);
        self.failure_with_group(public, observation, group, member_id)
    }

    fn session_failure(
        &self,
        error: SessionError,
        observation: &GroupCoordinatorObservation,
        group: &GroupRecordV1,
        member_id: MemberId,
    ) -> P6Failure {
        let public = match error {
            SessionError::Group(error) => {
                error.public_error().unwrap_or(PublicError::InvalidRequest)
            }
            SessionError::StaleTimeoutCandidate => PublicError::RebalanceInProgress,
            _ => PublicError::InvalidRequest,
        };
        self.failure_with_group(public, observation, Some(group), Some(member_id))
    }

    fn cooperative_failure(
        &self,
        error: CooperativeError,
        observation: &GroupCoordinatorObservation,
        group: Option<&GroupRecordV1>,
        member_id: Option<MemberId>,
    ) -> P6Failure {
        let public = match error {
            CooperativeError::Group(error) => {
                error.public_error().unwrap_or(PublicError::InvalidRequest)
            }
            CooperativeError::InvalidRevokeAck(_) | CooperativeError::NonCanonicalRevokeAck => {
                PublicError::InvalidRequest
            }
            CooperativeError::WrongPhase(_)
            | CooperativeError::OutstandingOwnerFence
            | CooperativeError::NoReadyActivation => PublicError::RebalanceInProgress,
            _ => PublicError::UnknownServerError,
        };
        self.failure_with_group(public, observation, group, member_id)
    }

    fn group_runtime_failure(
        &self,
        error: GroupCoordinatorError,
        observation: &GroupCoordinatorObservation,
        group: Option<&GroupRecordV1>,
        member_id: Option<MemberId>,
    ) -> P6Failure {
        let public = match error {
            GroupCoordinatorError::NotCoordinator { .. } => PublicError::NotLeader,
            GroupCoordinatorError::MutationAmbiguous { .. } => PublicError::RequestTimeout,
            GroupCoordinatorError::Busy => PublicError::BrokerBusy,
            GroupCoordinatorError::Replication(error) => error,
            GroupCoordinatorError::NotReady(_) => PublicError::BrokerNotAvailable,
            _ => PublicError::UnknownServerError,
        };
        self.failure_with_group(public, observation, group, member_id)
    }

    fn now_ms(&self) -> u64 {
        u64::try_from(self.started_at.elapsed().as_millis()).unwrap_or(u64::MAX)
    }
}

/// A Role::Leader observation alone is insufficient for committed group replay:
/// Raft publishes that role before the new term's leader barrier is committed.
/// Without this gate an empty pre-barrier fetch can be cached as a complete
/// recovery for the entire term, leaving surviving members without timers.
fn committed_local_group_leadership(
    status: PartitionStatus,
    local_broker_id: u32,
    expected_epoch: Option<u64>,
) -> bool {
    status.role == Role::Leader
        && status.leader_broker_id == Some(local_broker_id)
        && status.leader_epoch != 0
        && status.current_term_committed
        && expected_epoch.is_none_or(|epoch| epoch == status.leader_epoch)
}

fn validate_internal_topics(state: &ControllerState) -> Result<(), ()> {
    for descriptor in P6_INTERNAL_TOPICS {
        let topic = state.topic_by_name(descriptor.name).ok_or(())?;
        if topic.partition_count != descriptor.partition_count
            || topic.replication_factor != descriptor.replication_factor
            || topic.lifecycle != TopicLifecycle::Active
        {
            return Err(());
        }
        for partition in 0..topic.partition_count {
            let assignment = state
                .assignments()
                .get(&(topic.topic_id, partition))
                .ok_or(())?;
            if assignment.voters.len() != usize::from(P6_INTERNAL_TOPIC_REPLICATION_FACTOR) {
                return Err(());
            }
        }
    }
    Ok(())
}

fn topic_partition_counts(
    state: &ControllerState,
    members: &BTreeMap<MemberId, astralog_groups::MemberRecordV1>,
) -> Result<BTreeMap<astralog_common::TopicId, u32>, PublicError> {
    topic_partition_counts_for_subscriptions(
        state,
        members
            .values()
            .flat_map(|member| member.subscriptions.iter())
            .copied(),
    )
}

fn topic_partition_counts_for_subscriptions(
    state: &ControllerState,
    subscriptions: impl Iterator<Item = astralog_common::TopicId>,
) -> Result<BTreeMap<astralog_common::TopicId, u32>, PublicError> {
    let subscriptions = subscriptions.collect::<BTreeSet<_>>();
    let mut result = BTreeMap::new();
    for topic_id in subscriptions {
        let topic = state
            .topics()
            .get(&topic_id)
            .ok_or(PublicError::UnknownTopic)?;
        if topic.lifecycle != TopicLifecycle::Active {
            return Err(PublicError::UnknownTopic);
        }
        result.insert(topic_id, topic.partition_count);
    }
    Ok(result)
}

fn validate_authority(
    group: &GroupRecordV1,
    authority: &MemberAuthorityV0,
) -> Result<(), GroupStateError> {
    if group.group_id != authority.group_id {
        return Err(GroupStateError::WrongGroup);
    }
    if group.active_group_epoch != authority.group_epoch.get() {
        return Err(GroupStateError::GroupEpochStale {
            expected: authority.group_epoch.get(),
            actual: group.active_group_epoch,
        });
    }
    let member = group
        .members
        .get(&authority.member_id)
        .ok_or(GroupStateError::UnknownMember)?;
    if member.member_epoch != authority.member_epoch {
        return Err(GroupStateError::MemberEpochStale {
            expected: authority.member_epoch.get(),
            actual: member.member_epoch.get(),
        });
    }
    Ok(())
}

fn allocate_member_id(group: Option<&GroupRecordV1>) -> Result<MemberId, getrandom::Error> {
    loop {
        let mut bytes = [0_u8; 16];
        getrandom::fill(&mut bytes)?;
        if bytes == [0; 16] {
            continue;
        }
        if let Ok(member_id) = MemberId::new(bytes)
            && !group.is_some_and(|group| group.members.contains_key(&member_id))
        {
            return Ok(member_id);
        }
    }
}

fn assignments_for(group: &GroupRecordV1, member_id: MemberId) -> Vec<TopicPartitionV0> {
    group
        .active_assignments
        .iter()
        .filter_map(|(partition, owner)| (*owner == member_id).then_some(*partition))
        .collect()
}

fn revocations_for(group: &GroupRecordV1, member_id: MemberId) -> Vec<TopicPartitionV0> {
    group
        .pending_revocations
        .iter()
        .filter_map(|(partition, owner)| (*owner == member_id).then_some(*partition))
        .collect()
}

fn member_id_from_bytes(bytes: [u8; 16]) -> Result<MemberId, ()> {
    MemberId::new(bytes).map_err(|_| ())
}

fn local_broker_authority(
    state: &ControllerState,
    broker_id: u32,
) -> Result<BrokerAuthority, PublicError> {
    let broker = state
        .broker(broker_id)
        .ok_or(PublicError::BrokerNotAvailable)?;
    if matches!(
        broker.lifecycle,
        BrokerLifecycle::Fenced | BrokerLifecycle::Removed
    ) {
        return Err(PublicError::BrokerNotAvailable);
    }
    Ok(BrokerAuthority {
        broker_id,
        node_uuid: broker.node_uuid,
        incarnation_epoch: broker.incarnation_epoch,
    })
}

fn unknown_observation(epoch: u64, broker_id: u32) -> GroupCoordinatorObservation {
    GroupCoordinatorObservation {
        shard: 0,
        groups_topic_id: astralog_common::TopicId::new([0; 16]),
        hint: CoordinatorHintV0 {
            coordinator_broker_id: broker_id,
            coordinator_epoch: epoch,
            current_group_epoch: 0,
            current_member_epoch: 0,
        },
    }
}

fn encode_private_request(
    operation: u8,
    authority: BrokerAuthority,
    payload: &[u8],
) -> astralog_protocol::Result<Vec<u8>> {
    let mut writer = Writer::new();
    writer.put_u8(operation);
    writer.put_u32(authority.broker_id);
    writer.put_uuid(&authority.node_uuid);
    writer.put_u64(authority.incarnation_epoch);
    writer.put_bytes32(payload, PRIVATE_MAX_BODY, "private_p6_payload")?;
    Ok(writer.into_inner())
}

fn decode_private_request(
    reader: &mut Reader<'_>,
) -> astralog_protocol::Result<(u8, BrokerAuthority, Vec<u8>)> {
    let operation = reader.read_u8()?;
    let authority = BrokerAuthority {
        broker_id: reader.read_u32()?,
        node_uuid: reader.read_uuid()?,
        incarnation_epoch: reader.read_u64()?,
    };
    let payload = reader.read_bytes32(PRIVATE_MAX_BODY, "private_p6_payload")?;
    Ok((operation, authority, payload))
}

fn private_payload_group(operation: u8, payload: &[u8]) -> Result<GroupId, PublicError> {
    match operation {
        PRIVATE_OP_GROUP_FENCE | PRIVATE_OP_OWNER_FENCE => {
            match InternalRecordV1::decode(payload).map_err(|_| PublicError::InvalidRequest)? {
                InternalRecordV1::OffsetGroupFence(fence) => Ok(fence.group_id),
                InternalRecordV1::OffsetOwnerFence(fence) => Ok(fence.group_id),
                _ => Err(PublicError::InvalidRequest),
            }
        }
        PRIVATE_OP_COMMIT_OFFSETS => {
            let mut reader = Reader::new(payload);
            let group_bytes = reader
                .read_bytes32(PRIVATE_MAX_BODY, "private_group_record")
                .map_err(|_| PublicError::InvalidRequest)?;
            match InternalRecordV1::decode(&group_bytes).map_err(|_| PublicError::InvalidRequest)? {
                InternalRecordV1::GroupState(group) => Ok(group.group_id),
                _ => Err(PublicError::InvalidRequest),
            }
        }
        PRIVATE_OP_FETCH_OFFSETS => FetchOffsetsRequestV0::decode_body(payload)
            .map(|request| request.group_id)
            .map_err(|_| PublicError::InvalidRequest),
        PRIVATE_OP_RECOVER_FENCES => {
            let mut reader = Reader::new(payload);
            GroupId::new(
                reader
                    .read_string16(MAX_GROUP_ID_LEN, "group_id")
                    .map_err(|_| PublicError::InvalidRequest)?,
            )
            .map_err(|_| PublicError::InvalidRequest)
        }
        _ => Err(PublicError::InvalidRequest),
    }
}

fn encode_current_fences(
    state: &astralog_groups::OffsetStateMachine,
) -> astralog_protocol::Result<Vec<u8>> {
    let mut writer = Writer::new();
    writer.put_bool(state.group_fence().is_some());
    if let Some(fence) = state.group_fence() {
        let encoded = InternalRecordV1::OffsetGroupFence(fence.clone())
            .encode()
            .map_err(|_| astralog_protocol::ProtocolError::InvalidFrame("invalid group fence"))?;
        writer.put_bytes32(&encoded, PRIVATE_MAX_BODY, "private_group_fence")?;
    }
    writer.put_u32(
        u32::try_from(state.owner_fences().len())
            .map_err(|_| astralog_protocol::ProtocolError::LengthOverflow)?,
    );
    for fence in state.owner_fences().values() {
        let encoded = InternalRecordV1::OffsetOwnerFence(fence.clone())
            .encode()
            .map_err(|_| astralog_protocol::ProtocolError::InvalidFrame("invalid owner fence"))?;
        writer.put_bytes32(&encoded, PRIVATE_MAX_BODY, "private_owner_fence")?;
    }
    Ok(writer.into_inner())
}

fn decode_current_fences(bytes: &[u8]) -> astralog_protocol::Result<CurrentFences> {
    let mut reader = Reader::new(bytes);
    let group = if reader.read_bool()? {
        let encoded = reader.read_bytes32(PRIVATE_MAX_BODY, "private_group_fence")?;
        match InternalRecordV1::decode(&encoded)
            .map_err(|_| astralog_protocol::ProtocolError::InvalidFrame("invalid group fence"))?
        {
            InternalRecordV1::OffsetGroupFence(fence) => Some(fence),
            _ => {
                return Err(astralog_protocol::ProtocolError::InvalidFrame(
                    "wrong group fence kind",
                ));
            }
        }
    } else {
        None
    };
    let count = usize::try_from(reader.read_u32()?)
        .map_err(|_| astralog_protocol::ProtocolError::LengthOverflow)?;
    if count > astralog_protocol::api::MAX_GROUP_PARTITIONS {
        return Err(astralog_protocol::ProtocolError::InvalidFrame(
            "too many owner fences",
        ));
    }
    let mut owners = Vec::with_capacity(count);
    for _ in 0..count {
        let encoded = reader.read_bytes32(PRIVATE_MAX_BODY, "private_owner_fence")?;
        match InternalRecordV1::decode(&encoded)
            .map_err(|_| astralog_protocol::ProtocolError::InvalidFrame("invalid owner fence"))?
        {
            InternalRecordV1::OffsetOwnerFence(fence) => owners.push(fence),
            _ => {
                return Err(astralog_protocol::ProtocolError::InvalidFrame(
                    "wrong owner fence kind",
                ));
            }
        }
    }
    if !reader.is_empty() {
        return Err(astralog_protocol::ProtocolError::InvalidFrame(
            "trailing private P6 fence bytes",
        ));
    }
    Ok(CurrentFences { group, owners })
}

fn private_success_response(
    request: &WireFrame,
    leader_broker_id: u32,
    leader_epoch: u64,
    payload: Vec<u8>,
) -> WireFrame {
    private_response(
        request,
        PublicError::None,
        leader_broker_id,
        leader_epoch,
        payload,
    )
}

fn private_error_response(
    request: &WireFrame,
    error: PublicError,
    leader_broker_id: u32,
    leader_epoch: u64,
) -> WireFrame {
    private_response(request, error, leader_broker_id, leader_epoch, Vec::new())
}

fn private_response(
    request: &WireFrame,
    error: PublicError,
    leader_broker_id: u32,
    leader_epoch: u64,
    payload: Vec<u8>,
) -> WireFrame {
    let mut writer = Writer::new();
    writer.put_u32(leader_broker_id);
    writer.put_u64(leader_epoch);
    let _ = writer.put_bytes32(&payload, PRIVATE_MAX_BODY, "private_p6_reply");
    WireFrame {
        kind: FrameKind::Response,
        flags: 0,
        api_key: request.api_key,
        api_version: request.api_version,
        error_code: error as u16,
        correlation_id: request.correlation_id,
        timeout_ms: 0,
        throttle_ms: 0,
        header_tags: Vec::new(),
        body: writer.into_inner(),
    }
}

async fn send_private_request(
    host: &str,
    port: u16,
    body: Vec<u8>,
) -> Result<(PublicError, PrivateOffsetReply), bool> {
    let mut stream = time::timeout(PRIVATE_CONNECT_TIMEOUT, TcpStream::connect((host, port)))
        .await
        .map_err(|_| false)?
        .map_err(|_| false)?;
    stream.set_nodelay(true).map_err(|_| false)?;
    let hello = ClientHello::v1().encode().map_err(|_| false)?;
    time::timeout(PRIVATE_CONNECT_TIMEOUT, stream.write_all(&hello))
        .await
        .map_err(|_| false)?
        .map_err(|_| false)?;
    let mut hello_response = [0_u8; 16];
    time::timeout(
        PRIVATE_CONNECT_TIMEOUT,
        stream.read_exact(&mut hello_response),
    )
    .await
    .map_err(|_| false)?
    .map_err(|_| false)?;
    let server = ServerHello::decode(&hello_response).map_err(|_| false)?;
    if server.status != HelloStatus::Ok || server.selected_wire_version != WIRE_VERSION {
        return Err(false);
    }
    let request = WireFrame {
        kind: FrameKind::Request,
        flags: 0,
        api_key: PRIVATE_P6_OFFSET_API,
        api_version: 0,
        error_code: 0,
        correlation_id: 1,
        timeout_ms: 5_000,
        throttle_ms: 0,
        header_tags: Vec::new(),
        body,
    };
    let encoded = encode_frame(&request, MAX_WIRE_FRAME_LEN_V1).map_err(|_| false)?;
    time::timeout(PRIVATE_REQUEST_TIMEOUT, stream.write_all(&encoded))
        .await
        .map_err(|_| false)?
        .map_err(|_| false)?;
    let response = read_private_frame(&mut stream).await.map_err(|_| true)?;
    if response.kind != FrameKind::Response
        || response.api_key != PRIVATE_P6_OFFSET_API
        || response.api_version != 0
        || response.correlation_id != 1
    {
        return Err(true);
    }
    let error = PublicError::try_from(response.error_code).map_err(|_| true)?;
    let mut reader = Reader::new(&response.body);
    let leader_broker_id = reader.read_u32().map_err(|_| true)?;
    let _leader_epoch = reader.read_u64().map_err(|_| true)?;
    let payload = reader
        .read_bytes32(PRIVATE_MAX_BODY, "private_p6_reply")
        .map_err(|_| true)?
        .to_vec();
    if !reader.is_empty() {
        return Err(true);
    }
    Ok((
        error,
        PrivateOffsetReply {
            leader_broker_id,
            payload,
        },
    ))
}

async fn read_private_frame(stream: &mut TcpStream) -> astralog_protocol::Result<WireFrame> {
    let mut decoder = FrameDecoder::new(MAX_WIRE_FRAME_LEN_V1, MAX_WIRE_FRAME_LEN_V1 + 4)?;
    let deadline = time::Instant::now() + PRIVATE_REQUEST_TIMEOUT;
    let mut buffer = [0_u8; 8192];
    loop {
        let read = time::timeout_at(deadline, stream.read(&mut buffer))
            .await
            .map_err(|_| astralog_protocol::ProtocolError::InvalidFrame("private request timeout"))?
            .map_err(|_| astralog_protocol::ProtocolError::InvalidFrame("private request I/O"))?;
        if read == 0 {
            return Err(astralog_protocol::ProtocolError::InvalidFrame(
                "private connection closed",
            ));
        }
        let mut frames = decoder.push(&buffer[..read])?;
        if let Some(frame) = frames.pop() {
            return Ok(frame);
        }
    }
}

fn offset_error(error: OffsetCoordinatorError) -> (PublicError, u32, u64) {
    match error {
        OffsetCoordinatorError::NotLeader {
            leader_broker_id,
            leader_epoch,
        } => (PublicError::NotLeader, leader_broker_id, leader_epoch),
        OffsetCoordinatorError::MutationAmbiguous {
            leader_broker_id,
            leader_epoch,
            ..
        } => (PublicError::RequestTimeout, leader_broker_id, leader_epoch),
        OffsetCoordinatorError::Validation(error) => (error, UNKNOWN_BROKER_ID, UNKNOWN_EPOCH),
        OffsetCoordinatorError::Busy => (PublicError::BrokerBusy, UNKNOWN_BROKER_ID, UNKNOWN_EPOCH),
        OffsetCoordinatorError::Replication(error) => (error, UNKNOWN_BROKER_ID, UNKNOWN_EPOCH),
        OffsetCoordinatorError::NotReady(_) => (
            PublicError::BrokerNotAvailable,
            UNKNOWN_BROKER_ID,
            UNKNOWN_EPOCH,
        ),
        _ => (
            PublicError::UnknownServerError,
            UNKNOWN_BROKER_ID,
            UNKNOWN_EPOCH,
        ),
    }
}

#[cfg(test)]
mod leadership_recovery_tests {
    use super::*;

    #[test]
    fn pre_barrier_leader_does_not_install_or_cache_session_grace() {
        let pre_barrier = PartitionStatus {
            role: Role::Leader,
            leader_broker_id: Some(2),
            leader_epoch: 7,
            current_term_committed: false,
            log_start_offset: 0,
            high_watermark: 0,
        };
        // The exact state Raft exposes between election and barrier commit.
        assert!(!committed_local_group_leadership(pre_barrier, 2, None));
        assert!(!committed_local_group_leadership(pre_barrier, 2, Some(7)));

        let ready = PartitionStatus {
            current_term_committed: true,
            ..pre_barrier
        };
        assert!(committed_local_group_leadership(ready, 2, None));
        assert!(committed_local_group_leadership(ready, 2, Some(7)));
        assert!(!committed_local_group_leadership(ready, 2, Some(6)));
        assert!(!committed_local_group_leadership(ready, 3, None));
        assert!(!committed_local_group_leadership(
            PartitionStatus {
                role: Role::Follower,
                ..ready
            },
            2,
            None,
        ));
    }
}
