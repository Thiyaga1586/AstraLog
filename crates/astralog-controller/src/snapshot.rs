use std::collections::BTreeMap;

use astralog_common::TopicId;
use astralog_protocol::api::{MAX_HOST_LEN, MAX_TOPIC_NAME_LEN};

use crate::state::SnapshotStateParts;
use crate::{
    AssignmentGeneration, AssignmentRecord, BrokerLifecycle, BrokerRecord, ControllerError,
    ControllerState, FailureDomainPolicy, MovementId, MovementRecord, MovementStage, Result,
    TopicLifecycle, TopicRecord,
};

const SNAPSHOT_VERSION: u16 = 1;
const SNAPSHOT_HEADER_LEN: u16 = 40;

pub const MAX_CONTROLLER_BROKERS: usize = 100_000;
pub const MAX_CONTROLLER_TOPICS: usize = 100_000;
pub const MAX_CONTROLLER_ASSIGNMENTS: usize = 1_000_000;
pub const MAX_CONTROLLER_MOVEMENTS: usize = 100_000;
pub const MAX_CONTROLLER_SNAPSHOT_BYTES: usize = 64 * 1024 * 1024;

const MAX_BROKER_RECORD_BYTES: usize = 813;
const MAX_TOPIC_RECORD_BYTES: usize = 275;
const MAX_ASSIGNMENT_RECORD_BYTES: usize = 76;
const MAX_MOVEMENT_RECORD_BYTES: usize = 82;
const SNAPSHOT_FIXED_BYTES: usize = SNAPSHOT_HEADER_LEN as usize + 4 * 4 + 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotLimits {
    pub max_snapshot_bytes: usize,
    pub max_brokers: usize,
    pub max_topics: usize,
    pub max_assignments: usize,
    pub max_movements: usize,
    pub max_replicas_per_assignment: usize,
    pub max_host_bytes: usize,
    pub max_label_bytes: usize,
}

impl Default for SnapshotLimits {
    fn default() -> Self {
        Self {
            max_snapshot_bytes: MAX_CONTROLLER_SNAPSHOT_BYTES,
            max_brokers: MAX_CONTROLLER_BROKERS,
            max_topics: MAX_CONTROLLER_TOPICS,
            max_assignments: MAX_CONTROLLER_ASSIGNMENTS,
            max_movements: MAX_CONTROLLER_MOVEMENTS,
            max_replicas_per_assignment: 5,
            max_host_bytes: MAX_HOST_LEN,
            max_label_bytes: 128,
        }
    }
}

pub(crate) fn canonical_snapshot_worst_case_bytes(
    brokers: usize,
    topics: usize,
    assignments: usize,
    movements: usize,
) -> Option<usize> {
    let broker_bytes = brokers.checked_mul(MAX_BROKER_RECORD_BYTES)?;
    let topic_bytes = topics.checked_mul(MAX_TOPIC_RECORD_BYTES)?;
    let assignment_bytes = assignments.checked_mul(MAX_ASSIGNMENT_RECORD_BYTES)?;
    let movement_bytes = movements.checked_mul(MAX_MOVEMENT_RECORD_BYTES)?;
    SNAPSHOT_FIXED_BYTES
        .checked_add(broker_bytes)?
        .checked_add(topic_bytes)?
        .checked_add(assignment_bytes)?
        .checked_add(movement_bytes)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControllerSnapshot {
    pub state: ControllerState,
}

impl ControllerSnapshot {
    pub fn new(state: ControllerState) -> Self {
        Self { state }
    }
}

pub fn encode_controller_snapshot(
    snapshot: &ControllerSnapshot,
    limits: SnapshotLimits,
) -> Result<Vec<u8>> {
    let state = &snapshot.state;
    validate_counts(state, limits)?;
    let mut out = Vec::new();
    put_u16(&mut out, SNAPSHOT_VERSION);
    put_u16(&mut out, SNAPSHOT_HEADER_LEN);
    put_u64(&mut out, state.metadata_epoch());
    out.extend_from_slice(state.cluster_id());
    put_u16(&mut out, state.active_feature_level());
    put_u16(&mut out, 0);
    put_u64(&mut out, state.next_unallocated_producer_id());

    put_u32_len(&mut out, state.brokers().len(), "broker count")?;
    for broker in state.brokers().values() {
        encode_broker(&mut out, broker, limits)?;
    }

    put_u32_len(&mut out, state.topics().len(), "topic count")?;
    for topic in state.topics().values() {
        encode_topic(&mut out, topic)?;
    }

    put_u32_len(&mut out, state.assignments().len(), "assignment count")?;
    for assignment in state.assignments().values() {
        encode_assignment(&mut out, assignment, limits)?;
    }

    put_u32_len(&mut out, state.movements().len(), "movement count")?;
    for movement in state.movements().values() {
        encode_movement(&mut out, movement, limits)?;
    }

    if out.len().saturating_add(4) > limits.max_snapshot_bytes {
        return Err(ControllerError::LimitExceeded {
            what: "controller snapshot bytes",
            actual: out.len().saturating_add(4),
            max: limits.max_snapshot_bytes,
        });
    }
    let crc = crc32c::crc32c(&out);
    put_u32(&mut out, crc);
    Ok(out)
}

pub fn decode_controller_snapshot(
    bytes: &[u8],
    limits: SnapshotLimits,
) -> Result<ControllerSnapshot> {
    if bytes.len() > limits.max_snapshot_bytes {
        return Err(ControllerError::LimitExceeded {
            what: "controller snapshot bytes",
            actual: bytes.len(),
            max: limits.max_snapshot_bytes,
        });
    }
    if bytes.len() < usize::from(SNAPSHOT_HEADER_LEN) + 4 {
        return Err(ControllerError::Invalid("controller snapshot is truncated"));
    }
    let expected_crc = u32::from_le_bytes(
        bytes[bytes.len() - 4..]
            .try_into()
            .expect("four-byte CRC slice"),
    );
    let actual_crc = crc32c::crc32c(&bytes[..bytes.len() - 4]);
    if expected_crc != actual_crc {
        return Err(ControllerError::ChecksumMismatch {
            context: "controller snapshot",
            expected: expected_crc,
            actual: actual_crc,
        });
    }

    let mut reader = Reader::new(&bytes[..bytes.len() - 4]);
    if reader.u16()? != SNAPSHOT_VERSION || reader.u16()? != SNAPSHOT_HEADER_LEN {
        return Err(ControllerError::Invalid(
            "controller snapshot version/header mismatch",
        ));
    }
    let metadata_epoch = reader.u64()?;
    let cluster_id = reader.array16()?;
    let active_feature_level = reader.u16()?;
    if reader.u16()? != 0 {
        return Err(ControllerError::Invalid(
            "controller snapshot reserved field is non-zero",
        ));
    }
    let next_unallocated_producer_id = reader.u64()?;

    let broker_count = reader.bounded_count(limits.max_brokers, "broker count")?;
    let mut brokers = BTreeMap::new();
    for _ in 0..broker_count {
        let broker = decode_broker(&mut reader, limits)?;
        if brokers.insert(broker.broker_id, broker).is_some() {
            return Err(ControllerError::Invalid(
                "duplicate broker id in controller snapshot",
            ));
        }
    }

    let topic_count = reader.bounded_count(limits.max_topics, "topic count")?;
    let mut topics = BTreeMap::new();
    for _ in 0..topic_count {
        let topic = decode_topic(&mut reader)?;
        if topics.insert(topic.topic_id, topic).is_some() {
            return Err(ControllerError::Invalid(
                "duplicate topic id in controller snapshot",
            ));
        }
    }

    let assignment_count = reader.bounded_count(limits.max_assignments, "assignment count")?;
    let mut assignments = BTreeMap::new();
    for _ in 0..assignment_count {
        let assignment = decode_assignment(&mut reader, limits)?;
        let key = (assignment.topic_id, assignment.partition_id);
        if assignments.insert(key, assignment).is_some() {
            return Err(ControllerError::Invalid(
                "duplicate assignment in controller snapshot",
            ));
        }
    }

    let movement_count = reader.bounded_count(limits.max_movements, "movement count")?;
    let mut movements = BTreeMap::new();
    for _ in 0..movement_count {
        let movement = decode_movement(&mut reader, limits)?;
        if movements.insert(movement.movement_id, movement).is_some() {
            return Err(ControllerError::Invalid(
                "duplicate movement id in controller snapshot",
            ));
        }
    }
    if !reader.is_empty() {
        return Err(ControllerError::Invalid(
            "trailing bytes in controller snapshot",
        ));
    }

    let state = ControllerState::from_snapshot_parts(SnapshotStateParts {
        cluster_id,
        active_feature_level,
        metadata_epoch,
        next_unallocated_producer_id,
        brokers,
        topics,
        assignments,
        movements,
    })?;
    let snapshot = ControllerSnapshot::new(state);
    if encode_controller_snapshot(&snapshot, limits)? != bytes {
        return Err(ControllerError::Invalid(
            "controller snapshot is not canonical",
        ));
    }
    Ok(snapshot)
}

fn validate_counts(state: &ControllerState, limits: SnapshotLimits) -> Result<()> {
    for (what, actual, max) in [
        ("broker count", state.brokers().len(), limits.max_brokers),
        ("topic count", state.topics().len(), limits.max_topics),
        (
            "assignment count",
            state.assignments().len(),
            limits.max_assignments,
        ),
        (
            "movement count",
            state.movements().len(),
            limits.max_movements,
        ),
    ] {
        if actual > max {
            return Err(ControllerError::LimitExceeded { what, actual, max });
        }
    }
    Ok(())
}

fn encode_broker(out: &mut Vec<u8>, broker: &BrokerRecord, limits: SnapshotLimits) -> Result<()> {
    put_u32(out, broker.broker_id);
    out.extend_from_slice(&broker.node_uuid);
    put_u64(out, broker.incarnation_epoch);
    out.push(broker.lifecycle as u8);
    put_string16(
        out,
        &broker.client_host,
        limits.max_host_bytes,
        "client host",
    )?;
    put_u16(out, broker.client_port);
    put_string16(
        out,
        &broker.internal_host,
        limits.max_host_bytes,
        "internal host",
    )?;
    put_u16(out, broker.internal_port);
    put_optional_string(out, broker.rack.as_deref(), limits.max_label_bytes, "rack")?;
    put_optional_string(out, broker.zone.as_deref(), limits.max_label_bytes, "zone")?;
    put_u16(out, broker.min_feature);
    put_u16(out, broker.max_feature);
    Ok(())
}

fn decode_broker(reader: &mut Reader<'_>, limits: SnapshotLimits) -> Result<BrokerRecord> {
    let broker_id = reader.u32()?;
    Ok(BrokerRecord {
        broker_id,
        node_uuid: reader.array16()?,
        incarnation_epoch: reader.u64()?,
        lifecycle: decode_broker_lifecycle(reader.u8()?)?,
        client_host: reader.string16(limits.max_host_bytes, "client host")?,
        client_port: reader.u16()?,
        internal_host: reader.string16(limits.max_host_bytes, "internal host")?,
        internal_port: reader.u16()?,
        rack: reader.optional_string(limits.max_label_bytes, "rack")?,
        zone: reader.optional_string(limits.max_label_bytes, "zone")?,
        min_feature: reader.u16()?,
        max_feature: reader.u16()?,
    })
}

fn encode_topic(out: &mut Vec<u8>, topic: &TopicRecord) -> Result<()> {
    out.extend_from_slice(topic.topic_id.as_bytes());
    put_string16(out, &topic.topic_name, MAX_TOPIC_NAME_LEN, "topic name")?;
    put_u32(out, topic.partition_count);
    put_u16(out, topic.replication_factor);
    out.push(topic.lifecycle as u8);
    out.push(topic.failure_domain_policy as u8);
    Ok(())
}

fn decode_topic(reader: &mut Reader<'_>) -> Result<TopicRecord> {
    Ok(TopicRecord {
        topic_id: TopicId::new(reader.array16()?),
        topic_name: reader.string16(MAX_TOPIC_NAME_LEN, "topic name")?,
        partition_count: reader.u32()?,
        replication_factor: reader.u16()?,
        lifecycle: decode_topic_lifecycle(reader.u8()?)?,
        failure_domain_policy: decode_failure_domain_policy(reader.u8()?)?,
    })
}

fn encode_assignment(
    out: &mut Vec<u8>,
    assignment: &AssignmentRecord,
    limits: SnapshotLimits,
) -> Result<()> {
    out.extend_from_slice(assignment.topic_id.as_bytes());
    put_u32(out, assignment.partition_id);
    put_u64(out, assignment.assignment_generation.get());
    put_broker_array(out, &assignment.voters, limits)?;
    put_broker_array(out, &assignment.learners, limits)?;
    Ok(())
}

fn decode_assignment(reader: &mut Reader<'_>, limits: SnapshotLimits) -> Result<AssignmentRecord> {
    Ok(AssignmentRecord {
        topic_id: TopicId::new(reader.array16()?),
        partition_id: reader.u32()?,
        assignment_generation: AssignmentGeneration::new(reader.u64()?)?,
        voters: reader.broker_array(limits.max_replicas_per_assignment)?,
        learners: reader.broker_array(limits.max_replicas_per_assignment)?,
    })
}

fn encode_movement(
    out: &mut Vec<u8>,
    movement: &MovementRecord,
    limits: SnapshotLimits,
) -> Result<()> {
    out.extend_from_slice(movement.movement_id.as_bytes());
    out.extend_from_slice(movement.topic_id.as_bytes());
    put_u32(out, movement.partition_id);
    put_u64(out, movement.expected_assignment_generation.get());
    put_u32(out, movement.source_broker_id);
    put_u32(out, movement.destination_broker_id);
    out.push(movement.stage as u8);
    put_broker_array(out, &movement.target_voters, limits)?;
    match movement.preferred_leader {
        None => out.push(0),
        Some(value) => {
            out.push(1);
            put_u32(out, value);
        }
    }
    Ok(())
}

fn decode_movement(reader: &mut Reader<'_>, limits: SnapshotLimits) -> Result<MovementRecord> {
    Ok(MovementRecord {
        movement_id: MovementId::new(reader.array16()?)?,
        topic_id: TopicId::new(reader.array16()?),
        partition_id: reader.u32()?,
        expected_assignment_generation: AssignmentGeneration::new(reader.u64()?)?,
        source_broker_id: reader.u32()?,
        destination_broker_id: reader.u32()?,
        stage: decode_movement_stage(reader.u8()?)?,
        target_voters: reader.broker_array(limits.max_replicas_per_assignment)?,
        preferred_leader: match reader.u8()? {
            0 => None,
            1 => Some(reader.u32()?),
            _ => {
                return Err(ControllerError::Invalid(
                    "invalid preferred leader optional marker",
                ));
            }
        },
    })
}

fn put_broker_array(out: &mut Vec<u8>, values: &[u32], limits: SnapshotLimits) -> Result<()> {
    if values.len() > limits.max_replicas_per_assignment {
        return Err(ControllerError::LimitExceeded {
            what: "replica count",
            actual: values.len(),
            max: limits.max_replicas_per_assignment,
        });
    }
    put_u32_len(out, values.len(), "replica count")?;
    for value in values {
        put_u32(out, *value);
    }
    Ok(())
}

fn decode_broker_lifecycle(value: u8) -> Result<BrokerLifecycle> {
    match value {
        1 => Ok(BrokerLifecycle::Joining),
        2 => Ok(BrokerLifecycle::Active),
        3 => Ok(BrokerLifecycle::Draining),
        4 => Ok(BrokerLifecycle::Fenced),
        5 => Ok(BrokerLifecycle::Removed),
        _ => Err(ControllerError::Invalid("unknown broker lifecycle")),
    }
}

fn decode_topic_lifecycle(value: u8) -> Result<TopicLifecycle> {
    match value {
        1 => Ok(TopicLifecycle::Active),
        2 => Ok(TopicLifecycle::Deleting),
        _ => Err(ControllerError::Invalid("unknown topic lifecycle")),
    }
}

fn decode_failure_domain_policy(value: u8) -> Result<FailureDomainPolicy> {
    match value {
        0 => Ok(FailureDomainPolicy::Disabled),
        1 => Ok(FailureDomainPolicy::Preferred),
        2 => Ok(FailureDomainPolicy::Required),
        _ => Err(ControllerError::Invalid("unknown failure-domain policy")),
    }
}

fn decode_movement_stage(value: u8) -> Result<MovementStage> {
    match value {
        1 => Ok(MovementStage::Planned),
        2 => Ok(MovementStage::LearnerRequested),
        3 => Ok(MovementStage::LearnerCreated),
        4 => Ok(MovementStage::CatchingUp),
        5 => Ok(MovementStage::PromotionReady),
        6 => Ok(MovementStage::EnterJointRequested),
        7 => Ok(MovementStage::JointCommitted),
        8 => Ok(MovementStage::LeaveJointRequested),
        9 => Ok(MovementStage::NewStableCommitted),
        10 => Ok(MovementStage::LeadershipTransfer),
        11 => Ok(MovementStage::OldReplicaRemoveRequested),
        12 => Ok(MovementStage::OldReplicaRemoved),
        13 => Ok(MovementStage::Complete),
        _ => Err(ControllerError::Invalid("unknown movement stage")),
    }
}

fn put_string16(out: &mut Vec<u8>, value: &str, max: usize, what: &'static str) -> Result<()> {
    if value.len() > max {
        return Err(ControllerError::LimitExceeded {
            what,
            actual: value.len(),
            max,
        });
    }
    let len = u16::try_from(value.len())
        .map_err(|_| ControllerError::Invalid("string16 length overflow"))?;
    put_u16(out, len);
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn put_optional_string(
    out: &mut Vec<u8>,
    value: Option<&str>,
    max: usize,
    what: &'static str,
) -> Result<()> {
    match value {
        None => out.push(0),
        Some(value) => {
            out.push(1);
            put_string16(out, value, max, what)?;
        }
    }
    Ok(())
}

fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u32_len(out: &mut Vec<u8>, value: usize, what: &'static str) -> Result<()> {
    put_u32(
        out,
        u32::try_from(value)
            .map_err(|_| ControllerError::InvalidOwned(format!("{what} exceeds u32")))?,
    );
    Ok(())
}

struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn is_empty(&self) -> bool {
        self.position == self.bytes.len()
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(len)
            .ok_or(ControllerError::Invalid("snapshot length overflow"))?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or(ControllerError::Invalid("controller snapshot is truncated"))?;
        self.position = end;
        Ok(bytes)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(
            self.take(2)?.try_into().expect("two-byte slice"),
        ))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("four-byte slice"),
        ))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("eight-byte slice"),
        ))
    }

    fn array16(&mut self) -> Result<[u8; 16]> {
        Ok(self.take(16)?.try_into().expect("sixteen-byte slice"))
    }

    fn bounded_count(&mut self, max: usize, what: &'static str) -> Result<usize> {
        let count = usize::try_from(self.u32()?)
            .map_err(|_| ControllerError::Invalid("count conversion overflow"))?;
        if count > max {
            return Err(ControllerError::LimitExceeded {
                what,
                actual: count,
                max,
            });
        }
        Ok(count)
    }

    fn string16(&mut self, max: usize, what: &'static str) -> Result<String> {
        let len = usize::from(self.u16()?);
        if len > max {
            return Err(ControllerError::LimitExceeded {
                what,
                actual: len,
                max,
            });
        }
        let bytes = self.take(len)?;
        let text = core::str::from_utf8(bytes)
            .map_err(|_| ControllerError::Invalid("snapshot string is not UTF-8"))?;
        Ok(text.to_owned())
    }

    fn optional_string(&mut self, max: usize, what: &'static str) -> Result<Option<String>> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.string16(max, what)?)),
            _ => Err(ControllerError::Invalid(
                "invalid optional string presence marker",
            )),
        }
    }

    fn broker_array(&mut self, max: usize) -> Result<Vec<u32>> {
        let count = self.bounded_count(max, "replica count")?;
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            values.push(self.u32()?);
        }
        Ok(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ControllerCommand, encode_command};

    fn populated_state() -> ControllerState {
        let mut state = ControllerState::new([7; 16], 1, 1_000).unwrap();
        for (index, broker_id) in (1_u32..=3).enumerate() {
            let register = ControllerCommand::RegisterBroker {
                broker_id,
                node_uuid: [u8::try_from(broker_id).unwrap(); 16],
                initial_incarnation: 1,
                client_host: format!("broker-{broker_id}"),
                client_port: 9000 + u16::try_from(broker_id).unwrap(),
                internal_host: format!("broker-{broker_id}-internal"),
                internal_port: 10000 + u16::try_from(broker_id).unwrap(),
                rack: Some(format!("rack-{broker_id}")),
                zone: Some(format!("zone-{broker_id}")),
                min_feature: 1,
                max_feature: 3,
            };
            encode_command(&register, crate::CommandLimits::default()).unwrap();
            let base = u64::try_from(index).unwrap() * 2 + 1;
            state.apply(base, &register).unwrap();
            state
                .apply(
                    base + 1,
                    &ControllerCommand::SetBrokerLifecycle {
                        broker_id,
                        expected_incarnation: 1,
                        expected_lifecycle: BrokerLifecycle::Joining,
                        new_lifecycle: BrokerLifecycle::Active,
                    },
                )
                .unwrap();
        }
        let topic = TopicId::new([3; 16]);
        state
            .apply(
                7,
                &ControllerCommand::CreateTopic {
                    topic_id: topic,
                    topic_name: "orders".into(),
                    partition_count: 1,
                    replication_factor: 3,
                    failure_domain_policy: FailureDomainPolicy::Required,
                    assignments: vec![AssignmentRecord {
                        topic_id: topic,
                        partition_id: 0,
                        assignment_generation: AssignmentGeneration::new(1).unwrap(),
                        voters: vec![1, 2, 3],
                        learners: Vec::new(),
                    }],
                },
            )
            .unwrap();
        state
    }

    #[test]
    fn snapshot_round_trip_is_canonical_and_crc_protected() {
        let snapshot = ControllerSnapshot::new(populated_state());
        let bytes = encode_controller_snapshot(&snapshot, SnapshotLimits::default()).unwrap();
        assert_eq!(u16::from_le_bytes(bytes[0..2].try_into().unwrap()), 1);
        assert_eq!(u16::from_le_bytes(bytes[2..4].try_into().unwrap()), 40);
        let decoded = decode_controller_snapshot(&bytes, SnapshotLimits::default()).unwrap();
        assert_eq!(decoded, snapshot);

        let mut corrupt = bytes;
        corrupt[20] ^= 0x40;
        assert!(decode_controller_snapshot(&corrupt, SnapshotLimits::default()).is_err());
    }

    #[test]
    fn snapshot_decoder_rejects_trailing_duplicate_and_invalid_records() {
        let snapshot = ControllerSnapshot::new(populated_state());
        let mut bytes = encode_controller_snapshot(&snapshot, SnapshotLimits::default()).unwrap();
        let crc_offset = bytes.len() - 4;
        bytes.insert(crc_offset, 0);
        let new_crc = crc32c::crc32c(&bytes[..bytes.len() - 4]);
        let len = bytes.len();
        bytes[len - 4..].copy_from_slice(&new_crc.to_le_bytes());
        assert!(decode_controller_snapshot(&bytes, SnapshotLimits::default()).is_err());
    }
}
