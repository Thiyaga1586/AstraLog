use std::collections::BTreeMap;

use astralog_common::TopicId;

use crate::{
    CompressionPolicyV1, ControllerError, ControllerSnapshot, ControllerSnapshotV2,
    MAX_CONTROLLER_TOPICS, Result, SnapshotLimits, TopicPerformancePolicyV1,
    decode_controller_snapshot_v2, encode_controller_snapshot_v2,
};

const SNAPSHOT_VERSION_V3: u16 = 3;
const SNAPSHOT_HEADER_LEN_V3: u16 = 16;
const POLICY_RECORD_BYTES: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControllerSnapshotV3 {
    pub base: ControllerSnapshotV2,
    pub topic_performance: BTreeMap<TopicId, TopicPerformancePolicyV1>,
}

impl ControllerSnapshotV3 {
    pub fn from_state(state: crate::ControllerState) -> Self {
        Self {
            topic_performance: state.topic_performance_policies().clone(),
            base: ControllerSnapshotV2::from_state(state),
        }
    }

    pub fn into_controller_snapshot(self) -> Result<ControllerSnapshot> {
        let mut snapshot = self.base.into_controller_snapshot()?;
        snapshot
            .state
            .install_topic_performance_policies(self.topic_performance)?;
        Ok(snapshot)
    }
}

pub fn encode_controller_snapshot_v3(
    snapshot: &ControllerSnapshotV3,
    limits: SnapshotLimits,
) -> Result<Vec<u8>> {
    if snapshot.topic_performance.len() > MAX_CONTROLLER_TOPICS {
        return Err(ControllerError::LimitExceeded {
            what: "topic performance policy count",
            actual: snapshot.topic_performance.len(),
            max: MAX_CONTROLLER_TOPICS,
        });
    }
    let base_state = snapshot.base.clone().into_controller_snapshot()?;
    if snapshot.topic_performance.len() != base_state.state.topics().len()
        || base_state.state.topics().keys().any(|topic_id| {
            snapshot
                .topic_performance
                .get(topic_id)
                .is_none_or(|policy| policy.topic_id != *topic_id || policy.validate().is_err())
        })
    {
        return Err(ControllerError::Invalid(
            "P8 V3 policy table does not exactly cover snapshot topics",
        ));
    }
    let base = encode_controller_snapshot_v2(&snapshot.base, limits)?;
    let base_len = u32::try_from(base.len())
        .map_err(|_| ControllerError::Invalid("P8 V3 base snapshot length overflow"))?;
    let policy_count = u32::try_from(snapshot.topic_performance.len())
        .map_err(|_| ControllerError::Invalid("P8 V3 policy count overflow"))?;
    let total = usize::from(SNAPSHOT_HEADER_LEN_V3)
        .checked_add(base.len())
        .and_then(|value| {
            value.checked_add(
                snapshot
                    .topic_performance
                    .len()
                    .checked_mul(POLICY_RECORD_BYTES)?,
            )
        })
        .and_then(|value| value.checked_add(4))
        .ok_or(ControllerError::Invalid("P8 V3 snapshot length overflow"))?;
    if total > limits.max_snapshot_bytes {
        return Err(ControllerError::LimitExceeded {
            what: "controller snapshot bytes",
            actual: total,
            max: limits.max_snapshot_bytes,
        });
    }
    let mut out = Vec::with_capacity(total);
    put_u16(&mut out, SNAPSHOT_VERSION_V3);
    put_u16(&mut out, SNAPSHOT_HEADER_LEN_V3);
    put_u32(&mut out, base_len);
    put_u32(&mut out, policy_count);
    put_u32(&mut out, 0);
    out.extend_from_slice(&base);
    for (topic_id, policy) in &snapshot.topic_performance {
        out.extend_from_slice(topic_id.as_bytes());
        out.push(policy.compression as u8);
        out.extend_from_slice(&[0, 0, 0]);
        put_u32(&mut out, policy.max_record_bytes);
        put_u32(&mut out, policy.max_batch_records);
        put_u32(&mut out, policy.max_batch_uncompressed_bytes);
    }
    let crc = crc32c::crc32c(&out);
    put_u32(&mut out, crc);
    Ok(out)
}

pub fn decode_controller_snapshot_v3(
    bytes: &[u8],
    limits: SnapshotLimits,
) -> Result<ControllerSnapshotV3> {
    if bytes.len() > limits.max_snapshot_bytes {
        return Err(ControllerError::LimitExceeded {
            what: "controller snapshot bytes",
            actual: bytes.len(),
            max: limits.max_snapshot_bytes,
        });
    }
    if bytes.len() < usize::from(SNAPSHOT_HEADER_LEN_V3) + 4 {
        return Err(ControllerError::Invalid(
            "controller snapshot V3 is truncated",
        ));
    }
    let expected_crc = u32::from_le_bytes(
        bytes[bytes.len() - 4..]
            .try_into()
            .expect("4-byte CRC slice"),
    );
    let actual_crc = crc32c::crc32c(&bytes[..bytes.len() - 4]);
    if expected_crc != actual_crc {
        return Err(ControllerError::ChecksumMismatch {
            context: "controller snapshot V3",
            expected: expected_crc,
            actual: actual_crc,
        });
    }
    let mut reader = Reader::new(&bytes[..bytes.len() - 4]);
    if reader.u16()? != SNAPSHOT_VERSION_V3 || reader.u16()? != SNAPSHOT_HEADER_LEN_V3 {
        return Err(ControllerError::Invalid(
            "controller snapshot V3 version/header mismatch",
        ));
    }
    let base_len = usize::try_from(reader.u32()?)
        .map_err(|_| ControllerError::Invalid("P8 V3 base snapshot length overflow"))?;
    let policy_count = usize::try_from(reader.u32()?)
        .map_err(|_| ControllerError::Invalid("P8 V3 policy count overflow"))?;
    if policy_count > MAX_CONTROLLER_TOPICS {
        return Err(ControllerError::LimitExceeded {
            what: "topic performance policy count",
            actual: policy_count,
            max: MAX_CONTROLLER_TOPICS,
        });
    }
    if reader.u32()? != 0 {
        return Err(ControllerError::Invalid(
            "controller snapshot V3 reserved field is non-zero",
        ));
    }
    let policy_bytes = policy_count
        .checked_mul(POLICY_RECORD_BYTES)
        .ok_or(ControllerError::Invalid("P8 V3 policy bytes overflow"))?;
    if base_len > reader.remaining().saturating_sub(policy_bytes) {
        return Err(ControllerError::Invalid(
            "controller snapshot V3 base length does not fit",
        ));
    }
    let base = decode_controller_snapshot_v2(reader.take(base_len)?, limits)?;
    let mut topic_performance = BTreeMap::new();
    let mut previous: Option<TopicId> = None;
    for _ in 0..policy_count {
        let topic_id = TopicId::new(reader.array16()?);
        if previous.is_some_and(|prior| topic_id <= prior) {
            return Err(ControllerError::Invalid(
                "P8 V3 topic policy table must be strictly ordered",
            ));
        }
        let compression = CompressionPolicyV1::try_from(reader.u8()?)?;
        if reader.take(3)? != [0, 0, 0] {
            return Err(ControllerError::Invalid(
                "P8 V3 topic policy reserved bytes are non-zero",
            ));
        }
        let policy = TopicPerformancePolicyV1::new(
            topic_id,
            compression,
            reader.u32()?,
            reader.u32()?,
            reader.u32()?,
        )?;
        if topic_performance.insert(topic_id, policy).is_some() {
            return Err(ControllerError::Invalid(
                "duplicate topic performance policy in P8 V3 snapshot",
            ));
        }
        previous = Some(topic_id);
    }
    if !reader.is_empty() {
        return Err(ControllerError::Invalid(
            "trailing bytes in controller snapshot V3",
        ));
    }
    let snapshot = ControllerSnapshotV3 {
        base,
        topic_performance,
    };
    snapshot.clone().into_controller_snapshot()?;
    if encode_controller_snapshot_v3(&snapshot, limits)? != bytes {
        return Err(ControllerError::Invalid(
            "controller snapshot V3 is not canonical",
        ));
    }
    Ok(snapshot)
}

fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(ControllerError::Invalid("snapshot V3 reader overflow"))?;
        let value = self
            .bytes
            .get(self.pos..end)
            .ok_or(ControllerError::Invalid("snapshot V3 is truncated"))?;
        self.pos = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(
            self.take(2)?.try_into().expect("2 bytes"),
        ))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("4 bytes"),
        ))
    }

    fn array16(&mut self) -> Result<[u8; 16]> {
        Ok(self.take(16)?.try_into().expect("16 bytes"))
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.pos)
    }

    fn is_empty(&self) -> bool {
        self.remaining() == 0
    }
}

#[cfg(test)]
mod tests {
    use astralog_common::{ProducerEpoch, ProducerId};

    use super::*;
    use crate::{
        AssignmentGeneration, AssignmentRecord, BrokerLifecycle, CompressionPolicyV1,
        ControllerCommand, ControllerState, FailureDomainPolicy, ProducerSessionCommandV2,
        TopicPerformanceCommandV3,
    };

    fn register(max_feature: u16) -> ControllerCommand {
        ControllerCommand::RegisterBroker {
            broker_id: 1,
            node_uuid: [1; 16],
            initial_incarnation: 1,
            client_host: "broker-1".into(),
            client_port: 9001,
            internal_host: "broker-1-internal".into(),
            internal_port: 10001,
            rack: Some("rack-1".into()),
            zone: Some("zone-1".into()),
            min_feature: 1,
            max_feature,
        }
    }

    fn activate() -> ControllerCommand {
        ControllerCommand::SetBrokerLifecycle {
            broker_id: 1,
            expected_incarnation: 1,
            expected_lifecycle: BrokerLifecycle::Joining,
            new_lifecycle: BrokerLifecycle::Active,
        }
    }

    fn create(topic_id: TopicId, name: &str) -> ControllerCommand {
        ControllerCommand::CreateTopic {
            topic_id,
            topic_name: name.into(),
            partition_count: 1,
            replication_factor: 1,
            failure_domain_policy: FailureDomainPolicy::Disabled,
            assignments: vec![AssignmentRecord {
                topic_id,
                partition_id: 0,
                assignment_generation: AssignmentGeneration::new(1).unwrap(),
                voters: vec![1],
                learners: vec![],
            }],
        }
    }

    fn v3_state_two_topics() -> ControllerState {
        let mut state = ControllerState::new([0x88; 16], 1, 100).unwrap();
        state.apply(1, &register(4)).unwrap();
        state.apply(2, &activate()).unwrap();
        state
            .apply(
                3,
                &ControllerCommand::SetFeatureLevel {
                    expected_feature_level: 1,
                    new_feature_level: 4,
                },
            )
            .unwrap();
        for (index, value, codec) in [
            (4, 0x31, CompressionPolicyV1::Lz4),
            (5, 0x32, CompressionPolicyV1::Zstd),
        ] {
            let topic_id = TopicId::new([value; 16]);
            let base = create(topic_id, &format!("p8-{value}"));
            state
                .apply_topic_performance(
                    index,
                    &TopicPerformanceCommandV3::CreateTopicWithPerformance {
                        base,
                        performance: TopicPerformancePolicyV1::new(
                            topic_id,
                            codec,
                            1024,
                            100,
                            64 * 1024,
                        )
                        .unwrap(),
                    },
                )
                .unwrap();
        }
        state
    }

    #[test]
    fn p8_v3_snapshot_round_trip_is_canonical_and_preserves_policies() {
        let state = v3_state_two_topics();
        let bytes =
            crate::encode_controller_state_snapshot(&state, SnapshotLimits::default()).unwrap();
        assert_eq!(u16::from_le_bytes(bytes[..2].try_into().unwrap()), 3);
        let decoded =
            crate::decode_controller_state_snapshot(&bytes, SnapshotLimits::default()).unwrap();
        assert_eq!(decoded.state, state);
        assert_eq!(
            crate::encode_controller_state_snapshot(&decoded.state, SnapshotLimits::default())
                .unwrap(),
            bytes
        );
    }

    #[test]
    fn legacy_v1_and_v2_snapshots_derive_default_p8_policy() {
        let topic_id = TopicId::new([0x41; 16]);
        let mut v1 = ControllerState::new([0x91; 16], 1, 100).unwrap();
        v1.apply(1, &register(4)).unwrap();
        v1.apply(2, &activate()).unwrap();
        v1.apply(3, &create(topic_id, "legacy-v1")).unwrap();
        let v1_bytes = crate::encode_controller_snapshot(
            &ControllerSnapshot::new(v1),
            SnapshotLimits::default(),
        )
        .unwrap();
        let v1_decoded =
            crate::decode_controller_state_snapshot(&v1_bytes, SnapshotLimits::default()).unwrap();
        assert_eq!(
            v1_decoded.state.topic_performance_policy(topic_id),
            Some(&TopicPerformancePolicyV1::default_for(topic_id))
        );

        let topic_id = TopicId::new([0x42; 16]);
        let mut v2 = ControllerState::new([0x92; 16], 3, 200).unwrap();
        v2.apply(1, &register(4)).unwrap();
        v2.apply(2, &activate()).unwrap();
        v2.apply(3, &create(topic_id, "legacy-v2")).unwrap();
        v2.apply_producer_session(
            4,
            ProducerSessionCommandV2::InitializeSession {
                producer_id: ProducerId::new(200).unwrap(),
                producer_epoch: ProducerEpoch::new(1).unwrap(),
            },
        )
        .unwrap();
        let v2_bytes = encode_controller_snapshot_v2(
            &ControllerSnapshotV2::from_state(v2),
            SnapshotLimits::default(),
        )
        .unwrap();
        let v2_decoded =
            crate::decode_controller_state_snapshot(&v2_bytes, SnapshotLimits::default()).unwrap();
        assert_eq!(
            v2_decoded.state.topic_performance_policy(topic_id),
            Some(&TopicPerformancePolicyV1::default_for(topic_id))
        );
        assert_eq!(
            v2_decoded
                .state
                .producer_authority(ProducerId::new(200).unwrap())
                .unwrap()
                .producer_epoch,
            ProducerEpoch::new(1).unwrap()
        );
    }

    #[test]
    fn p8_v3_snapshot_rejects_unsorted_duplicate_invalid_and_reserved_policy_bytes() {
        let state = v3_state_two_topics();
        let bytes =
            crate::encode_controller_state_snapshot(&state, SnapshotLimits::default()).unwrap();
        let base_len =
            usize::try_from(u32::from_le_bytes(bytes[4..8].try_into().unwrap())).unwrap();
        let table = usize::from(SNAPSHOT_HEADER_LEN_V3) + base_len;

        let with_crc = |mut candidate: Vec<u8>| {
            let len = candidate.len();
            let crc = crc32c::crc32c(&candidate[..len - 4]);
            candidate[len - 4..].copy_from_slice(&crc.to_le_bytes());
            candidate
        };

        let mut unsorted = bytes.clone();
        let first = unsorted[table..table + POLICY_RECORD_BYTES].to_vec();
        let second =
            unsorted[table + POLICY_RECORD_BYTES..table + 2 * POLICY_RECORD_BYTES].to_vec();
        unsorted[table..table + POLICY_RECORD_BYTES].copy_from_slice(&second);
        unsorted[table + POLICY_RECORD_BYTES..table + 2 * POLICY_RECORD_BYTES]
            .copy_from_slice(&first);
        assert!(
            decode_controller_snapshot_v3(&with_crc(unsorted), SnapshotLimits::default()).is_err()
        );

        let mut duplicate = bytes.clone();
        let first = duplicate[table..table + POLICY_RECORD_BYTES].to_vec();
        duplicate[table + POLICY_RECORD_BYTES..table + 2 * POLICY_RECORD_BYTES]
            .copy_from_slice(&first);
        assert!(
            decode_controller_snapshot_v3(&with_crc(duplicate), SnapshotLimits::default()).is_err()
        );

        let mut unknown_codec = bytes.clone();
        unknown_codec[table + 16] = 9;
        assert!(
            decode_controller_snapshot_v3(&with_crc(unknown_codec), SnapshotLimits::default())
                .is_err()
        );

        let mut reserved = bytes.clone();
        reserved[table + 17] = 1;
        assert!(
            decode_controller_snapshot_v3(&with_crc(reserved), SnapshotLimits::default()).is_err()
        );
    }
}
