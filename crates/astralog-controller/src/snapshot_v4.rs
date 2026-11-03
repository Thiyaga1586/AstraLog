use std::collections::BTreeMap;

use astralog_common::TopicId;

use crate::{
    ControllerError, ControllerSnapshot, ControllerSnapshotV3, MAX_CONTROLLER_TOPICS, Result,
    SnapshotLimits, TopicRetentionPolicyV1, decode_controller_snapshot_v3,
    encode_controller_snapshot_v3,
    retention_codec::{RETENTION_POLICY_BYTES, Reader, put_retention, read_retention},
};

const SNAPSHOT_VERSION_V4: u16 = 4;
const SNAPSHOT_HEADER_LEN_V4: u16 = 16;
const POLICY_RECORD_BYTES: usize = 16 + RETENTION_POLICY_BYTES;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControllerSnapshotV4 {
    pub base: ControllerSnapshotV3,
    pub topic_retention: BTreeMap<TopicId, TopicRetentionPolicyV1>,
}

impl ControllerSnapshotV4 {
    pub fn from_state(state: crate::ControllerState) -> Self {
        let topic_retention = state.topic_retention_policies().clone();
        let mut base_state = state;
        let historical_defaults = base_state
            .topics()
            .keys()
            .copied()
            .map(|topic_id| (topic_id, TopicRetentionPolicyV1::default_for(topic_id)))
            .collect();
        base_state
            .install_topic_retention_policies(historical_defaults)
            .expect("valid controller state must accept canonical historical P9 defaults");
        Self {
            topic_retention,
            base: ControllerSnapshotV3::from_state(base_state),
        }
    }

    pub fn into_controller_snapshot(self) -> Result<ControllerSnapshot> {
        let mut snapshot = self.base.into_controller_snapshot()?;
        snapshot
            .state
            .install_topic_retention_policies(self.topic_retention)?;
        Ok(snapshot)
    }
}

pub fn encode_controller_snapshot_v4(
    snapshot: &ControllerSnapshotV4,
    limits: SnapshotLimits,
) -> Result<Vec<u8>> {
    if snapshot.topic_retention.len() > MAX_CONTROLLER_TOPICS {
        return Err(ControllerError::LimitExceeded {
            what: "topic retention policy count",
            actual: snapshot.topic_retention.len(),
            max: MAX_CONTROLLER_TOPICS,
        });
    }
    let base_state = snapshot.base.clone().into_controller_snapshot()?;
    if snapshot.topic_retention.len() != base_state.state.topics().len()
        || base_state.state.topics().keys().any(|topic_id| {
            snapshot
                .topic_retention
                .get(topic_id)
                .is_none_or(|policy| policy.topic_id != *topic_id || policy.validate().is_err())
        })
        || snapshot
            .topic_retention
            .keys()
            .any(|topic_id| !base_state.state.topics().contains_key(topic_id))
    {
        return Err(ControllerError::Invalid(
            "P9 V4 policy table does not exactly cover snapshot topics",
        ));
    }

    let base = encode_controller_snapshot_v3(&snapshot.base, limits)?;
    let base_len = u32::try_from(base.len())
        .map_err(|_| ControllerError::Invalid("P9 V4 base snapshot length overflow"))?;
    let policy_count = u32::try_from(snapshot.topic_retention.len())
        .map_err(|_| ControllerError::Invalid("P9 V4 policy count overflow"))?;
    let total = usize::from(SNAPSHOT_HEADER_LEN_V4)
        .checked_add(base.len())
        .and_then(|value| {
            value.checked_add(
                snapshot
                    .topic_retention
                    .len()
                    .checked_mul(POLICY_RECORD_BYTES)?,
            )
        })
        .and_then(|value| value.checked_add(4))
        .ok_or(ControllerError::Invalid("P9 V4 snapshot length overflow"))?;
    if total > limits.max_snapshot_bytes {
        return Err(ControllerError::LimitExceeded {
            what: "controller snapshot bytes",
            actual: total,
            max: limits.max_snapshot_bytes,
        });
    }

    let mut out = Vec::with_capacity(total);
    put_u16(&mut out, SNAPSHOT_VERSION_V4);
    put_u16(&mut out, SNAPSHOT_HEADER_LEN_V4);
    put_u32(&mut out, base_len);
    put_u32(&mut out, policy_count);
    put_u32(&mut out, 0);
    out.extend_from_slice(&base);
    for (topic_id, policy) in &snapshot.topic_retention {
        out.extend_from_slice(topic_id.as_bytes());
        put_retention(&mut out, *policy);
    }
    let crc = crc32c::crc32c(&out);
    put_u32(&mut out, crc);
    Ok(out)
}

pub fn decode_controller_snapshot_v4(
    bytes: &[u8],
    limits: SnapshotLimits,
) -> Result<ControllerSnapshotV4> {
    if bytes.len() > limits.max_snapshot_bytes {
        return Err(ControllerError::LimitExceeded {
            what: "controller snapshot bytes",
            actual: bytes.len(),
            max: limits.max_snapshot_bytes,
        });
    }
    if bytes.len() < usize::from(SNAPSHOT_HEADER_LEN_V4) + 4 {
        return Err(ControllerError::Invalid(
            "controller snapshot V4 is truncated",
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
            context: "controller snapshot V4",
            expected: expected_crc,
            actual: actual_crc,
        });
    }

    let mut reader = Reader::new(&bytes[..bytes.len() - 4]);
    if reader.u16()? != SNAPSHOT_VERSION_V4 || reader.u16()? != SNAPSHOT_HEADER_LEN_V4 {
        return Err(ControllerError::Invalid(
            "controller snapshot V4 version/header mismatch",
        ));
    }
    let base_len = usize::try_from(reader.u32()?)
        .map_err(|_| ControllerError::Invalid("P9 V4 base snapshot length overflow"))?;
    let policy_count = usize::try_from(reader.u32()?)
        .map_err(|_| ControllerError::Invalid("P9 V4 policy count overflow"))?;
    if policy_count > MAX_CONTROLLER_TOPICS {
        return Err(ControllerError::LimitExceeded {
            what: "topic retention policy count",
            actual: policy_count,
            max: MAX_CONTROLLER_TOPICS,
        });
    }
    if reader.u32()? != 0 {
        return Err(ControllerError::Invalid(
            "controller snapshot V4 reserved field is non-zero",
        ));
    }
    let policy_bytes = policy_count
        .checked_mul(POLICY_RECORD_BYTES)
        .ok_or(ControllerError::Invalid("P9 V4 policy bytes overflow"))?;
    if base_len > reader.remaining().saturating_sub(policy_bytes) {
        return Err(ControllerError::Invalid(
            "controller snapshot V4 base length does not fit",
        ));
    }
    let base = decode_controller_snapshot_v3(reader.take(base_len)?, limits)?;
    let mut topic_retention = BTreeMap::new();
    let mut previous: Option<TopicId> = None;
    for _ in 0..policy_count {
        let topic_id = TopicId::new(reader.array16()?);
        if previous.is_some_and(|prior| topic_id <= prior) {
            return Err(ControllerError::Invalid(
                "P9 V4 topic policy table must be strictly ordered",
            ));
        }
        let policy = read_retention(&mut reader, topic_id)?;
        if topic_retention.insert(topic_id, policy).is_some() {
            return Err(ControllerError::Invalid(
                "duplicate topic retention policy in P9 V4 snapshot",
            ));
        }
        previous = Some(topic_id);
    }
    if !reader.is_empty() {
        return Err(ControllerError::Invalid(
            "trailing bytes in controller snapshot V4",
        ));
    }
    let snapshot = ControllerSnapshotV4 {
        base,
        topic_retention,
    };
    snapshot.clone().into_controller_snapshot()?;
    if encode_controller_snapshot_v4(&snapshot, limits)? != bytes {
        return Err(ControllerError::Invalid(
            "controller snapshot V4 is not canonical",
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AssignmentGeneration, AssignmentRecord, CleanupPolicyV1, CompressionPolicyV1,
        ControllerCommand, ControllerSnapshotV2, FailureDomainPolicy, SubscriptionProtectionV1,
        TopicPerformancePolicyV1,
    };

    fn state_at_v4() -> crate::ControllerState {
        let mut state = crate::ControllerState::new([0x94; 16], 1, 1).unwrap();
        let register = ControllerCommand::RegisterBroker {
            broker_id: 1,
            node_uuid: [1; 16],
            initial_incarnation: 1,
            client_host: "broker-1".into(),
            client_port: 9092,
            internal_host: "broker-1-internal".into(),
            internal_port: 9093,
            rack: None,
            zone: None,
            min_feature: 1,
            max_feature: 5,
        };
        state.apply(1, &register).unwrap();
        state
            .apply(
                2,
                &ControllerCommand::SetBrokerLifecycle {
                    broker_id: 1,
                    expected_incarnation: 1,
                    expected_lifecycle: crate::BrokerLifecycle::Joining,
                    new_lifecycle: crate::BrokerLifecycle::Active,
                },
            )
            .unwrap();
        state
            .apply(
                3,
                &ControllerCommand::SetFeatureLevel {
                    expected_feature_level: 1,
                    new_feature_level: 5,
                },
            )
            .unwrap();
        let topic_id = TopicId::new([0x95; 16]);
        let base = ControllerCommand::CreateTopic {
            topic_id,
            topic_name: "retained".into(),
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
        };
        state
            .apply_topic_retention(
                4,
                &crate::TopicRetentionCommandV4::CreateTopicWithPerformanceAndRetention {
                    base,
                    performance: TopicPerformancePolicyV1::new(
                        topic_id,
                        CompressionPolicyV1::None,
                        crate::MAX_TOPIC_RECORD_BYTES,
                        crate::MAX_TOPIC_BATCH_RECORDS,
                        crate::DEFAULT_TOPIC_BATCH_UNCOMPRESSED_BYTES,
                    )
                    .unwrap(),
                    retention: TopicRetentionPolicyV1::new(
                        topic_id,
                        CleanupPolicyV1::CompactDelete,
                        Some(1000),
                        Some(4096),
                        500,
                        SubscriptionProtectionV1::Disabled,
                    )
                    .unwrap(),
                },
            )
            .unwrap();
        let second_topic_id = TopicId::new([0x96; 16]);
        state
            .apply_topic_retention(
                5,
                &crate::TopicRetentionCommandV4::CreateTopicWithPerformanceAndRetention {
                    base: ControllerCommand::CreateTopic {
                        topic_id: second_topic_id,
                        topic_name: "retained-second".into(),
                        partition_count: 1,
                        replication_factor: 1,
                        failure_domain_policy: FailureDomainPolicy::Disabled,
                        assignments: vec![AssignmentRecord {
                            topic_id: second_topic_id,
                            partition_id: 0,
                            assignment_generation: AssignmentGeneration::new(1).unwrap(),
                            voters: vec![1],
                            learners: vec![],
                        }],
                    },
                    performance: TopicPerformancePolicyV1::default_for(second_topic_id),
                    retention: TopicRetentionPolicyV1::default_for(second_topic_id),
                },
            )
            .unwrap();
        state
    }

    fn repair_crc(bytes: &mut [u8]) {
        let crc_offset = bytes.len() - 4;
        let crc = crc32c::crc32c(&bytes[..crc_offset]);
        bytes[crc_offset..].copy_from_slice(&crc.to_le_bytes());
    }

    #[test]
    fn p9_v4_snapshot_round_trip_and_old_snapshot_decoders_reject_it() {
        let snapshot = ControllerSnapshotV4::from_state(state_at_v4());
        let bytes = encode_controller_snapshot_v4(&snapshot, SnapshotLimits::default()).unwrap();
        assert_eq!(
            decode_controller_snapshot_v4(&bytes, SnapshotLimits::default()).unwrap(),
            snapshot
        );
        assert!(crate::decode_controller_snapshot(&bytes, SnapshotLimits::default()).is_err());
        assert!(crate::decode_controller_snapshot_v2(&bytes, SnapshotLimits::default()).is_err());
        assert!(crate::decode_controller_snapshot_v3(&bytes, SnapshotLimits::default()).is_err());
    }

    #[test]
    fn legacy_v3_snapshot_derives_historical_retention_default() {
        let state = state_at_v4();
        let topic_id = *state.topics().keys().next().unwrap();
        let v3 = ControllerSnapshotV3 {
            base: ControllerSnapshotV2::from_state(state.clone()),
            topic_performance: state.topic_performance_policies().clone(),
        };
        let bytes = encode_controller_snapshot_v3(&v3, SnapshotLimits::default()).unwrap();
        let decoded = decode_controller_snapshot_v3(&bytes, SnapshotLimits::default())
            .unwrap()
            .into_controller_snapshot()
            .unwrap();
        assert_eq!(
            decoded.state.topic_retention_policy(topic_id),
            Some(&TopicRetentionPolicyV1::default_for(topic_id))
        );
    }

    #[test]
    fn p9_v4_snapshot_rejects_unsorted_duplicate_or_orphan_policy_state() {
        let snapshot = ControllerSnapshotV4::from_state(state_at_v4());
        let mut orphan = snapshot.clone();
        let orphan_topic = TopicId::new([0xf0; 16]);
        orphan.topic_retention.insert(
            orphan_topic,
            TopicRetentionPolicyV1::default_for(orphan_topic),
        );
        assert!(encode_controller_snapshot_v4(&orphan, SnapshotLimits::default()).is_err());

        let mut invalid = snapshot.clone();
        let first = *invalid.topic_retention.keys().next().unwrap();
        invalid.topic_retention.insert(
            first,
            TopicRetentionPolicyV1 {
                tombstone_grace_ms: 0,
                ..invalid.topic_retention[&first]
            },
        );
        assert!(encode_controller_snapshot_v4(&invalid, SnapshotLimits::default()).is_err());

        let bytes = encode_controller_snapshot_v4(&snapshot, SnapshotLimits::default()).unwrap();
        let base_len = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
        let records = 16 + base_len;
        assert!(records + 2 * POLICY_RECORD_BYTES <= bytes.len() - 4);

        let mut unsorted = bytes.clone();
        let (prefix, suffix) = unsorted.split_at_mut(records + POLICY_RECORD_BYTES);
        prefix[records..records + POLICY_RECORD_BYTES]
            .swap_with_slice(&mut suffix[..POLICY_RECORD_BYTES]);
        repair_crc(&mut unsorted);
        assert!(decode_controller_snapshot_v4(&unsorted, SnapshotLimits::default()).is_err());

        let mut duplicate = bytes;
        let first_record = duplicate[records..records + POLICY_RECORD_BYTES].to_vec();
        duplicate[records + POLICY_RECORD_BYTES..records + 2 * POLICY_RECORD_BYTES]
            .copy_from_slice(&first_record);
        repair_crc(&mut duplicate);
        assert!(decode_controller_snapshot_v4(&duplicate, SnapshotLimits::default()).is_err());
    }
}
