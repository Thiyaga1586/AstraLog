use astralog_common::TopicId;

use crate::{
    CleanupPolicyV1, CommandLimits, CompressionPolicyV1, ControllerCommand, ControllerError,
    Result, SubscriptionProtectionV1, TopicPerformancePolicyV1, TopicRetentionPolicyV1,
    decode_command, encode_command,
};

const RETENTION_COMMAND_VERSION_V4: u16 = 4;
const KIND_CREATE_TOPIC_WITH_RETENTION: u16 = 1;
const KIND_ALTER_TOPIC_RETENTION: u16 = 2;
const HEADER_LEN: usize = 8;
const PERFORMANCE_BYTES: usize = 16;
pub(crate) const RETENTION_POLICY_BYTES: usize = 28;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicRetentionCommandV4 {
    CreateTopicWithPerformanceAndRetention {
        base: ControllerCommand,
        performance: TopicPerformancePolicyV1,
        retention: TopicRetentionPolicyV1,
    },
    AlterTopicRetentionPolicy {
        topic_id: TopicId,
        expected: TopicRetentionPolicyV1,
        new: TopicRetentionPolicyV1,
    },
}

pub fn encode_topic_retention_command_v4(
    command: &TopicRetentionCommandV4,
    limits: CommandLimits,
) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    let kind = match command {
        TopicRetentionCommandV4::CreateTopicWithPerformanceAndRetention {
            base,
            performance,
            retention,
        } => {
            let ControllerCommand::CreateTopic { topic_id, .. } = base else {
                return Err(ControllerError::Invalid(
                    "P9 V4 create command must wrap V1 CreateTopic",
                ));
            };
            if *topic_id != performance.topic_id || *topic_id != retention.topic_id {
                return Err(ControllerError::Invalid(
                    "P9 V4 topic policy ids differ from CreateTopic id",
                ));
            }
            performance.validate()?;
            retention.validate()?;
            let base_bytes = encode_command(base, limits)?;
            put_u32(
                &mut body,
                u32::try_from(base_bytes.len())
                    .map_err(|_| ControllerError::Invalid("P9 V4 base length overflow"))?,
            );
            body.extend_from_slice(&base_bytes);
            put_performance(&mut body, *performance);
            put_retention(&mut body, *retention);
            KIND_CREATE_TOPIC_WITH_RETENTION
        }
        TopicRetentionCommandV4::AlterTopicRetentionPolicy {
            topic_id,
            expected,
            new,
        } => {
            if *topic_id != expected.topic_id || *topic_id != new.topic_id {
                return Err(ControllerError::Invalid(
                    "P9 V4 alter policy ids differ from topic id",
                ));
            }
            expected.validate()?;
            new.validate()?;
            body.extend_from_slice(topic_id.as_bytes());
            put_retention(&mut body, *expected);
            put_retention(&mut body, *new);
            KIND_ALTER_TOPIC_RETENTION
        }
    };

    let total = HEADER_LEN
        .checked_add(body.len())
        .ok_or(ControllerError::Invalid("P9 V4 command length overflow"))?;
    if total > limits.max_command_bytes {
        return Err(ControllerError::LimitExceeded {
            what: "controller command bytes",
            actual: total,
            max: limits.max_command_bytes,
        });
    }
    let mut out = Vec::with_capacity(total);
    put_u16(&mut out, RETENTION_COMMAND_VERSION_V4);
    put_u16(&mut out, kind);
    put_u32(
        &mut out,
        u32::try_from(body.len())
            .map_err(|_| ControllerError::Invalid("P9 V4 body length overflow"))?,
    );
    out.extend_from_slice(&body);
    Ok(out)
}

pub fn decode_topic_retention_command_v4(
    bytes: &[u8],
    limits: CommandLimits,
) -> Result<TopicRetentionCommandV4> {
    if bytes.len() > limits.max_command_bytes {
        return Err(ControllerError::LimitExceeded {
            what: "controller command bytes",
            actual: bytes.len(),
            max: limits.max_command_bytes,
        });
    }
    let mut reader = Reader::new(bytes);
    if reader.u16()? != RETENTION_COMMAND_VERSION_V4 {
        return Err(ControllerError::Invalid(
            "unsupported topic retention command version",
        ));
    }
    let kind = reader.u16()?;
    let body_len = usize::try_from(reader.u32()?)
        .map_err(|_| ControllerError::Invalid("P9 V4 body length overflow"))?;
    if body_len != reader.remaining() {
        return Err(ControllerError::Invalid("P9 V4 body length mismatch"));
    }
    let command = match kind {
        KIND_CREATE_TOPIC_WITH_RETENTION => {
            let base_len = usize::try_from(reader.u32()?)
                .map_err(|_| ControllerError::Invalid("P9 V4 base length overflow"))?;
            if base_len
                > reader
                    .remaining()
                    .saturating_sub(PERFORMANCE_BYTES + RETENTION_POLICY_BYTES)
            {
                return Err(ControllerError::Invalid("P9 V4 base length does not fit"));
            }
            let base = decode_command(reader.take(base_len)?, limits)?;
            let topic_id = match &base {
                ControllerCommand::CreateTopic { topic_id, .. } => *topic_id,
                _ => {
                    return Err(ControllerError::Invalid(
                        "P9 V4 create command must wrap V1 CreateTopic",
                    ));
                }
            };
            let performance = read_performance(&mut reader, topic_id)?;
            let retention = read_retention(&mut reader, topic_id)?;
            TopicRetentionCommandV4::CreateTopicWithPerformanceAndRetention {
                base,
                performance,
                retention,
            }
        }
        KIND_ALTER_TOPIC_RETENTION => {
            let topic_id = TopicId::new(reader.array16()?);
            let expected = read_retention(&mut reader, topic_id)?;
            let new = read_retention(&mut reader, topic_id)?;
            TopicRetentionCommandV4::AlterTopicRetentionPolicy {
                topic_id,
                expected,
                new,
            }
        }
        _ => return Err(ControllerError::Invalid("unknown P9 V4 command kind")),
    };
    if !reader.is_empty() {
        return Err(ControllerError::Invalid(
            "trailing bytes in P9 V4 controller command",
        ));
    }
    if encode_topic_retention_command_v4(&command, limits)? != bytes {
        return Err(ControllerError::Invalid(
            "P9 V4 controller command is not canonical",
        ));
    }
    Ok(command)
}

pub(crate) fn put_retention(out: &mut Vec<u8>, policy: TopicRetentionPolicyV1) {
    out.push(policy.cleanup_policy as u8);
    out.push(policy.subscription_protection as u8);
    let mut flags = 0_u16;
    if policy.retention_ms.is_some() {
        flags |= 1;
    }
    if policy.retention_bytes.is_some() {
        flags |= 2;
    }
    put_u16(out, flags);
    put_u64(out, policy.retention_ms.unwrap_or(0));
    put_u64(out, policy.retention_bytes.unwrap_or(0));
    put_u64(out, policy.tombstone_grace_ms);
}

pub(crate) fn read_retention(
    reader: &mut Reader<'_>,
    topic_id: TopicId,
) -> Result<TopicRetentionPolicyV1> {
    let cleanup_policy = CleanupPolicyV1::try_from(reader.u8()?)?;
    let subscription_protection = SubscriptionProtectionV1::try_from(reader.u8()?)?;
    let flags = reader.u16()?;
    if flags & !0b11 != 0 {
        return Err(ControllerError::Invalid(
            "P9 retention option flags contain unknown bits",
        ));
    }
    let retention_ms_raw = reader.u64()?;
    let retention_bytes_raw = reader.u64()?;
    let tombstone_grace_ms = reader.u64()?;
    if flags & 1 == 0 && retention_ms_raw != 0 {
        return Err(ControllerError::Invalid(
            "P9 absent retention_ms must encode zero",
        ));
    }
    if flags & 2 == 0 && retention_bytes_raw != 0 {
        return Err(ControllerError::Invalid(
            "P9 absent retention_bytes must encode zero",
        ));
    }
    TopicRetentionPolicyV1::new(
        topic_id,
        cleanup_policy,
        (flags & 1 != 0).then_some(retention_ms_raw),
        (flags & 2 != 0).then_some(retention_bytes_raw),
        tombstone_grace_ms,
        subscription_protection,
    )
}

fn put_performance(out: &mut Vec<u8>, policy: TopicPerformancePolicyV1) {
    out.push(policy.compression as u8);
    out.extend_from_slice(&[0, 0, 0]);
    put_u32(out, policy.max_record_bytes);
    put_u32(out, policy.max_batch_records);
    put_u32(out, policy.max_batch_uncompressed_bytes);
}

fn read_performance(
    reader: &mut Reader<'_>,
    topic_id: TopicId,
) -> Result<TopicPerformancePolicyV1> {
    let compression = CompressionPolicyV1::try_from(reader.u8()?)?;
    if reader.take(3)? != [0, 0, 0] {
        return Err(ControllerError::Invalid(
            "P9 V4 performance reserved bytes are non-zero",
        ));
    }
    TopicPerformancePolicyV1::new(
        topic_id,
        compression,
        reader.u32()?,
        reader.u32()?,
        reader.u32()?,
    )
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

pub(crate) struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub(crate) const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    pub(crate) fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(ControllerError::Invalid("P9 V4 reader overflow"))?;
        let value = self
            .bytes
            .get(self.pos..end)
            .ok_or(ControllerError::Invalid("P9 V4 command is truncated"))?;
        self.pos = end;
        Ok(value)
    }

    pub(crate) fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(
            self.take(2)?.try_into().expect("2 bytes"),
        ))
    }

    pub(crate) fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("4 bytes"),
        ))
    }

    pub(crate) fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("8 bytes"),
        ))
    }

    pub(crate) fn array16(&mut self) -> Result<[u8; 16]> {
        Ok(self.take(16)?.try_into().expect("16 bytes"))
    }

    pub(crate) fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.pos)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.remaining() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AssignmentGeneration, AssignmentRecord, FailureDomainPolicy};

    fn create_command() -> TopicRetentionCommandV4 {
        let topic_id = TopicId::new([0x91; 16]);
        TopicRetentionCommandV4::CreateTopicWithPerformanceAndRetention {
            base: ControllerCommand::CreateTopic {
                topic_id,
                topic_name: "p9-topic".into(),
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
            },
            performance: TopicPerformancePolicyV1::new(
                topic_id,
                CompressionPolicyV1::Lz4,
                1024,
                100,
                64 * 1024,
            )
            .unwrap(),
            retention: TopicRetentionPolicyV1::new(
                topic_id,
                CleanupPolicyV1::CompactDelete,
                Some(10_000),
                Some(1_000_000),
                60_000,
                SubscriptionProtectionV1::Disabled,
            )
            .unwrap(),
        }
    }

    #[test]
    fn p9_v4_commands_are_canonical_and_old_decoders_reject_them() {
        let command = create_command();
        let bytes = encode_topic_retention_command_v4(&command, CommandLimits::default()).unwrap();
        assert_eq!(
            decode_topic_retention_command_v4(&bytes, CommandLimits::default()).unwrap(),
            command
        );
        assert!(decode_command(&bytes, CommandLimits::default()).is_err());
        assert!(crate::decode_producer_session_command_v2(&bytes).is_err());
        assert!(
            crate::decode_topic_performance_command_v3(&bytes, CommandLimits::default()).is_err()
        );

        let TopicRetentionCommandV4::CreateTopicWithPerformanceAndRetention { retention, .. } =
            command
        else {
            unreachable!();
        };
        let altered = TopicRetentionPolicyV1::new(
            retention.topic_id,
            CleanupPolicyV1::Delete,
            None,
            Some(1234),
            retention.tombstone_grace_ms,
            SubscriptionProtectionV1::Disabled,
        )
        .unwrap();
        let alter = TopicRetentionCommandV4::AlterTopicRetentionPolicy {
            topic_id: retention.topic_id,
            expected: retention,
            new: altered,
        };
        let bytes = encode_topic_retention_command_v4(&alter, CommandLimits::default()).unwrap();
        assert_eq!(
            decode_topic_retention_command_v4(&bytes, CommandLimits::default()).unwrap(),
            alter
        );
    }
}
