use crate::{
    CommandLimits, ControllerCommand, ControllerError, Result, TopicPerformancePolicyV1,
    decode_command, encode_command,
};

const PERFORMANCE_COMMAND_VERSION_V3: u16 = 3;
const KIND_CREATE_TOPIC_WITH_PERFORMANCE: u16 = 1;
const HEADER_LEN: usize = 8;
const POLICY_BYTES: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicPerformanceCommandV3 {
    CreateTopicWithPerformance {
        base: ControllerCommand,
        performance: TopicPerformancePolicyV1,
    },
}

pub fn encode_topic_performance_command_v3(
    command: &TopicPerformanceCommandV3,
    limits: CommandLimits,
) -> Result<Vec<u8>> {
    let (kind, base, performance) = match command {
        TopicPerformanceCommandV3::CreateTopicWithPerformance { base, performance } => {
            let ControllerCommand::CreateTopic { topic_id, .. } = base else {
                return Err(ControllerError::Invalid(
                    "P8 V3 create command must wrap V1 CreateTopic",
                ));
            };
            if *topic_id != performance.topic_id {
                return Err(ControllerError::Invalid(
                    "P8 V3 topic policy id differs from CreateTopic id",
                ));
            }
            performance.validate()?;
            (KIND_CREATE_TOPIC_WITH_PERFORMANCE, base, *performance)
        }
    };
    let base_bytes = encode_command(base, limits)?;
    let body_len = 4usize
        .checked_add(base_bytes.len())
        .and_then(|value| value.checked_add(POLICY_BYTES))
        .ok_or(ControllerError::Invalid("P8 V3 command length overflow"))?;
    let total = HEADER_LEN
        .checked_add(body_len)
        .ok_or(ControllerError::Invalid("P8 V3 command length overflow"))?;
    if total > limits.max_command_bytes {
        return Err(ControllerError::LimitExceeded {
            what: "controller command bytes",
            actual: total,
            max: limits.max_command_bytes,
        });
    }
    let mut out = Vec::with_capacity(total);
    put_u16(&mut out, PERFORMANCE_COMMAND_VERSION_V3);
    put_u16(&mut out, kind);
    put_u32(
        &mut out,
        u32::try_from(body_len)
            .map_err(|_| ControllerError::Invalid("P8 V3 body length overflow"))?,
    );
    put_u32(
        &mut out,
        u32::try_from(base_bytes.len())
            .map_err(|_| ControllerError::Invalid("P8 V3 base length overflow"))?,
    );
    out.extend_from_slice(&base_bytes);
    put_policy(&mut out, performance);
    Ok(out)
}

pub fn decode_topic_performance_command_v3(
    bytes: &[u8],
    limits: CommandLimits,
) -> Result<TopicPerformanceCommandV3> {
    if bytes.len() > limits.max_command_bytes {
        return Err(ControllerError::LimitExceeded {
            what: "controller command bytes",
            actual: bytes.len(),
            max: limits.max_command_bytes,
        });
    }
    let mut reader = Reader::new(bytes);
    if reader.u16()? != PERFORMANCE_COMMAND_VERSION_V3 {
        return Err(ControllerError::Invalid(
            "unsupported topic performance command version",
        ));
    }
    let kind = reader.u16()?;
    let body_len = usize::try_from(reader.u32()?)
        .map_err(|_| ControllerError::Invalid("P8 V3 body length overflow"))?;
    if body_len != reader.remaining() {
        return Err(ControllerError::Invalid("P8 V3 body length mismatch"));
    }
    let base_len = usize::try_from(reader.u32()?)
        .map_err(|_| ControllerError::Invalid("P8 V3 base length overflow"))?;
    if base_len > reader.remaining().saturating_sub(POLICY_BYTES) {
        return Err(ControllerError::Invalid("P8 V3 base length does not fit"));
    }
    let base = decode_command(reader.take(base_len)?, limits)?;
    let compression = crate::CompressionPolicyV1::try_from(reader.u8()?)?;
    if reader.take(3)? != [0, 0, 0] {
        return Err(ControllerError::Invalid(
            "P8 V3 policy reserved bytes are non-zero",
        ));
    }
    let topic_id = match &base {
        ControllerCommand::CreateTopic { topic_id, .. } => *topic_id,
        _ => {
            return Err(ControllerError::Invalid(
                "P8 V3 create command must wrap V1 CreateTopic",
            ));
        }
    };
    let performance = TopicPerformancePolicyV1::new(
        topic_id,
        compression,
        reader.u32()?,
        reader.u32()?,
        reader.u32()?,
    )?;
    if !reader.is_empty() {
        return Err(ControllerError::Invalid(
            "trailing bytes in P8 V3 controller command",
        ));
    }
    let command = match kind {
        KIND_CREATE_TOPIC_WITH_PERFORMANCE => {
            TopicPerformanceCommandV3::CreateTopicWithPerformance { base, performance }
        }
        _ => return Err(ControllerError::Invalid("unknown P8 V3 command kind")),
    };
    if encode_topic_performance_command_v3(&command, limits)? != bytes {
        return Err(ControllerError::Invalid(
            "P8 V3 controller command is not canonical",
        ));
    }
    Ok(command)
}

fn put_policy(out: &mut Vec<u8>, policy: TopicPerformancePolicyV1) {
    out.push(policy.compression as u8);
    out.extend_from_slice(&[0, 0, 0]);
    put_u32(out, policy.max_record_bytes);
    put_u32(out, policy.max_batch_records);
    put_u32(out, policy.max_batch_uncompressed_bytes);
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
            .ok_or(ControllerError::Invalid("P8 V3 reader overflow"))?;
        let value = self
            .bytes
            .get(self.pos..end)
            .ok_or(ControllerError::Invalid("P8 V3 command is truncated"))?;
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

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.pos)
    }

    fn is_empty(&self) -> bool {
        self.remaining() == 0
    }
}

#[cfg(test)]
mod tests {
    use astralog_common::TopicId;

    use super::*;
    use crate::{AssignmentGeneration, AssignmentRecord, CompressionPolicyV1, FailureDomainPolicy};

    fn command() -> TopicPerformanceCommandV3 {
        let topic_id = TopicId::new([0x81; 16]);
        TopicPerformanceCommandV3::CreateTopicWithPerformance {
            base: ControllerCommand::CreateTopic {
                topic_id,
                topic_name: "p8-topic".into(),
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
        }
    }

    #[test]
    fn p8_v3_command_is_canonical_and_old_decoders_reject_it() {
        let command = command();
        let bytes =
            encode_topic_performance_command_v3(&command, CommandLimits::default()).unwrap();
        assert_eq!(
            decode_topic_performance_command_v3(&bytes, CommandLimits::default()).unwrap(),
            command
        );
        assert!(decode_command(&bytes, CommandLimits::default()).is_err());
        assert!(crate::decode_producer_session_command_v2(&bytes).is_err());

        let mut corrupt = bytes.clone();
        let reserved = corrupt.len() - POLICY_BYTES + 1;
        corrupt[reserved] = 1;
        assert!(decode_topic_performance_command_v3(&corrupt, CommandLimits::default()).is_err());
    }
}
