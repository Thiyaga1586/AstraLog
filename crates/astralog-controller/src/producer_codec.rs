use astralog_common::{ProducerEpoch, ProducerId};

use crate::{
    CommandLimits, ControllerCommand, ControllerError, Result, TopicPerformanceCommandV3,
    TopicRetentionCommandV4, decode_command, decode_topic_performance_command_v3,
    decode_topic_retention_command_v4, encode_command, encode_topic_performance_command_v3,
    encode_topic_retention_command_v4,
};

const PRODUCER_COMMAND_VERSION_V2: u16 = 2;
const KIND_INITIALIZE_SESSION: u16 = 1;
const KIND_BUMP_EPOCH: u16 = 2;
const HEADER_LEN: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerSessionCommandV2 {
    InitializeSession {
        producer_id: ProducerId,
        producer_epoch: ProducerEpoch,
    },
    BumpEpoch {
        producer_id: ProducerId,
        expected_epoch: ProducerEpoch,
        new_epoch: ProducerEpoch,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControllerLogCommand {
    V1(ControllerCommand),
    ProducerV2(ProducerSessionCommandV2),
    PerformanceV3(TopicPerformanceCommandV3),
    RetentionV4(TopicRetentionCommandV4),
}

pub fn encode_controller_log_command(
    command: &ControllerLogCommand,
    limits: CommandLimits,
) -> Result<Vec<u8>> {
    match command {
        ControllerLogCommand::V1(command) => encode_command(command, limits),
        ControllerLogCommand::ProducerV2(command) => encode_producer_session_command_v2(*command),
        ControllerLogCommand::PerformanceV3(command) => {
            encode_topic_performance_command_v3(command, limits)
        }
        ControllerLogCommand::RetentionV4(command) => {
            encode_topic_retention_command_v4(command, limits)
        }
    }
}

pub fn decode_controller_log_command(
    bytes: &[u8],
    limits: CommandLimits,
) -> Result<ControllerLogCommand> {
    if bytes.len() < 2 {
        return Err(ControllerError::Invalid("controller command is truncated"));
    }
    match u16::from_le_bytes(bytes[..2].try_into().expect("2-byte command version")) {
        1 => Ok(ControllerLogCommand::V1(decode_command(bytes, limits)?)),
        PRODUCER_COMMAND_VERSION_V2 => Ok(ControllerLogCommand::ProducerV2(
            decode_producer_session_command_v2(bytes)?,
        )),
        3 => Ok(ControllerLogCommand::PerformanceV3(
            decode_topic_performance_command_v3(bytes, limits)?,
        )),
        4 => Ok(ControllerLogCommand::RetentionV4(
            decode_topic_retention_command_v4(bytes, limits)?,
        )),
        _ => Err(ControllerError::Invalid(
            "unsupported controller command version",
        )),
    }
}

pub fn encode_producer_session_command_v2(command: ProducerSessionCommandV2) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    let kind = match command {
        ProducerSessionCommandV2::InitializeSession {
            producer_id,
            producer_epoch,
        } => {
            if producer_epoch.get() != 1 {
                return Err(ControllerError::Invalid(
                    "new producer session must begin at epoch one",
                ));
            }
            put_u64(&mut body, producer_id.get());
            put_u32(&mut body, producer_epoch.get());
            KIND_INITIALIZE_SESSION
        }
        ProducerSessionCommandV2::BumpEpoch {
            producer_id,
            expected_epoch,
            new_epoch,
        } => {
            if expected_epoch.checked_next() != Some(new_epoch) {
                return Err(ControllerError::Invalid(
                    "producer epoch bump must advance by exactly one",
                ));
            }
            put_u64(&mut body, producer_id.get());
            put_u32(&mut body, expected_epoch.get());
            put_u32(&mut body, new_epoch.get());
            KIND_BUMP_EPOCH
        }
    };
    let mut out = Vec::with_capacity(HEADER_LEN + body.len());
    put_u16(&mut out, PRODUCER_COMMAND_VERSION_V2);
    put_u16(&mut out, kind);
    put_u32(
        &mut out,
        u32::try_from(body.len())
            .map_err(|_| ControllerError::Invalid("producer command body length overflow"))?,
    );
    out.extend_from_slice(&body);
    Ok(out)
}

pub fn decode_producer_session_command_v2(bytes: &[u8]) -> Result<ProducerSessionCommandV2> {
    let mut reader = Reader::new(bytes);
    if reader.u16()? != PRODUCER_COMMAND_VERSION_V2 {
        return Err(ControllerError::Invalid(
            "unsupported producer session command version",
        ));
    }
    let kind = reader.u16()?;
    let body_len = usize::try_from(reader.u32()?)
        .map_err(|_| ControllerError::Invalid("producer command body length overflow"))?;
    if body_len != reader.remaining() {
        return Err(ControllerError::Invalid(
            "producer command body length mismatch",
        ));
    }
    let producer_id = ProducerId::new(reader.u64()?)
        .ok_or(ControllerError::Invalid("producer id zero is reserved"))?;
    let command = match kind {
        KIND_INITIALIZE_SESSION => ProducerSessionCommandV2::InitializeSession {
            producer_id,
            producer_epoch: ProducerEpoch::new(reader.u32()?)
                .ok_or(ControllerError::Invalid("producer epoch zero is reserved"))?,
        },
        KIND_BUMP_EPOCH => ProducerSessionCommandV2::BumpEpoch {
            producer_id,
            expected_epoch: ProducerEpoch::new(reader.u32()?)
                .ok_or(ControllerError::Invalid("producer epoch zero is reserved"))?,
            new_epoch: ProducerEpoch::new(reader.u32()?)
                .ok_or(ControllerError::Invalid("producer epoch zero is reserved"))?,
        },
        _ => {
            return Err(ControllerError::Invalid(
                "unknown producer session command kind",
            ));
        }
    };
    if !reader.is_empty() {
        return Err(ControllerError::Invalid(
            "trailing bytes in producer session command",
        ));
    }
    if encode_producer_session_command_v2(command)? != bytes {
        return Err(ControllerError::Invalid(
            "producer session command is not canonical",
        ));
    }
    Ok(command)
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
            .ok_or(ControllerError::Invalid("producer command reader overflow"))?;
        let value = self
            .bytes
            .get(self.pos..end)
            .ok_or(ControllerError::Invalid("producer command is truncated"))?;
        self.pos = end;
        Ok(value)
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

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("8 bytes"),
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
    use super::*;
    use crate::{CommandLimits, decode_command};

    fn decode_hex(input: &str) -> Vec<u8> {
        let (pairs, remainder) = input.as_bytes().as_chunks::<2>();
        assert!(remainder.is_empty());
        pairs
            .iter()
            .map(|pair| u8::from_str_radix(core::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect()
    }

    #[test]
    fn producer_commands_v2_are_canonical_and_v1_decoder_rejects_them() {
        let initialize = ProducerSessionCommandV2::InitializeSession {
            producer_id: ProducerId::new(100).unwrap(),
            producer_epoch: ProducerEpoch::new(1).unwrap(),
        };
        let bytes = encode_producer_session_command_v2(initialize).unwrap();
        assert_eq!(
            bytes,
            decode_hex("020001000c000000640000000000000001000000")
        );
        assert_eq!(
            decode_producer_session_command_v2(&bytes).unwrap(),
            initialize
        );
        assert!(decode_command(&bytes, CommandLimits::default()).is_err());

        let bump = ProducerSessionCommandV2::BumpEpoch {
            producer_id: ProducerId::new(100).unwrap(),
            expected_epoch: ProducerEpoch::new(7).unwrap(),
            new_epoch: ProducerEpoch::new(8).unwrap(),
        };
        let bytes = encode_producer_session_command_v2(bump).unwrap();
        assert_eq!(
            bytes,
            decode_hex("020002001000000064000000000000000700000008000000")
        );
        assert_eq!(decode_producer_session_command_v2(&bytes).unwrap(), bump);

        assert_eq!(
            decode_controller_log_command(&bytes, CommandLimits::default()).unwrap(),
            ControllerLogCommand::ProducerV2(bump)
        );
    }

    #[test]
    fn producer_commands_reject_noncanonical_epoch_transitions() {
        assert!(
            encode_producer_session_command_v2(ProducerSessionCommandV2::InitializeSession {
                producer_id: ProducerId::new(1).unwrap(),
                producer_epoch: ProducerEpoch::new(2).unwrap(),
            })
            .is_err()
        );
        assert!(
            encode_producer_session_command_v2(ProducerSessionCommandV2::BumpEpoch {
                producer_id: ProducerId::new(1).unwrap(),
                expected_epoch: ProducerEpoch::new(2).unwrap(),
                new_epoch: ProducerEpoch::new(4).unwrap(),
            })
            .is_err()
        );
    }
}
