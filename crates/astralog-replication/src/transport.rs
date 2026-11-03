use astralog_common::TopicId;
use astralog_raft::RaftMessage;

use crate::{
    CLEAN_PROGRESS_REPORT_BYTES, CleanProgressReport, ReplicationError, Result,
    decode_raft_message, encode_raft_message,
};

const TRANSPORT_VERSION: u16 = 1;
const MESSAGE_KIND_RAFT: u16 = 1;
const MESSAGE_KIND_RAFT_REBOOTSTRAP: u16 = 2;
const MESSAGE_KIND_CLEAN_PROGRESS: u16 = 3;
const FIXED_BODY_LEN: usize = 40;
const CRC_LEN: usize = 4;
const MIN_FRAME_LEN: usize = FIXED_BODY_LEN + CRC_LEN;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportLimits {
    pub max_frame_bytes: usize,
}

impl Default for TransportLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 32 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalFrame {
    pub source_broker_id: u32,
    pub source_incarnation: u64,
    pub topic_id: TopicId,
    pub partition: u32,
    /// Exactly one of message or clean_progress is present. Existing Raft
    /// message kinds retain their frozen V1 byte encoding.
    pub message: Option<RaftMessage>,
    pub rebootstrap: Option<Vec<u8>>,
    pub clean_progress: Option<CleanProgressReport>,
}

pub fn encode_internal_frame(frame: &InternalFrame, limits: TransportLimits) -> Result<Vec<u8>> {
    let (message_kind, payload) = if let Some(report) = frame.clean_progress {
        if frame.message.is_some()
            || frame.rebootstrap.is_some()
            || report.source_broker_id != frame.source_broker_id
            || report.source_incarnation != frame.source_incarnation
            || report.topic_id != frame.topic_id
            || report.partition_id.get() != frame.partition
        {
            return Err(ReplicationError::Invalid(
                "clean-progress sideband must match frame identity and have no Raft payload",
            ));
        }
        (MESSAGE_KIND_CLEAN_PROGRESS, report.encode()?.to_vec())
    } else {
        let message = encode_raft_message(frame.message.as_ref().ok_or(
            ReplicationError::Invalid("internal frame missing Raft message"),
        )?)?;
        match &frame.rebootstrap {
            None => (MESSAGE_KIND_RAFT, message),
            Some(package) => {
                if !matches!(frame.message, Some(RaftMessage::InstallSnapshotRequest(_)))
                    || package.is_empty()
                {
                    return Err(ReplicationError::Invalid(
                        "rebootstrap payload is only valid with InstallSnapshotRequest",
                    ));
                }
                let message_len = u32::try_from(message.len())
                    .map_err(|_| ReplicationError::Invalid("Raft message length overflow"))?;
                let mut payload = Vec::with_capacity(4 + message.len() + package.len());
                payload.extend_from_slice(&message_len.to_be_bytes());
                payload.extend_from_slice(&message);
                payload.extend_from_slice(package);
                (MESSAGE_KIND_RAFT_REBOOTSTRAP, payload)
            }
        }
    };
    let frame_len = FIXED_BODY_LEN
        .checked_add(payload.len())
        .and_then(|value| value.checked_add(CRC_LEN))
        .ok_or(ReplicationError::Invalid("internal frame length overflow"))?;
    if frame_len > limits.max_frame_bytes {
        return Err(ReplicationError::LimitExceeded {
            what: "internal transport frame bytes",
            actual: frame_len,
            max: limits.max_frame_bytes,
        });
    }
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| ReplicationError::Invalid("internal payload length overflow"))?;
    let mut bytes = Vec::with_capacity(frame_len + 4);
    bytes.extend_from_slice(
        &u32::try_from(frame_len)
            .map_err(|_| ReplicationError::Invalid("internal frame length exceeds u32"))?
            .to_be_bytes(),
    );
    bytes.extend_from_slice(&TRANSPORT_VERSION.to_be_bytes());
    bytes.extend_from_slice(&message_kind.to_be_bytes());
    bytes.extend_from_slice(&frame.source_broker_id.to_be_bytes());
    bytes.extend_from_slice(&frame.source_incarnation.to_be_bytes());
    bytes.extend_from_slice(frame.topic_id.as_bytes());
    bytes.extend_from_slice(&frame.partition.to_be_bytes());
    bytes.extend_from_slice(&payload_len.to_be_bytes());
    bytes.extend_from_slice(&payload);
    let crc = crc32c::crc32c(&bytes[4..]);
    bytes.extend_from_slice(&crc.to_be_bytes());
    Ok(bytes)
}

pub fn decode_internal_frame(bytes: &[u8], limits: TransportLimits) -> Result<InternalFrame> {
    if bytes.len() < 4 + MIN_FRAME_LEN {
        return Err(ReplicationError::Invalid(
            "internal frame shorter than minimum",
        ));
    }
    let declared = usize::try_from(u32::from_be_bytes(
        bytes[0..4].try_into().expect("4-byte slice"),
    ))
    .map_err(|_| ReplicationError::Invalid("internal frame length overflow"))?;
    if declared < MIN_FRAME_LEN || declared > limits.max_frame_bytes {
        return Err(ReplicationError::Invalid(
            "invalid internal frame length prefix",
        ));
    }
    if declared.checked_add(4) != Some(bytes.len()) {
        return Err(ReplicationError::Invalid("internal frame length mismatch"));
    }
    if u16::from_be_bytes(bytes[4..6].try_into().expect("2-byte slice")) != TRANSPORT_VERSION {
        return Err(ReplicationError::Invalid(
            "unsupported internal transport version",
        ));
    }
    let message_kind = u16::from_be_bytes(bytes[6..8].try_into().expect("2-byte slice"));
    if !matches!(
        message_kind,
        MESSAGE_KIND_RAFT | MESSAGE_KIND_RAFT_REBOOTSTRAP | MESSAGE_KIND_CLEAN_PROGRESS
    ) {
        return Err(ReplicationError::Invalid(
            "unknown internal transport message kind",
        ));
    }
    let source_broker_id = u32::from_be_bytes(bytes[8..12].try_into().expect("4-byte slice"));
    let source_incarnation = u64::from_be_bytes(bytes[12..20].try_into().expect("8-byte slice"));
    let mut topic = [0_u8; 16];
    topic.copy_from_slice(&bytes[20..36]);
    let partition = u32::from_be_bytes(bytes[36..40].try_into().expect("4-byte slice"));
    let payload_len = usize::try_from(u32::from_be_bytes(
        bytes[40..44].try_into().expect("4-byte slice"),
    ))
    .map_err(|_| ReplicationError::Invalid("internal payload length overflow"))?;
    let payload_end = 44_usize
        .checked_add(payload_len)
        .ok_or(ReplicationError::Invalid(
            "internal payload length overflow",
        ))?;
    if payload_end.checked_add(CRC_LEN) != Some(bytes.len()) {
        return Err(ReplicationError::Invalid(
            "internal payload length mismatch",
        ));
    }
    let expected_crc = u32::from_be_bytes(
        bytes[payload_end..payload_end + CRC_LEN]
            .try_into()
            .expect("4-byte slice"),
    );
    let actual_crc = crc32c::crc32c(&bytes[4..payload_end]);
    if expected_crc != actual_crc {
        return Err(ReplicationError::ChecksumMismatch {
            context: "internal transport frame",
            expected: expected_crc,
            actual: actual_crc,
        });
    }
    let payload = &bytes[44..payload_end];
    let (message, rebootstrap, clean_progress) = if message_kind == MESSAGE_KIND_CLEAN_PROGRESS {
        if payload.len() != CLEAN_PROGRESS_REPORT_BYTES {
            return Err(ReplicationError::Invalid(
                "invalid P9-G fixed sideband length",
            ));
        }
        let report = CleanProgressReport::decode(payload)?;
        if report.topic_id != TopicId::new(topic)
            || report.partition_id.get() != partition
            || report.source_broker_id != source_broker_id
            || report.source_incarnation != source_incarnation
        {
            return Err(ReplicationError::Invalid(
                "P9-G sideband/header identity mismatch",
            ));
        }
        (None, None, Some(report))
    } else if message_kind == MESSAGE_KIND_RAFT {
        (Some(decode_raft_message(payload)?), None, None)
    } else {
        if payload.len() < 4 {
            return Err(ReplicationError::Invalid(
                "rebootstrap transport payload is truncated",
            ));
        }
        let message_len = usize::try_from(u32::from_be_bytes(
            payload[..4].try_into().expect("4-byte slice"),
        ))
        .map_err(|_| ReplicationError::Invalid("Raft message length overflow"))?;
        let message_end = 4_usize
            .checked_add(message_len)
            .ok_or(ReplicationError::Invalid("Raft message length overflow"))?;
        if message_end >= payload.len() {
            return Err(ReplicationError::Invalid(
                "rebootstrap transport package is missing",
            ));
        }
        let message = decode_raft_message(&payload[4..message_end])?;
        if !matches!(message, RaftMessage::InstallSnapshotRequest(_)) {
            return Err(ReplicationError::Invalid(
                "rebootstrap transport kind requires InstallSnapshotRequest",
            ));
        }
        (Some(message), Some(payload[message_end..].to_vec()), None)
    };
    Ok(InternalFrame {
        source_broker_id,
        source_incarnation,
        topic_id: TopicId::new(topic),
        partition,
        message,
        rebootstrap,
        clean_progress,
    })
}

#[derive(Debug)]
pub struct InternalFrameDecoder {
    buffer: Vec<u8>,
    limits: TransportLimits,
}

impl InternalFrameDecoder {
    pub fn new(limits: TransportLimits) -> Self {
        Self {
            buffer: Vec::new(),
            limits,
        }
    }

    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<InternalFrame>> {
        if self
            .buffer
            .len()
            .checked_add(bytes.len())
            .is_none_or(|len| len > self.limits.max_frame_bytes.saturating_add(4))
            && self.buffer.len() < 4
        {
            return Err(ReplicationError::LimitExceeded {
                what: "internal decoder buffered bytes",
                actual: self.buffer.len().saturating_add(bytes.len()),
                max: self.limits.max_frame_bytes.saturating_add(4),
            });
        }
        self.buffer.extend_from_slice(bytes);
        let mut frames = Vec::new();
        loop {
            if self.buffer.len() < 4 {
                break;
            }
            let frame_len = usize::try_from(u32::from_be_bytes(
                self.buffer[0..4].try_into().expect("4-byte slice"),
            ))
            .map_err(|_| ReplicationError::Invalid("internal frame length overflow"))?;
            if frame_len < MIN_FRAME_LEN || frame_len > self.limits.max_frame_bytes {
                return Err(ReplicationError::Invalid(
                    "invalid internal frame length prefix",
                ));
            }
            let total = frame_len
                .checked_add(4)
                .ok_or(ReplicationError::Invalid("internal frame length overflow"))?;
            if self.buffer.len() < total {
                if self.buffer.len() > self.limits.max_frame_bytes.saturating_add(4) {
                    return Err(ReplicationError::LimitExceeded {
                        what: "internal decoder buffered bytes",
                        actual: self.buffer.len(),
                        max: self.limits.max_frame_bytes.saturating_add(4),
                    });
                }
                break;
            }
            let frame = decode_internal_frame(&self.buffer[..total], self.limits)?;
            self.buffer.drain(..total);
            frames.push(frame);
        }
        Ok(frames)
    }

    pub fn buffered_bytes(&self) -> usize {
        self.buffer.len()
    }
}

#[cfg(test)]
mod tests {
    use astralog_raft::{LogIndex, NodeId, PreVoteRequest, Term};

    use super::*;

    fn frame() -> InternalFrame {
        InternalFrame {
            source_broker_id: 7,
            source_incarnation: 11,
            topic_id: TopicId::new([9; 16]),
            partition: 3,
            message: Some(RaftMessage::PreVoteRequest(PreVoteRequest {
                prospective_term: Term::new(5),
                candidate_id: NodeId::new(8).unwrap(),
                last_log_index: LogIndex::new(4),
                last_log_term: Term::new(3),
            })),
            rebootstrap: None,
            clean_progress: None,
        }
    }

    #[test]
    fn internal_frame_round_trip() {
        let value = frame();
        let bytes = encode_internal_frame(&value, TransportLimits::default()).unwrap();
        assert_eq!(
            decode_internal_frame(&bytes, TransportLimits::default()).unwrap(),
            value
        );
    }

    #[test]
    fn every_fragment_boundary_reassembles_one_frame() {
        let value = frame();
        let bytes = encode_internal_frame(&value, TransportLimits::default()).unwrap();
        for split in 0..bytes.len() {
            let mut decoder = InternalFrameDecoder::new(TransportLimits::default());
            assert!(decoder.push(&bytes[..split]).unwrap().is_empty());
            let decoded = decoder.push(&bytes[split..]).unwrap();
            assert_eq!(decoded, vec![value.clone()]);
            assert_eq!(decoder.buffered_bytes(), 0);
        }
    }

    #[test]
    fn crc_mutation_and_oversized_prefix_are_rejected() {
        let mut bytes = encode_internal_frame(&frame(), TransportLimits::default()).unwrap();
        bytes[20] ^= 1;
        assert!(matches!(
            decode_internal_frame(&bytes, TransportLimits::default()),
            Err(ReplicationError::ChecksumMismatch { .. })
        ));

        let limits = TransportLimits {
            max_frame_bytes: 128,
        };
        let mut decoder = InternalFrameDecoder::new(limits);
        assert!(decoder.push(&129_u32.to_be_bytes()).is_err());
    }

    #[test]
    fn decoder_handles_multiple_frames_without_resynchronization() {
        let value = frame();
        let first = encode_internal_frame(&value, TransportLimits::default()).unwrap();
        let mut second_value = value.clone();
        second_value.partition = 4;
        let second = encode_internal_frame(&second_value, TransportLimits::default()).unwrap();
        let mut bytes = first;
        bytes.extend_from_slice(&second);
        let mut decoder = InternalFrameDecoder::new(TransportLimits::default());
        assert_eq!(decoder.push(&bytes).unwrap(), vec![value, second_value]);
    }

    #[test]
    fn install_snapshot_can_carry_rebootstrap_package() {
        let mut value = frame();
        value.message = Some(RaftMessage::InstallSnapshotRequest(
            astralog_raft::InstallSnapshotRequest {
                term: Term::new(5),
                leader_id: NodeId::new(8).unwrap(),
                snapshot: astralog_raft::SnapshotMetadata::new(
                    LogIndex::new(4),
                    Term::new(3),
                    astralog_raft::StableConfiguration::new(
                        [NodeId::new(7).unwrap(), NodeId::new(8).unwrap()],
                        [],
                    )
                    .unwrap(),
                    astralog_raft::SnapshotId::new([6; 16]),
                )
                .unwrap(),
            },
        ));
        value.rebootstrap = Some(vec![1, 2, 3, 4]);
        let bytes = encode_internal_frame(&value, TransportLimits::default()).unwrap();
        assert_eq!(
            decode_internal_frame(&bytes, TransportLimits::default()).unwrap(),
            value
        );
    }

    #[test]
    fn p9g_sideband_frame_roundtrip_crc_and_identity_fence() {
        let mut value = frame();
        value.message = None;
        value.clean_progress = Some(CleanProgressReport {
            topic_id: value.topic_id,
            partition_id: astralog_common::PartitionId::new(value.partition),
            term: 8,
            leader_epoch: 8,
            source_broker_id: value.source_broker_id,
            source_incarnation: value.source_incarnation,
            assignment_generation: 12,
            clean_compacted_offset: astralog_common::StreamOffset::new(42),
        });
        let bytes = encode_internal_frame(&value, TransportLimits::default()).unwrap();
        assert_eq!(
            u16::from_be_bytes([bytes[6], bytes[7]]),
            MESSAGE_KIND_CLEAN_PROGRESS
        );
        assert_eq!(
            decode_internal_frame(&bytes, TransportLimits::default()).unwrap(),
            value
        );
        let mut corrupted = bytes;
        corrupted[64] ^= 1;
        assert!(decode_internal_frame(&corrupted, TransportLimits::default()).is_err());
        let mut mismatched = value;
        mismatched.source_broker_id = 8;
        assert!(encode_internal_frame(&mismatched, TransportLimits::default()).is_err());
    }
}
