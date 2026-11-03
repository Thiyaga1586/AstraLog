use std::collections::BTreeMap;

use astralog_common::{ProducerEpoch, ProducerId};

use crate::{
    ControllerError, ControllerSnapshot, Result, SnapshotLimits, decode_controller_snapshot,
    encode_controller_snapshot,
};

const SNAPSHOT_VERSION_V2: u16 = 2;
const SNAPSHOT_HEADER_LEN_V2: u16 = 16;
pub const MAX_PRODUCER_SESSIONS: usize = 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerSessionRecordV1 {
    pub producer_id: ProducerId,
    pub current_epoch: ProducerEpoch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControllerSnapshotV2 {
    pub base: ControllerSnapshot,
    pub producer_sessions: BTreeMap<ProducerId, ProducerEpoch>,
}

impl ControllerSnapshotV2 {
    pub fn from_state(state: crate::ControllerState) -> Self {
        let producer_sessions = state.producer_sessions().clone();
        Self {
            base: ControllerSnapshot::new(state),
            producer_sessions,
        }
    }

    pub fn into_controller_snapshot(self) -> Result<ControllerSnapshot> {
        let mut state = self.base.state;
        state.install_producer_sessions(self.producer_sessions)?;
        Ok(ControllerSnapshot::new(state))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodedControllerSnapshot {
    V1(ControllerSnapshot),
    V2(ControllerSnapshotV2),
    V3(crate::ControllerSnapshotV3),
    V4(crate::ControllerSnapshotV4),
}

pub fn encode_controller_state_snapshot(
    state: &crate::ControllerState,
    limits: SnapshotLimits,
) -> Result<Vec<u8>> {
    if state.active_feature_level() >= crate::P9_RETENTION_FEATURE_LEVEL {
        crate::encode_controller_snapshot_v4(
            &crate::ControllerSnapshotV4::from_state(state.clone()),
            limits,
        )
    } else if state.active_feature_level() >= crate::P8_PERFORMANCE_FEATURE_LEVEL {
        crate::encode_controller_snapshot_v3(
            &crate::ControllerSnapshotV3::from_state(state.clone()),
            limits,
        )
    } else if state.producer_sessions().is_empty() {
        encode_controller_snapshot(&ControllerSnapshot::new(state.clone()), limits)
    } else {
        encode_controller_snapshot_v2(&ControllerSnapshotV2::from_state(state.clone()), limits)
    }
}

pub fn decode_controller_state_snapshot(
    bytes: &[u8],
    limits: SnapshotLimits,
) -> Result<ControllerSnapshot> {
    match decode_controller_snapshot_any(bytes, limits)? {
        DecodedControllerSnapshot::V1(snapshot) => Ok(snapshot),
        DecodedControllerSnapshot::V2(snapshot) => snapshot.into_controller_snapshot(),
        DecodedControllerSnapshot::V3(snapshot) => snapshot.into_controller_snapshot(),
        DecodedControllerSnapshot::V4(snapshot) => snapshot.into_controller_snapshot(),
    }
}

pub fn encode_controller_snapshot_v2(
    snapshot: &ControllerSnapshotV2,
    limits: SnapshotLimits,
) -> Result<Vec<u8>> {
    if snapshot.producer_sessions.len() > MAX_PRODUCER_SESSIONS {
        return Err(ControllerError::LimitExceeded {
            what: "producer session count",
            actual: snapshot.producer_sessions.len(),
            max: MAX_PRODUCER_SESSIONS,
        });
    }
    let base = encode_controller_snapshot(&snapshot.base, limits)?;
    let base_len = u32::try_from(base.len())
        .map_err(|_| ControllerError::Invalid("base snapshot length overflow"))?;
    let session_count = u32::try_from(snapshot.producer_sessions.len())
        .map_err(|_| ControllerError::Invalid("producer session count overflow"))?;
    let sessions_bytes = snapshot
        .producer_sessions
        .len()
        .checked_mul(12)
        .ok_or(ControllerError::Invalid("producer session bytes overflow"))?;
    let total = usize::from(SNAPSHOT_HEADER_LEN_V2)
        .checked_add(base.len())
        .and_then(|value| value.checked_add(sessions_bytes))
        .and_then(|value| value.checked_add(4))
        .ok_or(ControllerError::Invalid(
            "controller snapshot V2 length overflow",
        ))?;
    if total > limits.max_snapshot_bytes {
        return Err(ControllerError::LimitExceeded {
            what: "controller snapshot bytes",
            actual: total,
            max: limits.max_snapshot_bytes,
        });
    }

    let mut out = Vec::with_capacity(total);
    put_u16(&mut out, SNAPSHOT_VERSION_V2);
    put_u16(&mut out, SNAPSHOT_HEADER_LEN_V2);
    put_u32(&mut out, base_len);
    put_u32(&mut out, session_count);
    put_u32(&mut out, 0);
    out.extend_from_slice(&base);
    for (producer_id, producer_epoch) in &snapshot.producer_sessions {
        put_u64(&mut out, producer_id.get());
        put_u32(&mut out, producer_epoch.get());
    }
    let crc = crc32c::crc32c(&out);
    put_u32(&mut out, crc);
    Ok(out)
}

pub fn decode_controller_snapshot_any(
    bytes: &[u8],
    limits: SnapshotLimits,
) -> Result<DecodedControllerSnapshot> {
    if bytes.len() < 2 {
        return Err(ControllerError::Invalid("controller snapshot is truncated"));
    }
    let version = u16::from_le_bytes(bytes[..2].try_into().expect("2 bytes"));
    match version {
        1 => Ok(DecodedControllerSnapshot::V1(decode_controller_snapshot(
            bytes, limits,
        )?)),
        SNAPSHOT_VERSION_V2 => Ok(DecodedControllerSnapshot::V2(
            decode_controller_snapshot_v2(bytes, limits)?,
        )),
        3 => Ok(DecodedControllerSnapshot::V3(
            crate::decode_controller_snapshot_v3(bytes, limits)?,
        )),
        4 => Ok(DecodedControllerSnapshot::V4(
            crate::decode_controller_snapshot_v4(bytes, limits)?,
        )),
        _ => Err(ControllerError::Invalid(
            "unsupported controller snapshot version",
        )),
    }
}

pub fn decode_controller_snapshot_v2(
    bytes: &[u8],
    limits: SnapshotLimits,
) -> Result<ControllerSnapshotV2> {
    if bytes.len() > limits.max_snapshot_bytes {
        return Err(ControllerError::LimitExceeded {
            what: "controller snapshot bytes",
            actual: bytes.len(),
            max: limits.max_snapshot_bytes,
        });
    }
    if bytes.len() < usize::from(SNAPSHOT_HEADER_LEN_V2) + 4 {
        return Err(ControllerError::Invalid(
            "controller snapshot V2 is truncated",
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
            context: "controller snapshot V2",
            expected: expected_crc,
            actual: actual_crc,
        });
    }

    let mut reader = Reader::new(&bytes[..bytes.len() - 4]);
    if reader.u16()? != SNAPSHOT_VERSION_V2 || reader.u16()? != SNAPSHOT_HEADER_LEN_V2 {
        return Err(ControllerError::Invalid(
            "controller snapshot V2 version/header mismatch",
        ));
    }
    let base_len = usize::try_from(reader.u32()?)
        .map_err(|_| ControllerError::Invalid("base snapshot length overflow"))?;
    let session_count = usize::try_from(reader.u32()?)
        .map_err(|_| ControllerError::Invalid("producer session count overflow"))?;
    if session_count > MAX_PRODUCER_SESSIONS {
        return Err(ControllerError::LimitExceeded {
            what: "producer session count",
            actual: session_count,
            max: MAX_PRODUCER_SESSIONS,
        });
    }
    if reader.u32()? != 0 {
        return Err(ControllerError::Invalid(
            "controller snapshot V2 reserved field is non-zero",
        ));
    }
    let minimum_sessions = session_count
        .checked_mul(12)
        .ok_or(ControllerError::Invalid("producer session bytes overflow"))?;
    if base_len > reader.remaining().saturating_sub(minimum_sessions) {
        return Err(ControllerError::Invalid(
            "controller snapshot V2 base length does not fit",
        ));
    }
    let base = decode_controller_snapshot(reader.take(base_len)?, limits)?;
    let mut producer_sessions = BTreeMap::new();
    let mut previous = None;
    for _ in 0..session_count {
        let producer_id = ProducerId::new(reader.u64()?)
            .ok_or(ControllerError::Invalid("producer id zero is reserved"))?;
        if previous.is_some_and(|prior: ProducerId| producer_id <= prior) {
            return Err(ControllerError::Invalid(
                "producer session table must be strictly ordered",
            ));
        }
        let producer_epoch = ProducerEpoch::new(reader.u32()?)
            .ok_or(ControllerError::Invalid("producer epoch zero is reserved"))?;
        producer_sessions.insert(producer_id, producer_epoch);
        previous = Some(producer_id);
    }
    if !reader.is_empty() {
        return Err(ControllerError::Invalid(
            "trailing bytes in controller snapshot V2",
        ));
    }
    let snapshot = ControllerSnapshotV2 {
        base,
        producer_sessions,
    };
    if encode_controller_snapshot_v2(&snapshot, limits)? != bytes {
        return Err(ControllerError::Invalid(
            "controller snapshot V2 is not canonical",
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
            .ok_or(ControllerError::Invalid("snapshot V2 reader overflow"))?;
        let value = self
            .bytes
            .get(self.pos..end)
            .ok_or(ControllerError::Invalid("snapshot V2 is truncated"))?;
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
    use crate::ControllerState;

    fn decode_hex(input: &str) -> Vec<u8> {
        let (pairs, remainder) = input.as_bytes().as_chunks::<2>();
        assert!(remainder.is_empty());
        pairs
            .iter()
            .map(|pair| u8::from_str_radix(core::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect()
    }

    fn base_snapshot() -> ControllerSnapshot {
        ControllerSnapshot::new(ControllerState::new([7; 16], 1, 100).unwrap())
    }

    #[test]
    fn v2_round_trip_wraps_exact_v1_snapshot_and_v1_decoder_rejects_v2() {
        let snapshot = ControllerSnapshotV2 {
            base: base_snapshot(),
            producer_sessions: BTreeMap::from([
                (
                    ProducerId::new(100).unwrap(),
                    ProducerEpoch::new(1).unwrap(),
                ),
                (
                    ProducerId::new(101).unwrap(),
                    ProducerEpoch::new(3).unwrap(),
                ),
            ]),
        };
        let bytes = encode_controller_snapshot_v2(&snapshot, SnapshotLimits::default()).unwrap();
        assert_eq!(
            bytes,
            decode_hex(
                "020010003c0000000200000000000000010028000000000000000000070707070707070707070707070707070100000064000000000000000000000000000000000000000000000097c338cb6400000000000000010000006500000000000000030000008f03c2ec"
            )
        );
        assert_eq!(
            decode_controller_snapshot_v2(&bytes, SnapshotLimits::default()).unwrap(),
            snapshot
        );
        assert!(decode_controller_snapshot(&bytes, SnapshotLimits::default()).is_err());
    }

    #[test]
    fn version_dispatch_keeps_v1_backward_decode_and_v2_crc_fails_closed() {
        let base = base_snapshot();
        let v1 = encode_controller_snapshot(&base, SnapshotLimits::default()).unwrap();
        assert_eq!(
            decode_controller_snapshot_any(&v1, SnapshotLimits::default()).unwrap(),
            DecodedControllerSnapshot::V1(base.clone())
        );

        let snapshot = ControllerSnapshotV2 {
            base,
            producer_sessions: BTreeMap::from([(
                ProducerId::new(100).unwrap(),
                ProducerEpoch::new(1).unwrap(),
            )]),
        };
        let mut bytes =
            encode_controller_snapshot_v2(&snapshot, SnapshotLimits::default()).unwrap();
        bytes[20] ^= 1;
        assert!(decode_controller_snapshot_v2(&bytes, SnapshotLimits::default()).is_err());
    }

    #[test]
    fn v2_decoder_rejects_noncanonical_session_order_with_valid_crc() {
        let snapshot = ControllerSnapshotV2 {
            base: base_snapshot(),
            producer_sessions: BTreeMap::from([
                (
                    ProducerId::new(100).unwrap(),
                    ProducerEpoch::new(1).unwrap(),
                ),
                (
                    ProducerId::new(101).unwrap(),
                    ProducerEpoch::new(3).unwrap(),
                ),
            ]),
        };
        let mut bytes =
            encode_controller_snapshot_v2(&snapshot, SnapshotLimits::default()).unwrap();
        let base_len =
            usize::try_from(u32::from_le_bytes(bytes[4..8].try_into().unwrap())).unwrap();
        let first = usize::from(SNAPSHOT_HEADER_LEN_V2) + base_len;
        let second = first + 12;
        let first_entry = bytes[first..first + 12].to_vec();
        let second_entry = bytes[second..second + 12].to_vec();
        bytes[first..first + 12].copy_from_slice(&second_entry);
        bytes[second..second + 12].copy_from_slice(&first_entry);
        let crc_offset = bytes.len() - 4;
        let crc = crc32c::crc32c(&bytes[..crc_offset]);
        bytes[crc_offset..].copy_from_slice(&crc.to_le_bytes());
        assert!(decode_controller_snapshot_v2(&bytes, SnapshotLimits::default()).is_err());
    }

    #[test]
    fn p7_v1_snapshot_migrates_to_empty_sessions_without_allocator_regression() {
        let state = ControllerState::new([8; 16], crate::P7_PRODUCER_FEATURE_LEVEL, 700).unwrap();
        let v1 = encode_controller_snapshot(
            &ControllerSnapshot::new(state.clone()),
            SnapshotLimits::default(),
        )
        .unwrap();
        let migrated = decode_controller_state_snapshot(&v1, SnapshotLimits::default()).unwrap();
        assert_eq!(migrated.state.next_unallocated_producer_id(), 700);
        assert!(migrated.state.producer_sessions().is_empty());
        assert_eq!(migrated.state, state);
    }

    #[test]
    fn p7_v2_state_snapshot_restores_sessions_and_rejects_invalid_authority() {
        let mut state =
            ControllerState::new([9; 16], crate::P7_PRODUCER_FEATURE_LEVEL, 100).unwrap();
        let producer_id = ProducerId::new(100).unwrap();
        let producer_epoch = ProducerEpoch::new(1).unwrap();
        state
            .apply_producer_session(
                1,
                crate::ProducerSessionCommandV2::InitializeSession {
                    producer_id,
                    producer_epoch,
                },
            )
            .unwrap();
        let bytes = encode_controller_state_snapshot(&state, SnapshotLimits::default()).unwrap();
        assert_eq!(u16::from_le_bytes(bytes[..2].try_into().unwrap()), 2);
        let restored = decode_controller_state_snapshot(&bytes, SnapshotLimits::default()).unwrap();
        assert_eq!(restored.state, state);

        let invalid = ControllerSnapshotV2 {
            base: ControllerSnapshot::new(
                ControllerState::new([9; 16], crate::P7_PRODUCER_FEATURE_LEVEL, 100).unwrap(),
            ),
            producer_sessions: BTreeMap::from([(producer_id, producer_epoch)]),
        };
        let bytes = encode_controller_snapshot_v2(&invalid, SnapshotLimits::default()).unwrap();
        assert!(decode_controller_state_snapshot(&bytes, SnapshotLimits::default()).is_err());
    }

    #[test]
    fn p7j_controller_snapshot_count_and_byte_one_over_fail_closed() {
        let snapshot = ControllerSnapshotV2 {
            base: base_snapshot(),
            producer_sessions: BTreeMap::from([(
                ProducerId::new(100).unwrap(),
                ProducerEpoch::new(1).unwrap(),
            )]),
        };
        let bytes = encode_controller_snapshot_v2(&snapshot, SnapshotLimits::default()).unwrap();

        let byte_limit = SnapshotLimits {
            max_snapshot_bytes: bytes.len() - 1,
            ..SnapshotLimits::default()
        };
        assert!(matches!(
            encode_controller_snapshot_v2(&snapshot, byte_limit),
            Err(ControllerError::LimitExceeded {
                what: "controller snapshot bytes",
                ..
            })
        ));
        assert!(matches!(
            decode_controller_snapshot_v2(&bytes, byte_limit),
            Err(ControllerError::LimitExceeded {
                what: "controller snapshot bytes",
                ..
            })
        ));

        let mut count_one_over = bytes.clone();
        count_one_over[8..12].copy_from_slice(
            &u32::try_from(MAX_PRODUCER_SESSIONS + 1)
                .unwrap()
                .to_le_bytes(),
        );
        let crc_offset = count_one_over.len() - 4;
        let crc = crc32c::crc32c(&count_one_over[..crc_offset]);
        count_one_over[crc_offset..].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            decode_controller_snapshot_v2(&count_one_over, SnapshotLimits::default()),
            Err(ControllerError::LimitExceeded {
                what: "producer session count",
                actual,
                max: MAX_PRODUCER_SESSIONS,
            }) if actual == MAX_PRODUCER_SESSIONS + 1
        ));
    }

    #[test]
    fn p7j_controller_producer_metadata_corruption_campaign_rejects_valid_crc_mutations() {
        let snapshot = ControllerSnapshotV2 {
            base: base_snapshot(),
            producer_sessions: BTreeMap::from([
                (
                    ProducerId::new(100).unwrap(),
                    ProducerEpoch::new(1).unwrap(),
                ),
                (
                    ProducerId::new(101).unwrap(),
                    ProducerEpoch::new(2).unwrap(),
                ),
            ]),
        };
        let canonical =
            encode_controller_snapshot_v2(&snapshot, SnapshotLimits::default()).unwrap();
        let base_len =
            usize::try_from(u32::from_le_bytes(canonical[4..8].try_into().unwrap())).unwrap();
        let first_session = usize::from(SNAPSHOT_HEADER_LEN_V2) + base_len;

        let mut mutations = Vec::new();

        let mut zero_id = canonical.clone();
        zero_id[first_session..first_session + 8].copy_from_slice(&0_u64.to_le_bytes());
        mutations.push(zero_id);

        let mut zero_epoch = canonical.clone();
        zero_epoch[first_session + 8..first_session + 12].copy_from_slice(&0_u32.to_le_bytes());
        mutations.push(zero_epoch);

        let mut reserved = canonical.clone();
        reserved[12..16].copy_from_slice(&1_u32.to_le_bytes());
        mutations.push(reserved);

        let mut noncanonical_order = canonical.clone();
        let second_session = first_session + 12;
        let first = noncanonical_order[first_session..first_session + 12].to_vec();
        let second = noncanonical_order[second_session..second_session + 12].to_vec();
        noncanonical_order[first_session..first_session + 12].copy_from_slice(&second);
        noncanonical_order[second_session..second_session + 12].copy_from_slice(&first);
        mutations.push(noncanonical_order);

        for mut bytes in mutations {
            let crc_offset = bytes.len() - 4;
            let crc = crc32c::crc32c(&bytes[..crc_offset]);
            bytes[crc_offset..].copy_from_slice(&crc.to_le_bytes());
            assert!(
                decode_controller_snapshot_v2(&bytes, SnapshotLimits::default()).is_err(),
                "P7-J corrupted producer authority metadata was accepted"
            );
        }
    }
}
