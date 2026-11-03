use astralog_raft::{
    AppendEntriesRequest, AppendEntriesResponse, EntryKind, InstallSnapshotRequest,
    InstallSnapshotResponse, LogEntry, LogIndex, NodeId, PreVoteRequest, PreVoteResponse,
    RaftMessage, RequestVoteRequest, RequestVoteResponse, SnapshotId, SnapshotMetadata,
    StableConfiguration, Term, TimeoutNow,
};

use crate::{ControllerError, Result};

const FRAME_MAGIC: &[u8; 8] = b"ASTCTRP1";
const FRAME_VERSION: u16 = 1;
const FRAME_HEADER_LEN: usize = 48;
const FRAME_FLAG_SNAPSHOT_PACKAGE: u16 = 0x0001;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControllerTransportLimits {
    pub max_frame_bytes: usize,
    pub max_raft_entries: usize,
    pub max_entry_payload_bytes: usize,
    pub max_snapshot_package_bytes: usize,
    pub max_controller_members: usize,
}

impl Default for ControllerTransportLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 64 * 1024 * 1024,
            max_raft_entries: 10_000,
            max_entry_payload_bytes: 4 * 1024 * 1024,
            max_snapshot_package_bytes: 64 * 1024 * 1024,
            max_controller_members: 5,
        }
    }
}

impl ControllerTransportLimits {
    pub fn validate(self) -> Result<()> {
        if self.max_frame_bytes < FRAME_HEADER_LEN + 4
            || self.max_raft_entries == 0
            || self.max_entry_payload_bytes == 0
            || self.max_snapshot_package_bytes == 0
            || self.max_controller_members < 5
        {
            return Err(ControllerError::Invalid(
                "invalid controller transport resource limits",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControllerFrame {
    pub cluster_id: [u8; 16],
    pub source_controller_id: u64,
    pub message: RaftMessage,
    pub snapshot_package: Option<Vec<u8>>,
}

pub fn encode_controller_frame(
    frame: &ControllerFrame,
    limits: ControllerTransportLimits,
) -> Result<Vec<u8>> {
    limits.validate()?;
    if frame.cluster_id == [0; 16] || frame.source_controller_id == 0 {
        return Err(ControllerError::Invalid(
            "controller transport frame has reserved identity",
        ));
    }
    let raft = encode_raft_message(&frame.message, limits)?;
    let package = frame.snapshot_package.as_deref().unwrap_or(&[]);
    if !package.is_empty() && !matches!(frame.message, RaftMessage::InstallSnapshotRequest(_)) {
        return Err(ControllerError::Invalid(
            "controller snapshot package is only valid with InstallSnapshotRequest",
        ));
    }
    if package.len() > limits.max_snapshot_package_bytes {
        return Err(ControllerError::LimitExceeded {
            what: "controller snapshot transport package",
            actual: package.len(),
            max: limits.max_snapshot_package_bytes,
        });
    }
    let flags = if package.is_empty() {
        0
    } else {
        FRAME_FLAG_SNAPSHOT_PACKAGE
    };
    let raft_len = u32::try_from(raft.len())
        .map_err(|_| ControllerError::Invalid("controller Raft payload length overflow"))?;
    let package_len = u32::try_from(package.len())
        .map_err(|_| ControllerError::Invalid("controller snapshot package length overflow"))?;
    let body_len = FRAME_HEADER_LEN
        .checked_add(raft.len())
        .and_then(|value| value.checked_add(package.len()))
        .and_then(|value| value.checked_add(4))
        .ok_or(ControllerError::Invalid("controller frame length overflow"))?;
    if body_len > limits.max_frame_bytes || body_len > u32::MAX as usize {
        return Err(ControllerError::LimitExceeded {
            what: "controller transport frame",
            actual: body_len,
            max: limits.max_frame_bytes,
        });
    }
    let mut out = Vec::with_capacity(4 + body_len);
    out.extend_from_slice(&(body_len as u32).to_be_bytes());
    out.extend_from_slice(FRAME_MAGIC);
    out.extend_from_slice(&FRAME_VERSION.to_be_bytes());
    out.extend_from_slice(&(FRAME_HEADER_LEN as u16).to_be_bytes());
    out.extend_from_slice(&frame.cluster_id);
    out.extend_from_slice(&frame.source_controller_id.to_be_bytes());
    out.extend_from_slice(&flags.to_be_bytes());
    out.extend_from_slice(&0_u16.to_be_bytes());
    out.extend_from_slice(&raft_len.to_be_bytes());
    out.extend_from_slice(&package_len.to_be_bytes());
    out.extend_from_slice(&raft);
    out.extend_from_slice(package);
    let crc = crc32c::crc32c(&out[4..]);
    out.extend_from_slice(&crc.to_be_bytes());
    Ok(out)
}

pub fn decode_controller_frame(
    bytes: &[u8],
    limits: ControllerTransportLimits,
) -> Result<ControllerFrame> {
    limits.validate()?;
    if bytes.len() < 4 + FRAME_HEADER_LEN + 4 {
        return Err(ControllerError::Invalid(
            "controller transport frame is truncated",
        ));
    }
    let body_len = usize::try_from(u32::from_be_bytes(
        bytes[0..4].try_into().expect("4-byte frame prefix"),
    ))
    .map_err(|_| ControllerError::Invalid("controller frame length overflow"))?;
    if body_len > limits.max_frame_bytes || bytes.len() != 4 + body_len {
        return Err(ControllerError::Invalid(
            "controller transport frame length mismatch",
        ));
    }
    let body = &bytes[4..];
    if &body[0..8] != FRAME_MAGIC
        || u16::from_be_bytes(body[8..10].try_into().expect("2-byte version")) != FRAME_VERSION
        || usize::from(u16::from_be_bytes(
            body[10..12].try_into().expect("2-byte header length"),
        )) != FRAME_HEADER_LEN
    {
        return Err(ControllerError::Invalid(
            "controller transport frame header mismatch",
        ));
    }
    let mut cluster_id = [0_u8; 16];
    cluster_id.copy_from_slice(&body[12..28]);
    let source_controller_id = u64::from_be_bytes(body[28..36].try_into().expect("8-byte id"));
    let flags = u16::from_be_bytes(body[36..38].try_into().expect("2-byte flags"));
    let reserved = u16::from_be_bytes(body[38..40].try_into().expect("2-byte reserved"));
    if flags & !FRAME_FLAG_SNAPSHOT_PACKAGE != 0 || reserved != 0 {
        return Err(ControllerError::Invalid(
            "controller transport frame has unsupported flags/reserved bits",
        ));
    }
    let raft_len = usize::try_from(u32::from_be_bytes(
        body[40..44].try_into().expect("4-byte raft length"),
    ))
    .map_err(|_| ControllerError::Invalid("controller Raft payload length overflow"))?;
    let package_len = usize::try_from(u32::from_be_bytes(
        body[44..48].try_into().expect("4-byte package length"),
    ))
    .map_err(|_| ControllerError::Invalid("controller snapshot package length overflow"))?;
    if package_len > limits.max_snapshot_package_bytes {
        return Err(ControllerError::LimitExceeded {
            what: "controller snapshot transport package",
            actual: package_len,
            max: limits.max_snapshot_package_bytes,
        });
    }
    let payload_end = FRAME_HEADER_LEN
        .checked_add(raft_len)
        .and_then(|value| value.checked_add(package_len))
        .ok_or(ControllerError::Invalid(
            "controller frame payload length overflow",
        ))?;
    if payload_end + 4 != body.len() {
        return Err(ControllerError::Invalid(
            "controller transport payload length mismatch",
        ));
    }
    let expected_crc = u32::from_be_bytes(
        body[payload_end..payload_end + 4]
            .try_into()
            .expect("4-byte frame crc"),
    );
    let actual_crc = crc32c::crc32c(&body[..payload_end]);
    if expected_crc != actual_crc {
        return Err(ControllerError::ChecksumMismatch {
            context: "controller transport frame",
            expected: expected_crc,
            actual: actual_crc,
        });
    }
    if cluster_id == [0; 16] || source_controller_id == 0 {
        return Err(ControllerError::Invalid(
            "controller transport frame has reserved identity",
        ));
    }
    let raft_start = FRAME_HEADER_LEN;
    let raft_end = raft_start + raft_len;
    let message = decode_raft_message(&body[raft_start..raft_end], limits)?;
    let package = &body[raft_end..payload_end];
    let snapshot_package = if flags & FRAME_FLAG_SNAPSHOT_PACKAGE != 0 {
        if package.is_empty() || !matches!(message, RaftMessage::InstallSnapshotRequest(_)) {
            return Err(ControllerError::Invalid(
                "invalid controller snapshot-package transport combination",
            ));
        }
        Some(package.to_vec())
    } else {
        if !package.is_empty() {
            return Err(ControllerError::Invalid(
                "controller frame carries undeclared snapshot bytes",
            ));
        }
        None
    };
    Ok(ControllerFrame {
        cluster_id,
        source_controller_id,
        message,
        snapshot_package,
    })
}

pub struct ControllerFrameDecoder {
    limits: ControllerTransportLimits,
    buffer: Vec<u8>,
}

impl ControllerFrameDecoder {
    pub fn new(limits: ControllerTransportLimits) -> Self {
        Self {
            limits,
            buffer: Vec::new(),
        }
    }

    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<ControllerFrame>> {
        self.buffer.extend_from_slice(bytes);
        let mut frames = Vec::new();
        loop {
            if self.buffer.len() < 4 {
                break;
            }
            let body_len = usize::try_from(u32::from_be_bytes(
                self.buffer[0..4].try_into().expect("4-byte frame prefix"),
            ))
            .map_err(|_| ControllerError::Invalid("controller frame length overflow"))?;
            if body_len > self.limits.max_frame_bytes || body_len < FRAME_HEADER_LEN + 4 {
                return Err(ControllerError::Invalid(
                    "controller frame prefix is outside allowed bounds",
                ));
            }
            let full = 4_usize
                .checked_add(body_len)
                .ok_or(ControllerError::Invalid("controller frame length overflow"))?;
            if self.buffer.len() < full {
                break;
            }
            let frame_bytes = self.buffer.drain(..full).collect::<Vec<_>>();
            frames.push(decode_controller_frame(&frame_bytes, self.limits)?);
        }
        Ok(frames)
    }
}

fn encode_raft_message(
    message: &RaftMessage,
    limits: ControllerTransportLimits,
) -> Result<Vec<u8>> {
    let mut writer = Writer::new();
    match message {
        RaftMessage::PreVoteRequest(value) => {
            writer.u16(1);
            writer.term(value.prospective_term);
            writer.node(value.candidate_id);
            writer.index(value.last_log_index);
            writer.term(value.last_log_term);
        }
        RaftMessage::PreVoteResponse(value) => {
            writer.u16(2);
            writer.term(value.prospective_term);
            writer.node(value.voter_id);
            writer.bool(value.vote_granted);
        }
        RaftMessage::RequestVoteRequest(value) => {
            writer.u16(3);
            writer.term(value.term);
            writer.node(value.candidate_id);
            writer.index(value.last_log_index);
            writer.term(value.last_log_term);
        }
        RaftMessage::RequestVoteResponse(value) => {
            writer.u16(4);
            writer.term(value.term);
            writer.node(value.voter_id);
            writer.bool(value.vote_granted);
        }
        RaftMessage::AppendEntriesRequest(value) => {
            writer.u16(5);
            writer.term(value.term);
            writer.node(value.leader_id);
            writer.index(value.prev_log_index);
            writer.term(value.prev_log_term);
            if value.entries.len() > limits.max_raft_entries {
                return Err(ControllerError::LimitExceeded {
                    what: "controller AppendEntries entry count",
                    actual: value.entries.len(),
                    max: limits.max_raft_entries,
                });
            }
            writer.u32_len(value.entries.len())?;
            for entry in &value.entries {
                encode_log_entry(&mut writer, entry, limits)?;
            }
            writer.index(value.leader_commit);
        }
        RaftMessage::AppendEntriesResponse(value) => {
            writer.u16(6);
            writer.term(value.term);
            writer.node(value.follower_id);
            writer.bool(value.success);
            writer.index(value.match_index);
            writer.bool(value.conflict_term.is_some());
            if let Some(term) = value.conflict_term {
                writer.term(term);
            }
            writer.index(value.conflict_index);
        }
        RaftMessage::InstallSnapshotRequest(value) => {
            writer.u16(7);
            writer.term(value.term);
            writer.node(value.leader_id);
            encode_snapshot_metadata(&mut writer, &value.snapshot, limits)?;
        }
        RaftMessage::InstallSnapshotResponse(value) => {
            writer.u16(8);
            writer.term(value.term);
            writer.node(value.follower_id);
            writer.bool(value.success);
            writer.index(value.match_index);
        }
        RaftMessage::TimeoutNow(value) => {
            writer.u16(9);
            writer.term(value.term);
            writer.node(value.leader_id);
        }
    }
    Ok(writer.finish())
}

fn decode_raft_message(bytes: &[u8], limits: ControllerTransportLimits) -> Result<RaftMessage> {
    let mut reader = Reader::new(bytes);
    let message = match reader.u16()? {
        1 => RaftMessage::PreVoteRequest(PreVoteRequest {
            prospective_term: reader.term()?,
            candidate_id: reader.node()?,
            last_log_index: reader.index()?,
            last_log_term: reader.term()?,
        }),
        2 => RaftMessage::PreVoteResponse(PreVoteResponse {
            prospective_term: reader.term()?,
            voter_id: reader.node()?,
            vote_granted: reader.bool()?,
        }),
        3 => RaftMessage::RequestVoteRequest(RequestVoteRequest {
            term: reader.term()?,
            candidate_id: reader.node()?,
            last_log_index: reader.index()?,
            last_log_term: reader.term()?,
        }),
        4 => RaftMessage::RequestVoteResponse(RequestVoteResponse {
            term: reader.term()?,
            voter_id: reader.node()?,
            vote_granted: reader.bool()?,
        }),
        5 => {
            let term = reader.term()?;
            let leader_id = reader.node()?;
            let prev_log_index = reader.index()?;
            let prev_log_term = reader.term()?;
            let count = reader.count(
                limits.max_raft_entries,
                "controller AppendEntries entry count",
            )?;
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                entries.push(decode_log_entry(&mut reader, limits)?);
            }
            RaftMessage::AppendEntriesRequest(AppendEntriesRequest {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: reader.index()?,
            })
        }
        6 => RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
            term: reader.term()?,
            follower_id: reader.node()?,
            success: reader.bool()?,
            match_index: reader.index()?,
            conflict_term: if reader.bool()? {
                Some(reader.term()?)
            } else {
                None
            },
            conflict_index: reader.index()?,
        }),
        7 => RaftMessage::InstallSnapshotRequest(InstallSnapshotRequest {
            term: reader.term()?,
            leader_id: reader.node()?,
            snapshot: decode_snapshot_metadata(&mut reader, limits)?,
        }),
        8 => RaftMessage::InstallSnapshotResponse(InstallSnapshotResponse {
            term: reader.term()?,
            follower_id: reader.node()?,
            success: reader.bool()?,
            match_index: reader.index()?,
        }),
        9 => RaftMessage::TimeoutNow(TimeoutNow {
            term: reader.term()?,
            leader_id: reader.node()?,
        }),
        _ => {
            return Err(ControllerError::Invalid(
                "unknown controller Raft message kind",
            ));
        }
    };
    reader.finish()?;
    if encode_raft_message(&message, limits)? != bytes {
        return Err(ControllerError::Invalid(
            "controller Raft message encoding is not canonical",
        ));
    }
    Ok(message)
}

fn encode_log_entry(
    writer: &mut Writer,
    entry: &LogEntry,
    limits: ControllerTransportLimits,
) -> Result<()> {
    writer.term(entry.term);
    writer.index(entry.index);
    match &entry.kind {
        EntryKind::Application(payload) => {
            if payload.len() > limits.max_entry_payload_bytes {
                return Err(ControllerError::LimitExceeded {
                    what: "controller Raft application payload",
                    actual: payload.len(),
                    max: limits.max_entry_payload_bytes,
                });
            }
            writer.u8(1);
            writer.bytes32(payload)?;
        }
        EntryKind::LeaderBarrier => writer.u8(2),
        EntryKind::Configuration(_) => {
            return Err(ControllerError::Invalid(
                "dynamic controller membership is outside P5 transport scope",
            ));
        }
    }
    Ok(())
}

fn decode_log_entry(
    reader: &mut Reader<'_>,
    limits: ControllerTransportLimits,
) -> Result<LogEntry> {
    let term = reader.term()?;
    let index = reader.index()?;
    let kind = match reader.u8()? {
        1 => EntryKind::Application(
            reader
                .bytes32(
                    limits.max_entry_payload_bytes,
                    "controller Raft application payload",
                )?
                .to_vec(),
        ),
        2 => EntryKind::LeaderBarrier,
        _ => {
            return Err(ControllerError::Invalid(
                "unsupported controller Raft entry kind",
            ));
        }
    };
    Ok(LogEntry::new(term, index, kind))
}

fn encode_snapshot_metadata(
    writer: &mut Writer,
    metadata: &SnapshotMetadata,
    limits: ControllerTransportLimits,
) -> Result<()> {
    writer.index(metadata.last_included_index);
    writer.term(metadata.last_included_term);
    writer.raw(metadata.snapshot_id.as_bytes());
    encode_stable_config(writer, &metadata.configuration, limits)
}

fn decode_snapshot_metadata(
    reader: &mut Reader<'_>,
    limits: ControllerTransportLimits,
) -> Result<SnapshotMetadata> {
    let index = reader.index()?;
    let term = reader.term()?;
    let mut snapshot_id = [0_u8; 16];
    snapshot_id.copy_from_slice(reader.raw(16)?);
    let configuration = decode_stable_config(reader, limits)?;
    Ok(SnapshotMetadata::new(
        index,
        term,
        configuration,
        SnapshotId::new(snapshot_id),
    )?)
}

fn encode_stable_config(
    writer: &mut Writer,
    config: &StableConfiguration,
    limits: ControllerTransportLimits,
) -> Result<()> {
    if config.voter_count() > limits.max_controller_members
        || config.learners().len() > limits.max_controller_members
    {
        return Err(ControllerError::LimitExceeded {
            what: "controller Raft membership",
            actual: config.voter_count().saturating_add(config.learners().len()),
            max: limits.max_controller_members * 2,
        });
    }
    writer.u16_len(config.voter_count())?;
    writer.u16_len(config.learners().len())?;
    for node in config.voters() {
        writer.node(node);
    }
    for node in config.learners() {
        writer.node(node);
    }
    Ok(())
}

fn decode_stable_config(
    reader: &mut Reader<'_>,
    limits: ControllerTransportLimits,
) -> Result<StableConfiguration> {
    let voter_count = usize::from(reader.u16()?);
    let learner_count = usize::from(reader.u16()?);
    if voter_count == 0
        || voter_count > limits.max_controller_members
        || learner_count > limits.max_controller_members
    {
        return Err(ControllerError::Invalid(
            "invalid controller Raft stable configuration size",
        ));
    }
    let mut voters = Vec::with_capacity(voter_count);
    let mut learners = Vec::with_capacity(learner_count);
    for _ in 0..voter_count {
        voters.push(reader.node()?);
    }
    for _ in 0..learner_count {
        learners.push(reader.node()?);
    }
    Ok(StableConfiguration::new(voters, learners)?)
}

struct Writer {
    bytes: Vec<u8>,
}

impl Writer {
    fn new() -> Self {
        Self { bytes: Vec::new() }
    }
    fn finish(self) -> Vec<u8> {
        self.bytes
    }
    fn raw(&mut self, bytes: &[u8]) {
        self.bytes.extend_from_slice(bytes);
    }
    fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }
    fn bool(&mut self, value: bool) {
        self.u8(u8::from(value));
    }
    fn u16(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }
    fn u16_len(&mut self, value: usize) -> Result<()> {
        self.u16(
            u16::try_from(value).map_err(|_| {
                ControllerError::Invalid("controller transport u16 length overflow")
            })?,
        );
        Ok(())
    }
    fn u32_len(&mut self, value: usize) -> Result<()> {
        self.bytes.extend_from_slice(
            &u32::try_from(value)
                .map_err(|_| ControllerError::Invalid("controller transport u32 length overflow"))?
                .to_be_bytes(),
        );
        Ok(())
    }
    fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }
    fn node(&mut self, value: NodeId) {
        self.u64(value.get());
    }
    fn term(&mut self, value: Term) {
        self.u64(value.get());
    }
    fn index(&mut self, value: LogIndex) {
        self.u64(value.get());
    }
    fn bytes32(&mut self, bytes: &[u8]) -> Result<()> {
        self.u32_len(bytes.len())?;
        self.raw(bytes);
        Ok(())
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }
    fn raw(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(ControllerError::Invalid(
                "controller transport decoder overflow",
            ))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(ControllerError::Invalid(
                "controller transport payload is truncated",
            ))?;
        self.offset = end;
        Ok(value)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.raw(1)?[0])
    }
    fn bool(&mut self) -> Result<bool> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(ControllerError::Invalid(
                "controller transport boolean is non-canonical",
            )),
        }
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(
            self.raw(2)?.try_into().expect("2-byte transport field"),
        ))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(
            self.raw(4)?.try_into().expect("4-byte transport field"),
        ))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(
            self.raw(8)?.try_into().expect("8-byte transport field"),
        ))
    }
    fn node(&mut self) -> Result<NodeId> {
        Ok(NodeId::new(self.u64()?)?)
    }
    fn term(&mut self) -> Result<Term> {
        Ok(Term::new(self.u64()?))
    }
    fn index(&mut self) -> Result<LogIndex> {
        Ok(LogIndex::new(self.u64()?))
    }
    fn count(&mut self, max: usize, what: &'static str) -> Result<usize> {
        let count = usize::try_from(self.u32()?)
            .map_err(|_| ControllerError::Invalid("controller transport count overflow"))?;
        if count > max {
            return Err(ControllerError::LimitExceeded {
                what,
                actual: count,
                max,
            });
        }
        Ok(count)
    }
    fn bytes32(&mut self, max: usize, what: &'static str) -> Result<&'a [u8]> {
        let len = self.count(max, what)?;
        self.raw(len)
    }
    fn finish(self) -> Result<()> {
        if self.offset != self.bytes.len() {
            return Err(ControllerError::Invalid(
                "controller transport payload has trailing bytes",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(value: u64) -> NodeId {
        NodeId::new(value).unwrap()
    }

    fn frame() -> ControllerFrame {
        ControllerFrame {
            cluster_id: [7; 16],
            source_controller_id: 1,
            message: RaftMessage::PreVoteRequest(PreVoteRequest {
                prospective_term: Term::new(2),
                candidate_id: node(1),
                last_log_index: LogIndex::new(3),
                last_log_term: Term::new(1),
            }),
            snapshot_package: None,
        }
    }

    #[test]
    fn controller_frame_round_trip_crc_fragmentation_and_no_resync() {
        let limits = ControllerTransportLimits::default();
        let value = frame();
        let bytes = encode_controller_frame(&value, limits).unwrap();
        assert_eq!(decode_controller_frame(&bytes, limits).unwrap(), value);
        for split in 0..bytes.len() {
            let mut decoder = ControllerFrameDecoder::new(limits);
            assert!(decoder.push(&bytes[..split]).unwrap().is_empty());
            let frames = decoder.push(&bytes[split..]).unwrap();
            assert_eq!(frames, vec![value.clone()]);
        }
        let mut corrupt = bytes.clone();
        corrupt[20] ^= 1;
        assert!(decode_controller_frame(&corrupt, limits).is_err());
        let mut oversized = bytes;
        oversized[..4].copy_from_slice(&u32::MAX.to_be_bytes());
        let mut decoder = ControllerFrameDecoder::new(limits);
        assert!(decoder.push(&oversized).is_err());
    }

    #[test]
    fn snapshot_package_requires_install_snapshot_message() {
        let limits = ControllerTransportLimits::default();
        let mut value = frame();
        value.snapshot_package = Some(vec![1, 2, 3]);
        assert!(encode_controller_frame(&value, limits).is_err());
    }
}
