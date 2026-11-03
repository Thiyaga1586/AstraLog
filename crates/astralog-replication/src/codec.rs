use astralog_raft::{
    AppendEntriesRequest, AppendEntriesResponse, ConfigurationChange, EntryKind,
    InstallSnapshotRequest, InstallSnapshotResponse, LogEntry, LogIndex, NodeId, PreVoteRequest,
    PreVoteResponse, RaftMessage, RequestVoteRequest, RequestVoteResponse, SnapshotId,
    SnapshotMetadata, StableConfiguration, Term, TimeoutNow,
};

use crate::{ReplicationError, Result};

const MAX_MEMBERS: usize = 1024;
const MAX_ENTRIES: usize = 10_000;
const MAX_ENTRY_PAYLOAD: usize = 16 * 1024 * 1024;

pub(crate) fn encode_configuration_change(change: &ConfigurationChange) -> Result<Vec<u8>> {
    let mut writer = Writer::new();
    match change {
        ConfigurationChange::EnterJoint { old, new } => {
            writer.u8(1);
            encode_stable_config(&mut writer, old)?;
            encode_stable_config(&mut writer, new)?;
        }
        ConfigurationChange::LeaveJoint { new } => {
            writer.u8(2);
            encode_stable_config(&mut writer, new)?;
        }
    }
    Ok(writer.finish())
}

pub(crate) fn decode_configuration_change(bytes: &[u8]) -> Result<ConfigurationChange> {
    let mut reader = Reader::new(bytes);
    let change = match reader.u8()? {
        1 => ConfigurationChange::EnterJoint {
            old: decode_stable_config(&mut reader)?,
            new: decode_stable_config(&mut reader)?,
        },
        2 => ConfigurationChange::LeaveJoint {
            new: decode_stable_config(&mut reader)?,
        },
        _ => {
            return Err(ReplicationError::Invalid(
                "unknown configuration change kind",
            ));
        }
    };
    reader.finish()?;
    if encode_configuration_change(&change)? != bytes {
        return Err(ReplicationError::Invalid(
            "configuration change encoding is not canonical",
        ));
    }
    Ok(change)
}

pub(crate) fn encode_snapshot_metadata(metadata: &SnapshotMetadata) -> Result<Vec<u8>> {
    let mut writer = Writer::new();
    writer.u64(metadata.last_included_index.get());
    writer.u64(metadata.last_included_term.get());
    writer.bytes(metadata.snapshot_id.as_bytes());
    encode_stable_config(&mut writer, &metadata.configuration)?;
    Ok(writer.finish())
}

pub(crate) fn decode_snapshot_metadata(bytes: &[u8]) -> Result<SnapshotMetadata> {
    let mut reader = Reader::new(bytes);
    let index = LogIndex::new(reader.u64()?);
    let term = Term::new(reader.u64()?);
    let mut id = [0_u8; 16];
    id.copy_from_slice(reader.bytes(16)?);
    let configuration = decode_stable_config(&mut reader)?;
    reader.finish()?;
    Ok(SnapshotMetadata::new(
        index,
        term,
        configuration,
        SnapshotId::new(id),
    )?)
}

pub fn encode_raft_message(message: &RaftMessage) -> Result<Vec<u8>> {
    let mut writer = Writer::new();
    match message {
        RaftMessage::PreVoteRequest(value) => {
            writer.u16(1);
            writer.u64(value.prospective_term.get());
            writer.node(value.candidate_id);
            writer.index(value.last_log_index);
            writer.term(value.last_log_term);
        }
        RaftMessage::PreVoteResponse(value) => {
            writer.u16(2);
            writer.u64(value.prospective_term.get());
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
            if value.entries.len() > MAX_ENTRIES {
                return Err(ReplicationError::LimitExceeded {
                    what: "AppendEntries entry count",
                    actual: value.entries.len(),
                    max: MAX_ENTRIES,
                });
            }
            writer.u32(
                u32::try_from(value.entries.len())
                    .map_err(|_| ReplicationError::Invalid("entry count overflow"))?,
            );
            for entry in &value.entries {
                encode_log_entry(&mut writer, entry)?;
            }
            writer.index(value.leader_commit);
        }
        RaftMessage::AppendEntriesResponse(value) => {
            writer.u16(6);
            writer.term(value.term);
            writer.node(value.follower_id);
            writer.bool(value.success);
            writer.index(value.match_index);
            match value.conflict_term {
                None => writer.bool(false),
                Some(term) => {
                    writer.bool(true);
                    writer.term(term);
                }
            }
            writer.index(value.conflict_index);
        }
        RaftMessage::InstallSnapshotRequest(value) => {
            writer.u16(7);
            writer.term(value.term);
            writer.node(value.leader_id);
            let snapshot = encode_snapshot_metadata(&value.snapshot)?;
            writer.len_bytes(&snapshot)?;
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

pub fn decode_raft_message(bytes: &[u8]) -> Result<RaftMessage> {
    let mut reader = Reader::new(bytes);
    let message = match reader.u16()? {
        1 => RaftMessage::PreVoteRequest(PreVoteRequest {
            prospective_term: Term::new(reader.u64()?),
            candidate_id: reader.node()?,
            last_log_index: reader.index()?,
            last_log_term: reader.term()?,
        }),
        2 => RaftMessage::PreVoteResponse(PreVoteResponse {
            prospective_term: Term::new(reader.u64()?),
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
            let count = reader.bounded_count(MAX_ENTRIES, "AppendEntries entry count")?;
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                entries.push(decode_log_entry(&mut reader)?);
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
        7 => {
            let term = reader.term()?;
            let leader_id = reader.node()?;
            let snapshot = decode_snapshot_metadata(
                reader.len_bytes(MAX_ENTRY_PAYLOAD, "snapshot metadata")?,
            )?;
            RaftMessage::InstallSnapshotRequest(InstallSnapshotRequest {
                term,
                leader_id,
                snapshot,
            })
        }
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
        _ => return Err(ReplicationError::Invalid("unknown Raft message kind")),
    };
    reader.finish()?;
    if encode_raft_message(&message)? != bytes {
        return Err(ReplicationError::Invalid(
            "Raft message encoding is not canonical",
        ));
    }
    Ok(message)
}

fn encode_log_entry(writer: &mut Writer, entry: &LogEntry) -> Result<()> {
    writer.term(entry.term);
    writer.index(entry.index);
    match &entry.kind {
        EntryKind::Application(payload) => {
            writer.u8(1);
            if payload.len() > MAX_ENTRY_PAYLOAD {
                return Err(ReplicationError::LimitExceeded {
                    what: "Raft application payload",
                    actual: payload.len(),
                    max: MAX_ENTRY_PAYLOAD,
                });
            }
            writer.len_bytes(payload)?;
        }
        EntryKind::LeaderBarrier => writer.u8(2),
        EntryKind::Configuration(change) => {
            writer.u8(3);
            let bytes = encode_configuration_change(change)?;
            writer.len_bytes(&bytes)?;
        }
    }
    Ok(())
}

fn decode_log_entry(reader: &mut Reader<'_>) -> Result<LogEntry> {
    let term = reader.term()?;
    let index = reader.index()?;
    let kind = match reader.u8()? {
        1 => EntryKind::Application(
            reader
                .len_bytes(MAX_ENTRY_PAYLOAD, "Raft application payload")?
                .to_vec(),
        ),
        2 => EntryKind::LeaderBarrier,
        3 => EntryKind::Configuration(decode_configuration_change(
            reader.len_bytes(MAX_ENTRY_PAYLOAD, "configuration change")?,
        )?),
        _ => return Err(ReplicationError::Invalid("unknown Raft entry kind")),
    };
    Ok(LogEntry::new(term, index, kind))
}

fn encode_stable_config(writer: &mut Writer, config: &StableConfiguration) -> Result<()> {
    if config.voter_count() > MAX_MEMBERS || config.learners().len() > MAX_MEMBERS {
        return Err(ReplicationError::LimitExceeded {
            what: "Raft configuration members",
            actual: config.voter_count().saturating_add(config.learners().len()),
            max: MAX_MEMBERS * 2,
        });
    }
    writer.u16(
        u16::try_from(config.voter_count())
            .map_err(|_| ReplicationError::Invalid("voter count overflow"))?,
    );
    writer.u16(
        u16::try_from(config.learners().len())
            .map_err(|_| ReplicationError::Invalid("learner count overflow"))?,
    );
    for node in config.voters() {
        writer.node(node);
    }
    for node in config.learners() {
        writer.node(node);
    }
    Ok(())
}

fn decode_stable_config(reader: &mut Reader<'_>) -> Result<StableConfiguration> {
    let voter_count = usize::from(reader.u16()?);
    let learner_count = usize::from(reader.u16()?);
    if voter_count == 0 || voter_count > MAX_MEMBERS || learner_count > MAX_MEMBERS {
        return Err(ReplicationError::Invalid(
            "invalid stable configuration size",
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

    fn bytes(&mut self, bytes: &[u8]) {
        self.bytes.extend_from_slice(bytes);
    }

    fn len_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.u32(
            u32::try_from(bytes.len()).map_err(|_| ReplicationError::Invalid("length overflow"))?,
        );
        self.bytes(bytes);
        Ok(())
    }

    fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn bool(&mut self, value: bool) {
        self.u8(u8::from(value));
    }

    fn u16(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
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
}

struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn finish(&self) -> Result<()> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(ReplicationError::Invalid("trailing bytes in Raft codec"))
        }
    }

    fn bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(len)
            .ok_or(ReplicationError::Invalid("codec length overflow"))?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or(ReplicationError::Invalid("truncated Raft codec"))?;
        self.position = end;
        Ok(bytes)
    }

    fn len_bytes(&mut self, max: usize, what: &'static str) -> Result<&'a [u8]> {
        let len = usize::try_from(self.u32()?)
            .map_err(|_| ReplicationError::Invalid("length overflow"))?;
        if len > max {
            return Err(ReplicationError::LimitExceeded {
                what,
                actual: len,
                max,
            });
        }
        self.bytes(len)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.bytes(1)?[0])
    }

    fn bool(&mut self) -> Result<bool> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(ReplicationError::Invalid("invalid boolean")),
        }
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(
            self.bytes(2)?.try_into().expect("2-byte slice"),
        ))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(
            self.bytes(4)?.try_into().expect("4-byte slice"),
        ))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(
            self.bytes(8)?.try_into().expect("8-byte slice"),
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

    fn bounded_count(&mut self, max: usize, what: &'static str) -> Result<usize> {
        let count = usize::try_from(self.u32()?)
            .map_err(|_| ReplicationError::Invalid("count overflow"))?;
        if count > max {
            return Err(ReplicationError::LimitExceeded {
                what,
                actual: count,
                max,
            });
        }
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(value: u64) -> NodeId {
        NodeId::new(value).unwrap()
    }

    #[test]
    fn all_raft_message_variants_round_trip() {
        let config = StableConfiguration::new([node(1), node(2), node(3)], [node(4)]).unwrap();
        let snapshot = SnapshotMetadata::new(
            LogIndex::new(8),
            Term::new(3),
            config.clone(),
            SnapshotId::new([7; 16]),
        )
        .unwrap();
        let messages = vec![
            RaftMessage::PreVoteRequest(PreVoteRequest {
                prospective_term: Term::new(4),
                candidate_id: node(1),
                last_log_index: LogIndex::new(8),
                last_log_term: Term::new(3),
            }),
            RaftMessage::PreVoteResponse(PreVoteResponse {
                prospective_term: Term::new(4),
                voter_id: node(2),
                vote_granted: true,
            }),
            RaftMessage::RequestVoteRequest(RequestVoteRequest {
                term: Term::new(4),
                candidate_id: node(1),
                last_log_index: LogIndex::new(8),
                last_log_term: Term::new(3),
            }),
            RaftMessage::RequestVoteResponse(RequestVoteResponse {
                term: Term::new(4),
                voter_id: node(2),
                vote_granted: false,
            }),
            RaftMessage::AppendEntriesRequest(AppendEntriesRequest {
                term: Term::new(4),
                leader_id: node(1),
                prev_log_index: LogIndex::new(8),
                prev_log_term: Term::new(3),
                entries: vec![LogEntry::new(
                    Term::new(4),
                    LogIndex::new(9),
                    EntryKind::Application(b"payload".to_vec()),
                )],
                leader_commit: LogIndex::new(8),
            }),
            RaftMessage::AppendEntriesResponse(AppendEntriesResponse {
                term: Term::new(4),
                follower_id: node(2),
                success: false,
                match_index: LogIndex::new(7),
                conflict_term: Some(Term::new(2)),
                conflict_index: LogIndex::new(5),
            }),
            RaftMessage::InstallSnapshotRequest(InstallSnapshotRequest {
                term: Term::new(4),
                leader_id: node(1),
                snapshot,
            }),
            RaftMessage::InstallSnapshotResponse(InstallSnapshotResponse {
                term: Term::new(4),
                follower_id: node(2),
                success: true,
                match_index: LogIndex::new(8),
            }),
            RaftMessage::TimeoutNow(TimeoutNow {
                term: Term::new(4),
                leader_id: node(1),
            }),
        ];
        for message in messages {
            let encoded = encode_raft_message(&message).unwrap();
            assert_eq!(decode_raft_message(&encoded).unwrap(), message);
        }
    }

    #[test]
    fn configuration_change_codec_is_deterministic() {
        let old = StableConfiguration::new([node(3), node(1), node(2)], []).unwrap();
        let new = StableConfiguration::new([node(2), node(3), node(4)], []).unwrap();
        let change = ConfigurationChange::EnterJoint { old, new };
        let bytes = encode_configuration_change(&change).unwrap();
        assert_eq!(decode_configuration_change(&bytes).unwrap(), change);
    }
}
