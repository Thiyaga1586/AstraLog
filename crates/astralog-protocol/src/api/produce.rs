use astralog_common::{ProducerAuthority, ProducerEpoch, ProducerId, ProducerSequence, TopicId};

use crate::{
    ProtocolError, Reader, Result, TaggedField, Writer,
    api::{
        BodyCodec, MAX_PARTITIONS_PER_REQUEST, MAX_RECORD_HEADER_KEY_LEN, MAX_RECORD_HEADERS,
        MAX_RECORD_KEY_LEN, MAX_RECORD_VALUE_LEN, MAX_RECORDS_PER_PARTITION, finish_body,
        put_top_level_tags, read_bounded_count,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AckMode {
    None = 0,
    Leader = 1,
    Quorum = 2,
}

impl TryFrom<u8> for AckMode {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::Leader),
            2 => Ok(Self::Quorum),
            _ => Err(ProtocolError::InvalidFrame("invalid ack mode")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireHeaderV0 {
    pub key: String,
    pub value: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireRecordV0 {
    pub timestamp_ms: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<WireHeaderV0>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducePartitionRequestV0 {
    pub topic_id: TopicId,
    pub partition: u32,
    pub leader_epoch: u64,
    pub records: Vec<WireRecordV0>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceRequestV0 {
    pub acks: AckMode,
    pub partitions: Vec<ProducePartitionRequestV0>,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducePartitionResponseV0 {
    pub topic_id: TopicId,
    pub partition: u32,
    pub error_code: u16,
    pub base_offset: u64,
    pub last_offset: u64,
    pub leader_broker_id: u32,
    pub leader_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceResponseV0 {
    pub partitions: Vec<ProducePartitionResponseV0>,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducePartitionRequestV1 {
    pub topic_id: TopicId,
    pub partition: u32,
    pub leader_epoch: u64,
    pub base_sequence: ProducerSequence,
    pub records: Vec<WireRecordV0>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceRequestV1 {
    pub authority: ProducerAuthority,
    pub partitions: Vec<ProducePartitionRequestV1>,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducePartitionResponseV1 {
    pub topic_id: TopicId,
    pub partition: u32,
    pub error_code: u16,
    pub base_offset: u64,
    pub last_offset: u64,
    pub leader_broker_id: u32,
    pub leader_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceResponseV1 {
    pub partitions: Vec<ProducePartitionResponseV1>,
    pub tags: Vec<TaggedField>,
}

impl WireRecordV0 {
    pub(crate) fn encode_into(&self, writer: &mut Writer) -> Result<()> {
        if self.timestamp_ms < -1 {
            return Err(ProtocolError::InvalidFrame("invalid record timestamp"));
        }
        if self.headers.len() > MAX_RECORD_HEADERS {
            return Err(ProtocolError::FieldTooLarge {
                field: "record_headers",
                len: self.headers.len(),
                max: MAX_RECORD_HEADERS,
            });
        }
        writer.put_i64(self.timestamp_ms);
        writer.put_nullable_bytes32(self.key.as_deref(), MAX_RECORD_KEY_LEN, "record_key")?;
        writer.put_nullable_bytes32(self.value.as_deref(), MAX_RECORD_VALUE_LEN, "record_value")?;
        writer
            .put_u16(u16::try_from(self.headers.len()).map_err(|_| ProtocolError::LengthOverflow)?);
        for header in &self.headers {
            writer.put_string16(&header.key, MAX_RECORD_HEADER_KEY_LEN, "record_header_key")?;
            writer.put_nullable_bytes32(
                header.value.as_deref(),
                MAX_RECORD_VALUE_LEN,
                "record_header_value",
            )?;
        }
        Ok(())
    }

    pub(crate) fn decode_from(reader: &mut Reader<'_>) -> Result<Self> {
        let timestamp_ms = reader.read_i64()?;
        if timestamp_ms < -1 {
            return Err(ProtocolError::InvalidFrame("invalid record timestamp"));
        }
        let key = reader.read_nullable_bytes32(MAX_RECORD_KEY_LEN, "record_key")?;
        let value = reader.read_nullable_bytes32(MAX_RECORD_VALUE_LEN, "record_value")?;
        let header_count = usize::from(reader.read_u16()?);
        if header_count > MAX_RECORD_HEADERS {
            return Err(ProtocolError::FieldTooLarge {
                field: "record_headers",
                len: header_count,
                max: MAX_RECORD_HEADERS,
            });
        }
        let mut headers = Vec::with_capacity(header_count);
        for _ in 0..header_count {
            headers.push(WireHeaderV0 {
                key: reader.read_string16(MAX_RECORD_HEADER_KEY_LEN, "record_header_key")?,
                value: reader.read_nullable_bytes32(MAX_RECORD_VALUE_LEN, "record_header_value")?,
            });
        }
        Ok(Self {
            timestamp_ms,
            key,
            value,
            headers,
        })
    }

    pub fn encoded_logical_len(&self) -> Result<usize> {
        let mut writer = Writer::new();
        self.encode_into(&mut writer)?;
        Ok(writer.as_slice().len())
    }
}

pub fn canonical_record_set_digest(records: &[WireRecordV0]) -> Result<[u8; 32]> {
    if records.is_empty() || records.len() > MAX_RECORDS_PER_PARTITION {
        return Err(ProtocolError::InvalidFrame(
            "idempotent record set must contain a bounded non-zero record count",
        ));
    }
    let mut writer = Writer::new();
    writer.put_u32(u32::try_from(records.len()).map_err(|_| ProtocolError::LengthOverflow)?);
    for record in records {
        if record.timestamp_ms < 0 {
            return Err(ProtocolError::InvalidFrame(
                "idempotent Produce requires explicit non-negative timestamps",
            ));
        }
        record.encode_into(&mut writer)?;
    }
    Ok(crate::sha256(writer.as_slice()))
}

impl BodyCodec for ProduceRequestV0 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        if self.partitions.len() > MAX_PARTITIONS_PER_REQUEST {
            return Err(ProtocolError::FieldTooLarge {
                field: "produce_partitions",
                len: self.partitions.len(),
                max: MAX_PARTITIONS_PER_REQUEST,
            });
        }
        let mut writer = Writer::new();
        writer.put_u8(self.acks as u8);
        writer.put_u32(
            u32::try_from(self.partitions.len()).map_err(|_| ProtocolError::LengthOverflow)?,
        );
        let mut seen = std::collections::BTreeSet::new();
        for partition in &self.partitions {
            if !seen.insert((*partition.topic_id.as_bytes(), partition.partition)) {
                return Err(ProtocolError::InvalidFrame(
                    "duplicate topic/partition in Produce request",
                ));
            }
            if partition.records.len() > MAX_RECORDS_PER_PARTITION {
                return Err(ProtocolError::FieldTooLarge {
                    field: "produce_records",
                    len: partition.records.len(),
                    max: MAX_RECORDS_PER_PARTITION,
                });
            }
            writer.put_topic_id(partition.topic_id);
            writer.put_u32(partition.partition);
            writer.put_u64(partition.leader_epoch);
            writer.put_u32(
                u32::try_from(partition.records.len())
                    .map_err(|_| ProtocolError::LengthOverflow)?,
            );
            for record in &partition.records {
                record.encode_into(&mut writer)?;
            }
        }
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let acks = AckMode::try_from(reader.read_u8()?)?;
        let partition_count = read_bounded_count(
            &mut reader,
            MAX_PARTITIONS_PER_REQUEST,
            32,
            "produce_partitions",
        )?;
        let mut partitions = Vec::with_capacity(partition_count);
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..partition_count {
            let topic_id = reader.read_topic_id()?;
            let partition = reader.read_u32()?;
            if !seen.insert((*topic_id.as_bytes(), partition)) {
                return Err(ProtocolError::InvalidFrame(
                    "duplicate topic/partition in Produce request",
                ));
            }
            let leader_epoch = reader.read_u64()?;
            let record_count = read_bounded_count(
                &mut reader,
                MAX_RECORDS_PER_PARTITION,
                18,
                "produce_records",
            )?;
            let mut records = Vec::with_capacity(record_count);
            for _ in 0..record_count {
                records.push(WireRecordV0::decode_from(&mut reader)?);
            }
            partitions.push(ProducePartitionRequestV0 {
                topic_id,
                partition,
                leader_epoch,
                records,
            });
        }
        let tags = finish_body(&mut reader)?;
        Ok(Self {
            acks,
            partitions,
            tags,
        })
    }
}

impl BodyCodec for ProduceResponseV0 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        if self.partitions.len() > MAX_PARTITIONS_PER_REQUEST {
            return Err(ProtocolError::FieldTooLarge {
                field: "produce_results",
                len: self.partitions.len(),
                max: MAX_PARTITIONS_PER_REQUEST,
            });
        }
        let mut writer = Writer::new();
        writer.put_u32(
            u32::try_from(self.partitions.len()).map_err(|_| ProtocolError::LengthOverflow)?,
        );
        for partition in &self.partitions {
            writer.put_topic_id(partition.topic_id);
            writer.put_u32(partition.partition);
            writer.put_u16(partition.error_code);
            writer.put_u64(partition.base_offset);
            writer.put_u64(partition.last_offset);
            writer.put_u32(partition.leader_broker_id);
            writer.put_u64(partition.leader_epoch);
        }
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let count = read_bounded_count(
            &mut reader,
            MAX_PARTITIONS_PER_REQUEST,
            50,
            "produce_results",
        )?;
        let mut partitions = Vec::with_capacity(count);
        for _ in 0..count {
            partitions.push(ProducePartitionResponseV0 {
                topic_id: reader.read_topic_id()?,
                partition: reader.read_u32()?,
                error_code: reader.read_u16()?,
                base_offset: reader.read_u64()?,
                last_offset: reader.read_u64()?,
                leader_broker_id: reader.read_u32()?,
                leader_epoch: reader.read_u64()?,
            });
        }
        let tags = finish_body(&mut reader)?;
        Ok(Self { partitions, tags })
    }
}

impl BodyCodec for ProduceRequestV1 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        if self.partitions.is_empty() || self.partitions.len() > MAX_PARTITIONS_PER_REQUEST {
            return Err(ProtocolError::InvalidFrame(
                "idempotent Produce requires a bounded non-zero partition count",
            ));
        }
        let mut writer = Writer::new();
        writer.put_u8(AckMode::Quorum as u8);
        writer.put_u64(self.authority.producer_id.get());
        writer.put_u32(self.authority.producer_epoch.get());
        writer.put_u32(
            u32::try_from(self.partitions.len()).map_err(|_| ProtocolError::LengthOverflow)?,
        );
        let mut seen = std::collections::BTreeSet::new();
        for partition in &self.partitions {
            if !seen.insert((*partition.topic_id.as_bytes(), partition.partition)) {
                return Err(ProtocolError::InvalidFrame(
                    "duplicate topic/partition in idempotent Produce request",
                ));
            }
            if partition.records.is_empty() || partition.records.len() > MAX_RECORDS_PER_PARTITION {
                return Err(ProtocolError::InvalidFrame(
                    "idempotent Produce partition requires a bounded non-zero record count",
                ));
            }
            let record_count = u32::try_from(partition.records.len())
                .map_err(|_| ProtocolError::LengthOverflow)?;
            if partition
                .base_sequence
                .checked_next_after(record_count)
                .is_none()
            {
                return Err(ProtocolError::InvalidFrame(
                    "idempotent Produce sequence range cannot wrap",
                ));
            }
            writer.put_topic_id(partition.topic_id);
            writer.put_u32(partition.partition);
            writer.put_u64(partition.leader_epoch);
            writer.put_u32(partition.base_sequence.get());
            writer.put_u32(record_count);
            for record in &partition.records {
                if record.timestamp_ms < 0 {
                    return Err(ProtocolError::InvalidFrame(
                        "idempotent Produce requires explicit non-negative timestamps",
                    ));
                }
                record.encode_into(&mut writer)?;
            }
        }
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        if AckMode::try_from(reader.read_u8()?)? != AckMode::Quorum {
            return Err(ProtocolError::InvalidFrame(
                "idempotent Produce requires quorum acknowledgements",
            ));
        }
        let producer_id = ProducerId::new(reader.read_u64()?).ok_or(
            ProtocolError::InvalidFrame("idempotent Produce producer id zero is reserved"),
        )?;
        let producer_epoch = ProducerEpoch::new(reader.read_u32()?).ok_or(
            ProtocolError::InvalidFrame("idempotent Produce producer epoch zero is reserved"),
        )?;
        let partition_count = read_bounded_count(
            &mut reader,
            MAX_PARTITIONS_PER_REQUEST,
            36,
            "idempotent_produce_partitions",
        )?;
        if partition_count == 0 {
            return Err(ProtocolError::InvalidFrame(
                "idempotent Produce requires at least one partition",
            ));
        }
        let mut partitions = Vec::with_capacity(partition_count);
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..partition_count {
            let topic_id = reader.read_topic_id()?;
            let partition = reader.read_u32()?;
            if !seen.insert((*topic_id.as_bytes(), partition)) {
                return Err(ProtocolError::InvalidFrame(
                    "duplicate topic/partition in idempotent Produce request",
                ));
            }
            let leader_epoch = reader.read_u64()?;
            let base_sequence = ProducerSequence::new(reader.read_u32()?);
            let record_count = read_bounded_count(
                &mut reader,
                MAX_RECORDS_PER_PARTITION,
                18,
                "idempotent_produce_records",
            )?;
            if record_count == 0 {
                return Err(ProtocolError::InvalidFrame(
                    "idempotent Produce partition requires at least one record",
                ));
            }
            let count_u32 =
                u32::try_from(record_count).map_err(|_| ProtocolError::LengthOverflow)?;
            if base_sequence.checked_next_after(count_u32).is_none() {
                return Err(ProtocolError::InvalidFrame(
                    "idempotent Produce sequence range cannot wrap",
                ));
            }
            let mut records = Vec::with_capacity(record_count);
            for _ in 0..record_count {
                let record = WireRecordV0::decode_from(&mut reader)?;
                if record.timestamp_ms < 0 {
                    return Err(ProtocolError::InvalidFrame(
                        "idempotent Produce requires explicit non-negative timestamps",
                    ));
                }
                records.push(record);
            }
            partitions.push(ProducePartitionRequestV1 {
                topic_id,
                partition,
                leader_epoch,
                base_sequence,
                records,
            });
        }
        let tags = finish_body(&mut reader)?;
        Ok(Self {
            authority: ProducerAuthority::new(producer_id, producer_epoch),
            partitions,
            tags,
        })
    }
}

impl BodyCodec for ProduceResponseV1 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        if self.partitions.len() > MAX_PARTITIONS_PER_REQUEST {
            return Err(ProtocolError::FieldTooLarge {
                field: "idempotent_produce_results",
                len: self.partitions.len(),
                max: MAX_PARTITIONS_PER_REQUEST,
            });
        }
        let mut writer = Writer::new();
        writer.put_u32(
            u32::try_from(self.partitions.len()).map_err(|_| ProtocolError::LengthOverflow)?,
        );
        for partition in &self.partitions {
            writer.put_topic_id(partition.topic_id);
            writer.put_u32(partition.partition);
            writer.put_u16(partition.error_code);
            writer.put_u64(partition.base_offset);
            writer.put_u64(partition.last_offset);
            writer.put_u32(partition.leader_broker_id);
            writer.put_u64(partition.leader_epoch);
        }
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let count = read_bounded_count(
            &mut reader,
            MAX_PARTITIONS_PER_REQUEST,
            50,
            "idempotent_produce_results",
        )?;
        let mut partitions = Vec::with_capacity(count);
        for _ in 0..count {
            partitions.push(ProducePartitionResponseV1 {
                topic_id: reader.read_topic_id()?,
                partition: reader.read_u32()?,
                error_code: reader.read_u16()?,
                base_offset: reader.read_u64()?,
                last_offset: reader.read_u64()?,
                leader_broker_id: reader.read_u32()?,
                leader_epoch: reader.read_u64()?,
            });
        }
        let tags = finish_body(&mut reader)?;
        Ok(Self { partitions, tags })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn produce_round_trip() {
        let request = ProduceRequestV0 {
            acks: AckMode::Quorum,
            partitions: vec![ProducePartitionRequestV0 {
                topic_id: TopicId::new([9; 16]),
                partition: 2,
                leader_epoch: u64::MAX,
                records: vec![WireRecordV0 {
                    timestamp_ms: -1,
                    key: Some(b"k".to_vec()),
                    value: Some(b"v".to_vec()),
                    headers: vec![WireHeaderV0 {
                        key: "h".into(),
                        value: Some(vec![1, 2]),
                    }],
                }],
            }],
            tags: vec![],
        };
        assert_eq!(
            ProduceRequestV0::decode_body(&request.encode_body().unwrap()).unwrap(),
            request
        );
    }

    #[test]
    fn encoder_rejects_duplicate_partition_entries() {
        let entry = ProducePartitionRequestV0 {
            topic_id: TopicId::new([7; 16]),
            partition: 0,
            leader_epoch: u64::MAX,
            records: vec![WireRecordV0 {
                timestamp_ms: 1,
                key: None,
                value: Some(vec![1]),
                headers: vec![],
            }],
        };
        let request = ProduceRequestV0 {
            acks: AckMode::Leader,
            partitions: vec![entry.clone(), entry],
            tags: vec![],
        };
        assert!(matches!(
            request.encode_body(),
            Err(ProtocolError::InvalidFrame(
                "duplicate topic/partition in Produce request"
            ))
        ));
    }

    #[test]
    fn idempotent_produce_round_trip_and_digest_are_canonical() {
        let records = vec![WireRecordV0 {
            timestamp_ms: 1_700_000_000_000,
            key: Some(b"k".to_vec()),
            value: Some(b"v".to_vec()),
            headers: vec![WireHeaderV0 {
                key: "h".into(),
                value: Some(vec![1, 2]),
            }],
        }];
        let request = ProduceRequestV1 {
            authority: ProducerAuthority::new(
                ProducerId::new(7).unwrap(),
                ProducerEpoch::new(2).unwrap(),
            ),
            partitions: vec![ProducePartitionRequestV1 {
                topic_id: TopicId::new([9; 16]),
                partition: 2,
                leader_epoch: 4,
                base_sequence: ProducerSequence::new(10),
                records: records.clone(),
            }],
            tags: vec![],
        };
        let bytes = request.encode_body().unwrap();
        assert_eq!(ProduceRequestV1::decode_body(&bytes).unwrap(), request);
        assert_eq!(
            canonical_record_set_digest(&records).unwrap(),
            canonical_record_set_digest(&records).unwrap()
        );

        let mut changed = records;
        changed[0].headers[0].value = Some(vec![2, 1]);
        assert_ne!(
            canonical_record_set_digest(&changed).unwrap(),
            canonical_record_set_digest(&request.partitions[0].records).unwrap()
        );
    }

    #[test]
    fn idempotent_produce_rejects_hidden_timestamps_and_sequence_wrap() {
        let base = ProduceRequestV1 {
            authority: ProducerAuthority::new(
                ProducerId::new(1).unwrap(),
                ProducerEpoch::new(1).unwrap(),
            ),
            partitions: vec![ProducePartitionRequestV1 {
                topic_id: TopicId::new([1; 16]),
                partition: 0,
                leader_epoch: 1,
                base_sequence: ProducerSequence::ZERO,
                records: vec![WireRecordV0 {
                    timestamp_ms: -1,
                    key: None,
                    value: Some(vec![1]),
                    headers: vec![],
                }],
            }],
            tags: vec![],
        };
        assert!(base.encode_body().is_err());

        let mut wrap = base;
        wrap.partitions[0].base_sequence = ProducerSequence::new(u32::MAX);
        wrap.partitions[0].records[0].timestamp_ms = 1;
        assert!(wrap.encode_body().is_err());
    }
}
