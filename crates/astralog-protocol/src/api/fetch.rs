use astralog_common::TopicId;

use crate::{
    ProtocolError, Reader, Result, TaggedField, Writer,
    api::{
        BodyCodec, MAX_PARTITIONS_PER_REQUEST, MAX_RECORDS_PER_PARTITION, WireRecordV0,
        finish_body, put_top_level_tags, read_bounded_count,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IsolationLevel {
    ReadCommitted = 0,
}

impl TryFrom<u8> for IsolationLevel {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::ReadCommitted),
            _ => Err(ProtocolError::InvalidFrame("invalid isolation level")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchPartitionRequestV0 {
    pub topic_id: TopicId,
    pub partition: u32,
    pub leader_epoch: u64,
    pub fetch_offset: u64,
    pub max_bytes: u32,
    pub max_records: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchRequestV0 {
    pub isolation: IsolationLevel,
    pub min_bytes: u32,
    pub max_wait_ms: u32,
    pub partitions: Vec<FetchPartitionRequestV0>,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchRecordV0 {
    pub stream_offset: u64,
    pub record: WireRecordV0,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchPartitionResponseV0 {
    pub topic_id: TopicId,
    pub partition: u32,
    pub error_code: u16,
    pub leader_broker_id: u32,
    pub leader_epoch: u64,
    pub log_start_offset: u64,
    pub high_watermark: u64,
    pub next_offset: u64,
    pub records: Vec<FetchRecordV0>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchResponseV0 {
    pub partitions: Vec<FetchPartitionResponseV0>,
    pub tags: Vec<TaggedField>,
}

impl BodyCodec for FetchRequestV0 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        if self.max_wait_ms > 60_000 {
            return Err(ProtocolError::InvalidFrame(
                "Fetch max_wait_ms exceeds V1 limit",
            ));
        }
        if self.partitions.len() > MAX_PARTITIONS_PER_REQUEST {
            return Err(ProtocolError::FieldTooLarge {
                field: "fetch_partitions",
                len: self.partitions.len(),
                max: MAX_PARTITIONS_PER_REQUEST,
            });
        }
        let mut writer = Writer::new();
        writer.put_u8(self.isolation as u8);
        writer.put_u32(self.min_bytes);
        writer.put_u32(self.max_wait_ms);
        writer.put_u32(
            u32::try_from(self.partitions.len()).map_err(|_| ProtocolError::LengthOverflow)?,
        );
        for partition in &self.partitions {
            if partition.max_bytes == 0 || partition.max_records == 0 {
                return Err(ProtocolError::InvalidFrame(
                    "Fetch max_bytes/max_records must be positive",
                ));
            }
            writer.put_topic_id(partition.topic_id);
            writer.put_u32(partition.partition);
            writer.put_u64(partition.leader_epoch);
            writer.put_u64(partition.fetch_offset);
            writer.put_u32(partition.max_bytes);
            writer.put_u32(partition.max_records);
        }
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let isolation = IsolationLevel::try_from(reader.read_u8()?)?;
        let min_bytes = reader.read_u32()?;
        let max_wait_ms = reader.read_u32()?;
        if max_wait_ms > 60_000 {
            return Err(ProtocolError::InvalidFrame(
                "Fetch max_wait_ms exceeds V1 limit",
            ));
        }
        let count = read_bounded_count(
            &mut reader,
            MAX_PARTITIONS_PER_REQUEST,
            44,
            "fetch_partitions",
        )?;
        let mut partitions = Vec::with_capacity(count);
        for _ in 0..count {
            let partition = FetchPartitionRequestV0 {
                topic_id: reader.read_topic_id()?,
                partition: reader.read_u32()?,
                leader_epoch: reader.read_u64()?,
                fetch_offset: reader.read_u64()?,
                max_bytes: reader.read_u32()?,
                max_records: reader.read_u32()?,
            };
            if partition.max_bytes == 0 || partition.max_records == 0 {
                return Err(ProtocolError::InvalidFrame(
                    "Fetch max_bytes/max_records must be positive",
                ));
            }
            partitions.push(partition);
        }
        let tags = finish_body(&mut reader)?;
        Ok(Self {
            isolation,
            min_bytes,
            max_wait_ms,
            partitions,
            tags,
        })
    }
}

impl BodyCodec for FetchResponseV0 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        if self.partitions.len() > MAX_PARTITIONS_PER_REQUEST {
            return Err(ProtocolError::FieldTooLarge {
                field: "fetch_results",
                len: self.partitions.len(),
                max: MAX_PARTITIONS_PER_REQUEST,
            });
        }
        let mut writer = Writer::new();
        writer.put_u32(
            u32::try_from(self.partitions.len()).map_err(|_| ProtocolError::LengthOverflow)?,
        );
        for partition in &self.partitions {
            if partition.records.len() > MAX_RECORDS_PER_PARTITION {
                return Err(ProtocolError::FieldTooLarge {
                    field: "fetch_records",
                    len: partition.records.len(),
                    max: MAX_RECORDS_PER_PARTITION,
                });
            }
            writer.put_topic_id(partition.topic_id);
            writer.put_u32(partition.partition);
            writer.put_u16(partition.error_code);
            writer.put_u32(partition.leader_broker_id);
            writer.put_u64(partition.leader_epoch);
            writer.put_u64(partition.log_start_offset);
            writer.put_u64(partition.high_watermark);
            writer.put_u64(partition.next_offset);
            writer.put_u32(
                u32::try_from(partition.records.len())
                    .map_err(|_| ProtocolError::LengthOverflow)?,
            );
            for record in &partition.records {
                writer.put_u64(record.stream_offset);
                record.record.encode_into(&mut writer)?;
            }
        }
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let count =
            read_bounded_count(&mut reader, MAX_PARTITIONS_PER_REQUEST, 62, "fetch_results")?;
        let mut partitions = Vec::with_capacity(count);
        for _ in 0..count {
            let topic_id = reader.read_topic_id()?;
            let partition = reader.read_u32()?;
            let error_code = reader.read_u16()?;
            let leader_broker_id = reader.read_u32()?;
            let leader_epoch = reader.read_u64()?;
            let log_start_offset = reader.read_u64()?;
            let high_watermark = reader.read_u64()?;
            let next_offset = reader.read_u64()?;
            let record_count =
                read_bounded_count(&mut reader, MAX_RECORDS_PER_PARTITION, 26, "fetch_records")?;
            let mut records = Vec::with_capacity(record_count);
            for _ in 0..record_count {
                records.push(FetchRecordV0 {
                    stream_offset: reader.read_u64()?,
                    record: WireRecordV0::decode_from(&mut reader)?,
                });
            }
            partitions.push(FetchPartitionResponseV0 {
                topic_id,
                partition,
                error_code,
                leader_broker_id,
                leader_epoch,
                log_start_offset,
                high_watermark,
                next_offset,
                records,
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
    fn fetch_round_trip() {
        let request = FetchRequestV0 {
            isolation: IsolationLevel::ReadCommitted,
            min_bytes: 1,
            max_wait_ms: 500,
            partitions: vec![FetchPartitionRequestV0 {
                topic_id: TopicId::new([1; 16]),
                partition: 0,
                leader_epoch: u64::MAX,
                fetch_offset: 7,
                max_bytes: 4096,
                max_records: 100,
            }],
            tags: vec![],
        };
        assert_eq!(
            FetchRequestV0::decode_body(&request.encode_body().unwrap()).unwrap(),
            request
        );
    }
}
