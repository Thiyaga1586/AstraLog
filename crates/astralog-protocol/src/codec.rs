use astralog_common::TopicId;

use crate::{ProtocolError, Result};

pub const MAX_TAG_COUNT: usize = 64;
pub const MAX_TAG_VALUE_LEN: usize = 65_536;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaggedField {
    pub id: u16,
    pub value: Vec<u8>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Writer {
    bytes: Vec<u8>,
}

impl Writer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(capacity),
        }
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.bytes
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    pub fn put_u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    pub fn put_u16(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub fn put_u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub fn put_u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub fn put_i64(&mut self, value: i64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub fn put_bool(&mut self, value: bool) {
        self.put_u8(u8::from(value));
    }

    pub fn put_bytes(&mut self, bytes: &[u8]) {
        self.bytes.extend_from_slice(bytes);
    }

    pub fn put_topic_id(&mut self, topic_id: TopicId) {
        self.put_bytes(topic_id.as_bytes());
    }

    pub fn put_uuid(&mut self, value: &[u8; 16]) {
        self.put_bytes(value);
    }

    pub fn put_string16(&mut self, value: &str, max: usize, field: &'static str) -> Result<()> {
        let bytes = value.as_bytes();
        if bytes.len() > max {
            return Err(ProtocolError::FieldTooLarge {
                field,
                len: bytes.len(),
                max,
            });
        }
        let len = u16::try_from(bytes.len()).map_err(|_| ProtocolError::LengthOverflow)?;
        if len == u16::MAX {
            return Err(ProtocolError::LengthOverflow);
        }
        self.put_u16(len);
        self.put_bytes(bytes);
        Ok(())
    }

    pub fn put_nullable_string16(
        &mut self,
        value: Option<&str>,
        max: usize,
        field: &'static str,
    ) -> Result<()> {
        match value {
            Some(value) => self.put_string16(value, max, field),
            None => {
                self.put_u16(u16::MAX);
                Ok(())
            }
        }
    }

    pub fn put_bytes32(&mut self, value: &[u8], max: usize, field: &'static str) -> Result<()> {
        if value.len() > max {
            return Err(ProtocolError::FieldTooLarge {
                field,
                len: value.len(),
                max,
            });
        }
        let len = u32::try_from(value.len()).map_err(|_| ProtocolError::LengthOverflow)?;
        if len == u32::MAX {
            return Err(ProtocolError::LengthOverflow);
        }
        self.put_u32(len);
        self.put_bytes(value);
        Ok(())
    }

    pub fn put_nullable_bytes32(
        &mut self,
        value: Option<&[u8]>,
        max: usize,
        field: &'static str,
    ) -> Result<()> {
        match value {
            Some(value) => self.put_bytes32(value, max, field),
            None => {
                self.put_u32(u32::MAX);
                Ok(())
            }
        }
    }

    pub fn put_tagged_fields(&mut self, tags: &[TaggedField]) -> Result<()> {
        validate_tags(tags)?;
        let count = u16::try_from(tags.len()).map_err(|_| ProtocolError::LengthOverflow)?;
        self.put_u16(count);
        for tag in tags {
            self.put_u16(tag.id);
            self.put_u32(
                u32::try_from(tag.value.len()).map_err(|_| ProtocolError::LengthOverflow)?,
            );
            self.put_bytes(&tag.value);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    pub fn position(self) -> usize {
        self.pos
    }

    pub fn remaining(self) -> usize {
        self.bytes.len().saturating_sub(self.pos)
    }

    pub fn is_empty(self) -> bool {
        self.remaining() == 0
    }

    pub fn read_exact(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(ProtocolError::LengthOverflow)?;
        if end > self.bytes.len() {
            return Err(ProtocolError::UnexpectedEof {
                needed: len,
                remaining: self.remaining(),
            });
        }
        let result = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(result)
    }

    pub fn read_u8(&mut self) -> Result<u8> {
        Ok(self.read_exact(1)?[0])
    }

    pub fn read_u16(&mut self) -> Result<u16> {
        let bytes: [u8; 2] = self
            .read_exact(2)?
            .try_into()
            .map_err(|_| ProtocolError::InvalidFrame("u16 width"))?;
        Ok(u16::from_be_bytes(bytes))
    }

    pub fn read_u32(&mut self) -> Result<u32> {
        let bytes: [u8; 4] = self
            .read_exact(4)?
            .try_into()
            .map_err(|_| ProtocolError::InvalidFrame("u32 width"))?;
        Ok(u32::from_be_bytes(bytes))
    }

    pub fn read_u64(&mut self) -> Result<u64> {
        let bytes: [u8; 8] = self
            .read_exact(8)?
            .try_into()
            .map_err(|_| ProtocolError::InvalidFrame("u64 width"))?;
        Ok(u64::from_be_bytes(bytes))
    }

    pub fn read_i64(&mut self) -> Result<i64> {
        let bytes: [u8; 8] = self
            .read_exact(8)?
            .try_into()
            .map_err(|_| ProtocolError::InvalidFrame("i64 width"))?;
        Ok(i64::from_be_bytes(bytes))
    }

    pub fn read_bool(&mut self) -> Result<bool> {
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(ProtocolError::InvalidBoolean(value)),
        }
    }

    pub fn read_topic_id(&mut self) -> Result<TopicId> {
        let bytes: [u8; 16] = self
            .read_exact(16)?
            .try_into()
            .map_err(|_| ProtocolError::InvalidFrame("uuid width"))?;
        Ok(TopicId::new(bytes))
    }

    pub fn read_uuid(&mut self) -> Result<[u8; 16]> {
        self.read_exact(16)?
            .try_into()
            .map_err(|_| ProtocolError::InvalidFrame("uuid width"))
    }

    pub fn read_string16(&mut self, max: usize, field: &'static str) -> Result<String> {
        let len = usize::from(self.read_u16()?);
        if len == usize::from(u16::MAX) {
            return Err(ProtocolError::InvalidFrame(
                "null sentinel used for non-null string",
            ));
        }
        if len > max {
            return Err(ProtocolError::FieldTooLarge { field, len, max });
        }
        let bytes = self.read_exact(len)?;
        let value = core::str::from_utf8(bytes).map_err(|_| ProtocolError::InvalidUtf8)?;
        Ok(value.to_owned())
    }

    pub fn read_nullable_string16(
        &mut self,
        max: usize,
        field: &'static str,
    ) -> Result<Option<String>> {
        let len = usize::from(self.read_u16()?);
        if len == usize::from(u16::MAX) {
            return Ok(None);
        }
        if len > max {
            return Err(ProtocolError::FieldTooLarge { field, len, max });
        }
        let bytes = self.read_exact(len)?;
        let value = core::str::from_utf8(bytes).map_err(|_| ProtocolError::InvalidUtf8)?;
        Ok(Some(value.to_owned()))
    }

    pub fn read_bytes32(&mut self, max: usize, field: &'static str) -> Result<Vec<u8>> {
        let raw_len = self.read_u32()?;
        if raw_len == u32::MAX {
            return Err(ProtocolError::InvalidFrame(
                "null sentinel used for non-null bytes",
            ));
        }
        let len = usize::try_from(raw_len).map_err(|_| ProtocolError::LengthOverflow)?;
        if len > max {
            return Err(ProtocolError::FieldTooLarge { field, len, max });
        }
        Ok(self.read_exact(len)?.to_vec())
    }

    pub fn read_nullable_bytes32(
        &mut self,
        max: usize,
        field: &'static str,
    ) -> Result<Option<Vec<u8>>> {
        let raw_len = self.read_u32()?;
        if raw_len == u32::MAX {
            return Ok(None);
        }
        let len = usize::try_from(raw_len).map_err(|_| ProtocolError::LengthOverflow)?;
        if len > max {
            return Err(ProtocolError::FieldTooLarge { field, len, max });
        }
        Ok(Some(self.read_exact(len)?.to_vec()))
    }

    pub fn read_tagged_fields(&mut self) -> Result<Vec<TaggedField>> {
        let count = usize::from(self.read_u16()?);
        if count > MAX_TAG_COUNT {
            return Err(ProtocolError::TooManyTags(count));
        }
        let mut tags = Vec::with_capacity(count);
        let mut previous = None;
        for _ in 0..count {
            let id = self.read_u16()?;
            if let Some(previous) = previous
                && id <= previous
            {
                return Err(ProtocolError::InvalidTagOrder {
                    previous,
                    current: id,
                });
            }
            let len =
                usize::try_from(self.read_u32()?).map_err(|_| ProtocolError::LengthOverflow)?;
            if len > MAX_TAG_VALUE_LEN {
                return Err(ProtocolError::TagTooLarge { tag: id, len });
            }
            let value = self.read_exact(len)?.to_vec();
            tags.push(TaggedField { id, value });
            previous = Some(id);
        }
        Ok(tags)
    }
}

pub fn validate_tags(tags: &[TaggedField]) -> Result<()> {
    if tags.len() > MAX_TAG_COUNT {
        return Err(ProtocolError::TooManyTags(tags.len()));
    }
    let mut previous = None;
    for tag in tags {
        if tag.value.len() > MAX_TAG_VALUE_LEN {
            return Err(ProtocolError::TagTooLarge {
                tag: tag.id,
                len: tag.value.len(),
            });
        }
        if let Some(previous) = previous
            && tag.id <= previous
        {
            return Err(ProtocolError::InvalidTagOrder {
                previous,
                current: tag.id,
            });
        }
        previous = Some(tag.id);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primitives_round_trip_big_endian() {
        let topic = TopicId::new([0xAB; 16]);
        let tags = vec![
            TaggedField {
                id: 1,
                value: b"client".to_vec(),
            },
            TaggedField {
                id: 2,
                value: vec![7; 16],
            },
        ];
        let mut writer = Writer::new();
        writer.put_u16(0x1234);
        writer.put_u32(0x1020_3040);
        writer.put_u64(0x0102_0304_0506_0708);
        writer.put_i64(-2);
        writer.put_bool(true);
        writer.put_topic_id(topic);
        writer.put_string16("astra", 64, "name").unwrap();
        writer.put_nullable_bytes32(None, 64, "value").unwrap();
        writer.put_tagged_fields(&tags).unwrap();

        let bytes = writer.into_inner();
        assert_eq!(&bytes[0..2], &[0x12, 0x34]);

        let mut reader = Reader::new(&bytes);
        assert_eq!(reader.read_u16().unwrap(), 0x1234);
        assert_eq!(reader.read_u32().unwrap(), 0x1020_3040);
        assert_eq!(reader.read_u64().unwrap(), 0x0102_0304_0506_0708);
        assert_eq!(reader.read_i64().unwrap(), -2);
        assert!(reader.read_bool().unwrap());
        assert_eq!(reader.read_topic_id().unwrap(), topic);
        assert_eq!(reader.read_string16(64, "name").unwrap(), "astra");
        assert_eq!(reader.read_nullable_bytes32(64, "value").unwrap(), None);
        assert_eq!(reader.read_tagged_fields().unwrap(), tags);
        assert!(reader.is_empty());
    }

    #[test]
    fn tag_order_is_strict() {
        let tags = [
            TaggedField {
                id: 2,
                value: vec![],
            },
            TaggedField {
                id: 2,
                value: vec![],
            },
        ];
        assert!(matches!(
            validate_tags(&tags),
            Err(ProtocolError::InvalidTagOrder { .. })
        ));
    }

    #[test]
    fn field_length_is_checked_before_copy() {
        let bytes = [0, 0, 0, 10, 1, 2];
        let mut reader = Reader::new(&bytes);
        assert!(matches!(
            reader.read_bytes32(4, "payload"),
            Err(ProtocolError::FieldTooLarge { .. })
        ));
        assert_eq!(reader.position(), 4);
    }
}
