use crate::{
    ProtocolError, Result, TaggedField, WIRE_VERSION, Writer,
    codec::{Reader, validate_tags},
};

pub const MAX_WIRE_FRAME_LEN_V1: usize = 64 * 1024 * 1024;
pub const FRAME_FIXED_HEADER_LEN: usize = 40;
pub const FRAME_CRC_LEN: usize = 4;
pub const MIN_WIRE_FRAME_LEN: usize = FRAME_FIXED_HEADER_LEN + FRAME_CRC_LEN;
pub const FRAME_MAGIC: &[u8; 4] = b"ALV1";
pub const NO_RESPONSE_FLAG: u8 = 1;
const CLIENT_ID_TAG: u16 = 1;
const TRACE_ID_TAG: u16 = 2;
const MAX_CLIENT_ID_LEN: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameKind {
    Request = 0,
    Response = 1,
}

impl TryFrom<u8> for FrameKind {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Request),
            1 => Ok(Self::Response),
            _ => Err(ProtocolError::InvalidFrame("unknown frame kind")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireFrame {
    pub kind: FrameKind,
    pub flags: u8,
    pub api_key: u16,
    pub api_version: u16,
    pub error_code: u16,
    pub correlation_id: u64,
    pub timeout_ms: u32,
    pub throttle_ms: u32,
    pub header_tags: Vec<TaggedField>,
    pub body: Vec<u8>,
}

impl WireFrame {
    pub fn validate_header_semantics(&self) -> Result<()> {
        if self.flags & !NO_RESPONSE_FLAG != 0 {
            return Err(ProtocolError::InvalidFrame("reserved frame flag is set"));
        }
        match self.kind {
            FrameKind::Request => {
                if self.error_code != 0 {
                    return Err(ProtocolError::InvalidFrame(
                        "request error_code must be zero",
                    ));
                }
                if self.throttle_ms != 0 {
                    return Err(ProtocolError::InvalidFrame(
                        "request throttle_ms must be zero",
                    ));
                }
                if self.flags & NO_RESPONSE_FLAG != 0 {
                    if self.api_key != 10 || self.correlation_id != 0 {
                        return Err(ProtocolError::InvalidFrame(
                            "NO_RESPONSE is valid only for Produce with correlation zero",
                        ));
                    }
                } else {
                    if self.correlation_id == 0 {
                        return Err(ProtocolError::InvalidFrame(
                            "response-bearing request correlation id is zero",
                        ));
                    }
                    if !(1..=300_000).contains(&self.timeout_ms) {
                        return Err(ProtocolError::InvalidFrame(
                            "request timeout outside Wire V1 range",
                        ));
                    }
                }
            }
            FrameKind::Response => {
                if self.flags != 0 {
                    return Err(ProtocolError::InvalidFrame("response flags must be zero"));
                }
                if self.timeout_ms != 0 {
                    return Err(ProtocolError::InvalidFrame(
                        "response timeout_ms must be zero",
                    ));
                }
                if self.correlation_id == 0 {
                    return Err(ProtocolError::InvalidFrame(
                        "response correlation id is zero",
                    ));
                }
            }
        }
        validate_header_tags(&self.header_tags)
    }
}

fn validate_header_tags(tags: &[TaggedField]) -> Result<()> {
    validate_tags(tags)?;
    for tag in tags {
        match tag.id {
            CLIENT_ID_TAG => {
                if tag.value.len() > MAX_CLIENT_ID_LEN {
                    return Err(ProtocolError::FieldTooLarge {
                        field: "header.CLIENT_ID",
                        len: tag.value.len(),
                        max: MAX_CLIENT_ID_LEN,
                    });
                }
                core::str::from_utf8(&tag.value).map_err(|_| ProtocolError::InvalidUtf8)?;
            }
            TRACE_ID_TAG if tag.value.len() != 16 => {
                return Err(ProtocolError::InvalidFrame(
                    "TRACE_ID header tag must be exactly 16 bytes",
                ));
            }
            TRACE_ID_TAG => {}
            _ => {}
        }
    }
    Ok(())
}

pub fn encode_frame(frame: &WireFrame, negotiated_max: usize) -> Result<Vec<u8>> {
    frame.validate_header_semantics()?;
    let max = negotiated_max.min(MAX_WIRE_FRAME_LEN_V1);
    if max < MIN_WIRE_FRAME_LEN {
        return Err(ProtocolError::InvalidFrame(
            "negotiated maximum is below minimum frame length",
        ));
    }

    let header_tag_bytes = if frame.header_tags.is_empty() {
        Vec::new()
    } else {
        let mut writer = Writer::new();
        writer.put_tagged_fields(&frame.header_tags)?;
        writer.into_inner()
    };
    let tags_len =
        u32::try_from(header_tag_bytes.len()).map_err(|_| ProtocolError::LengthOverflow)?;
    let body_len = u32::try_from(frame.body.len()).map_err(|_| ProtocolError::LengthOverflow)?;
    let header_len = FRAME_FIXED_HEADER_LEN
        .checked_add(header_tag_bytes.len())
        .ok_or(ProtocolError::LengthOverflow)?;
    let frame_len = header_len
        .checked_add(frame.body.len())
        .and_then(|len| len.checked_add(FRAME_CRC_LEN))
        .ok_or(ProtocolError::LengthOverflow)?;
    if frame_len > max {
        return Err(ProtocolError::FrameTooLarge {
            len: frame_len,
            max,
        });
    }

    let mut writer = Writer::with_capacity(frame_len + 4);
    writer.put_u32(u32::try_from(frame_len).map_err(|_| ProtocolError::LengthOverflow)?);
    writer.put_bytes(FRAME_MAGIC);
    writer.put_u16(WIRE_VERSION);
    writer.put_u16(u16::try_from(header_len).map_err(|_| ProtocolError::LengthOverflow)?);
    writer.put_u8(frame.kind as u8);
    writer.put_u8(frame.flags);
    writer.put_u16(frame.api_key);
    writer.put_u16(frame.api_version);
    writer.put_u16(frame.error_code);
    writer.put_u64(frame.correlation_id);
    writer.put_u32(frame.timeout_ms);
    writer.put_u32(frame.throttle_ms);
    writer.put_u32(body_len);
    writer.put_u32(tags_len);
    writer.put_bytes(&header_tag_bytes);
    writer.put_bytes(&frame.body);
    let crc = crc32c::crc32c(&writer.as_slice()[4..]);
    writer.put_u32(crc);
    Ok(writer.into_inner())
}

pub fn decode_frame(bytes: &[u8], negotiated_max: usize) -> Result<WireFrame> {
    if bytes.len() < 4 {
        return Err(ProtocolError::UnexpectedEof {
            needed: 4,
            remaining: bytes.len(),
        });
    }

    let declared = usize::try_from(u32::from_be_bytes(
        bytes[0..4]
            .try_into()
            .map_err(|_| ProtocolError::InvalidFrame("length prefix"))?,
    ))
    .map_err(|_| ProtocolError::LengthOverflow)?;
    let max = negotiated_max.min(MAX_WIRE_FRAME_LEN_V1);
    if declared < MIN_WIRE_FRAME_LEN {
        return Err(ProtocolError::InvalidFrame("frame length below minimum"));
    }
    if declared > max {
        return Err(ProtocolError::FrameTooLarge { len: declared, max });
    }
    let actual = bytes.len() - 4;
    if declared != actual {
        return Err(ProtocolError::FrameLengthMismatch { declared, actual });
    }

    let crc_offset = bytes
        .len()
        .checked_sub(FRAME_CRC_LEN)
        .ok_or(ProtocolError::InvalidFrame("missing CRC trailer"))?;
    let stored_crc = u32::from_be_bytes(
        bytes[crc_offset..]
            .try_into()
            .map_err(|_| ProtocolError::InvalidFrame("CRC trailer width"))?,
    );
    let actual_crc = crc32c::crc32c(&bytes[4..crc_offset]);
    if stored_crc != actual_crc {
        return Err(ProtocolError::CrcMismatch {
            expected: stored_crc,
            actual: actual_crc,
        });
    }

    let mut reader = Reader::new(&bytes[4..crc_offset]);
    if reader.read_exact(4)? != FRAME_MAGIC {
        return Err(ProtocolError::InvalidFrame("frame magic"));
    }
    if reader.read_u16()? != WIRE_VERSION {
        return Err(ProtocolError::InvalidFrame("wire version"));
    }
    let header_len = usize::from(reader.read_u16()?);
    let kind = FrameKind::try_from(reader.read_u8()?)?;
    let flags = reader.read_u8()?;
    let api_key = reader.read_u16()?;
    let api_version = reader.read_u16()?;
    let error_code = reader.read_u16()?;
    let correlation_id = reader.read_u64()?;
    let timeout_ms = reader.read_u32()?;
    let throttle_ms = reader.read_u32()?;
    let body_len =
        usize::try_from(reader.read_u32()?).map_err(|_| ProtocolError::LengthOverflow)?;
    let tags_len =
        usize::try_from(reader.read_u32()?).map_err(|_| ProtocolError::LengthOverflow)?;

    let expected_header_len = FRAME_FIXED_HEADER_LEN
        .checked_add(tags_len)
        .ok_or(ProtocolError::LengthOverflow)?;
    if header_len != expected_header_len {
        return Err(ProtocolError::InvalidFrame("header_len/tags_len mismatch"));
    }
    let expected_payload_len = header_len
        .checked_add(body_len)
        .ok_or(ProtocolError::LengthOverflow)?;
    if expected_payload_len != crc_offset - 4 {
        return Err(ProtocolError::InvalidFrame("header/body length mismatch"));
    }

    let header_tags = if tags_len == 0 {
        Vec::new()
    } else {
        let tag_bytes = reader.read_exact(tags_len)?;
        let mut tag_reader = Reader::new(tag_bytes);
        let tags = tag_reader.read_tagged_fields()?;
        if tags.is_empty() || !tag_reader.is_empty() {
            return Err(ProtocolError::InvalidFrame(
                "non-canonical header tag block",
            ));
        }
        tags
    };
    let body = reader.read_exact(body_len)?.to_vec();
    if !reader.is_empty() {
        return Err(ProtocolError::InvalidFrame("trailing bytes before CRC"));
    }

    let frame = WireFrame {
        kind,
        flags,
        api_key,
        api_version,
        error_code,
        correlation_id,
        timeout_ms,
        throttle_ms,
        header_tags,
        body,
    };
    frame.validate_header_semantics()?;
    Ok(frame)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reseal_crc(bytes: &mut [u8]) {
        let crc_offset = bytes.len() - FRAME_CRC_LEN;
        let crc = crc32c::crc32c(&bytes[4..crc_offset]);
        bytes[crc_offset..].copy_from_slice(&crc.to_be_bytes());
    }

    fn request() -> WireFrame {
        WireFrame {
            kind: FrameKind::Request,
            flags: 0,
            api_key: 0,
            api_version: 0,
            error_code: 0,
            correlation_id: 7,
            timeout_ms: 5_000,
            throttle_ms: 0,
            header_tags: vec![TaggedField {
                id: 1,
                value: b"test-client".to_vec(),
            }],
            body: vec![1, 2, 3, 4],
        }
    }

    #[test]
    fn frame_round_trip_and_crc() {
        let frame = request();
        let bytes = encode_frame(&frame, MAX_WIRE_FRAME_LEN_V1).unwrap();
        assert_eq!(decode_frame(&bytes, MAX_WIRE_FRAME_LEN_V1).unwrap(), frame);
    }

    #[test]
    fn every_single_bit_body_mutation_breaks_crc() {
        let frame = request();
        let original = encode_frame(&frame, MAX_WIRE_FRAME_LEN_V1).unwrap();
        let tag_len = 2 + 2 + 4 + b"test-client".len();
        let body_start = 4 + FRAME_FIXED_HEADER_LEN + tag_len;
        for byte_index in body_start..body_start + frame.body.len() {
            for bit in 0..8 {
                let mut bytes = original.clone();
                bytes[byte_index] ^= 1 << bit;
                assert!(matches!(
                    decode_frame(&bytes, MAX_WIRE_FRAME_LEN_V1),
                    Err(ProtocolError::CrcMismatch { .. })
                ));
            }
        }
    }

    #[test]
    fn oversized_declared_length_is_rejected_before_body_parse() {
        let bytes = 1024_u32.to_be_bytes();
        assert!(matches!(
            decode_frame(&bytes, 512),
            Err(ProtocolError::FrameTooLarge {
                len: 1024,
                max: 512
            })
        ));
    }

    #[test]
    fn no_response_requires_produce_and_zero_correlation() {
        let mut frame = request();
        frame.flags = NO_RESPONSE_FLAG;
        assert!(matches!(
            encode_frame(&frame, MAX_WIRE_FRAME_LEN_V1),
            Err(ProtocolError::InvalidFrame(_))
        ));
        frame.api_key = 10;
        frame.correlation_id = 0;
        frame.timeout_ms = 0;
        assert!(encode_frame(&frame, MAX_WIRE_FRAME_LEN_V1).is_ok());
    }

    #[test]
    fn unknown_frame_kind_is_rejected_after_valid_crc() {
        let mut bytes = encode_frame(&request(), MAX_WIRE_FRAME_LEN_V1).unwrap();
        bytes[12] = 9;
        reseal_crc(&mut bytes);
        assert!(matches!(
            decode_frame(&bytes, MAX_WIRE_FRAME_LEN_V1),
            Err(ProtocolError::InvalidFrame("unknown frame kind"))
        ));
    }

    #[test]
    fn reserved_frame_flag_is_rejected_after_valid_crc() {
        let mut bytes = encode_frame(&request(), MAX_WIRE_FRAME_LEN_V1).unwrap();
        bytes[13] = 0x80;
        reseal_crc(&mut bytes);
        assert!(matches!(
            decode_frame(&bytes, MAX_WIRE_FRAME_LEN_V1),
            Err(ProtocolError::InvalidFrame("reserved frame flag is set"))
        ));
    }

    #[test]
    fn inconsistent_header_length_is_rejected_after_valid_crc() {
        let mut bytes = encode_frame(&request(), MAX_WIRE_FRAME_LEN_V1).unwrap();
        let header_len = u16::from_be_bytes([bytes[10], bytes[11]]);
        bytes[10..12].copy_from_slice(&header_len.saturating_add(1).to_be_bytes());
        reseal_crc(&mut bytes);
        assert!(matches!(
            decode_frame(&bytes, MAX_WIRE_FRAME_LEN_V1),
            Err(ProtocolError::InvalidFrame("header_len/tags_len mismatch"))
        ));
    }

    #[test]
    fn inconsistent_body_length_is_rejected_after_valid_crc() {
        let mut bytes = encode_frame(&request(), MAX_WIRE_FRAME_LEN_V1).unwrap();
        let body_len = u32::from_be_bytes(bytes[36..40].try_into().unwrap());
        bytes[36..40].copy_from_slice(&body_len.saturating_add(1).to_be_bytes());
        reseal_crc(&mut bytes);
        assert!(matches!(
            decode_frame(&bytes, MAX_WIRE_FRAME_LEN_V1),
            Err(ProtocolError::InvalidFrame("header/body length mismatch"))
        ));
    }

    #[test]
    fn unsorted_header_tags_are_rejected_before_encoding() {
        let mut frame = request();
        frame.header_tags = vec![
            TaggedField {
                id: 2,
                value: vec![1],
            },
            TaggedField {
                id: 1,
                value: vec![2],
            },
        ];
        assert!(matches!(
            encode_frame(&frame, MAX_WIRE_FRAME_LEN_V1),
            Err(ProtocolError::InvalidTagOrder { .. })
        ));
    }

    #[test]
    fn known_header_tag_semantics_are_enforced() {
        let mut frame = request();
        frame.header_tags = vec![TaggedField {
            id: CLIENT_ID_TAG,
            value: vec![0xFF],
        }];
        assert!(matches!(
            encode_frame(&frame, MAX_WIRE_FRAME_LEN_V1),
            Err(ProtocolError::InvalidUtf8)
        ));

        frame.header_tags = vec![TaggedField {
            id: CLIENT_ID_TAG,
            value: vec![b'a'; MAX_CLIENT_ID_LEN + 1],
        }];
        assert!(matches!(
            encode_frame(&frame, MAX_WIRE_FRAME_LEN_V1),
            Err(ProtocolError::FieldTooLarge {
                field: "header.CLIENT_ID",
                ..
            })
        ));

        frame.header_tags = vec![TaggedField {
            id: TRACE_ID_TAG,
            value: vec![0; 15],
        }];
        assert!(matches!(
            encode_frame(&frame, MAX_WIRE_FRAME_LEN_V1),
            Err(ProtocolError::InvalidFrame(
                "TRACE_ID header tag must be exactly 16 bytes"
            ))
        ));
    }
}
