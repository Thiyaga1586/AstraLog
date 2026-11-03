use crate::{
    MAX_WIRE_FRAME_LEN_V1, MIN_WIRE_FRAME_LEN, ProtocolError, Result, WireFrame, decode_frame,
};

#[derive(Debug, Clone)]
pub struct FrameDecoder {
    buffer: Vec<u8>,
    max_frame_len: usize,
    max_buffer_len: usize,
}

impl FrameDecoder {
    pub fn new(max_frame_len: usize, max_buffer_len: usize) -> Result<Self> {
        let max_frame_len = max_frame_len.min(MAX_WIRE_FRAME_LEN_V1);
        if max_frame_len < MIN_WIRE_FRAME_LEN {
            return Err(ProtocolError::InvalidFrame(
                "decoder max frame length is below Wire V1 minimum",
            ));
        }
        let required = max_frame_len
            .checked_add(4)
            .ok_or(ProtocolError::LengthOverflow)?;
        if max_buffer_len < required {
            return Err(ProtocolError::InvalidFrame(
                "decoder buffer budget cannot hold one maximum frame",
            ));
        }
        Ok(Self {
            buffer: Vec::new(),
            max_frame_len,
            max_buffer_len,
        })
    }

    pub fn buffered_len(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_idle(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn reset(&mut self) {
        self.buffer.clear();
    }

    pub fn push(&mut self, mut input: &[u8]) -> Result<Vec<WireFrame>> {
        let mut frames = Vec::new();

        while !input.is_empty() {
            if self.buffer.len() < 4 {
                let needed = 4 - self.buffer.len();
                let take = needed.min(input.len());
                self.extend_bounded(&input[..take])?;
                input = &input[take..];
                if self.buffer.len() < 4 {
                    break;
                }
            }

            let declared = usize::try_from(u32::from_be_bytes(
                self.buffer[0..4]
                    .try_into()
                    .map_err(|_| ProtocolError::InvalidFrame("length prefix"))?,
            ))
            .map_err(|_| ProtocolError::LengthOverflow)?;
            if declared < MIN_WIRE_FRAME_LEN {
                return Err(ProtocolError::InvalidFrame("frame length below minimum"));
            }
            if declared > self.max_frame_len {
                return Err(ProtocolError::FrameTooLarge {
                    len: declared,
                    max: self.max_frame_len,
                });
            }
            let total = declared
                .checked_add(4)
                .ok_or(ProtocolError::LengthOverflow)?;
            if total > self.max_buffer_len {
                return Err(ProtocolError::FrameTooLarge {
                    len: declared,
                    max: self.max_buffer_len.saturating_sub(4),
                });
            }

            if self.buffer.len() < total {
                let needed = total - self.buffer.len();
                let take = needed.min(input.len());
                self.extend_bounded(&input[..take])?;
                input = &input[take..];
                if self.buffer.len() < total {
                    break;
                }
            }

            let frame = decode_frame(&self.buffer[..total], self.max_frame_len)?;
            frames.push(frame);
            self.buffer.drain(..total);
        }

        Ok(frames)
    }

    fn extend_bounded(&mut self, bytes: &[u8]) -> Result<()> {
        let next_len = self
            .buffer
            .len()
            .checked_add(bytes.len())
            .ok_or(ProtocolError::LengthOverflow)?;
        if next_len > self.max_buffer_len {
            return Err(ProtocolError::FrameTooLarge {
                len: next_len.saturating_sub(4),
                max: self.max_buffer_len.saturating_sub(4),
            });
        }
        self.buffer.extend_from_slice(bytes);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{FrameKind, MAX_WIRE_FRAME_LEN_V1, TaggedField, WireFrame, encode_frame};

    use super::*;

    fn sample_frame(correlation_id: u64) -> WireFrame {
        WireFrame {
            kind: FrameKind::Request,
            flags: 0,
            api_key: 0,
            api_version: 0,
            error_code: 0,
            correlation_id,
            timeout_ms: 5_000,
            throttle_ms: 0,
            header_tags: vec![TaggedField {
                id: 1,
                value: b"decoder-test".to_vec(),
            }],
            body: vec![0, 0],
        }
    }

    fn decoder() -> FrameDecoder {
        FrameDecoder::new(1024 * 1024, 1024 * 1024 + 4).unwrap()
    }

    #[test]
    fn every_fragment_boundary_reassembles_one_frame() {
        let frame = sample_frame(9);
        let encoded = encode_frame(&frame, MAX_WIRE_FRAME_LEN_V1).unwrap();
        for cut in 1..encoded.len() {
            let mut decoder = decoder();
            assert!(decoder.push(&encoded[..cut]).unwrap().is_empty());
            let frames = decoder.push(&encoded[cut..]).unwrap();
            assert_eq!(frames, vec![frame.clone()]);
            assert!(decoder.is_idle());
        }
    }

    #[test]
    fn one_byte_at_a_time_is_supported() {
        let frame = sample_frame(11);
        let encoded = encode_frame(&frame, MAX_WIRE_FRAME_LEN_V1).unwrap();
        let mut decoder = decoder();
        let mut frames = Vec::new();
        for byte in encoded {
            frames.extend(decoder.push(&[byte]).unwrap());
        }
        assert_eq!(frames, vec![frame]);
        assert!(decoder.is_idle());
    }

    #[test]
    fn multiple_frames_in_one_read_are_supported() {
        let first = sample_frame(1);
        let second = sample_frame(2);
        let mut encoded = encode_frame(&first, MAX_WIRE_FRAME_LEN_V1).unwrap();
        encoded.extend_from_slice(&encode_frame(&second, MAX_WIRE_FRAME_LEN_V1).unwrap());

        let mut decoder = decoder();
        assert_eq!(decoder.push(&encoded).unwrap(), vec![first, second]);
        assert!(decoder.is_idle());
    }

    #[test]
    fn oversized_prefix_is_rejected_as_soon_as_prefix_is_complete() {
        let mut decoder = FrameDecoder::new(512, 516).unwrap();
        let prefix = 513_u32.to_be_bytes();
        assert!(matches!(
            decoder.push(&prefix),
            Err(ProtocolError::FrameTooLarge { len: 513, max: 512 })
        ));
        assert_eq!(decoder.buffered_len(), 4);
    }

    #[test]
    fn below_minimum_prefix_is_rejected_as_soon_as_prefix_is_complete() {
        let mut decoder = decoder();
        let prefix = u32::try_from(MIN_WIRE_FRAME_LEN - 1).unwrap().to_be_bytes();
        assert!(matches!(
            decoder.push(&prefix),
            Err(ProtocolError::InvalidFrame("frame length below minimum"))
        ));
        assert_eq!(decoder.buffered_len(), 4);
    }

    #[test]
    fn deterministic_arbitrary_input_does_not_panic() {
        let mut state = 0xD1CE_BAAD_F00D_u64;
        for len in 0..512 {
            let mut bytes = vec![0_u8; len];
            for byte in &mut bytes {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                *byte = (state >> 32) as u8;
            }
            let mut decoder = decoder();
            let _ = decoder.push(&bytes);
        }
    }
}
