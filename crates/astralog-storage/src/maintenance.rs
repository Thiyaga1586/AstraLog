use astralog_common::StreamOffset;

use crate::{Result, StorageError};

/// Storage V1 control kind reserved by P9 for committed logical-retention authority.
///
/// The numeric range is deliberately disjoint from the older P4 control kinds.
pub const P9_CONTROL_ADVANCE_LOG_START_OFFSET: u16 = 0x0901;
pub const P9_CONTROL_PRODUCER_STATE_CHECKPOINT: u16 = 0x0902;
pub const P9_CONTROL_ADVANCE_COMPACTION_SAFE_OFFSET: u16 = 0x0903;
pub const P9_MAINTENANCE_CONTROL_VERSION: u16 = 1;

pub fn encode_log_start_offset_body(offset: StreamOffset) -> [u8; 8] {
    offset.get().to_le_bytes()
}

pub fn decode_log_start_offset_body(body: &[u8]) -> Result<StreamOffset> {
    let bytes: [u8; 8] = body.try_into().map_err(|_| {
        StorageError::InvalidFormat("P9 AdvanceLogStartOffset body must be exactly eight bytes")
    })?;
    Ok(StreamOffset::new(u64::from_le_bytes(bytes)))
}

pub fn encode_compaction_safe_offset_body(offset: StreamOffset) -> [u8; 8] {
    offset.get().to_le_bytes()
}

pub fn decode_compaction_safe_offset_body(body: &[u8]) -> Result<StreamOffset> {
    let bytes: [u8; 8] = body.try_into().map_err(|_| {
        StorageError::InvalidFormat(
            "P9 AdvanceCompactionSafeOffset body must be exactly eight bytes",
        )
    })?;
    Ok(StreamOffset::new(u64::from_le_bytes(bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn p9d_log_start_control_body_is_exact_and_canonical() {
        let offset = StreamOffset::new(0x0102_0304_0506_0708);
        let bytes = encode_log_start_offset_body(offset);
        assert_eq!(bytes, [8, 7, 6, 5, 4, 3, 2, 1]);
        assert_eq!(decode_log_start_offset_body(&bytes).unwrap(), offset);
        assert!(decode_log_start_offset_body(&bytes[..7]).is_err());
    }

    #[test]
    fn p9g_safe_offset_control_body_is_exact_and_canonical() {
        let value = StreamOffset::new(0x0102_0304_0506_0708);
        let bytes = encode_compaction_safe_offset_body(value);
        assert_eq!(bytes, [8, 7, 6, 5, 4, 3, 2, 1]);
        assert_eq!(decode_compaction_safe_offset_body(&bytes).unwrap(), value);
        assert!(decode_compaction_safe_offset_body(&bytes[..7]).is_err());
    }
}
