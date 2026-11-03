#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageLimits {
    pub max_frame_bytes: usize,
    pub max_uncompressed_payload_bytes: usize,
    pub max_record_bytes: usize,
    pub max_key_bytes: usize,
    pub max_header_count: usize,
    pub max_header_name_bytes: usize,
}

impl Default for StorageLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 16 * 1024 * 1024,
            max_uncompressed_payload_bytes: 16 * 1024 * 1024 - 96,
            max_record_bytes: 8 * 1024 * 1024,
            max_key_bytes: 1024 * 1024,
            max_header_count: 1024,
            max_header_name_bytes: u16::MAX as usize,
        }
    }
}
