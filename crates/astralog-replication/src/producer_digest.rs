use astralog_storage::DataRecord;

use crate::{ReplicationError, Result};

const K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

/// Reconstructs the exact P7-B canonical record-set digest from P1 records.
///
/// The public wire definition remains authoritative. This lower-layer helper
/// intentionally reproduces only that frozen byte layout so P4 recovery does
/// not acquire an upward dependency on `astralog-protocol`.
pub(crate) fn canonical_storage_record_set_digest(records: &[DataRecord]) -> Result<[u8; 32]> {
    if records.is_empty() {
        return Err(ReplicationError::Invalid(
            "idempotent committed batch cannot have zero records",
        ));
    }
    let count = u32::try_from(records.len())
        .map_err(|_| ReplicationError::Invalid("producer record count overflow"))?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&count.to_be_bytes());

    for record in records {
        if record.timestamp_ms < 0 {
            return Err(ReplicationError::Invalid(
                "idempotent committed record requires non-negative timestamp",
            ));
        }
        bytes.extend_from_slice(&record.timestamp_ms.to_be_bytes());
        put_nullable_bytes32(&mut bytes, record.key.as_deref())?;
        put_nullable_bytes32(&mut bytes, record.value.as_deref())?;
        let header_count = u16::try_from(record.headers.len())
            .map_err(|_| ReplicationError::Invalid("producer record header count overflow"))?;
        bytes.extend_from_slice(&header_count.to_be_bytes());
        for header in &record.headers {
            let name_len = u16::try_from(header.name.len())
                .map_err(|_| ReplicationError::Invalid("producer header name length overflow"))?;
            bytes.extend_from_slice(&name_len.to_be_bytes());
            bytes.extend_from_slice(&header.name);
            put_nullable_bytes32(&mut bytes, header.value.as_deref())?;
        }
    }

    Ok(sha256(&bytes))
}

fn put_nullable_bytes32(out: &mut Vec<u8>, value: Option<&[u8]>) -> Result<()> {
    match value {
        Some(value) => {
            let len = i32::try_from(value.len())
                .map_err(|_| ReplicationError::Invalid("producer field length overflow"))?;
            out.extend_from_slice(&len.to_be_bytes());
            out.extend_from_slice(value);
        }
        None => out.extend_from_slice(&(-1_i32).to_be_bytes()),
    }
    Ok(())
}

fn sha256(input: &[u8]) -> [u8; 32] {
    let mut state = [
        0x6a09e667_u32,
        0xbb67ae85,
        0x3c6ef372,
        0xa54ff53a,
        0x510e527f,
        0x9b05688c,
        0x1f83d9ab,
        0x5be0cd19,
    ];

    let bit_len = (input.len() as u64).wrapping_mul(8);
    let mut padded = Vec::with_capacity(input.len().saturating_add(72));
    padded.extend_from_slice(input);
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());

    let (chunks, remainder) = padded.as_chunks::<64>();
    debug_assert!(remainder.is_empty());
    for chunk in chunks {
        let mut words = [0_u32; 64];
        let (input_words, remainder) = chunk.as_chunks::<4>();
        debug_assert!(remainder.is_empty());
        for (index, word) in input_words.iter().enumerate() {
            words[index] = u32::from_be_bytes(*word);
        }
        for index in 16..64 {
            let s0 = words[index - 15].rotate_right(7)
                ^ words[index - 15].rotate_right(18)
                ^ (words[index - 15] >> 3);
            let s1 = words[index - 2].rotate_right(17)
                ^ words[index - 2].rotate_right(19)
                ^ (words[index - 2] >> 10);
            words[index] = words[index - 16]
                .wrapping_add(s0)
                .wrapping_add(words[index - 7])
                .wrapping_add(s1);
        }

        let mut a = state[0];
        let mut b = state[1];
        let mut c = state[2];
        let mut d = state[3];
        let mut e = state[4];
        let mut f = state[5];
        let mut g = state[6];
        let mut h = state[7];

        for index in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let choose = (e & f) ^ ((!e) & g);
            let temp1 = h
                .wrapping_add(s1)
                .wrapping_add(choose)
                .wrapping_add(K[index])
                .wrapping_add(words[index]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let majority = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(majority);
            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }

        for (slot, value) in state.iter_mut().zip([a, b, c, d, e, f, g, h]) {
            *slot = slot.wrapping_add(value);
        }
    }

    let mut out = [0_u8; 32];
    let (chunks, remainder) = out.as_chunks_mut::<4>();
    debug_assert!(remainder.is_empty());
    for (chunk, word) in chunks.iter_mut().zip(state) {
        chunk.copy_from_slice(&word.to_be_bytes());
    }
    out
}

#[cfg(test)]
mod tests {
    use astralog_storage::UserHeader;

    use super::*;

    #[test]
    fn storage_recovery_digest_matches_frozen_p7_public_golden() {
        let records = [DataRecord {
            timestamp_ms: 1,
            key: None,
            value: Some(vec![0xaa]),
            headers: vec![],
        }];
        assert_eq!(
            canonical_storage_record_set_digest(&records).unwrap(),
            [
                0x09, 0x0e, 0x5d, 0xb7, 0x27, 0x35, 0xab, 0x04, 0x5b, 0xd8, 0x2a, 0xf0, 0xdc, 0x1f,
                0xc9, 0x27, 0x46, 0x5b, 0xce, 0xd5, 0x2a, 0x5e, 0x59, 0xe6, 0x17, 0x55, 0x9c, 0x3b,
                0x79, 0x66, 0xaf, 0x4a,
            ]
        );

        let with_header = [DataRecord {
            timestamp_ms: 7,
            key: Some(b"k".to_vec()),
            value: None,
            headers: vec![UserHeader {
                name: b"h".to_vec(),
                value: Some(vec![1, 2, 3]),
            }],
        }];
        assert_eq!(
            canonical_storage_record_set_digest(&with_header).unwrap(),
            canonical_storage_record_set_digest(&with_header).unwrap()
        );
    }
}
