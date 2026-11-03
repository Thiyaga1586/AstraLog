use crate::{ProtocolError, Result};

pub fn murmur3_x86_32(bytes: &[u8], seed: u32) -> u32 {
    const C1: u32 = 0xCC9E_2D51;
    const C2: u32 = 0x1B87_3593;

    let mut hash = seed;
    let (chunks, tail) = bytes.as_chunks::<4>();
    for chunk in chunks {
        let mut k = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        k = k.wrapping_mul(C1);
        k = k.rotate_left(15);
        k = k.wrapping_mul(C2);
        hash ^= k;
        hash = hash.rotate_left(13);
        hash = hash.wrapping_mul(5).wrapping_add(0xE654_6B64);
    }

    let mut k = 0_u32;
    match tail.len() {
        3 => {
            k ^= u32::from(tail[2]) << 16;
            k ^= u32::from(tail[1]) << 8;
            k ^= u32::from(tail[0]);
        }
        2 => {
            k ^= u32::from(tail[1]) << 8;
            k ^= u32::from(tail[0]);
        }
        1 => k ^= u32::from(tail[0]),
        0 => {}
        _ => unreachable!("chunks_exact(4) remainder is at most three bytes"),
    }
    if !tail.is_empty() {
        k = k.wrapping_mul(C1);
        k = k.rotate_left(15);
        k = k.wrapping_mul(C2);
        hash ^= k;
    }

    hash ^= u32::try_from(bytes.len()).unwrap_or(u32::MAX);
    hash ^= hash >> 16;
    hash = hash.wrapping_mul(0x85EB_CA6B);
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(0xC2B2_AE35);
    hash ^= hash >> 16;
    hash
}

pub fn partition_for_key(key: &[u8], partition_count: u32) -> Result<u32> {
    if partition_count == 0 {
        return Err(ProtocolError::InvalidPartitionCount);
    }
    let hash = murmur3_x86_32(key, 0);
    let positive = hash & 0x7FFF_FFFF;
    Ok(positive % partition_count)
}

pub trait PartitionRng {
    fn next_u32(&mut self) -> u32;
}

#[derive(Debug, Clone)]
pub struct StickyPartitioner<R> {
    rng: R,
    current: Option<u32>,
    previous: Option<u32>,
}

impl<R: PartitionRng> StickyPartitioner<R> {
    pub fn new(rng: R) -> Self {
        Self {
            rng,
            current: None,
            previous: None,
        }
    }

    pub fn current(&self) -> Option<u32> {
        self.current
    }

    pub fn choose(&mut self, writable: &[u32]) -> Result<u32> {
        if writable.is_empty() {
            return Err(ProtocolError::NoWritablePartitions);
        }
        if let Some(current) = self.current
            && writable.contains(&current)
        {
            return Ok(current);
        }

        let avoid_previous = writable.len() > 1 && self.previous.is_some();
        let candidate_count = if avoid_previous {
            writable
                .iter()
                .filter(|partition| Some(**partition) != self.previous)
                .count()
        } else {
            writable.len()
        };
        let index = uniform_index(&mut self.rng, candidate_count)?;
        let selected = if avoid_previous {
            writable
                .iter()
                .copied()
                .filter(|partition| Some(*partition) != self.previous)
                .nth(index)
                .ok_or(ProtocolError::NoWritablePartitions)?
        } else {
            writable[index]
        };
        self.current = Some(selected);
        Ok(selected)
    }

    pub fn on_batch_closed(&mut self) {
        self.previous = self.current.take().or(self.previous);
    }

    pub fn invalidate(&mut self, partition: u32) {
        if self.current == Some(partition) {
            self.previous = self.current.take();
        }
    }
}

fn uniform_index<R: PartitionRng>(rng: &mut R, count: usize) -> Result<usize> {
    if count == 0 {
        return Err(ProtocolError::NoWritablePartitions);
    }
    let bound = u64::try_from(count).map_err(|_| ProtocolError::LengthOverflow)?;
    let range = u64::from(u32::MAX) + 1;
    let zone = (range / bound) * bound;
    loop {
        let value = u64::from(rng.next_u32());
        if value < zone {
            return usize::try_from(value % bound).map_err(|_| ProtocolError::LengthOverflow);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone)]
    struct SequenceRng {
        values: Vec<u32>,
        index: usize,
    }

    impl PartitionRng for SequenceRng {
        fn next_u32(&mut self) -> u32 {
            let value = self.values[self.index % self.values.len()];
            self.index += 1;
            value
        }
    }

    #[test]
    fn frozen_murmur_vectors_match_spec() {
        let cases: &[(&[u8], u32, [u32; 3])] = &[
            (b"", 0x0000_0000, [0, 0, 0]),
            (b"foo", 0xF6A5_C420, [1, 4, 0]),
            (b"hello", 0x248B_FA47, [1, 6, 7]),
            (b"key", 0xE29C_6A44, [2, 6, 4]),
            (b"orders-123", 0x93E8_6727, [0, 3, 7]),
        ];
        for (key, expected_hash, expected_partitions) in cases {
            assert_eq!(murmur3_x86_32(key, 0), *expected_hash);
            assert_eq!(partition_for_key(key, 3).unwrap(), expected_partitions[0]);
            assert_eq!(partition_for_key(key, 7).unwrap(), expected_partitions[1]);
            assert_eq!(partition_for_key(key, 16).unwrap(), expected_partitions[2]);
        }

        let binary: Vec<u8> = (0_u8..16).collect();
        assert_eq!(murmur3_x86_32(&binary, 0), 0x1915_73DD);
        assert_eq!(partition_for_key(&binary, 3).unwrap(), 1);
        assert_eq!(partition_for_key(&binary, 7).unwrap(), 6);
        assert_eq!(partition_for_key(&binary, 16).unwrap(), 13);

        let pi = "π".as_bytes();
        assert_eq!(murmur3_x86_32(pi, 0), 0xF195_4DF6);
        assert_eq!(partition_for_key(pi, 3).unwrap(), 0);
        assert_eq!(partition_for_key(pi, 7).unwrap(), 6);
        assert_eq!(partition_for_key(pi, 16).unwrap(), 6);
    }

    #[test]
    fn sticky_partitioner_rotates_after_batch_close() {
        let rng = SequenceRng {
            values: vec![1, 0, 2],
            index: 0,
        };
        let mut sticky = StickyPartitioner::new(rng);
        let writable = [0, 1, 2];
        let first = sticky.choose(&writable).unwrap();
        assert_eq!(first, sticky.choose(&writable).unwrap());
        sticky.on_batch_closed();
        assert_ne!(first, sticky.choose(&writable).unwrap());
    }

    #[test]
    fn sticky_partitioner_invalidates_unavailable_partition() {
        let rng = SequenceRng {
            values: vec![0, 0, 0],
            index: 0,
        };
        let mut sticky = StickyPartitioner::new(rng);
        assert_eq!(sticky.choose(&[0, 1]).unwrap(), 0);
        sticky.invalidate(0);
        assert_eq!(sticky.choose(&[1]).unwrap(), 1);
    }
}
