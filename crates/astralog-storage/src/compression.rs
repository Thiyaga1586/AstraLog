use crate::{Compression, Result, StorageError};

pub(crate) const P8_ZSTD_LEVEL: i32 = 3;

pub(crate) fn compress_data_payload(
    requested: Compression,
    raw: &[u8],
) -> Result<(Compression, Vec<u8>)> {
    let compressed = match requested {
        Compression::None => return Ok((Compression::None, raw.to_vec())),
        Compression::Lz4 => lz4_flex::block::compress(raw),
        Compression::Zstd => zstd::bulk::compress(raw, P8_ZSTD_LEVEL).map_err(|error| {
            StorageError::InvalidFormatOwned(format!("zstd compression failed: {error}"))
        })?,
    };

    if compressed.len() < raw.len() {
        Ok((requested, compressed))
    } else {
        Ok((Compression::None, raw.to_vec()))
    }
}

pub(crate) fn decompress_data_payload(
    compression: Compression,
    stored: &[u8],
    expected_len: usize,
) -> Result<Vec<u8>> {
    let mut out = vec![0_u8; expected_len];
    let written = match compression {
        Compression::None => {
            return Err(StorageError::InvalidFormat(
                "decompression helper cannot be used for compression=none",
            ));
        }
        Compression::Lz4 => {
            lz4_flex::block::decompress_into(stored, &mut out).map_err(|error| {
                StorageError::InvalidFormatOwned(format!("lz4 decompression failed: {error}"))
            })?
        }
        Compression::Zstd => {
            let frame_size =
                zstd::zstd_safe::find_frame_compressed_size(stored).map_err(|code| {
                    StorageError::InvalidFormatOwned(format!(
                        "zstd frame-size validation failed: {}",
                        zstd::zstd_safe::get_error_name(code),
                    ))
                })?;
            if frame_size != stored.len() {
                return Err(StorageError::InvalidFormat(
                    "zstd payload contains trailing codec data",
                ));
            }
            zstd::bulk::decompress_to_buffer(stored, &mut out).map_err(|error| {
                StorageError::InvalidFormatOwned(format!("zstd decompression failed: {error}"))
            })?
        }
    };
    if written != expected_len {
        return Err(StorageError::InvalidFormatOwned(format!(
            "decompressed payload length mismatch: {written} != {expected_len}",
        )));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn p8_codec_profiles_are_deterministic_and_no_gain_falls_back_to_none() {
        let compressible = vec![b'A'; 4096];
        for codec in [Compression::Lz4, Compression::Zstd] {
            let first = compress_data_payload(codec, &compressible).unwrap();
            let second = compress_data_payload(codec, &compressible).unwrap();
            assert_eq!(first, second);
            assert_eq!(first.0, codec);
            assert!(first.1.len() < compressible.len());
            assert_eq!(
                decompress_data_payload(codec, &first.1, compressible.len()).unwrap(),
                compressible,
            );
        }

        let tiny = [0x7f_u8];
        for codec in [Compression::Lz4, Compression::Zstd] {
            let stored = compress_data_payload(codec, &tiny).unwrap();
            assert_eq!(stored, (Compression::None, tiny.to_vec()));
        }
    }

    #[test]
    fn p8_codec_decoders_reject_truncation_trailing_and_wrong_output_size() {
        let raw = vec![b'B'; 2048];
        for codec in [Compression::Lz4, Compression::Zstd] {
            let (_, stored) = compress_data_payload(codec, &raw).unwrap();

            let truncated = &stored[..stored.len() - 1];
            assert!(decompress_data_payload(codec, truncated, raw.len()).is_err());

            let mut trailing = stored.clone();
            trailing.push(0);
            assert!(decompress_data_payload(codec, &trailing, raw.len()).is_err());

            assert!(decompress_data_payload(codec, &stored, raw.len() - 1).is_err());
            assert!(decompress_data_payload(codec, &stored, raw.len() + 1).is_err());
        }
    }
}
