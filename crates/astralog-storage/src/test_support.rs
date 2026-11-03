use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

pub(crate) fn test_directory(component: &str, label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("test clock must be after Unix epoch")
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "astralog-{component}-{label}-{}-{nanos}",
        std::process::id(),
    ));
    fs::create_dir_all(&path).expect("test directory creation must succeed");
    path
}

pub(crate) fn decode_hex(input: &str) -> Vec<u8> {
    let input = input.trim();
    assert!(input.len().is_multiple_of(2), "hex fixture has odd length");
    let (pairs, remainder) = input.as_bytes().as_chunks::<2>();
    assert!(remainder.is_empty(), "hex fixture has odd length");
    pairs
        .iter()
        .map(|pair| {
            let high = nibble(pair[0]);
            let low = nibble(pair[1]);
            (high << 4) | low
        })
        .collect()
}

fn nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        b'A'..=b'F' => byte - b'A' + 10,
        _ => panic!("invalid hex fixture byte"),
    }
}
