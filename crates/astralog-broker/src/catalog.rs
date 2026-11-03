use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use astralog_common::TopicId;

use crate::{BrokerError, Result};

const CATALOG_FILE: &str = "catalog.log";
const RECORD_MAGIC: &[u8; 4] = b"ACR1";
const RECORD_VERSION: u16 = 1;
const MAX_RECORD_LEN: usize = 1024 * 1024;
const RECORD_HEADER_LEN: usize = 8;
const RECORD_TRAILER_LEN: usize = 4;

const OP_INIT: u8 = 1;
const OP_BUMP_INCARNATION: u8 = 2;
const OP_CREATE_TOPIC: u8 = 3;
const OP_DELETE_TOPIC: u8 = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicDefinition {
    pub topic_id: TopicId,
    pub name: String,
    pub partition_count: u32,
    pub replication_factor: u16,
}

#[derive(Debug, Clone)]
pub struct CatalogSnapshot {
    pub cluster_id: [u8; 16],
    pub node_uuid: [u8; 16],
    pub broker_id: u32,
    pub incarnation_epoch: u64,
    pub metadata_epoch: u64,
    pub topics: Vec<TopicDefinition>,
}

pub struct Catalog {
    path: PathBuf,
    file: File,
    cluster_id: [u8; 16],
    node_uuid: [u8; 16],
    broker_id: u32,
    incarnation_epoch: u64,
    metadata_epoch: u64,
    topics_by_name: BTreeMap<String, TopicDefinition>,
    topic_names_by_id: BTreeMap<[u8; 16], String>,
}

impl Catalog {
    pub fn open_or_create(directory: &Path, broker_id: u32) -> Result<Self> {
        Self::open_or_create_internal(directory, broker_id, None, None)
    }

    pub fn open_or_create_with_identity(
        directory: &Path,
        broker_id: u32,
        cluster_id: [u8; 16],
        node_uuid: [u8; 16],
    ) -> Result<Self> {
        Self::open_or_create_internal(directory, broker_id, Some(cluster_id), Some(node_uuid))
    }

    fn open_or_create_internal(
        directory: &Path,
        broker_id: u32,
        expected_cluster_id: Option<[u8; 16]>,
        expected_node_uuid: Option<[u8; 16]>,
    ) -> Result<Self> {
        fs::create_dir_all(directory)?;
        sync_directory_parent(directory)?;
        sync_directory(directory)?;
        let path = directory.join(CATALOG_FILE);
        if !path.exists() {
            let file = OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(&path)?;
            let mut catalog = Self {
                path,
                file,
                cluster_id: match expected_cluster_id {
                    Some(value) => value,
                    None => random_uuid()?,
                },
                node_uuid: match expected_node_uuid {
                    Some(value) => value,
                    None => random_uuid()?,
                },
                broker_id,
                incarnation_epoch: 1,
                metadata_epoch: 1,
                topics_by_name: BTreeMap::new(),
                topic_names_by_id: BTreeMap::new(),
            };
            let payload = encode_init(
                catalog.cluster_id,
                catalog.node_uuid,
                broker_id,
                catalog.incarnation_epoch,
                catalog.metadata_epoch,
            );
            catalog.append_payload(&payload)?;
            sync_directory(directory)?;
            return Ok(catalog);
        }

        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        let mut catalog = Self {
            path,
            file,
            cluster_id: [0; 16],
            node_uuid: [0; 16],
            broker_id,
            incarnation_epoch: 0,
            metadata_epoch: 0,
            topics_by_name: BTreeMap::new(),
            topic_names_by_id: BTreeMap::new(),
        };
        catalog.replay()?;
        if catalog.broker_id != broker_id {
            return Err(BrokerError::CatalogOwned(format!(
                "configured broker id {broker_id} does not match durable broker id {}",
                catalog.broker_id
            )));
        }
        if expected_cluster_id.is_some_and(|expected| expected != catalog.cluster_id) {
            return Err(BrokerError::Catalog(
                "configured static cluster id does not match durable catalog cluster id",
            ));
        }
        if expected_node_uuid.is_some_and(|expected| expected != catalog.node_uuid) {
            return Err(BrokerError::Catalog(
                "configured static node UUID does not match durable catalog node UUID",
            ));
        }
        Ok(catalog)
    }

    pub fn snapshot(&self) -> CatalogSnapshot {
        CatalogSnapshot {
            cluster_id: self.cluster_id,
            node_uuid: self.node_uuid,
            broker_id: self.broker_id,
            incarnation_epoch: self.incarnation_epoch,
            metadata_epoch: self.metadata_epoch,
            topics: self.topics_by_name.values().cloned().collect(),
        }
    }

    pub fn topic_by_name(&self, name: &str) -> Option<TopicDefinition> {
        self.topics_by_name.get(name).cloned()
    }

    pub fn topic_by_id(&self, id: TopicId) -> Option<TopicDefinition> {
        self.topic_names_by_id
            .get(id.as_bytes())
            .and_then(|name| self.topics_by_name.get(name))
            .cloned()
    }

    pub fn bump_incarnation(&mut self) -> Result<()> {
        let incarnation_epoch = self
            .incarnation_epoch
            .checked_add(1)
            .ok_or(BrokerError::Catalog("incarnation epoch overflow"))?;
        let metadata_epoch = self
            .metadata_epoch
            .checked_add(1)
            .ok_or(BrokerError::Catalog("metadata epoch overflow"))?;
        let payload = encode_bump_incarnation(incarnation_epoch, metadata_epoch);
        self.append_payload(&payload)?;
        self.incarnation_epoch = incarnation_epoch;
        self.metadata_epoch = metadata_epoch;
        Ok(())
    }

    pub fn create_topic(
        &mut self,
        name: String,
        partition_count: u32,
        replication_factor: u16,
    ) -> Result<TopicDefinition> {
        if self.topics_by_name.contains_key(&name) {
            return Err(BrokerError::Catalog("topic already exists"));
        }
        let topic_id = loop {
            let candidate = TopicId::new(random_uuid()?);
            if !self.topic_names_by_id.contains_key(candidate.as_bytes()) {
                break candidate;
            }
        };
        let metadata_epoch = self
            .metadata_epoch
            .checked_add(1)
            .ok_or(BrokerError::Catalog("metadata epoch overflow"))?;
        let topic = TopicDefinition {
            topic_id,
            name,
            partition_count,
            replication_factor,
        };
        let payload = encode_create_topic(metadata_epoch, &topic)?;
        self.append_payload(&payload)?;
        self.metadata_epoch = metadata_epoch;
        self.topic_names_by_id
            .insert(*topic.topic_id.as_bytes(), topic.name.clone());
        self.topics_by_name
            .insert(topic.name.clone(), topic.clone());
        Ok(topic)
    }

    pub fn ensure_topic(&mut self, topic: TopicDefinition) -> Result<TopicDefinition> {
        if let Some(existing) = self.topics_by_name.get(&topic.name).cloned() {
            if existing == topic {
                return Ok(existing);
            }
            return Err(BrokerError::Catalog(
                "static topic name conflicts with durable topic definition",
            ));
        }
        if self
            .topic_names_by_id
            .contains_key(topic.topic_id.as_bytes())
        {
            return Err(BrokerError::Catalog(
                "static topic id conflicts with durable topic definition",
            ));
        }
        let metadata_epoch = self
            .metadata_epoch
            .checked_add(1)
            .ok_or(BrokerError::Catalog("metadata epoch overflow"))?;
        let payload = encode_create_topic(metadata_epoch, &topic)?;
        self.append_payload(&payload)?;
        self.metadata_epoch = metadata_epoch;
        self.topic_names_by_id
            .insert(*topic.topic_id.as_bytes(), topic.name.clone());
        self.topics_by_name
            .insert(topic.name.clone(), topic.clone());
        Ok(topic)
    }

    pub fn delete_topic(&mut self, name: &str) -> Result<TopicDefinition> {
        let topic = self
            .topics_by_name
            .get(name)
            .cloned()
            .ok_or(BrokerError::Catalog("unknown topic"))?;
        let metadata_epoch = self
            .metadata_epoch
            .checked_add(1)
            .ok_or(BrokerError::Catalog("metadata epoch overflow"))?;
        let payload = encode_delete_topic(metadata_epoch, topic.topic_id);
        self.append_payload(&payload)?;
        self.metadata_epoch = metadata_epoch;
        self.topics_by_name.remove(name);
        self.topic_names_by_id.remove(topic.topic_id.as_bytes());
        Ok(topic)
    }

    fn replay(&mut self) -> Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        let file_len = self.file.metadata()?.len();
        let mut offset = 0_u64;
        let mut saw_init = false;

        while offset < file_len {
            let remaining = file_len - offset;
            if remaining < 4 {
                self.truncate_incomplete_tail(offset)?;
                break;
            }
            let mut len_bytes = [0_u8; 4];
            self.file.read_exact(&mut len_bytes)?;
            let payload_len = usize::try_from(u32::from_be_bytes(len_bytes))
                .map_err(|_| BrokerError::Catalog("catalog record length overflow"))?;
            if !(RECORD_HEADER_LEN..=MAX_RECORD_LEN).contains(&payload_len) {
                return Err(BrokerError::CatalogOwned(format!(
                    "invalid catalog record length {payload_len} at byte {offset}"
                )));
            }
            let total = 4_u64
                .checked_add(
                    u64::try_from(payload_len)
                        .map_err(|_| BrokerError::Catalog("catalog record length overflow"))?,
                )
                .and_then(|value| value.checked_add(RECORD_TRAILER_LEN as u64))
                .ok_or(BrokerError::Catalog("catalog record size overflow"))?;
            if offset + total > file_len {
                self.truncate_incomplete_tail(offset)?;
                break;
            }

            let mut payload = vec![0_u8; payload_len];
            self.file.read_exact(&mut payload)?;
            let mut crc_bytes = [0_u8; 4];
            self.file.read_exact(&mut crc_bytes)?;
            let stored_crc = u32::from_be_bytes(crc_bytes);
            let actual_crc = crc32c::crc32c(&payload);
            if stored_crc != actual_crc {
                return Err(BrokerError::CatalogOwned(format!(
                    "catalog CRC mismatch at byte {offset}: stored {stored_crc:#010x}, computed {actual_crc:#010x}"
                )));
            }
            let op = decode_record_header(&payload)?;
            if !saw_init && op != OP_INIT {
                return Err(BrokerError::Catalog(
                    "catalog first record is not initialization",
                ));
            }
            if saw_init && op == OP_INIT {
                return Err(BrokerError::Catalog(
                    "catalog contains duplicate initialization record",
                ));
            }
            self.apply_payload(&payload)?;
            saw_init = true;
            offset += total;
        }

        if !saw_init {
            return Err(BrokerError::Catalog("catalog has no initialization record"));
        }
        self.file.seek(SeekFrom::End(0))?;
        Ok(())
    }

    fn apply_payload(&mut self, payload: &[u8]) -> Result<()> {
        let op = decode_record_header(payload)?;
        let mut cursor = RECORD_HEADER_LEN;
        match op {
            OP_INIT => {
                self.cluster_id = read_array_16(payload, &mut cursor)?;
                self.node_uuid = read_array_16(payload, &mut cursor)?;
                self.broker_id = read_u32(payload, &mut cursor)?;
                self.incarnation_epoch = read_u64(payload, &mut cursor)?;
                self.metadata_epoch = read_u64(payload, &mut cursor)?;
            }
            OP_BUMP_INCARNATION => {
                let incarnation = read_u64(payload, &mut cursor)?;
                let metadata = read_u64(payload, &mut cursor)?;
                if incarnation <= self.incarnation_epoch || metadata <= self.metadata_epoch {
                    return Err(BrokerError::Catalog(
                        "catalog incarnation/metadata epoch did not advance",
                    ));
                }
                self.incarnation_epoch = incarnation;
                self.metadata_epoch = metadata;
            }
            OP_CREATE_TOPIC => {
                let metadata = read_u64(payload, &mut cursor)?;
                if metadata <= self.metadata_epoch {
                    return Err(BrokerError::Catalog(
                        "catalog metadata epoch did not advance",
                    ));
                }
                let topic_id = TopicId::new(read_array_16(payload, &mut cursor)?);
                let name_len = usize::from(read_u16(payload, &mut cursor)?);
                let name_bytes = read_exact(payload, &mut cursor, name_len)?;
                let name = std::str::from_utf8(name_bytes)
                    .map_err(|_| BrokerError::Catalog("catalog topic name is not UTF-8"))?
                    .to_owned();
                let partition_count = read_u32(payload, &mut cursor)?;
                let replication_factor = read_u16(payload, &mut cursor)?;
                if self.topics_by_name.contains_key(&name)
                    || self.topic_names_by_id.contains_key(topic_id.as_bytes())
                {
                    return Err(BrokerError::Catalog(
                        "catalog create duplicates existing topic identity",
                    ));
                }
                let topic = TopicDefinition {
                    topic_id,
                    name: name.clone(),
                    partition_count,
                    replication_factor,
                };
                self.metadata_epoch = metadata;
                self.topic_names_by_id
                    .insert(*topic_id.as_bytes(), name.clone());
                self.topics_by_name.insert(name, topic);
            }
            OP_DELETE_TOPIC => {
                let metadata = read_u64(payload, &mut cursor)?;
                if metadata <= self.metadata_epoch {
                    return Err(BrokerError::Catalog(
                        "catalog metadata epoch did not advance",
                    ));
                }
                let topic_id = TopicId::new(read_array_16(payload, &mut cursor)?);
                let name = self.topic_names_by_id.remove(topic_id.as_bytes()).ok_or(
                    BrokerError::Catalog("catalog delete references unknown topic"),
                )?;
                self.topics_by_name.remove(&name);
                self.metadata_epoch = metadata;
            }
            _ => return Err(BrokerError::Catalog("unknown catalog operation")),
        }
        if cursor != payload.len() {
            return Err(BrokerError::Catalog("catalog record has trailing bytes"));
        }
        Ok(())
    }

    fn append_payload(&mut self, payload: &[u8]) -> Result<()> {
        if payload.len() > MAX_RECORD_LEN {
            return Err(BrokerError::Catalog("catalog record exceeds maximum"));
        }
        let previous_len = self.file.metadata()?.len();
        self.file.seek(SeekFrom::End(0))?;
        let mut encoded = Vec::with_capacity(4 + payload.len() + 4);
        encoded.extend_from_slice(
            &u32::try_from(payload.len())
                .map_err(|_| BrokerError::Catalog("catalog record length overflow"))?
                .to_be_bytes(),
        );
        encoded.extend_from_slice(payload);
        encoded.extend_from_slice(&crc32c::crc32c(payload).to_be_bytes());
        if let Err(err) = self.file.write_all(&encoded) {
            let _ = self.file.set_len(previous_len);
            let _ = self.file.sync_all();
            return Err(err.into());
        }
        self.file.sync_data()?;
        Ok(())
    }

    fn truncate_incomplete_tail(&mut self, offset: u64) -> Result<()> {
        self.file.set_len(offset)?;
        self.file.sync_all()?;
        self.file.seek(SeekFrom::Start(offset))?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn record_header(op: u8) -> Vec<u8> {
    let mut payload = Vec::with_capacity(64);
    payload.extend_from_slice(RECORD_MAGIC);
    payload.extend_from_slice(&RECORD_VERSION.to_be_bytes());
    payload.push(op);
    payload.push(0);
    payload
}

fn encode_init(
    cluster_id: [u8; 16],
    node_uuid: [u8; 16],
    broker_id: u32,
    incarnation_epoch: u64,
    metadata_epoch: u64,
) -> Vec<u8> {
    let mut payload = record_header(OP_INIT);
    payload.extend_from_slice(&cluster_id);
    payload.extend_from_slice(&node_uuid);
    payload.extend_from_slice(&broker_id.to_be_bytes());
    payload.extend_from_slice(&incarnation_epoch.to_be_bytes());
    payload.extend_from_slice(&metadata_epoch.to_be_bytes());
    payload
}

fn encode_bump_incarnation(incarnation_epoch: u64, metadata_epoch: u64) -> Vec<u8> {
    let mut payload = record_header(OP_BUMP_INCARNATION);
    payload.extend_from_slice(&incarnation_epoch.to_be_bytes());
    payload.extend_from_slice(&metadata_epoch.to_be_bytes());
    payload
}

fn encode_create_topic(metadata_epoch: u64, topic: &TopicDefinition) -> Result<Vec<u8>> {
    let name = topic.name.as_bytes();
    let name_len = u16::try_from(name.len())
        .map_err(|_| BrokerError::Catalog("topic name length overflow"))?;
    let mut payload = record_header(OP_CREATE_TOPIC);
    payload.extend_from_slice(&metadata_epoch.to_be_bytes());
    payload.extend_from_slice(topic.topic_id.as_bytes());
    payload.extend_from_slice(&name_len.to_be_bytes());
    payload.extend_from_slice(name);
    payload.extend_from_slice(&topic.partition_count.to_be_bytes());
    payload.extend_from_slice(&topic.replication_factor.to_be_bytes());
    Ok(payload)
}

fn encode_delete_topic(metadata_epoch: u64, topic_id: TopicId) -> Vec<u8> {
    let mut payload = record_header(OP_DELETE_TOPIC);
    payload.extend_from_slice(&metadata_epoch.to_be_bytes());
    payload.extend_from_slice(topic_id.as_bytes());
    payload
}

fn decode_record_header(payload: &[u8]) -> Result<u8> {
    if payload.len() < RECORD_HEADER_LEN {
        return Err(BrokerError::Catalog("catalog payload is too short"));
    }
    if &payload[0..4] != RECORD_MAGIC {
        return Err(BrokerError::Catalog("catalog record magic mismatch"));
    }
    if u16::from_be_bytes([payload[4], payload[5]]) != RECORD_VERSION {
        return Err(BrokerError::Catalog("unsupported catalog record version"));
    }
    if payload[7] != 0 {
        return Err(BrokerError::Catalog("catalog reserved byte is nonzero"));
    }
    Ok(payload[6])
}

fn read_exact<'a>(payload: &'a [u8], cursor: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = cursor
        .checked_add(len)
        .ok_or(BrokerError::Catalog("catalog cursor overflow"))?;
    if end > payload.len() {
        return Err(BrokerError::Catalog("catalog record is truncated"));
    }
    let bytes = &payload[*cursor..end];
    *cursor = end;
    Ok(bytes)
}

fn read_u16(payload: &[u8], cursor: &mut usize) -> Result<u16> {
    let bytes: [u8; 2] = read_exact(payload, cursor, 2)?
        .try_into()
        .map_err(|_| BrokerError::Catalog("u16 width"))?;
    Ok(u16::from_be_bytes(bytes))
}

fn read_u32(payload: &[u8], cursor: &mut usize) -> Result<u32> {
    let bytes: [u8; 4] = read_exact(payload, cursor, 4)?
        .try_into()
        .map_err(|_| BrokerError::Catalog("u32 width"))?;
    Ok(u32::from_be_bytes(bytes))
}

fn read_u64(payload: &[u8], cursor: &mut usize) -> Result<u64> {
    let bytes: [u8; 8] = read_exact(payload, cursor, 8)?
        .try_into()
        .map_err(|_| BrokerError::Catalog("u64 width"))?;
    Ok(u64::from_be_bytes(bytes))
}

fn read_array_16(payload: &[u8], cursor: &mut usize) -> Result<[u8; 16]> {
    read_exact(payload, cursor, 16)?
        .try_into()
        .map_err(|_| BrokerError::Catalog("uuid width"))
}

fn random_uuid() -> Result<[u8; 16]> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes)?;
    bytes[6] = (bytes[6] & 0x0F) | 0x40;
    bytes[8] = (bytes[8] & 0x3F) | 0x80;
    Ok(bytes)
}

#[cfg(unix)]
fn sync_directory(directory: &Path) -> Result<()> {
    File::open(directory)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_directory(_directory: &Path) -> Result<()> {
    Ok(())
}

fn sync_directory_parent(directory: &Path) -> Result<()> {
    if let Some(parent) = directory.parent() {
        sync_directory(parent)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "astralog-catalog-{name}-{}-{nonce}",
            std::process::id()
        ))
    }

    #[test]
    fn catalog_survives_restart_and_epochs_advance() {
        let dir = temp_dir("restart");
        let first_cluster;
        let first_node;
        let topic;
        {
            let mut catalog = Catalog::open_or_create(&dir, 7).unwrap();
            first_cluster = catalog.snapshot().cluster_id;
            first_node = catalog.snapshot().node_uuid;
            catalog.bump_incarnation().unwrap();
            topic = catalog.create_topic("orders".into(), 3, 1).unwrap();
        }
        {
            let catalog = Catalog::open_or_create(&dir, 7).unwrap();
            let snapshot = catalog.snapshot();
            assert_eq!(snapshot.cluster_id, first_cluster);
            assert_eq!(snapshot.node_uuid, first_node);
            assert_eq!(catalog.topic_by_name("orders"), Some(topic));
            assert!(snapshot.metadata_epoch >= 3);
        }
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn incomplete_tail_is_truncated_but_complete_crc_corruption_is_loud() {
        let dir = temp_dir("tail");
        let path;
        {
            let catalog = Catalog::open_or_create(&dir, 1).unwrap();
            path = catalog.path().to_path_buf();
        }
        let valid_len = fs::metadata(&path).unwrap().len();
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(&[0, 0]).unwrap();
            file.sync_all().unwrap();
        }
        let catalog = Catalog::open_or_create(&dir, 1).unwrap();
        drop(catalog);
        assert_eq!(fs::metadata(&path).unwrap().len(), valid_len);

        let mut bytes = fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        fs::write(&path, bytes).unwrap();
        assert!(Catalog::open_or_create(&dir, 1).is_err());
        let _ = fs::remove_dir_all(dir);
    }
}
