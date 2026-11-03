use crate::{ConfigurationChange, LogIndex, Term};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryKind {
    Application(Vec<u8>),
    LeaderBarrier,
    Configuration(ConfigurationChange),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub term: Term,
    pub index: LogIndex,
    pub kind: EntryKind,
}

impl LogEntry {
    pub fn new(term: Term, index: LogIndex, kind: EntryKind) -> Self {
        Self { term, index, kind }
    }
}
