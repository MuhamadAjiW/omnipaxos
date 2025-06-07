use omnipaxos::{
    ballot_leader_election::Ballot,
    erasure::log_entry::DefaultLogEntry,
    storage::{Entry, StopSign, Storage, StorageOp, StorageResult},
};

use crate::memory_storage::MemoryStorage;

/// Adapter for erasure coded log entries
#[derive(Clone)]
pub struct ErasureMemoryStorage {
    /// The inner memory storage that holds the log entries.
    pub inner: MemoryStorage<DefaultLogEntry>,
}

impl ErasureMemoryStorage {
    /// Creates a new instance of `ErasureMemoryStorage`.
    pub fn new() -> Self {
        Self {
            inner: MemoryStorage::default(),
        }
    }
}

impl Storage<DefaultLogEntry> for ErasureMemoryStorage {
    fn write_atomically(&mut self, ops: Vec<StorageOp<DefaultLogEntry>>) -> StorageResult<()> {
        self.inner.write_atomically(ops)
    }
    fn append_entry(&mut self, entry: DefaultLogEntry) -> StorageResult<()> {
        self.inner.append_entry(entry)
    }
    fn append_entries(&mut self, entries: Vec<DefaultLogEntry>) -> StorageResult<()> {
        self.inner.append_entries(entries)
    }
    fn append_on_prefix(
        &mut self,
        from_idx: usize,
        entries: Vec<DefaultLogEntry>,
    ) -> StorageResult<()> {
        self.inner.append_on_prefix(from_idx, entries)
    }
    fn set_promise(&mut self, n_prom: Ballot) -> StorageResult<()> {
        self.inner.set_promise(n_prom)
    }
    fn set_decided_idx(&mut self, ld: usize) -> StorageResult<()> {
        self.inner.set_decided_idx(ld)
    }
    fn get_decided_idx(&self) -> StorageResult<usize> {
        self.inner.get_decided_idx()
    }
    fn set_accepted_round(&mut self, na: Ballot) -> StorageResult<()> {
        self.inner.set_accepted_round(na)
    }
    fn get_accepted_round(&self) -> StorageResult<Option<Ballot>> {
        self.inner.get_accepted_round()
    }
    fn get_entries(&self, from: usize, to: usize) -> StorageResult<Vec<DefaultLogEntry>> {
        self.inner.get_entries(from, to)
    }
    fn get_log_len(&self) -> StorageResult<usize> {
        self.inner.get_log_len()
    }
    fn get_suffix(&self, from: usize) -> StorageResult<Vec<DefaultLogEntry>> {
        self.inner.get_suffix(from)
    }
    fn get_promise(&self) -> StorageResult<Option<Ballot>> {
        self.inner.get_promise()
    }
    fn set_stopsign(&mut self, s: Option<StopSign>) -> StorageResult<()> {
        self.inner.set_stopsign(s)
    }
    fn get_stopsign(&self) -> StorageResult<Option<StopSign>> {
        self.inner.get_stopsign()
    }
    fn trim(&mut self, trimmed_idx: usize) -> StorageResult<()> {
        self.inner.trim(trimmed_idx)
    }
    fn set_compacted_idx(&mut self, compact_idx: usize) -> StorageResult<()> {
        self.inner.set_compacted_idx(compact_idx)
    }
    fn get_compacted_idx(&self) -> StorageResult<usize> {
        self.inner.get_compacted_idx()
    }
    fn set_snapshot(
        &mut self,
        _snapshot: Option<<DefaultLogEntry as Entry>::Snapshot>,
    ) -> StorageResult<()> {
        Ok(())
    }
    fn get_snapshot(&self) -> StorageResult<Option<<DefaultLogEntry as Entry>::Snapshot>> {
        Ok(None)
    }
}
