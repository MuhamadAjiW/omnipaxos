use crate::{
    memory_storage::MemoryStorage,
    persistent_storage::{PersistentStorage, PersistentStorageConfig},
};
use omnipaxos::{
    ballot_leader_election::Ballot,
    storage::{Entry, StopSign, Storage, StorageOp, StorageResult},
};
use serde::{Deserialize, Serialize};

/// A hybrid storage implementation for SequencePaxos that combines in-memory and persistent storage.
pub struct HybridStorage<T>
where
    T: Entry,
{
    /// In-memory storage for fast access and writes.
    mem: MemoryStorage<T>,
    /// Persistent storage for durability and long-term storage.
    disk: PersistentStorage<T>,
}

impl<T> HybridStorage<T>
where
    T: Entry + Serialize + for<'a> Deserialize<'a>,
    T::Snapshot: Serialize + for<'a> Deserialize<'a>,
{
    /// Opens a hybrid storage backend at the given path.
    pub fn open(persistent_storage_config: PersistentStorageConfig) -> Self {
        Self {
            mem: MemoryStorage::default(),
            disk: PersistentStorage::open(persistent_storage_config),
        }
    }
}

impl<T> Storage<T> for HybridStorage<T>
where
    T: Entry + Serialize + for<'a> Deserialize<'a>,
    T::Snapshot: Serialize + for<'a> Deserialize<'a>,
{
    fn append_entry(&mut self, entry: T) -> StorageResult<()> {
        self.mem.append_entry(entry.clone())?;
        self.disk.append_entry(entry)?;
        Ok(())
    }
    fn append_entries(&mut self, entries: Vec<T>) -> StorageResult<()> {
        self.mem.append_entries(entries.clone())?;
        self.disk.append_entries(entries)?;
        Ok(())
    }
    fn append_on_prefix(&mut self, from_idx: usize, entries: Vec<T>) -> StorageResult<()> {
        self.mem.append_on_prefix(from_idx, entries.clone())?;
        self.disk.append_on_prefix(from_idx, entries)?;
        Ok(())
    }
    fn trim(&mut self, idx: usize) -> StorageResult<()> {
        self.mem.trim(idx)?;
        self.disk.trim(idx)?;
        Ok(())
    }
    fn get_log_len(&self) -> StorageResult<usize> {
        let mem_len = self.mem.get_log_len()?;
        let disk_len = self.disk.get_log_len()?;
        Ok(mem_len.max(disk_len))
    }
    fn get_entries(&self, start: usize, end: usize) -> StorageResult<Vec<T>> {
        let mem_entries = self.mem.get_entries(start, end)?;
        if !mem_entries.is_empty() {
            Ok(mem_entries)
        } else {
            self.disk.get_entries(start, end)
        }
    }
    fn get_suffix(&self, from: usize) -> StorageResult<Vec<T>> {
        let mem_suffix = self.mem.get_suffix(from)?;
        if !mem_suffix.is_empty() {
            Ok(mem_suffix)
        } else {
            self.disk.get_suffix(from)
        }
    }
    fn set_promise(&mut self, n: Ballot) -> StorageResult<()> {
        self.mem.set_promise(n)?;
        self.disk.set_promise(n)?;
        Ok(())
    }
    fn get_promise(&self) -> StorageResult<Option<Ballot>> {
        self.disk.get_promise()
    }
    fn set_decided_idx(&mut self, idx: usize) -> StorageResult<()> {
        self.mem.set_decided_idx(idx)?;
        self.disk.set_decided_idx(idx)?;
        Ok(())
    }
    fn get_decided_idx(&self) -> StorageResult<usize> {
        self.disk.get_decided_idx()
    }
    fn set_accepted_round(&mut self, n: Ballot) -> StorageResult<()> {
        self.mem.set_accepted_round(n)?;
        self.disk.set_accepted_round(n)?;
        Ok(())
    }
    fn get_accepted_round(&self) -> StorageResult<Option<Ballot>> {
        self.disk.get_accepted_round()
    }
    fn set_stopsign(&mut self, ss: Option<StopSign>) -> StorageResult<()> {
        self.mem.set_stopsign(ss.clone())?;
        self.disk.set_stopsign(ss)?;
        Ok(())
    }
    fn get_stopsign(&self) -> StorageResult<Option<StopSign>> {
        self.disk.get_stopsign()
    }
    fn set_compacted_idx(&mut self, idx: usize) -> StorageResult<()> {
        self.mem.set_compacted_idx(idx)?;
        self.disk.set_compacted_idx(idx)?;
        Ok(())
    }
    fn get_compacted_idx(&self) -> StorageResult<usize> {
        self.disk.get_compacted_idx()
    }
    fn set_snapshot(&mut self, snapshot: Option<T::Snapshot>) -> StorageResult<()> {
        // Write snapshot to both backends
        self.mem.set_snapshot(snapshot.clone())?;
        self.disk.set_snapshot(snapshot)?;
        Ok(())
    }
    fn get_snapshot(&self) -> StorageResult<Option<T::Snapshot>> {
        // Prefer persistent storage for durability
        self.disk.get_snapshot()
    }
    fn write_atomically(&mut self, ops: Vec<StorageOp<T>>) -> StorageResult<()> {
        // StorageOp<T> is not Clone, so we can only pass the ops to one backend.
        // For durability, we apply atomic writes to the persistent backend only.
        self.disk.write_atomically(ops)?;
        Ok(())
    }
}
