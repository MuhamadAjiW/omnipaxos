use omnipaxos::{
    erasure::{
        ec_service::EntryFragment,
        log_entry::{ECEntry, OperationType},
    },
    storage::{Entry, Snapshot},
};
use serde::{Deserialize, Serialize};

/// A default log entry struct implementing the LogEntry trait
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestECEntry {
    /// The type of operation performed on the log entry, e.g., SET or DELETE.
    pub operation: OperationType,
    /// The key of the log entry, which is a unique identifier for the value.
    pub key: String,
    /// The value of the log entry, which is a fragment of the original log entry.
    pub value: EntryFragment,
}

impl Entry for TestECEntry {
    type Snapshot = TestECEntrySnapshot;
}

impl ECEntry for TestECEntry {
    fn operation(&self) -> &OperationType {
        &self.operation
    }
    fn key(&self) -> &str {
        &self.key
    }
    fn value(&self) -> &EntryFragment {
        &self.value
    }
}

impl TestECEntry {
    /// Creates a new TestECEntry with the given operation, key, and value.
    pub fn new(operation: OperationType, key: String, value: EntryFragment) -> Self {
        TestECEntry {
            operation,
            key,
            value,
        }
    }

    /// Temporary transitional testing purposes, delete later
    pub fn dummy(number: usize) -> Self {
        let dummy_key = format!("key_{}", number);
        let dummy_value = EntryFragment::new(number, vec![0; 10]); // Example fragment with dummy data

        TestECEntry::new(OperationType::SET, dummy_key, dummy_value)
    }
}

/// Temporary transitional testing purposes, delete later
/// A snapshot of the TestECEntry log entries, containing the latest entry and all snapshotted entries.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TestECEntrySnapshot {
    /// The latest entry in the snapshot, if available.
    pub latest_entry: Option<TestECEntry>,
    /// All snapshotted entries, including the latest entry.
    pub snapshotted: Vec<TestECEntry>,
}

impl Snapshot<TestECEntry> for TestECEntrySnapshot {
    fn create(entries: &[TestECEntry]) -> Self {
        Self {
            latest_entry: entries.last().cloned(),
            snapshotted: entries.to_vec(),
        }
    }

    fn merge(&mut self, delta: Self) {
        if !delta.snapshotted.is_empty() {
            self.latest_entry = delta.snapshotted.last().cloned();
            self.snapshotted.extend(delta.snapshotted);
        }
    }

    fn use_snapshots() -> bool {
        true
    }
}

impl TestECEntrySnapshot {
    /// Creates a new TestECEntrySnapshot with the given entries.
    pub fn contains_key(&self, key: &str) -> bool {
        self.snapshotted.iter().any(|x| x.key == key)
    }
}
