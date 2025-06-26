use std::collections::HashMap;

use omnipaxos::erasure::ec_service::EntryFragment;
use omnipaxos::erasure::log_entry::{ECEntry, OperationType};
use omnipaxos::macros::Entry;
use omnipaxos::storage::Snapshot;
use serde::{Deserialize, Serialize};

#[derive(Entry, Clone, Debug)]
#[snapshot(ECKVSnapshot)]
pub struct ECKeyValue {
    pub key: String,
    pub fragment: EntryFragment,
    pub op: OperationType,
}

impl ECEntry for ECKeyValue {
    fn operation(&self) -> &OperationType {
        &self.op
    }
    fn key(&self) -> &str {
        &self.key
    }
    fn value(&self) -> &EntryFragment {
        &self.fragment
    }
    fn from_parts(key: String, fragment: EntryFragment, op: OperationType) -> Self
    where
        Self: Sized,
    {
        Self { key, fragment, op }
    }
}

// The snapshot does not need to keep track of the operation type,
// as it is not needed for the snapshot itself. Operation type is only needed during the consensus.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ECKVSnapshot {
    snapshotted: HashMap<String, EntryFragment>,
}

impl Snapshot<ECKeyValue> for ECKVSnapshot {
    fn create(entries: &[ECKeyValue]) -> Self {
        let mut snapshotted = HashMap::new();
        for e in entries {
            snapshotted.insert(e.key.clone(), e.fragment.clone());
        }
        Self { snapshotted }
    }

    fn merge(&mut self, delta: Self) {
        for (k, v) in delta.snapshotted {
            self.snapshotted.insert(k, v);
        }
    }

    fn use_snapshots() -> bool {
        true
    }
}
