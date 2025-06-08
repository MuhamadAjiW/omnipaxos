use omnipaxos::erasure::ec_service::EntryFragment;
use omnipaxos::erasure::log_entry::{ECEntry, OperationType};
use omnipaxos::storage::Entry;

#[derive(Clone, Debug)]
pub struct ECKeyValue {
    pub key: String,
    pub fragment: EntryFragment,
    pub op: OperationType,
}

impl Entry for ECKeyValue {}

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
}
