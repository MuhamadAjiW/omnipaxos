use core::fmt;

use serde::{Deserialize, Serialize};

use crate::{
    erasure::ec_service::EntryFragment,
    storage::{Entry, NoSnapshot},
};

// _TODO: Make a more generic type later
/// The type of the operation performed on the log entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationType {
    /// A SET operation, e.g., writing a value to the log.
    SET,
    /// A DELETE operation, e.g., removing a value from the log.
    DELETE,
}
impl fmt::Display for OperationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let str;
        match self {
            OperationType::SET => str = "SET",
            OperationType::DELETE => str = "DEL",
        }

        write!(f, "{}", str)
    }
}

/// Trait for a log entry for erasure coded consensus.
pub trait ECEntry: Entry {
    /// The operation type, e.g., SET or DELETE.
    fn operation(&self) -> &OperationType;
    /// The key of the log entry, which is a unique identifier for the value.
    fn key(&self) -> &str;
    /// The value of the log entry, which is a fragment of the original log entry.
    fn value(&self) -> &EntryFragment;
}

/// A default log entry struct implementing the LogEntry trait
#[derive(Clone, Debug, PartialEq)]
pub struct DefaultECEntry {
    /// The type of operation performed on the log entry, e.g., SET or DELETE.
    pub operation: OperationType,
    /// The key of the log entry, which is a unique identifier for the value.
    pub key: String,
    /// The value of the log entry, which is a fragment of the original log entry.
    pub value: EntryFragment,
}

impl Entry for DefaultECEntry {
    type Snapshot = NoSnapshot;
}

impl ECEntry for DefaultECEntry {
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
