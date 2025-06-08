use core::fmt;

use serde::{Deserialize, Serialize};

use crate::{erasure::ec_service::EntryFragment, storage::Entry};

// _TODO: Make a more generic type later
/// The type of the operation performed on the log entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationType {
    /// A NULL operation, e.g., a placeholder or a non operation in the log. Used for control messages.
    NULL,
    /// A SET operation, e.g., writing a value to the log.
    SET,
    /// A DELETE operation, e.g., removing a value from the log.
    DELETE,
}
impl fmt::Display for OperationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let str;
        match self {
            OperationType::NULL => str = "NULL",
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
    /// Creates a new log entry from the given key, fragment, and operation type.
    fn from_parts(key: String, fragment: EntryFragment, op: OperationType) -> Self
    where
        Self: Sized;
}
