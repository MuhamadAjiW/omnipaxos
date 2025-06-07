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
/// A log entry for erasure coded consensus: key is not sharded, value is a fragment.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogEntry {
    /// The operation type, e.g., SET or DELETE.
    pub operation: OperationType,
    /// The key of the log entry, which is a unique identifier for the value.
    pub key: String,
    /// The value of the log entry, which is a fragment of the original log entry.
    pub value: EntryFragment,
}

impl Entry for LogEntry {
    type Snapshot = NoSnapshot;
    #[cfg(feature = "unicache")]
    type Encoded = ();
    #[cfg(feature = "unicache")]
    type Encodable = ();
    #[cfg(feature = "unicache")]
    type NotEncodable = ();
    #[cfg(all(feature = "unicache"))]
    type EncodeResult = ();
    #[cfg(all(feature = "unicache"))]
    type UniCache = ();
}
