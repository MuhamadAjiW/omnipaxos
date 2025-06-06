//! A library of storage implementations for [OmniPaxos](https://crates.io/crates/omnipaxos)

#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![deny(missing_docs)]
/// an in-memory storage implementation with fast read and writes
pub mod memory_storage;

/// an on-disk storage implementation with persistence for the replica state and the log.
#[cfg(feature = "persistent_storage")]
pub mod persistent_storage;

/// a hybrid storage implementation that combines in-memory and persistent storage
#[cfg(feature = "hybrid_storage")]
pub mod hybrid_storage;
