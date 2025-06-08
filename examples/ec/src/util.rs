use omnipaxos::erasure::ec_service::{ECService, EntryFragment};

pub fn serialize_value(value: &str) -> Vec<u8> {
    value.as_bytes().to_vec()
}

pub fn assign_fragment_for_node(node_id: usize, log_idx: usize, total_shards: usize) -> usize {
    ECService::fragment_index_for_node(node_id, log_idx, total_shards)
}
