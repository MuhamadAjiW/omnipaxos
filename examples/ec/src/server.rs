use crate::ec::ECKeyValue;
use omnipaxos::erasure::ec_service::{ECService, EntryFragment};
use omnipaxos::erasure::log_entry::OperationType;
use omnipaxos::sequence_paxos_ec::SequencePaxosEC;
use omnipaxos::storage::memory_storage::MemoryStorage;

pub struct ECServer {
    pub id: usize,
    pub paxos: SequencePaxosEC<ECKeyValue, MemoryStorage<ECKeyValue>>,
    pub ec_service: ECService,
}

impl ECServer {
    pub fn propose(&mut self, key: String, value: String) {
        let bytes = value.into_bytes();
        let fragments = self.ec_service.encode(&bytes).unwrap();
        let frag_idx = ECService::fragment_index_for_node(
            self.id,
            0,
            self.ec_service.data_shards + self.ec_service.parity_shards,
        );
        let fragment = fragments[frag_idx].clone();
        let entry = ECKeyValue {
            key,
            fragment,
            op: OperationType::SET,
        };
        let _ = self.paxos.append(entry);
    }
    // Retrieval would require collecting fragments from all servers in a real deployment
}
