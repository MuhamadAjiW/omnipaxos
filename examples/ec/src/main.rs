mod ec;
mod server;
mod util;

use omnipaxos::erasure::ec_service::ECService;
use omnipaxos::sequence_paxos_ec::SequencePaxosEC;
use omnipaxos::storage::memory_storage::MemoryStorage;
use server::ECServer;

fn main() {
    // Example: 3 data shards, 2 parity shards
    let ec_service = ECService::new(3, 2).unwrap();
    let storage = MemoryStorage::default();
    // You would need to build a real SequencePaxosEC config here
    // let paxos = SequencePaxosEC::<ECKeyValue, _>::with(config, storage);
    // let mut server = ECServer { id: 0, paxos, ec_service };
    // server.propose("key1".to_string(), "value1".to_string());
    println!("EC Paxos KV example setup");
}
