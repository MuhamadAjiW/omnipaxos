use reed_solomon_erasure::{galois_8::ReedSolomon, Error as RSError};
use serde::{Deserialize, Serialize};

/// A fragment of a log entry for erasure-coded consensus.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntryFragment {
    /// The index of the fragment in the original log entry.
    pub idx: usize,
    /// The data of the fragment, which is a slice of the original log entry.
    pub data: Vec<u8>,
}

impl EntryFragment {
    /// Creates a new `EntryFragment` with the given index and data.
    pub fn new(idx: usize, data: Vec<u8>) -> Self {
        Self { idx, data }
    }
}

/// Utility for encoding and decoding log entries using Reed-Solomon erasure coding.
#[derive(Clone, Debug)]
pub struct ECService {
    /// The number of data shards.
    pub data_shards: usize,
    /// The number of parity shards.
    pub parity_shards: usize,
    /// The Reed-Solomon erasure coding instance.
    pub rs: ReedSolomon,
}

impl ECService {
    /// The number of bytes used for the length prefix metadata of the original log entry.
    pub const LENGTH_PREFIX_BYTES: usize = std::mem::size_of::<usize>(); // Limited to system usize, should be good enough for most cases

    /// Creates a new `ECService` with the specified number of data and parity shards.
    pub fn new(data_shards: usize, parity_shards: usize) -> Result<Self, RSError> {
        let rs = ReedSolomon::new(data_shards, parity_shards)?;
        Ok(Self {
            data_shards,
            parity_shards,
            rs,
        })
    }

    /// Encode a log entry into fragments.
    pub fn encode(&self, value: &Vec<u8>) -> Result<Vec<EntryFragment>, RSError> {
        let original_payload_len = value.len();
        let mut data_with_len_prefix: Vec<u8> =
            Vec::with_capacity(ECService::LENGTH_PREFIX_BYTES + value.len());

        data_with_len_prefix.extend_from_slice(&original_payload_len.to_be_bytes());
        data_with_len_prefix.extend_from_slice(value);

        let payload_len = data_with_len_prefix.len();

        let shard_size = (payload_len + self.data_shards - 1) / self.data_shards;
        let padded_len = shard_size * self.data_shards;

        let mut padded_payload = data_with_len_prefix.clone();
        padded_payload.resize(padded_len, 0);

        let mut shards: Vec<Vec<u8>> = Vec::with_capacity(self.data_shards + self.parity_shards);
        for i in 0..self.data_shards {
            let start = i * shard_size;
            let end = start + shard_size;
            shards.push(padded_payload[start..end].to_vec());
        }
        for _ in 0..self.parity_shards {
            shards.push(vec![0; shard_size]);
        }
        self.rs
            .encode(&mut shards)
            .expect("Failed to encode message");

        let mut fragments = Vec::with_capacity(self.data_shards + self.parity_shards);
        for (i, shard) in shards.into_iter().enumerate() {
            fragments.push(EntryFragment::new(i, shard));
        }

        Ok(fragments)
    }

    /// Decode fragments into the original log entry.
    pub fn decode(&self, fragments: &Vec<EntryFragment>) -> Result<Vec<u8>, RSError> {
        let total_shards = self.data_shards + self.parity_shards;

        // Convert fragments to the format needed by reed_solomon_erasure
        let mut shards: Vec<Option<Vec<u8>>> = vec![None; total_shards];
        for fragment in fragments {
            if fragment.idx < total_shards {
                shards[fragment.idx] = Some(fragment.data.clone());
            }
        }

        // Check if we have enough shards to reconstruct
        let present_shards = shards.iter().filter(|s| s.is_some()).count();
        if present_shards < self.data_shards {
            return Err(reed_solomon_erasure::Error::TooFewShardsPresent);
        }

        self.rs.reconstruct(&mut shards)?;

        let mut reconstructed_padded_with_len = Vec::new();
        for i in 0..self.data_shards {
            reconstructed_padded_with_len.extend_from_slice(shards[i].as_ref().unwrap());
        }

        if reconstructed_padded_with_len.len() < Self::LENGTH_PREFIX_BYTES {
            return Err(reed_solomon_erasure::Error::IncorrectShardSize);
        }

        let len_bytes: [u8; Self::LENGTH_PREFIX_BYTES] = reconstructed_padded_with_len
            [0..Self::LENGTH_PREFIX_BYTES]
            .try_into()
            .expect("Slice length does not match array length for length prefix");

        let original_len_u64 = usize::from_be_bytes(len_bytes);
        let original_len = original_len_u64 as usize; // Just in case for 32 bit systems

        let actual_data_start_index = Self::LENGTH_PREFIX_BYTES;

        if original_len + actual_data_start_index > reconstructed_padded_with_len.len() {
            return Err(reed_solomon_erasure::Error::IncorrectShardSize);
        }

        let reconstructed_data = reconstructed_padded_with_len[actual_data_start_index..]
            .to_vec()
            .into_iter()
            .take(original_len)
            .collect();

        Ok(reconstructed_data)
    }

    /// Helper to assign a fragment index to a node for a given log index.
    pub fn fragment_index_for_node(node_id: usize, log_idx: usize, total_shards: usize) -> usize {
        // Example: round-robin assignment
        (node_id + log_idx) % total_shards
    }
}
