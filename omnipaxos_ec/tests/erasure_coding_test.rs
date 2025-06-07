#[cfg(test)]
mod tests {
    use omnipaxos_ec::erasure::{ECService, EntryFragment};

    fn setup_ec_service(shard_count: usize, parity_count: usize) -> ECService {
        ECService::new(shard_count, parity_count).unwrap()
    }

    fn sample_data(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 256) as u8).collect()
    }

    fn to_fragments(fragments: Vec<EntryFragment>) -> Vec<Option<EntryFragment>> {
        fragments.into_iter().map(Some).collect()
    }

    #[test]
    fn test_encode_decode_full() {
        let ec = setup_ec_service(4, 2);
        let original_data = sample_data(100);

        let fragments = ec.encode(&original_data).expect("Encoding failed");
        assert_eq!(fragments.len(), ec.data_shards + ec.parity_shards);

        let expected_payload_len_with_prefix = original_data.len() + ECService::LENGTH_PREFIX_BYTES;
        let expected_shard_size =
            (expected_payload_len_with_prefix + ec.data_shards - 1) / ec.data_shards;

        for fragment in &fragments {
            assert_eq!(fragment.data.len(), expected_shard_size);
        }

        let data_to_reconstruct = to_fragments(fragments);
        let reconstructed_data = ec
            .decode(
                &data_to_reconstruct
                    .iter()
                    .filter_map(|f| f.clone())
                    .collect(),
            )
            .expect("Reconstruction failed");

        assert_eq!(reconstructed_data, original_data);
    }

    #[test]
    fn test_encode_decode_missing_data_shards() {
        let ec = setup_ec_service(5, 3);
        let original_data = sample_data(500);

        let fragments = ec.encode(&original_data).expect("Encoding failed");
        let mut data_to_reconstruct = to_fragments(fragments);
        data_to_reconstruct[0] = None;
        data_to_reconstruct[1] = None;
        data_to_reconstruct[2] = None;

        let available_fragments: Vec<EntryFragment> = data_to_reconstruct
            .iter()
            .filter_map(|f| f.clone())
            .collect();
        let reconstructed_data = ec
            .decode(&available_fragments)
            .expect("Reconstruction failed with missing data shards");

        assert_eq!(reconstructed_data, original_data);
    }

    #[test]
    fn test_encode_decode_missing_parity_shards() {
        let ec = setup_ec_service(4, 2);
        let original_data = sample_data(250);

        let fragments = ec.encode(&original_data).expect("Encoding failed");
        let mut data_to_reconstruct = to_fragments(fragments);
        data_to_reconstruct[ec.data_shards] = None;
        data_to_reconstruct[ec.data_shards + 1] = None;

        let available_fragments: Vec<EntryFragment> = data_to_reconstruct
            .iter()
            .filter_map(|f| f.clone())
            .collect();
        let reconstructed_data = ec
            .decode(&available_fragments)
            .expect("Reconstruction failed with missing parity shards");

        assert_eq!(reconstructed_data, original_data);
    }

    #[test]
    fn test_encode_decode_missing_mixed_shards() {
        let ec = setup_ec_service(6, 4);
        let original_data = sample_data(1024);

        let fragments = ec.encode(&original_data).expect("Encoding failed");
        let mut data_to_reconstruct = to_fragments(fragments);
        data_to_reconstruct[0] = None;
        data_to_reconstruct[1] = None;
        data_to_reconstruct[ec.data_shards] = None;
        data_to_reconstruct[ec.data_shards + 1] = None;

        let available_fragments: Vec<EntryFragment> = data_to_reconstruct
            .iter()
            .filter_map(|f| f.clone())
            .collect();
        let reconstructed_data = ec
            .decode(&available_fragments)
            .expect("Reconstruction failed with missing mixed shards");

        assert_eq!(reconstructed_data, original_data);
    }

    #[test]
    fn test_encode_decode_too_many_missing_shards() {
        let ec = setup_ec_service(3, 2);
        let original_data = sample_data(200);

        let fragments = ec.encode(&original_data).expect("Encoding failed");
        let mut data_to_reconstruct = to_fragments(fragments);
        data_to_reconstruct[0] = None;
        data_to_reconstruct[1] = None;
        data_to_reconstruct[2] = None;

        let available_fragments: Vec<EntryFragment> = data_to_reconstruct
            .iter()
            .filter_map(|f| f.clone())
            .collect();
        let result = ec.decode(&available_fragments);
        assert!(result.is_err());
    }

    #[test]
    fn test_encode_empty_data() {
        let ec = setup_ec_service(3, 2);
        let original_data = vec![];

        let fragments = ec.encode(&original_data).expect("Encoding failed");
        assert_eq!(fragments.len(), ec.data_shards + ec.parity_shards);

        let expected_payload_len_with_prefix = ECService::LENGTH_PREFIX_BYTES;
        let expected_shard_size =
            (expected_payload_len_with_prefix + ec.data_shards - 1) / ec.data_shards;

        for fragment in &fragments {
            assert_eq!(fragment.data.len(), expected_shard_size);
        }

        let data_to_reconstruct = to_fragments(fragments);
        let available_fragments: Vec<EntryFragment> = data_to_reconstruct
            .iter()
            .filter_map(|f| f.clone())
            .collect();
        let reconstructed_data = ec
            .decode(&available_fragments)
            .expect("Reconstruction failed for empty data");

        assert_eq!(reconstructed_data, original_data);
    }

    #[test]
    fn test_encode_no_padding() {
        let ec = setup_ec_service(4, 2);
        let original_data_len = 251; // Prime length
        let total_data_len_with_prefix_base = original_data_len + ECService::LENGTH_PREFIX_BYTES;
        let shard_size_calculated =
            (total_data_len_with_prefix_base + ec.data_shards - 1) / ec.data_shards;

        let total_data_len_with_prefix = shard_size_calculated * ec.data_shards;
        let final_original_data_len = total_data_len_with_prefix - ECService::LENGTH_PREFIX_BYTES;

        let original_data = sample_data(final_original_data_len);

        let fragments = ec.encode(&original_data).expect("Encoding failed");
        assert_eq!(fragments.len(), ec.data_shards + ec.parity_shards);

        for fragment in &fragments {
            assert_eq!(fragment.data.len(), shard_size_calculated);
        }

        let data_to_reconstruct = to_fragments(fragments);
        let available_fragments: Vec<EntryFragment> = data_to_reconstruct
            .iter()
            .filter_map(|f| f.clone())
            .collect();
        let reconstructed_data = ec
            .decode(&available_fragments)
            .expect("Reconstruction failed with no padding");

        assert_eq!(reconstructed_data, original_data);
    }

    #[test]
    fn test_encode_decode_different_configs() {
        let configs = vec![(3, 1), (5, 2), (10, 4), (1, 1), (2, 2)];

        for (shard_count, parity_count) in configs {
            let ec = setup_ec_service(shard_count, parity_count);
            let original_data = sample_data(shard_count * 50 + 10);

            let fragments = ec.encode(&original_data).expect("Encoding failed");
            assert_eq!(fragments.len(), shard_count + parity_count);

            let mut data_to_reconstruct: Vec<Option<EntryFragment>> =
                fragments.into_iter().map(Some).collect();
            let missing_count = parity_count / 2;
            for i in 0..missing_count {
                if i < data_to_reconstruct.len() {
                    data_to_reconstruct[i] = None;
                }
            }

            let available_fragments: Vec<EntryFragment> = data_to_reconstruct
                .iter()
                .filter_map(|f| f.clone())
                .collect();
            let reconstructed_data = ec.decode(&available_fragments).expect(&format!(
                "Reconstruction failed for config {}/{} with {} missing shards",
                shard_count, parity_count, missing_count
            ));

            assert_eq!(
                reconstructed_data, original_data,
                "Data mismatch for config {}/{}",
                shard_count, parity_count
            );
        }
    }
}
