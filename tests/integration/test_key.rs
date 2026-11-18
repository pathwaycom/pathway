// Copyright © 2026 Pathway

use std::hash::{BuildHasher, Hash};

use pathway_engine::engine::Key;
use xxhash_rust::xxh3::Xxh3Builder;

fn hash_with<T: Hash>(value: &T) -> u64 {
    Xxh3Builder::default().hash_one(value)
}

const SAMPLES: [u128; 6] = [
    0,
    1,
    u128::MAX,
    0x40_10_8D_33_B7,
    0xDE_AD_BE_EF_DE_AD_BE_EF_DE_AD_BE_EF_DE_AD_BE_EF,
    (1 << 64) + 0xFF_FF,
];

/// Hash-based consumers (hash maps, exchange routing, HyperLogLog sketches)
/// must observe the same hash for a key as for the plain `u128` it encodes.
#[test]
fn test_key_hashes_like_u128() {
    for value in SAMPLES {
        assert_eq!(hash_with(&Key::from_u128(value)), hash_with(&value));
    }
}

/// Ordering must be the numeric `u128` order, including across the 64-bit boundary.
#[test]
fn test_key_orders_like_u128() {
    let mut keys: Vec<Key> = SAMPLES.iter().map(|v| Key::from_u128(*v)).collect();
    keys.sort();
    let mut values = SAMPLES.to_vec();
    values.sort_unstable();
    assert_eq!(keys.iter().map(|k| k.as_u128()).collect::<Vec<_>>(), values);
}

/// The little-endian byte form must round-trip and match the `u128` encoding.
#[test]
fn test_key_le_bytes_round_trip() {
    for value in SAMPLES {
        let key = Key::from_u128(value);
        assert_eq!(key.to_le_bytes(), value.to_le_bytes());
        assert_eq!(Key::from_le_bytes(key.to_le_bytes()), key);
    }
}
