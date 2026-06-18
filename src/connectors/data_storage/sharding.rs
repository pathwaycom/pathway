// Copyright © 2026 Pathway

use xxhash_rust::xxh3::xxh3_64;

/// Distributes work items (files, shards, ...) deterministically across
/// parallel workers.
///
/// Each item is owned by exactly one worker, selected by a stable hash of the
/// item's key. The hash is deterministic across workers and processes, so all
/// workers agree on the assignment without any coordination, and the work
/// parallelizes across them.
#[derive(Debug, Clone, Copy)]
pub struct ShardSelector {
    worker_index: u64,
    total_workers: u64,
}

impl ShardSelector {
    pub fn new(worker_index: usize, total_workers: usize) -> Self {
        Self {
            worker_index: worker_index as u64,
            total_workers: total_workers as u64,
        }
    }

    /// Returns `true` if the item identified by `key` is owned by this worker.
    pub fn owns(&self, key: &[u8]) -> bool {
        self.total_workers <= 1 || xxh3_64(key) % self.total_workers == self.worker_index
    }
}
