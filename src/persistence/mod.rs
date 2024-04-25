// Copyright © 2024 Pathway

use std::sync::{Arc, Mutex};

use xxhash_rust::xxh3::Xxh3 as Hasher;

use crate::connectors::snapshot::WriteSnapshotEvent;

pub mod config;
pub mod frontier;
pub mod metadata_backends;
pub mod state;
pub mod tracker;

pub type PersistentId = u128;
pub type ExternalPersistentId = String;
pub type SharedSnapshotWriter = Arc<Mutex<Box<dyn WriteSnapshotEvent>>>;

pub trait IntoPersistentId {
    fn into_persistent_id(self) -> PersistentId;
}

impl IntoPersistentId for ExternalPersistentId {
    fn into_persistent_id(self) -> PersistentId {
        let mut hasher = Hasher::default();
        hasher.update(self.as_bytes());
        hasher.digest128()
    }
}
