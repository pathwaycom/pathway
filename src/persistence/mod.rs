// Copyright © 2024 Pathway

use std::sync::{Arc, Mutex};

use crate::connectors::snapshot::WriteSnapshotEvent;
use xxhash_rust::xxh3::Xxh3 as Hasher;

pub mod backends;
pub mod config;
pub mod frontier;
pub mod input_snapshot;
pub mod state;
pub mod tracker;

pub type PersistentId = u128;
pub type ExternalPersistentId = String;
pub type SharedSnapshotWriter = Arc<Mutex<Box<dyn WriteSnapshotEvent>>>;

pub use backends::Error;

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
