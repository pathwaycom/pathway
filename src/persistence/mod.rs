use std::sync::{Arc, Mutex};

use crate::connectors::snapshot::SnapshotWriter;

pub mod config;
pub mod frontier;
pub mod metadata_backends;
pub mod state;
pub mod sync;
pub mod tracker;

pub type PersistentId = u128;
pub type ExternalPersistentId = String;
pub type SharedSnapshotWriter = Arc<Mutex<dyn SnapshotWriter>>;
