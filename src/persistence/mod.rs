// Copyright © 2024 Pathway

use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::persistence::input_snapshot::InputSnapshotWriter;
use crate::persistence::operator_snapshot::OperatorSnapshotWriter;

use xxhash_rust::xxh3::Xxh3 as Hasher;

use crate::engine::Timestamp;

pub mod backends;
pub mod cached_object_storage;
pub mod config;
pub mod frontier;
pub mod input_snapshot;
pub mod operator_snapshot;
pub mod state;
pub mod tracker;

pub type PersistentId = u128;
pub type UniqueName = String;
pub type SharedSnapshotWriter = Arc<Mutex<InputSnapshotWriter>>;
pub type SharedOperatorSnapshotWriter<D, R> =
    Arc<Mutex<dyn OperatorSnapshotWriter<Timestamp, D, R>>>;

pub use backends::Error;

pub trait IntoPersistentId {
    fn into_persistent_id(self) -> PersistentId;
}

impl IntoPersistentId for UniqueName {
    fn into_persistent_id(self) -> PersistentId {
        let mut hasher = Hasher::default();
        hasher.update(self.as_bytes());
        hasher.digest128()
    }
}

#[allow(clippy::module_name_repetitions)]
pub trait PersistenceTime {
    fn persistence_time() -> Self;

    fn is_from_persistence(&self) -> bool;

    //TODO: better name
    //has to be a transitive relation
    fn should_be_saved_together(&self, other: &Self, snapshot_interval: Duration) -> bool;

    #[must_use]
    fn most_recent_possible_snapshot_time(&self, snapshot_interval: Duration) -> Self;
}

impl PersistenceTime for Timestamp {
    fn persistence_time() -> Self {
        Self(0)
    }

    fn should_be_saved_together(&self, other: &Self, snapshot_interval: Duration) -> bool {
        // note: when changing granulaty of Timestamp, change num_milliseconds to a new granularity
        #[allow(clippy::cast_sign_loss)]
        let div = u64::try_from(snapshot_interval.as_millis())
            .expect("snapshot interval milliseconds should fit in u64");
        if div == 0 {
            self.0 == other.0
        } else {
            self.0 / div == other.0 / div
        }
    }

    fn most_recent_possible_snapshot_time(&self, snapshot_interval: Duration) -> Timestamp {
        #[allow(clippy::cast_sign_loss)]
        let div = u64::try_from(snapshot_interval.as_millis())
            .expect("snapshot interval milliseconds should fit in u64");
        if div == 0 {
            *self
        } else {
            Self((self.0 / div) * div)
        }
    }

    fn is_from_persistence(&self) -> bool {
        self.0 <= 1 // check also neu time
    }
}
