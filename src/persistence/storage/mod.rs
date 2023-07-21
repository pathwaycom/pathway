pub mod filesystem;

pub use filesystem::FileSystem;

use crate::connectors::data_storage::StorageType;
use crate::connectors::{OffsetKey, OffsetValue};
use crate::persistence::frontier::OffsetAntichain;
use crate::persistence::PersistentId;

/*
    TODO:

    Apparently, we are going to follow the same structure in, say, S3. So the trait Storage
    should evolve into a struct PosixLikeStorage, which then accepts an instance of trait
    PosixLikeAccessor, capable of ls, read, write, directory creation.
*/

pub trait Storage: Send {
    fn save_offset(
        &mut self,
        persistent_id: PersistentId,
        offset_key: &OffsetKey,
        offset_value: &OffsetValue,
    );
    fn frontier_for(&self, persistent_id: PersistentId) -> OffsetAntichain;

    fn register_input_source(&mut self, persistent_id: PersistentId, storage_type: &StorageType);
    fn accept_finalized_timestamp(&mut self, timestamp: u64);
    fn last_advanced_timestamp(&self) -> u64;
}
