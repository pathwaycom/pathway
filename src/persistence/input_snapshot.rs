use futures::channel::oneshot::Receiver as OneShotReceiver;
use serde::{Deserialize, Serialize};

use crate::engine::{Key, Timestamp, Value};
use crate::persistence::frontier::OffsetAntichain;
use crate::persistence::Error;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Event {
    Insert(Key, Vec<Value>),
    Delete(Key, Vec<Value>),
    Upsert(Key, Option<Vec<Value>>),
    AdvanceTime(Timestamp, OffsetAntichain),
    Finished,
}

// To be implemented...
// This class is supposed to be the generalized version for SnapshotReader class
// from connectors/snapshot.rs
//
// The difference from connectors/snapshot.rs is that there shouldn't be
// individual implementations for S3, filesystem, etc. Instead,
// SnapshotBackend implementation will be used to access the storage.
#[allow(clippy::module_name_repetitions)]
pub struct InputSnapshotReader {}

impl InputSnapshotReader {
    /// This method will be called every so often to read the persisted snapshot.
    /// When there are no entries left, it must return `Event::Finished`.
    pub fn read(&mut self) -> Result<Event, Error> {
        todo!()
    }

    /// This method will be called when the read is done.
    /// It must return the most actual frontier corresponding to the data read.
    pub fn last_frontier(&self) -> &OffsetAntichain {
        todo!()
    }
}

// To be implemented...
// This class is supposed to have the same role as WriteSnapshotEvent trait
// implementers from connectors/snapshot.rs
//
// Similarly, the main difference is that it takes the implementation of
// SnapshotBackend in the constructor and uses it to perform the snapshotting.
#[allow(clippy::module_name_repetitions)]
pub struct InputSnapshotWriter {}

impl InputSnapshotWriter {
    /// A non-blocking call, pushing an entry in the buffer.
    /// The buffer should not be flushed in the same thread.
    pub fn write(&mut self, _event: &Event) -> Result<(), Error> {
        todo!()
    }

    /// Flush the entries which are currently present in the buffer.
    /// The result returned must be waited and return an `Ok()` when the data is uploaded.
    ///
    /// We use `futures::channel::oneshot::channel` here instead of Future/Promise
    /// because it uses modern Rust Futures that are also used by `async`.
    pub fn flush(&mut self) -> OneShotReceiver<Result<(), Error>> {
        todo!()
    }
}
