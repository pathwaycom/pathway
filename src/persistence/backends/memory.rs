// Copyright © 2024 Pathway

use log::{error, warn};
use std::collections::HashMap;

use futures::channel::oneshot;
use futures::channel::oneshot::Receiver as OneShotReceiver;

use crate::persistence::backends::PersistenceBackend;
use crate::persistence::Error;

// Memory KV-storage. Must not be available for the user.
// Used as a backend for `CachedObjectStorage` when persistence is not enabled.
#[derive(Debug, Default)]
#[allow(clippy::module_name_repetitions)]
pub struct MemoryKVStorage {
    values: HashMap<String, Vec<u8>>,
}

#[derive(Debug, thiserror::Error)]
#[allow(clippy::module_name_repetitions)]
pub enum MemoryKVStorageError {
    #[error("key not found: {0}")]
    NoSuchKey(String),
}

impl MemoryKVStorage {
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
        }
    }
}

impl PersistenceBackend for MemoryKVStorage {
    fn list_keys(&self) -> Result<Vec<String>, Error> {
        let keys = self.values.keys();
        let keys_owned: Vec<String> = keys.cloned().collect();
        Ok(keys_owned)
    }

    fn get_value(&self, key: &str) -> Result<Vec<u8>, Error> {
        let value = self.values.get(key);
        if let Some(value) = value {
            Ok(value.clone())
        } else {
            let storage_error = MemoryKVStorageError::NoSuchKey(key.into());
            Err(Error::Memory(storage_error))
        }
    }

    fn put_value(&mut self, key: &str, value: Vec<u8>) -> OneShotReceiver<Result<(), Error>> {
        let (sender, receiver) = oneshot::channel();
        sender
            .send(Ok(()))
            .expect("The receiver must still be listening for the result of the put_value");
        self.values.insert(key.to_string(), value);
        receiver
    }

    fn remove_key(&mut self, key: &str) -> Result<(), Error> {
        if self.values.remove(key).is_none() {
            warn!(
                "The key '{key}' was requested to be removed, but it's not available in the cache."
            );
        }
        Ok(())
    }
}
