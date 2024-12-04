// Copyright © 2024 Pathway

use log::error;

use futures::channel::oneshot;

use crate::persistence::backends::PersistenceBackend;
use crate::persistence::Error;

use super::BackendPutFuture;

#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct MockKVStorage {}

impl PersistenceBackend for MockKVStorage {
    fn list_keys(&self) -> Result<Vec<String>, Error> {
        Ok(vec![])
    }

    fn get_value(&self, _key: &str) -> Result<Vec<u8>, Error> {
        unreachable!()
    }

    fn put_value(&mut self, _key: &str, _value: Vec<u8>) -> BackendPutFuture {
        let (sender, receiver) = oneshot::channel();
        let send_result = sender.send(Ok(()));
        if let Err(unsent_flush_result) = send_result {
            error!(
                "The receiver no longer waits for the result of this save: {unsent_flush_result:?}"
            );
        }
        receiver
    }

    fn remove_key(&self, _key: &str) -> Result<(), Error> {
        Ok(())
    }
}
