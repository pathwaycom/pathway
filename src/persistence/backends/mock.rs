// Copyright © 2024 Pathway

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
        sender
            .send(Ok(()))
            .expect("The receiver must still be listening for the result of the put_value");
        receiver
    }

    fn remove_key(&mut self, _key: &str) -> Result<(), Error> {
        Ok(())
    }
}
