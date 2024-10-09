// Copyright © 2024 Pathway

use log::error;

use futures::channel::oneshot;
use futures::channel::oneshot::Receiver as OneShotReceiver;

use crate::persistence::backends::MetadataBackend;
use crate::persistence::Error;

#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct MockKVStorage {}

impl MetadataBackend for MockKVStorage {
    fn list_keys(&self) -> Result<Vec<String>, Error> {
        Ok(vec![])
    }

    fn get_value(&self, _key: &str) -> Result<Vec<u8>, Error> {
        unreachable!()
    }

    fn put_value(&mut self, _key: &str, _value: &[u8]) -> OneShotReceiver<Result<(), Error>> {
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
