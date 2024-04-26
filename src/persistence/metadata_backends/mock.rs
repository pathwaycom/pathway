// Copyright © 2024 Pathway

use crate::persistence::metadata_backends::{Error, MetadataBackend};

#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct MockKVStorage {}

impl MetadataBackend for MockKVStorage {
    fn list_keys(&self) -> Result<Vec<String>, Error> {
        Ok(vec![])
    }

    fn get_value(&self, _key: &str) -> Result<String, Error> {
        unreachable!()
    }

    fn put_value(&mut self, _key: &str, _value: &str) -> Result<(), Error> {
        Ok(())
    }

    fn remove_key(&self, _key: &str) -> Result<(), Error> {
        Ok(())
    }
}
