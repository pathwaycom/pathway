// Copyright © 2024 Pathway

use s3::bucket::Bucket as S3Bucket;

use crate::persistence::metadata_backends::{Error, MetadataBackend};

#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct S3KVStorage {
    bucket: S3Bucket,
    root_path: String,
}

impl S3KVStorage {
    pub fn new(bucket: S3Bucket, root_path: &str) -> Self {
        let mut root_path_prepared = root_path.to_string();
        if !root_path.ends_with('/') {
            root_path_prepared += "/";
        }
        Self {
            bucket,
            root_path: root_path_prepared,
        }
    }

    fn full_key_path(&self, key: &str) -> String {
        self.root_path.clone() + key
    }
}

impl MetadataBackend for S3KVStorage {
    fn list_keys(&self) -> Result<Vec<String>, Error> {
        let mut keys = Vec::new();

        let object_lists = self.bucket.list(self.root_path.clone(), None)?;
        let prefix_len = self.root_path.len();

        for list in &object_lists {
            for object in &list.contents {
                let key: &str = &object.key;
                assert!(key.len() > self.root_path.len());
                keys.push(key[prefix_len..].to_string());
            }
        }

        Ok(keys)
    }

    fn get_value(&self, key: &str) -> Result<String, Error> {
        let full_key_path = self.full_key_path(key);
        let response_data = self.bucket.get_object(full_key_path)?;
        Ok(response_data.to_string()?)
    }

    fn put_value(&mut self, key: &str, value: &str) -> Result<(), Error> {
        let full_key_path = self.full_key_path(key);
        let _ = self.bucket.put_object(full_key_path, value.as_bytes())?;
        Ok(())
    }

    fn remove_key(&self, key: &str) -> Result<(), Error> {
        let full_key_path = self.full_key_path(key);
        let _ = self.bucket.delete_object(full_key_path)?;
        Ok(())
    }
}
