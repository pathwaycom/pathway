// Copyright © 2024 Pathway

use log::{error, warn};

use std::path::{Path, PathBuf};

use crate::fs_helpers::ensure_directory;
use crate::persistence::metadata_backends::{Error, MetadataBackend};

#[derive(Debug)]
pub struct FilesystemKVStorage {
    root_path: PathBuf,
}

impl FilesystemKVStorage {
    pub fn new(root_path: &Path) -> Result<Self, Error> {
        ensure_directory(root_path)?;
        Ok(Self {
            root_path: root_path.to_path_buf(),
        })
    }
}

impl MetadataBackend for FilesystemKVStorage {
    fn list_keys(&self) -> Result<Vec<String>, Error> {
        let mut keys = Vec::new();

        for entry in std::fs::read_dir(&self.root_path)? {
            if let Err(e) = entry {
                error!("Error while doing the folder scan: {e}. Output may duplicate a part of previous run");
                continue;
            }
            let entry = entry.unwrap();
            let file_type = entry.file_type();
            match file_type {
                Ok(file_type) => {
                    if !file_type.is_file() {
                        continue;
                    }
                    match entry.file_name().into_string() {
                        Ok(key) => keys.push(key),
                        Err(name) => warn!("Non-Unicode file name: {name:?}"),
                    };
                }
                Err(e) => {
                    error!("Couldn't detect file type for {entry:?}: {e}");
                }
            }
        }

        Ok(keys)
    }

    fn get_value(&self, key: &str) -> Result<String, Error> {
        Ok(std::fs::read_to_string(self.root_path.join(key))?)
    }

    fn put_value(&mut self, key: &str, value: &str) -> Result<(), Error> {
        std::fs::write(self.root_path.join(key), value)?;
        Ok(())
    }

    fn remove_key(&self, key: &str) -> Result<(), Error> {
        std::fs::remove_file(self.root_path.join(key))?;
        Ok(())
    }
}
