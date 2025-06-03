// Copyright © 2024 Pathway

use log::warn;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use futures::channel::oneshot;
use glob::Pattern as GlobPattern;

use crate::fs_helpers::ensure_directory;
use crate::persistence::backends::PersistenceBackend;
use crate::persistence::Error;

use super::BackendPutFuture;

const TEMPORARY_OBJECT_SUFFIX: &str = ".tmp";

#[derive(Debug)]
pub struct FilesystemKVStorage {
    root_path: PathBuf,
    root_glob_pattern: GlobPattern,
    path_prefix_len: usize,
}

impl FilesystemKVStorage {
    pub fn new(root_path: &Path) -> Result<Self, Error> {
        let root_path_str = root_path.to_str().ok_or(Error::PathIsNotUtf8)?;
        let root_glob_pattern = GlobPattern::new(&format!("{root_path_str}/**/*"))?;
        ensure_directory(root_path)?;
        Ok(Self {
            root_path: root_path.to_path_buf(),
            root_glob_pattern,
            path_prefix_len: root_path_str.len() + 1,
        })
    }

    fn write_file(temp_path: &Path, final_path: &Path, value: &[u8]) -> Result<(), Error> {
        let mut output_file = File::create(temp_path)?;
        output_file.write_all(value)?;
        // Note: if we need Pathway to tolerate not only Pathway failures,
        // but only OS crash or power loss, the below line must be uncommented.
        // output_file.sync_all()?;
        std::fs::rename(temp_path, final_path)?;
        Ok(())
    }
}

impl PersistenceBackend for FilesystemKVStorage {
    fn list_keys(&self) -> Result<Vec<String>, Error> {
        let mut keys = Vec::new();
        let file_and_folder_paths = glob::glob(self.root_glob_pattern.as_str())?.flatten();
        for entry in file_and_folder_paths {
            if !entry.is_file() {
                continue;
            }
            if let Some(path_str) = entry.to_str() {
                let is_temporary = path_str.ends_with(TEMPORARY_OBJECT_SUFFIX);
                if !is_temporary {
                    let key = path_str[self.path_prefix_len..].to_string();
                    keys.push(key);
                }
            } else {
                warn!("The path is not UTF-8 encoded: {entry:?}");
            }
        }
        Ok(keys)
    }

    fn get_value(&self, key: &str) -> Result<Vec<u8>, Error> {
        Ok(std::fs::read(self.root_path.join(key))?)
    }

    fn put_value(&mut self, key: &str, value: Vec<u8>) -> BackendPutFuture {
        let (sender, receiver) = oneshot::channel();

        let tmp_path = self
            .root_path
            .join(key.to_owned() + TEMPORARY_OBJECT_SUFFIX);
        let final_path = self.root_path.join(key);
        let put_value_result = Self::write_file(&tmp_path, &final_path, &value);
        sender
            .send(put_value_result)
            .expect("The receiver must still be listening for the result of the put_value");
        receiver
    }

    fn remove_key(&mut self, key: &str) -> Result<(), Error> {
        std::fs::remove_file(self.root_path.join(key))?;
        Ok(())
    }
}
