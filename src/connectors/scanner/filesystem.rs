use std::ffi::OsStr;
use std::fmt::Debug;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;

use log::error;

use crate::connectors::metadata::FileLikeMetadata;
use crate::connectors::scanner::{PosixLikeScanner, QueuedAction};
use crate::connectors::ReadError;
use crate::persistence::cached_object_storage::CachedObjectStorage;

use glob::Pattern as GlobPattern;

#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct FilesystemScanner {
    path: GlobPattern,
    object_pattern: String,
}

impl PosixLikeScanner for FilesystemScanner {
    fn object_metadata(
        &mut self,
        object_path: &[u8],
    ) -> Result<Option<FileLikeMetadata>, ReadError> {
        let path: PathBuf = OsStr::from_bytes(object_path).into();
        match std::fs::metadata(&path) {
            Ok(metadata) => Ok(Some(FileLikeMetadata::from_fs_meta(&path, &metadata))),
            Err(e) => {
                if matches!(e.kind(), std::io::ErrorKind::NotFound) {
                    Ok(None)
                } else {
                    Err(ReadError::Io(e))
                }
            }
        }
    }

    fn read_object(&mut self, object_path: &[u8]) -> Result<Vec<u8>, ReadError> {
        let path: PathBuf = OsStr::from_bytes(object_path).into();
        Ok(std::fs::read(path)?)
    }

    fn next_scanner_actions(
        &mut self,
        are_deletions_enabled: bool,
        cached_object_storage: &CachedObjectStorage,
    ) -> Result<Vec<QueuedAction>, ReadError> {
        let mut result = Vec::new();
        if are_deletions_enabled {
            result.append(&mut Self::new_deletion_and_replacement_actions(
                cached_object_storage,
            ));
        }
        result.append(&mut self.new_insertion_actions(cached_object_storage)?);
        Ok(result)
    }

    fn short_description(&self) -> String {
        format!("FileSystem({})", self.path)
    }
}

impl FilesystemScanner {
    pub fn new(path: &str, object_pattern: &str) -> Result<FilesystemScanner, ReadError> {
        let path_glob = GlobPattern::new(path)?;
        Ok(Self {
            path: path_glob,
            object_pattern: object_pattern.to_string(),
        })
    }

    fn new_deletion_and_replacement_actions(
        cached_object_storage: &CachedObjectStorage,
    ) -> Vec<QueuedAction> {
        let mut result = Vec::new();
        for (encoded_path, stored_metadata) in cached_object_storage.get_iter() {
            let path: PathBuf = OsStr::from_bytes(encoded_path).into();
            match std::fs::metadata(&path) {
                Err(e) => {
                    let is_deleted = e.kind() == std::io::ErrorKind::NotFound;
                    if is_deleted {
                        result.push(QueuedAction::Delete(encoded_path.clone()));
                    }
                }
                Ok(metadata) => {
                    let actual_metadata = FileLikeMetadata::from_fs_meta(&path, &metadata);
                    let is_updated = stored_metadata.is_changed(&actual_metadata);
                    if is_updated {
                        result.push(QueuedAction::Update(encoded_path.clone(), actual_metadata));
                    }
                }
            };
        }
        result
    }

    fn new_insertion_actions(
        &mut self,
        cached_object_storage: &CachedObjectStorage,
    ) -> Result<Vec<QueuedAction>, ReadError> {
        let mut result = Vec::new();
        for entry in self.get_matching_file_paths()? {
            let object_key = entry.as_os_str().as_bytes();
            if cached_object_storage.contains_object(object_key) {
                continue;
            }
            let metadata = match std::fs::metadata(&entry) {
                Err(_) => continue,
                Ok(metadata) => FileLikeMetadata::from_fs_meta(&entry, &metadata),
            };
            result.push(QueuedAction::Read(object_key.into(), metadata));
        }
        Ok(result)
    }

    fn get_matching_file_paths(&self) -> Result<Vec<PathBuf>, ReadError> {
        let mut result = Vec::new();

        let file_and_folder_paths = glob::glob(self.path.as_str())?.flatten();
        for entry in file_and_folder_paths {
            // If an entry is a file, it should just be added to result
            if entry.is_file() {
                result.push(entry);
                continue;
            }

            // Otherwise scan all files in all subdirectories and add them
            let Some(path) = entry.to_str() else {
                error!("Non-unicode paths are not supported. Ignoring: {entry:?}");
                continue;
            };

            let folder_scan_pattern = format!("{path}/**/{}", self.object_pattern);
            let folder_contents = glob::glob(&folder_scan_pattern)?.flatten();
            for nested_entry in folder_contents {
                if nested_entry.is_file() {
                    result.push(nested_entry);
                }
            }
        }

        Ok(result)
    }
}
