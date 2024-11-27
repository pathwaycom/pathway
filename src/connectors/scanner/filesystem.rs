use rand::Rng;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::env;
use std::ffi::OsStr;
use std::fmt::Debug;
use std::fs::File;
use std::io;
use std::io::Read;
use std::mem::take;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, SystemTime};

use log::{error, warn};
use tempfile::{tempdir, TempDir};
use xxhash_rust::xxh3::Xxh3 as Hasher;

use crate::connectors::data_storage::{ConnectorMode, DataEventType};
use crate::connectors::metadata::SourceMetadata;
use crate::connectors::scanner::PosixLikeScanner;
use crate::connectors::{ReadError, ReadResult};
use crate::fs_helpers::ensure_directory;
use crate::persistence::PersistentId;
use crate::timestamp::current_unix_timestamp_secs;

use glob::Pattern as GlobPattern;

#[derive(Clone, Debug)]
pub enum PosixScannerAction {
    Read(Arc<PathBuf>),
    Delete(Arc<PathBuf>),
}

#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct FilesystemScanner {
    path: GlobPattern,
    cache_directory_path: Option<PathBuf>,
    streaming_mode: ConnectorMode,
    object_pattern: String,

    // Mapping from the path of the loaded file to its modification timestamp
    known_files: HashMap<PathBuf, u64>,

    current_action: Option<PosixScannerAction>,
    cached_modify_times: HashMap<PathBuf, Option<SystemTime>>,
    next_file_for_insertion: Option<PathBuf>,
    cached_metadata: HashMap<PathBuf, Option<SourceMetadata>>,
    scanner_actions_queue: VecDeque<PosixScannerAction>,

    // Storage is deleted on object destruction, so we need to store it
    // for the connector's life time
    _connector_tmp_storage: Option<TempDir>,
}

impl PosixLikeScanner for FilesystemScanner {
    fn current_object_reader(
        &mut self,
    ) -> Result<Option<Box<dyn Read + Send + 'static>>, ReadError> {
        let raw_file_path = match &self.current_action {
            Some(PosixScannerAction::Read(path)) => path.clone().as_os_str().as_bytes().to_vec(),
            Some(PosixScannerAction::Delete(path)) => self
                .cached_file_path(path)
                .map(|x| x.as_os_str().as_bytes().to_vec())
                .expect("Cached file path must be present"),
            None => return Ok(None),
        };
        let file_path: PathBuf = OsStr::from_bytes(&raw_file_path).into();
        let opened_file = File::open(file_path.as_path())?;
        let reader = Box::new(opened_file);
        Ok(Some(reader))
    }

    fn data_event_type(&self) -> Option<DataEventType> {
        self.current_action
            .as_ref()
            .map(|current_action| match current_action {
                PosixScannerAction::Read(_) => DataEventType::Insert,
                PosixScannerAction::Delete(_) => DataEventType::Delete,
            })
    }

    fn current_offset_object(&self) -> Option<Arc<[u8]>> {
        match &self.current_action {
            Some(PosixScannerAction::Read(path) | PosixScannerAction::Delete(path)) => {
                Some(path.clone().as_os_str().as_bytes().into())
            }
            None => None,
        }
    }

    fn seek_to_object(&mut self, seek_file_path: &[u8]) -> Result<(), ReadError> {
        let seek_file_path: PathBuf = OsStr::from_bytes(seek_file_path).into();
        if self.streaming_mode.are_deletions_enabled() {
            warn!("seek for snapshot mode may not work correctly in case deletions take place");
        }

        self.known_files.clear();
        let target_modify_time = match std::fs::metadata(&seek_file_path) {
            Ok(metadata) => metadata.modified()?,
            Err(e) => {
                if !matches!(e.kind(), std::io::ErrorKind::NotFound) {
                    return Err(ReadError::Io(e));
                }
                warn!(
                    "Unable to restore state: last persisted file {seek_file_path:?} not found in directory. Processing all files in directory."
                );
                return Ok(());
            }
        };
        let matching_files: Vec<PathBuf> = self.get_matching_file_paths()?;
        for entry in matching_files {
            if !entry.is_file() {
                continue;
            }
            let Some(modify_time) = self.modify_time(&entry) else {
                continue;
            };
            if (modify_time, entry.as_path()) <= (target_modify_time, &seek_file_path) {
                let modify_timestamp = modify_time
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("System time should be after the Unix epoch")
                    .as_secs();
                self.known_files.insert(entry, modify_timestamp);
            }
        }
        self.current_action = Some(PosixScannerAction::Read(Arc::new(seek_file_path.clone())));

        Ok(())
    }

    fn next_scanner_action(&mut self) -> Result<Option<ReadResult>, ReadError> {
        // If there is an ongoing action, we must finalize it
        // and emit the corresponding event.
        if let Some(current_action) = take(&mut self.current_action) {
            match current_action {
                PosixScannerAction::Delete(path) => {
                    let cached_path = self
                        .cached_file_path(&path)
                        .expect("in case of enabled deletions cache should exist");
                    std::fs::remove_file(cached_path)?;

                    // Whether we can commit or not depends on whether this deletion was a part
                    // of a file replacement or not. Therefore we can use `self.next_file_for_insertion`
                    // to learn if it's the case.
                    return Ok(Some(ReadResult::FinishedSource {
                        commit_allowed: self.next_file_for_insertion.is_none(),
                    }));
                }
                PosixScannerAction::Read(_) => {
                    // The insertion is done. There are two cases that the can involve the replacement:
                    // 1. It was a file replacement. Then it means that the file replacement has finished
                    // and we can proceed with allowing commits.
                    // 2. It was a file insertion. Then it means that the file insertion is done and we
                    // can also proceed with allowing commits.
                    //
                    // So whatever case we have, we can proceed with allowing deletions.
                    return Ok(Some(ReadResult::FinishedSource {
                        commit_allowed: true,
                    }));
                }
            }
        }

        // File modification is handled as combination of its deletion and insertion
        // If a file was deleted in the last action, now we must add it, and after that
        // we may allow commits.
        if let Some(next_file_for_insertion) = take(&mut self.next_file_for_insertion) {
            if next_file_for_insertion.exists() {
                return Ok(Some(
                    self.initiate_file_insertion(&next_file_for_insertion)?,
                ));
            }

            // The scheduled insertion after deletion is impossible because
            // the file has already been deleted.
            // The action was done in full now, and we can allow commits.
            return Ok(Some(ReadResult::FinishedSource {
                commit_allowed: true,
            }));
        }

        if self.scanner_actions_queue.is_empty() {
            self.enqueue_next_actions()?;
        }

        // Find the next valid action to execute
        loop {
            match self.scanner_actions_queue.pop_front() {
                Some(PosixScannerAction::Read(path)) => {
                    let next_action = self.initiate_file_insertion(&path);
                    match next_action {
                        Ok(next_action) => return Ok(Some(next_action)),
                        Err(e) => {
                            // If there was a planned action to add a file, but
                            // it no longer exists, proceed to the next planned action
                            if e.kind() == std::io::ErrorKind::NotFound {
                                continue;
                            }
                            return Err(ReadError::Io(e));
                        }
                    };
                }
                Some(PosixScannerAction::Delete(path)) => {
                    return Ok(Some(self.initiate_file_deletion(&path)))
                }
                None => {
                    if self.streaming_mode.is_polling_enabled() {
                        sleep(Self::sleep_duration());
                        self.enqueue_next_actions()?;
                    } else {
                        return Ok(None);
                    }
                }
            }
        }
    }
}

impl FilesystemScanner {
    pub fn new(
        path: &str,
        persistent_id: Option<PersistentId>,
        streaming_mode: ConnectorMode,
        object_pattern: &str,
    ) -> Result<FilesystemScanner, ReadError> {
        let path_glob = GlobPattern::new(path)?;

        let (cache_directory_path, connector_tmp_storage) = {
            if streaming_mode.are_deletions_enabled() {
                if let Ok(root_dir_str_path) = env::var("PATHWAY_PERSISTENT_STORAGE") {
                    let root_dir_path = Path::new(&root_dir_str_path);
                    ensure_directory(root_dir_path)?;
                    let unique_id =
                        persistent_id.unwrap_or_else(|| rand::thread_rng().gen::<u128>());
                    let connector_tmp_directory = root_dir_path.join(format!("cache-{unique_id}"));
                    ensure_directory(&connector_tmp_directory)?;
                    (Some(connector_tmp_directory), None)
                } else {
                    let cache_tmp_storage = tempdir()?;
                    let connector_tmp_directory = cache_tmp_storage.path();
                    (
                        Some(connector_tmp_directory.to_path_buf()),
                        Some(cache_tmp_storage),
                    )
                }
            } else {
                (None, None)
            }
        };

        Ok(Self {
            path: path_glob,
            streaming_mode,
            cache_directory_path,

            object_pattern: object_pattern.to_string(),
            known_files: HashMap::new(),
            current_action: None,
            cached_modify_times: HashMap::new(),
            next_file_for_insertion: None,
            cached_metadata: HashMap::new(),
            scanner_actions_queue: VecDeque::new(),
            _connector_tmp_storage: connector_tmp_storage,
        })
    }

    fn modify_time(&mut self, entry: &Path) -> Option<SystemTime> {
        if self.streaming_mode.are_deletions_enabled() {
            // If deletions are enabled, we also need to handle the case when the modification
            // time of an entry changes. Hence, we can't just memorize it once.
            entry.metadata().ok()?.modified().ok()
        } else {
            *self
                .cached_modify_times
                .entry(entry.to_path_buf())
                .or_insert_with(|| entry.metadata().ok()?.modified().ok())
        }
    }

    fn enqueue_next_actions(&mut self) -> Result<(), ReadError> {
        if self.streaming_mode.are_deletions_enabled() {
            self.enqueue_deletion_entries();
        }
        self.enqueue_insertion_entries()
    }

    fn enqueue_deletion_entries(&mut self) {
        let mut paths_for_deletion = Vec::new();
        for (path, modified_at) in &self.known_files {
            let metadata = std::fs::metadata(path);
            let needs_deletion = {
                match metadata {
                    Err(e) => e.kind() == std::io::ErrorKind::NotFound,
                    Ok(metadata) => {
                        if let Ok(new_modification_time) = metadata.modified() {
                            let modified_at_new = new_modification_time
                                .duration_since(SystemTime::UNIX_EPOCH)
                                .expect("System time should be after the Unix epoch")
                                .as_secs();
                            modified_at_new != *modified_at
                        } else {
                            false
                        }
                    }
                }
            };
            if needs_deletion {
                paths_for_deletion.push(path.clone());
            }
        }
        paths_for_deletion.sort_unstable();
        for path in paths_for_deletion {
            self.scanner_actions_queue
                .push_back(PosixScannerAction::Delete(Arc::new(path.clone())));
        }
    }

    fn cached_file_path(&self, path: &Path) -> Option<PathBuf> {
        self.cache_directory_path.as_ref().map(|root_path| {
            let mut hasher = Hasher::default();
            hasher.update(path.as_os_str().as_bytes());
            root_path.join(format!("{}", hasher.digest128()))
        })
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

    fn enqueue_insertion_entries(&mut self) -> Result<(), ReadError> {
        let matching_files: Vec<PathBuf> = self.get_matching_file_paths()?;
        let mut new_detected_files: Vec<(SystemTime, PathBuf)> = Vec::new();
        for entry in matching_files {
            if !entry.is_file() || self.known_files.contains_key(&(*entry)) {
                continue;
            }
            let Some(modify_time) = self.modify_time(&entry) else {
                continue;
            };
            new_detected_files.push((modify_time, entry));
        }

        new_detected_files.sort_unstable();
        for (_, path) in new_detected_files {
            self.scanner_actions_queue
                .push_back(PosixScannerAction::Read(Arc::new(path.clone())));
        }

        Ok(())
    }

    fn initiate_file_insertion(&mut self, new_file_name: &PathBuf) -> io::Result<ReadResult> {
        let new_file_meta =
            SourceMetadata::from_fs_meta(new_file_name, &std::fs::metadata(new_file_name)?);
        let cached_path = self.cached_file_path(new_file_name);
        if let Some(cached_path) = cached_path {
            std::fs::copy(new_file_name, cached_path)?;
        }

        // The file has been successfully saved at this point.
        // Now we can change the internal state.

        self.cached_metadata
            .insert(new_file_name.clone(), Some(new_file_meta.clone()));
        self.known_files.insert(
            new_file_name.clone(),
            new_file_meta
                .modified_at
                .unwrap_or(current_unix_timestamp_secs()),
        );

        self.current_action = Some(PosixScannerAction::Read(Arc::new(new_file_name.clone())));
        Ok(ReadResult::NewSource(Some(new_file_meta)))
    }

    fn initiate_file_deletion(&mut self, path: &PathBuf) -> ReadResult {
        // Metadata of the deleted file must be the same as when it was added
        // so that the deletion event is processed correctly by timely. To achieve
        // this, we just take the cached metadata
        let old_metadata = self
            .cached_metadata
            .remove(path)
            .expect("inconsistency between known_files and cached_metadata");

        self.known_files.remove(&path.clone().clone());
        self.current_action = Some(PosixScannerAction::Delete(Arc::new(path.clone())));
        if path.exists() {
            // If the path exists it means file modification. In this scenatio, the file
            // needs first to be deleted and then to be inserted again.
            self.next_file_for_insertion = Some(path.clone());
        }

        ReadResult::NewSource(old_metadata)
    }

    fn sleep_duration() -> Duration {
        Duration::from_millis(500)
    }
}
