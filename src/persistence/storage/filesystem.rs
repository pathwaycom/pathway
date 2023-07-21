use std::cmp::max;
use std::collections::HashMap;
use std::fs::File;
use std::io::ErrorKind as IoErrorKind;
use std::io::Write;
use std::io::{BufRead, BufReader};
use std::mem::swap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::thread::{sleep, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use log::{error, warn};

use crate::connectors::data_storage::StorageType;
use crate::connectors::{OffsetKey, OffsetValue};
use crate::fs_helpers::ensure_directory;
use crate::persistence::frontier::{OffsetAntichain, OffsetAntichainCollection};
use crate::persistence::storage::Storage as MetadataStorage;
use crate::persistence::{Error, PersistentId};

const METADATA_TERMINAL: &str = "~";

pub enum SaveStatePolicy {
    OnUpdate,
    Background(Duration),
}

#[derive(Clone, Debug)]
enum StorageStatus {
    UpToDate,
    HasChanges,
    Terminated,
}

#[derive(Clone, Debug)]
struct StorageState {
    frontiers: OffsetAntichainCollection,
    storage_types: HashMap<PersistentId, StorageType>,
    status: StorageStatus,
    last_advanced_timestamp: u64,
}

impl StorageState {
    fn new() -> Self {
        Self {
            frontiers: OffsetAntichainCollection::new(),
            storage_types: HashMap::new(),
            status: StorageStatus::UpToDate,
            last_advanced_timestamp: 0,
        }
    }

    fn load(path: &Path) -> Result<Self, Error> {
        let file =
            File::open(path).map_err(|e| Error::OffsetsFileBroken("Failed to open", Some(e)))?;
        let mut reader = BufReader::new(file);

        let mut version_string = String::default();
        let mut serialized_collection = String::default();
        let mut storage_types = String::new();
        let mut last_advanced_timestamp = String::new();
        let mut terminal = String::default();

        reader
            .read_line(&mut version_string)
            .map_err(|_| Error::OffsetsFileBroken("Failed to read version string", None))?;
        reader
            .read_line(&mut serialized_collection)
            .map_err(|_| Error::OffsetsFileBroken("Failed to read offsets", None))?;
        reader
            .read_line(&mut storage_types)
            .map_err(|_| Error::OffsetsFileBroken("Failed to read storage types", None))?;
        reader
            .read_line(&mut last_advanced_timestamp)
            .map_err(|_| {
                Error::OffsetsFileBroken("Failed to read last advanced timestamp", None)
            })?;
        reader
            .read_line(&mut terminal)
            .map_err(|_| Error::OffsetsFileBroken("Failed to read terminal character", None))?;

        if terminal != METADATA_TERMINAL {
            return Err(Error::OffsetsFileBroken("Invalid terminal character", None));
        }

        Ok(Self {
            storage_types: serde_json::from_str::<HashMap<PersistentId, StorageType>>(
                storage_types.trim_end(),
            )
            .map_err(Error::IncorrectSerializedStorageTypes)?,
            frontiers: OffsetAntichainCollection::deserialize(serialized_collection.trim_end())?,
            status: StorageStatus::UpToDate,
            last_advanced_timestamp: last_advanced_timestamp
                .trim_end()
                .parse()
                .map_err(Error::IncorrectSerializedTimestamp)?,
        })
    }

    fn save(&self, path: &Path) -> Result<(), Error> {
        let mut file = File::create(path)
            .map_err(|e| Error::SavingOffsetsFailed("Failed to open the dump file", e))?;

        let collection_serialized = self.frontiers.serialize();
        let storage_types = serde_json::to_string(&self.storage_types).unwrap();
        let last_advanced_timestamp = format!("{}", self.last_advanced_timestamp);
        write!(
            file,
            "0\n{collection_serialized}\n{storage_types}\n{last_advanced_timestamp}\n{METADATA_TERMINAL}"
        )
        .map_err(|e| Error::SavingOffsetsFailed("Failed to write state in the file", e))?;

        Ok(())
    }
}

pub struct FileSystem {
    state: Arc<Mutex<StorageState>>,

    current_file_to_use: PathBuf,
    next_file_to_use: PathBuf,

    save_state_policy: SaveStatePolicy,
    saving_thread: Option<JoinHandle<()>>,
}

impl Drop for FileSystem {
    fn drop(&mut self) {
        if let Some(saving_thread) = self.saving_thread.take() {
            self.state.lock().unwrap().status = StorageStatus::Terminated;
            saving_thread
                .join()
                .expect("Unexpected panic in the storage saver thread");
        }
    }
}

impl FileSystem {
    pub fn new(
        path: &Path,
        worker_id: usize,
        save_state_policy: SaveStatePolicy,
    ) -> Result<Self, Error> {
        ensure_directory(path).map_err(|e| {
            Error::StorageInitializationFailed("root directory not created", Some(e))
        })?;

        let mut state = StorageState::new();

        for entry in (std::fs::read_dir(path).map_err(|e| {
            Error::StorageInitializationFailed("Failed to read directory {path}", Some(e))
        })?)
        .flatten()
        {
            if let Ok(file_type) = entry.file_type() {
                if !file_type.is_file() {
                    continue;
                }
                let block_path = entry.path();

                match StorageState::load(&block_path) {
                    Ok(other_state) => {
                        for (persistent_id, storage_type) in &other_state.storage_types {
                            state.storage_types.insert(*persistent_id, *storage_type);
                        }
                        state.frontiers.update_with_collection(
                            other_state.frontiers,
                            &other_state.storage_types,
                        );
                        state.last_advanced_timestamp = max(
                            state.last_advanced_timestamp,
                            other_state.last_advanced_timestamp,
                        );
                    }
                    Err(e) => {
                        error!("Failed to parse offset block: {e}");
                        if let Err(e) = std::fs::remove_file(block_path) {
                            /*
                                The block may have already been removed by the metadata
                                storage instance, which started in a different thread.

                                If this is the case, it should not be considered an initialization
                                failure, since the erroneous block has been cleared.
                            */
                            if e.kind() != IoErrorKind::NotFound {
                                return Err(Error::StorageInitializationFailed(
                                    "Failed to clean broken offset block",
                                    Some(e),
                                ));
                            }
                        }
                    }
                };
            }
        }

        let current_timestamp = {
            let now = SystemTime::now();

            now.duration_since(UNIX_EPOCH)
                .expect("Failed to acquire system time")
                .as_secs()
        };
        let current_file_to_use = path.join(format!("{current_timestamp}-{worker_id}-0"));
        let next_file_to_use = path.join(format!("{current_timestamp}-{worker_id}-1"));

        let internal_state = Arc::new(Mutex::new(state));

        let state_saver_thread = {
            if let SaveStatePolicy::Background(storage_update_interval) = save_state_policy {
                let internal_state_for_updater = internal_state.clone();
                let current_file_for_updater = current_file_to_use.clone();
                let next_file_for_updater = next_file_to_use.clone();

                let thread_name = format!("pathway:background_metadata_saver-{}", path.display());

                let updater_join_handle = thread::Builder::new()
                    .name(thread_name)
                    .spawn(move || {
                        Self::periodically_save_state(
                            &internal_state_for_updater,
                            &current_file_for_updater,
                            &next_file_for_updater,
                            storage_update_interval,
                        );
                    })
                    .expect("background metadata saver thread creation failed");

                Some(updater_join_handle)
            } else {
                None
            }
        };

        Ok(FileSystem {
            state: internal_state,

            current_file_to_use,
            next_file_to_use,

            save_state_policy,
            saving_thread: state_saver_thread,
        })
    }

    fn periodically_save_state(
        state: &Arc<Mutex<StorageState>>,
        first_state_file: &Path,
        second_state_file: &Path,
        storage_update_interval: Duration,
    ) {
        let mut current_file_to_use_in_updater = &first_state_file;
        let mut next_file_to_use_in_updater = &second_state_file;

        loop {
            let iteration_start = SystemTime::now();
            {
                let status: StorageStatus = state.lock().unwrap().status.clone();
                match status {
                    StorageStatus::UpToDate => continue,
                    StorageStatus::HasChanges | StorageStatus::Terminated => {
                        {
                            let mut state_guard = state.lock().unwrap();
                            if matches!(state_guard.status, StorageStatus::HasChanges) {
                                state_guard.status = StorageStatus::UpToDate;
                            }
                        }

                        let save_result = {
                            // By cloning the state we avoid doing IO-heavy operation undex mutex
                            let state_clone = state.lock().unwrap().clone();
                            state_clone.save(current_file_to_use_in_updater)
                        };

                        let mut state_guard = state.lock().unwrap();
                        if let Err(e) = save_result {
                            if matches!(state_guard.status, StorageStatus::UpToDate) {
                                state_guard.status = StorageStatus::HasChanges;
                            }
                            error!("Failed to save the state: {e}");
                        } else {
                            if matches!(state_guard.status, StorageStatus::Terminated) {
                                if let Err(e) = state_guard.save(current_file_to_use_in_updater) {
                                    warn!("Failed to write the remaining offsets, entries may duplicate at the next run ({e})");
                                }
                                break;
                            }

                            swap(
                                &mut current_file_to_use_in_updater,
                                &mut next_file_to_use_in_updater,
                            );
                        }
                    }
                }
            }

            let iteration_took = iteration_start.elapsed().unwrap_or(Duration::ZERO);
            if storage_update_interval > iteration_took {
                sleep(storage_update_interval - iteration_took);
            } else {
                warn!("Storage update taking more than update interval");
            }
        }
    }

    fn dump_state(&mut self) -> Result<(), Error> {
        self.state.lock().unwrap().save(&self.current_file_to_use)?;
        swap(&mut self.current_file_to_use, &mut self.next_file_to_use);
        Ok(())
    }

    fn notify_on_change(&mut self) {
        match self.save_state_policy {
            SaveStatePolicy::OnUpdate => {
                if let Err(e) = self.dump_state() {
                    error!("Failed to save the state: {e}");
                }
            }
            SaveStatePolicy::Background(_) => {
                let mut state_guard = self.state.lock().unwrap();
                if matches!(state_guard.status, StorageStatus::UpToDate) {
                    state_guard.status = StorageStatus::HasChanges;
                }
            }
        }
    }
}

impl MetadataStorage for FileSystem {
    fn frontier_for(&self, persistent_id: PersistentId) -> OffsetAntichain {
        self.state
            .lock()
            .unwrap()
            .frontiers
            .antichain_for_storage(persistent_id)
    }

    fn register_input_source(&mut self, persistent_id: PersistentId, storage_type: &StorageType) {
        self.state
            .lock()
            .unwrap()
            .storage_types
            .insert(persistent_id, *storage_type);
    }

    fn save_offset(
        &mut self,
        persistent_id: PersistentId,
        offset_key: &OffsetKey,
        offset_value: &OffsetValue,
    ) {
        self.state.lock().unwrap().frontiers.advance_offset(
            persistent_id,
            offset_key.clone(),
            offset_value.clone(),
        );

        self.notify_on_change();
    }

    fn accept_finalized_timestamp(&mut self, timestamp: u64) {
        self.state.lock().unwrap().last_advanced_timestamp = timestamp;
        self.notify_on_change();
    }

    fn last_advanced_timestamp(&self) -> u64 {
        self.state.lock().unwrap().last_advanced_timestamp
    }
}
