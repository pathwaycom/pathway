// Copyright © 2024 Pathway

use log::{error, info, warn};
use std::collections::VecDeque;
use std::io::Cursor;
use std::mem::take;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use crate::connectors::data_storage::ConnectorMode;
use crate::connectors::data_tokenize::Tokenize;
use crate::connectors::scanner::{PosixLikeScanner, QueuedAction};
use crate::connectors::{
    DataEventType, OffsetKey, OffsetValue, ReadError, ReadResult, Reader, StorageType,
};
use crate::persistence::backends::MemoryKVStorage;
use crate::persistence::cached_object_storage::CachedObjectStorage;
use crate::persistence::frontier::OffsetAntichain;
use crate::persistence::tracker::WorkerPersistentStorage;
use crate::persistence::PersistentId;

struct CurrentAction {
    action: QueuedAction,
    offset_path: Arc<[u8]>,
}

impl From<QueuedAction> for CurrentAction {
    fn from(action: QueuedAction) -> Self {
        Self {
            offset_path: action.path().into(),
            action,
        }
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct PosixLikeReader {
    scanner: Box<dyn PosixLikeScanner>,
    tokenizer: Box<dyn Tokenize>,
    persistent_id: Option<PersistentId>,
    streaming_mode: ConnectorMode,

    total_entries_read: u64,
    had_queue_refresh: bool,
    cached_object_storage: CachedObjectStorage,
    current_action: Option<CurrentAction>,
    scanner_actions_queue: VecDeque<QueuedAction>,
}

impl PosixLikeReader {
    pub fn new(
        scanner: Box<dyn PosixLikeScanner>,
        tokenizer: Box<dyn Tokenize>,
        streaming_mode: ConnectorMode,
        persistent_id: Option<PersistentId>,
    ) -> Result<Self, ReadError> {
        Ok(Self {
            scanner,
            tokenizer,
            streaming_mode,
            persistent_id,

            total_entries_read: 0,
            had_queue_refresh: false,
            current_action: None,
            scanner_actions_queue: VecDeque::new(),
            cached_object_storage: CachedObjectStorage::new(Box::new(MemoryKVStorage::new()))?,
        })
    }
}

impl Reader for PosixLikeReader {
    fn seek(&mut self, frontier: &OffsetAntichain) -> Result<(), ReadError> {
        // FIXME: having only a path in the offset is not enough.
        // Suppose there were several modifications of a file, at 12:01, 12:02, 12:03.
        // All of them have been processed by the scanner and sent on the outside. This way,
        // the latest known version of this file corresponds to 12:03.
        //
        // Now suppose that the threshold time on the recovery corresponds to 12:02. Then
        // the version that corresponds to 12:02 must be loaded to the CachedObjectStorage.
        //
        // Moreover, all further read object modifications must be omitted and treated as
        // something that the scanner has never met.
        //
        // It can be done if:
        // 1. We use metadata as an offset, not just an object name. The metadata has the `seen_at`
        // field that can work as a version already.
        // 2. We store all versions in the CachedObjectStorage.
        // 3. On recovery, we remove use the version that corresponds to the provided metadata.
        // 4. After the recovery is done, we delete everything that has `seen_at` greater than
        // `seen_at` of the target object as well as objects with the same `seen_at` and greater
        // `path` lexicographically.
        //
        // Also to be noted that `seen_at` must be handled correctly in case the objects are
        // downloaded via thread pool. If the metadata is formed not while downloading, it
        // will be OK. Anyway, an assert in `CachedObjectStorage` that checks that the pairs
        // of (`seen_at`, `path`) form an increasing sequence, would help.
        let offset_value = frontier.get_offset(&OffsetKey::Empty);
        let Some(offset_value) = offset_value else {
            self.cached_object_storage.clear()?;
            return Ok(());
        };
        let Some(OffsetValue::PosixLikeOffset {
            total_entries_read,
            path: object_path_arc,
            bytes_offset: _,
        }) = offset_value.as_posix_like_offset()
        else {
            warn!("Incorrect type of offset value in PosixLike frontier: {offset_value:?}");
            return Ok(());
        };

        let stored_metadata = self
            .cached_object_storage
            .stored_metadata(object_path_arc.as_ref())
            .expect("Cached object storage must contain metadata for a cached object");
        self.scanner_actions_queue.clear();
        self.current_action =
            Some(QueuedAction::Read(object_path_arc.to_vec(), stored_metadata.clone()).into());
        let are_deletions_enabled = self.are_deletions_enabled();
        let actual_metadata = self.scanner.object_metadata(object_path_arc.as_ref())?;
        if let Some(metadata) = actual_metadata {
            let reread_needed = stored_metadata.is_changed(&metadata);
            if reread_needed && are_deletions_enabled {
                info!(
                    "The last read object has changed since it was last read. It will be reread."
                );
                self.scanner_actions_queue
                    .push_back(QueuedAction::Update(object_path_arc.to_vec(), metadata));
                self.current_action = None;
            }
        } else if are_deletions_enabled {
            info!("The last read object is no longer present in the source. It will be removed from the engine.");
            self.scanner_actions_queue
                .push_back(QueuedAction::Delete(object_path_arc.to_vec()));
            self.current_action = None;
        }

        // No need to set up a tokenizer here: `current_action` is set only
        // in case the object had already been read in full and requires no
        // further processing.
        self.total_entries_read = total_entries_read;
        Ok(())
    }

    fn initialize_cached_objects_storage(
        &mut self,
        persistence_manager: &WorkerPersistentStorage,
        persistent_id: PersistentId,
    ) -> Result<(), ReadError> {
        self.cached_object_storage =
            persistence_manager.create_cached_object_storage(persistent_id)?;
        Ok(())
    }

    fn read(&mut self) -> Result<ReadResult, ReadError> {
        // Try to continue to read the current object.
        let maybe_entry = self.tokenizer.next_entry()?;
        if let Some((entry, bytes_offset)) = maybe_entry {
            self.total_entries_read += 1;
            let offset = (
                OffsetKey::Empty,
                OffsetValue::PosixLikeOffset {
                    total_entries_read: self.total_entries_read,
                    path: self.current_action.as_ref().unwrap().offset_path.clone(),
                    bytes_offset,
                },
            );
            return Ok(ReadResult::Data(entry, offset));
        }

        // We've failed to read the current object because it's over.
        // Then let's try to find the next one.
        let next_read_result = self.next_scanner_action()?;
        if let Some(next_read_result) = next_read_result {
            return Ok(next_read_result);
        }

        Ok(ReadResult::Finished)
    }

    fn persistent_id(&self) -> Option<PersistentId> {
        self.persistent_id
    }

    fn update_persistent_id(&mut self, persistent_id: Option<PersistentId>) {
        self.persistent_id = persistent_id;
    }

    fn storage_type(&self) -> StorageType {
        StorageType::PosixLike
    }
}

impl PosixLikeReader {
    fn next_scanner_action(&mut self) -> Result<Option<ReadResult>, ReadError> {
        // If there is an ongoing action, we must finalize it
        // and emit the corresponding event.
        if let Some(current_action) = take(&mut self.current_action) {
            let commit_allowed = match current_action.action {
                QueuedAction::Delete(path) => {
                    self.cached_object_storage
                        .remove_object(path.as_ref())
                        .expect("Cached object storage doesn't contain an indexed object");
                    true
                }
                QueuedAction::Update(path, metadata) => {
                    self.scanner_actions_queue
                        .push_front(QueuedAction::Read(path, metadata));
                    false
                }
                QueuedAction::Read(path, _) => {
                    let are_deletions_enabled = self.are_deletions_enabled();
                    let is_persisted = self.persistent_id.is_some();
                    if !is_persisted && !are_deletions_enabled {
                        // Don't store a copy in memory if it won't be
                        // needed for undoing an object.
                        self.cached_object_storage
                            .remove_object(path.as_ref())
                            .expect("Removal from InMemory cache should not fail");
                    }
                    true
                }
            };
            return Ok(Some(ReadResult::FinishedSource { commit_allowed }));
        }

        // Find the next valid action to execute
        let are_deletions_enabled = self.are_deletions_enabled();
        loop {
            let action = self.scanner_actions_queue.pop_front();
            match &action {
                Some(QueuedAction::Read(path, metadata)) => {
                    let Ok(cached_object_contents) = self.scanner.read_object(path.as_ref()) else {
                        error!("Failed to get contents of a queued object {metadata:?}");
                        continue;
                    };
                    let contents_for_caching = if are_deletions_enabled {
                        cached_object_contents.clone()
                    } else {
                        Vec::with_capacity(0)
                    };
                    self.cached_object_storage.place_object(
                        path.as_ref(),
                        contents_for_caching,
                        metadata.clone(),
                    )?;
                    let reader = Box::new(Cursor::new(cached_object_contents));
                    self.tokenizer
                        .set_new_reader(reader, DataEventType::Insert)?;
                    let result = ReadResult::NewSource(Some(metadata.clone()));
                    self.current_action = Some(action.unwrap().into());
                    return Ok(Some(result));
                }
                Some(QueuedAction::Delete(path) | QueuedAction::Update(path, _)) => {
                    let old_metadata = self
                        .cached_object_storage
                        .stored_metadata(path.as_ref())
                        .expect("Metadata for all indexed objects must be stored in the engine");
                    let cached_object_contents = self
                        .cached_object_storage
                        .get_object(path.as_ref())
                        .expect("Copy of a cached object must be present to perform deletion");
                    let reader = Box::new(Cursor::new(cached_object_contents));
                    self.tokenizer
                        .set_new_reader(reader, DataEventType::Delete)?;
                    let result = ReadResult::NewSource(Some(old_metadata.clone()));
                    self.current_action = Some(action.unwrap().into());
                    return Ok(Some(result));
                }
                None => {
                    if self.streaming_mode.is_polling_enabled() || !self.had_queue_refresh {
                        self.had_queue_refresh = true;
                        let new_actions = self.scanner.next_scanner_actions(
                            are_deletions_enabled,
                            &self.cached_object_storage,
                        )?;
                        for action in new_actions {
                            self.scanner_actions_queue.push_back(action);
                        }
                        if self.scanner_actions_queue.is_empty() {
                            // Don't poll the backend too often.
                            sleep(Self::sleep_duration());
                        }
                    } else {
                        return Ok(None);
                    }
                }
            }
        }
    }

    fn are_deletions_enabled(&self) -> bool {
        self.persistent_id.is_some() || self.streaming_mode.is_polling_enabled()
    }

    fn sleep_duration() -> Duration {
        Duration::from_millis(500)
    }
}
