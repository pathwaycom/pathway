// Copyright © 2024 Pathway

use log::warn;

use crate::connectors::data_tokenize::Tokenize;
use crate::connectors::scanner::PosixLikeScanner;
use crate::connectors::{DataEventType, ReadError, ReadResult, Reader, StorageType};
use crate::connectors::{OffsetKey, OffsetValue};
use crate::persistence::frontier::OffsetAntichain;
use crate::persistence::PersistentId;

#[allow(clippy::module_name_repetitions)]
pub struct PosixLikeReader {
    scanner: Box<dyn PosixLikeScanner>,
    tokenizer: Box<dyn Tokenize>,
    persistent_id: Option<PersistentId>,

    total_entries_read: u64,
    deferred_read_result: Option<ReadResult>,
}

impl PosixLikeReader {
    pub fn new(
        scanner: Box<dyn PosixLikeScanner>,
        tokenizer: Box<dyn Tokenize>,
        persistent_id: Option<PersistentId>,
    ) -> Self {
        Self {
            scanner,
            tokenizer,
            persistent_id,

            total_entries_read: 0,
            deferred_read_result: None,
        }
    }
}

impl Reader for PosixLikeReader {
    fn seek(&mut self, frontier: &OffsetAntichain) -> Result<(), ReadError> {
        let offset_value = frontier.get_offset(&OffsetKey::Empty);
        let Some(offset_value) = offset_value else {
            return Ok(());
        };
        let Some(OffsetValue::PosixLikeOffset {
            total_entries_read,
            path: object_path_arc,
            bytes_offset,
        }) = offset_value.as_posix_like_offset()
        else {
            warn!("Incorrect type of offset value in PosixLike frontier: {offset_value:?}");
            return Ok(());
        };

        // Scanner part: detect already processed objects
        self.scanner.seek_to_object(object_path_arc.as_ref())?;

        // If the last object read is missing from the offset, the program will read all objects in the directory.
        // We could consider alternative strategies:
        //  1. Return an error and stop the program;
        //  2. Track all objects that have been read in the offset (though this would greatly increase the offset size);
        //  3. Only use the object's last modification time.
        let Ok(Some(reader)) = self.scanner.current_object_reader() else {
            return Ok(());
        };
        self.total_entries_read = total_entries_read;

        // Seek within a particular object
        self.tokenizer
            .set_new_reader(reader, DataEventType::Insert)?;
        self.tokenizer.seek(bytes_offset)?;

        Ok(())
    }

    fn read(&mut self) -> Result<ReadResult, ReadError> {
        if let Some(deferred_read_result) = self.deferred_read_result.take() {
            return Ok(deferred_read_result);
        }

        // Try to continue to read the current object.
        let maybe_entry = self.tokenizer.next_entry()?;
        if let Some((entry, bytes_offset)) = maybe_entry {
            self.total_entries_read += 1;
            let offset = (
                OffsetKey::Empty,
                OffsetValue::PosixLikeOffset {
                    total_entries_read: self.total_entries_read,
                    path: self.scanner.current_offset_object().clone().unwrap(),
                    bytes_offset,
                },
            );
            return Ok(ReadResult::Data(entry, offset));
        }

        // We've failed to read the current object because it's over.
        // Then let's try to find the next one.
        let next_read_result = self.scanner.next_scanner_action()?;
        if let Some(next_read_result) = next_read_result {
            if let Some(reader) = self.scanner.current_object_reader()? {
                self.tokenizer.set_new_reader(
                    reader,
                    self.scanner.data_event_type().expect(
                        "inconsistent: current object is determined but the event type isn't",
                    ),
                )?;
            }
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
