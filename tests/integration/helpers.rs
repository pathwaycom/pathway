// Copyright © 2024 Pathway

use std::collections::HashMap;
use std::path::Path;
use std::sync::{mpsc, mpsc::Receiver, Arc, Mutex};
use std::thread;
use std::time::Duration;

use pathway_engine::engine::error::DynError;
use pathway_engine::engine::{report_error::ReportError, Error};
use pathway_engine::persistence::config::{
    MetadataStorageConfig, PersistenceManagerOuterConfig, StreamStorageConfig,
};
use pathway_engine::persistence::tracker::WorkerPersistentStorage;

use pathway_engine::connectors::data_format::{
    ErrorRemovalLogic, ParseResult, ParsedEvent, ParsedEventWithErrors, Parser,
};
use pathway_engine::connectors::data_storage::{
    DataEventType, ReadResult, Reader, ReaderBuilder, ReaderContext,
};
use pathway_engine::connectors::snapshot::Event as SnapshotEvent;
use pathway_engine::connectors::{Connector, Entry, PersistenceMode, SnapshotAccess};
use pathway_engine::engine::{Key, Timestamp, TotalFrontier, Value};
use pathway_engine::persistence::frontier::OffsetAntichain;

#[derive(Debug)]
pub struct FullReadResult {
    pub raw_entries: Vec<Entry>,
    pub snapshot_entries: Vec<SnapshotEvent>,
    pub new_parsed_entries: Vec<ParsedEvent>,
}

#[derive(Debug, Default)]
pub struct PanicErrorReporter {}

impl ReportError for PanicErrorReporter {
    fn report(&self, error: Error) {
        panic!("Error: {error:?}");
    }
}

pub fn full_cycle_read(
    reader: Box<dyn ReaderBuilder>,
    parser: &mut dyn Parser,
    persistent_storage: Option<&Arc<Mutex<WorkerPersistentStorage>>>,
) -> FullReadResult {
    let maybe_persistent_id = reader.persistent_id();

    let offsets = Arc::new(Mutex::new(HashMap::<Timestamp, OffsetAntichain>::new()));
    if let Some(persistent_storage) = persistent_storage {
        if let Some(persistent_id) = reader.persistent_id() {
            persistent_storage
                .lock()
                .unwrap()
                .register_input_source(persistent_id, &reader.storage_type());
        }
    }

    let main_thread = thread::current();
    let (sender, receiver) = mpsc::channel();
    let mut snapshot_writer =
        Connector::snapshot_writer(reader.as_ref(), persistent_storage, SnapshotAccess::Full)
            .unwrap();

    let mut reader = reader.build().expect("building the reader failed");
    Connector::read_snapshot(
        &mut *reader,
        persistent_storage,
        &sender,
        PersistenceMode::Batch,
        SnapshotAccess::Full,
    );

    let reporter = PanicErrorReporter::default();
    Connector::read_realtime_updates(&mut *reader, &sender, &main_thread, &reporter);
    let result = get_entries_in_receiver(receiver);

    let has_persistent_storage = persistent_storage.is_some();
    let mut frontier = OffsetAntichain::new();

    let mut new_parsed_entries = Vec::new();
    let mut snapshot_entries = Vec::new();
    let mut rewind_finish_sentinel_seen = false;
    for entry in &result {
        match entry {
            Entry::Realtime(_) => assert!(rewind_finish_sentinel_seen),
            Entry::RewindFinishSentinel(_) => {
                assert!(!rewind_finish_sentinel_seen);
                rewind_finish_sentinel_seen = true;
            }
            Entry::Snapshot(_) => assert!(!rewind_finish_sentinel_seen),
        }

        match entry {
            Entry::Realtime(ReadResult::Data(raw_data, (offset_key, offset_value))) => {
                frontier.advance_offset(offset_key.clone(), offset_value.clone());

                let events = parser.parse(raw_data).unwrap();
                for event in events {
                    let event = event.replace_errors();
                    if let Some(ref mut snapshot_writer) = snapshot_writer {
                        let snapshot_event = match event {
                            ParsedEvent::Insert((_, ref values)) => {
                                let key = Key::random();
                                SnapshotEvent::Insert(key, values.clone())
                            }
                            ParsedEvent::Delete((_, _)) | ParsedEvent::Upsert((_, _)) => {
                                todo!("delete and upsert aren't supported in this test")
                            }
                            ParsedEvent::AdvanceTime => {
                                SnapshotEvent::AdvanceTime(Timestamp(1), frontier.clone())
                            }
                        };
                        snapshot_writer
                            .lock()
                            .unwrap()
                            .write(&snapshot_event)
                            .unwrap();
                    }
                    new_parsed_entries.push(event);
                }
            }
            Entry::Realtime(ReadResult::FinishedSource { .. }) => continue,
            Entry::Realtime(ReadResult::NewSource(metadata)) => {
                parser.on_new_source_started(metadata.as_ref());
            }
            Entry::Snapshot(snapshot_entry) => {
                snapshot_entries.push(snapshot_entry.clone());
            }
            Entry::RewindFinishSentinel(_) => {
                rewind_finish_sentinel_seen = true;
            }
            _ => {}
        }
    }

    if let Some(ref mut snapshot_writer) = snapshot_writer {
        let finished_event = SnapshotEvent::AdvanceTime(Timestamp(1), frontier.clone());
        snapshot_writer
            .lock()
            .unwrap()
            .write(&finished_event)
            .unwrap();
    }

    assert!(rewind_finish_sentinel_seen);

    if maybe_persistent_id.is_some() && has_persistent_storage {
        let prev_finalized_time = persistent_storage
            .unwrap()
            .lock()
            .unwrap()
            .last_finalized_timestamp();

        let TotalFrontier::At(prev_finalized_time) = prev_finalized_time else {
            panic!("expected finite time");
        };

        offsets
            .lock()
            .unwrap()
            .insert(Timestamp(prev_finalized_time.0 + 1), frontier);

        let last_sink_id = persistent_storage.unwrap().lock().unwrap().register_sink();
        for sink_id in 0..=last_sink_id {
            persistent_storage
                .as_ref()
                .unwrap()
                .lock()
                .unwrap()
                .update_sink_finalized_time(sink_id, Some(Timestamp(prev_finalized_time.0 + 2)));
        }
    }

    FullReadResult {
        raw_entries: result,
        snapshot_entries,
        new_parsed_entries,
    }
}

pub fn read_data_from_reader(
    mut reader: Box<dyn Reader>,
    mut parser: Box<dyn Parser>,
) -> eyre::Result<Vec<ParsedEvent>> {
    let mut read_lines = Vec::new();
    loop {
        let read_result = reader.read()?;
        match read_result {
            ReadResult::Data(bytes, _) => {
                let parse_result = parser.parse(&bytes);
                if let Ok(entries) = parse_result {
                    for entry in entries {
                        let entry = entry.replace_errors();
                        read_lines.push(entry);
                    }
                } else {
                    panic!("Unexpected erroneous reply: {parse_result:?}");
                }
            }
            ReadResult::FinishedSource { .. } => continue,
            ReadResult::NewSource(metadata) => parser.on_new_source_started(metadata.as_ref()),
            ReadResult::Finished => break,
        }
    }
    Ok(read_lines)
}

pub fn create_persistence_manager(
    fs_path: &Path,
    recreate: bool,
) -> Arc<Mutex<WorkerPersistentStorage>> {
    if recreate {
        let _ = std::fs::remove_dir_all(fs_path);
    }

    Arc::new(Mutex::new(
        WorkerPersistentStorage::new(
            PersistenceManagerOuterConfig::new(
                Duration::ZERO,
                MetadataStorageConfig::Filesystem(fs_path.to_path_buf()),
                StreamStorageConfig::Filesystem(fs_path.to_path_buf()),
                SnapshotAccess::Full,
                PersistenceMode::Batch,
                true,
            )
            .into_inner(0, 1),
        )
        .expect("Failed to create persistence manager"),
    ))
}

pub fn get_entries_in_receiver<T>(receiver: Receiver<T>) -> Vec<T> {
    let mut result = Vec::new();
    while let Ok(entry) = receiver.recv_timeout(Duration::from_secs(1)) {
        result.push(entry);
    }
    result
}

pub enum ErrorPlacement {
    Message,
    Key,
    Value(usize),
}

impl ErrorPlacement {
    fn extract_errors(&self, parse_result: ParseResult) -> Vec<DynError> {
        match self {
            Self::Message => vec![parse_result
                .expect_err("error should be in the message but it is not present there")],
            Self::Key => parse_result
                .expect("error should be contained in key, not in message")
                .into_iter()
                .map(|result| match result {
                    ParsedEventWithErrors::Insert((key, _))
                    | ParsedEventWithErrors::Delete((key, _))
                    | ParsedEventWithErrors::Upsert((key, _)) => key
                        .expect("key has to be Some to contain error")
                        .expect_err("error should be in the key but it is not present there"),
                    ParsedEventWithErrors::AdvanceTime => {
                        panic!("expected error, found AdvanceTime")
                    }
                })
                .collect(),
            Self::Value(i) => parse_result
                .expect("error should be contained in values, not in message")
                .into_iter()
                .map(|result| match result {
                    ParsedEventWithErrors::Insert((_, values))
                    | ParsedEventWithErrors::Delete((_, values))
                    | ParsedEventWithErrors::Upsert((_, Some(values))) => {
                        values.into_iter().nth(*i).unwrap().expect_err(
                            format!(
                                "error should be in the values[{}] but it is not present there",
                                *i
                            )
                            .as_str(),
                        )
                    }
                    _ => panic!("expected event with data"),
                })
                .collect(),
        }
    }
}

pub fn assert_error_shown(
    mut reader: Box<dyn Reader>,
    parser: Box<dyn Parser>,
    error_formatted_text: &str,
    error_placement: ErrorPlacement,
) {
    let _ = reader
        .read()
        .expect("new data source read event should not fail");
    let row_read_result = reader
        .read()
        .expect("first line read event should not fail");
    match row_read_result {
        ReadResult::Data(bytes, _) => {
            assert_error_shown_for_reader_context(
                &bytes,
                parser,
                error_formatted_text,
                error_placement,
            );
        }
        _ => panic!("row_read_result is not Data"),
    }
}

pub fn assert_error_shown_for_reader_context(
    context: &ReaderContext,
    mut parser: Box<dyn Parser>,
    error_formatted_text: &str,
    error_placement: ErrorPlacement,
) {
    let row_parse_result = parser.parse(context);
    println!("{:?}", row_parse_result);
    let errors = error_placement.extract_errors(row_parse_result);
    for e in errors {
        eprintln!("Error details: {e:?}");
        assert_eq!(format!("{e}"), error_formatted_text.to_string());
    }
}

pub fn assert_error_shown_for_raw_data(
    raw_data: &[u8],
    parser: Box<dyn Parser>,
    error_formatted_text: &str,
    error_placement: ErrorPlacement,
) {
    assert_error_shown_for_reader_context(
        &ReaderContext::from_raw_bytes(DataEventType::Insert, raw_data.to_vec()),
        parser,
        error_formatted_text,
        error_placement,
    );
}

pub trait ReplaceErrors {
    fn replace_errors(self) -> ParsedEvent;
}

impl ReplaceErrors for ParsedEventWithErrors {
    fn replace_errors(self) -> ParsedEvent {
        let logic: ErrorRemovalLogic = Box::new(|values| {
            values
                .into_iter()
                .map(|value| Ok(value.unwrap_or(Value::Error)))
                .collect()
        });
        self.remove_errors(&logic).expect("key shouldn't be error")
    }
}
