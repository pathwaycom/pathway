// Copyright © 2024 Pathway

use std::collections::HashMap;
use std::path::Path;
use std::sync::{mpsc, mpsc::Receiver, Arc, Mutex};
use std::thread;
use std::time::Duration;

use pathway_engine::engine::{report_error::ReportError, Error};
use pathway_engine::persistence::config::{
    MetadataStorageConfig, PersistenceManagerOuterConfig, StreamStorageConfig,
};
use pathway_engine::persistence::metadata_backends::FilesystemKVStorage;
use pathway_engine::persistence::state::MetadataAccessor;
use pathway_engine::persistence::tracker::SingleWorkerPersistentStorage;

use pathway_engine::connectors::data_format::{ParsedEvent, Parser};
use pathway_engine::connectors::data_storage::{
    DataEventType, ReadResult, Reader, ReaderBuilder, ReaderContext,
};
use pathway_engine::connectors::snapshot::Event as SnapshotEvent;
use pathway_engine::connectors::{Connector, Entry, PersistenceMode, SnapshotAccess};
use pathway_engine::engine::{Key, Timestamp};
use pathway_engine::persistence::frontier::OffsetAntichain;
use pathway_engine::persistence::sync::{
    SharedWorkersPersistenceCoordinator, WorkersPersistenceCoordinator,
};

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
    persistent_storage: Option<&Arc<Mutex<SingleWorkerPersistentStorage>>>,
    global_tracker: Option<&SharedWorkersPersistenceCoordinator>,
) -> FullReadResult {
    let maybe_persistent_id = reader.persistent_id();

    let offsets = Arc::new(Mutex::new(HashMap::<Timestamp, OffsetAntichain>::new()));
    if let Some(persistent_storage) = persistent_storage {
        if let Some(persistent_id) = reader.persistent_id() {
            persistent_storage.lock().unwrap().register_input_source(
                persistent_id,
                &reader.storage_type(),
                offsets.clone(),
            );
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
            Entry::RewindFinishSentinel => {
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
                    if let Some(ref mut snapshot_writer) = snapshot_writer {
                        let snapshot_event = match event {
                            ParsedEvent::Insert((_, ref values)) => {
                                let key = Key::random();
                                SnapshotEvent::Insert(key, values.clone())
                            }
                            ParsedEvent::Delete((_, _)) | ParsedEvent::Upsert((_, _)) => {
                                todo!("delete and upsert aren't supported in this test")
                            }
                            ParsedEvent::AdvanceTime => SnapshotEvent::AdvanceTime(Timestamp(1)),
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
            Entry::RewindFinishSentinel => {
                rewind_finish_sentinel_seen = true;
            }
            _ => {}
        }
    }

    assert!(rewind_finish_sentinel_seen);

    if maybe_persistent_id.is_some() && has_persistent_storage {
        let prev_finalized_time = persistent_storage
            .unwrap()
            .lock()
            .unwrap()
            .last_finalized_timestamp();
        offsets
            .lock()
            .unwrap()
            .insert(Timestamp(prev_finalized_time.0 + 1), frontier);

        let last_sink_id = persistent_storage.unwrap().lock().unwrap().register_sink();
        for sink_id in 0..=last_sink_id {
            global_tracker
                .as_ref()
                .unwrap()
                .lock()
                .unwrap()
                .accept_finalized_timestamp(0, sink_id, Some(Timestamp(prev_finalized_time.0 + 2)));
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
) -> (
    Arc<Mutex<SingleWorkerPersistentStorage>>,
    SharedWorkersPersistenceCoordinator,
) {
    if recreate {
        let _ = std::fs::remove_dir_all(fs_path);
    }

    let global_tracker = Arc::new(Mutex::new(WorkersPersistenceCoordinator::new(
        Duration::ZERO,
        1,
        1,
    )));

    let tracker = Arc::new(Mutex::new(
        SingleWorkerPersistentStorage::new(
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
    ));
    global_tracker
        .lock()
        .unwrap()
        .register_worker(tracker.clone());

    (tracker, global_tracker)
}

pub fn create_metadata_storage(fs_path: &Path, recreate: bool) -> MetadataAccessor {
    if recreate {
        let _ = std::fs::remove_dir_all(fs_path);
    }

    let backend = Box::new(FilesystemKVStorage::new(fs_path).expect("Backend creation failed"));
    MetadataAccessor::new(backend, 0).expect("Storage creation failed")
}

pub fn get_entries_in_receiver<T>(receiver: Receiver<T>) -> Vec<T> {
    let mut result = Vec::new();
    while let Ok(entry) = receiver.recv_timeout(Duration::from_secs(1)) {
        result.push(entry);
    }
    result
}

pub fn data_parsing_fails(
    mut reader: Box<dyn Reader>,
    mut parser: Box<dyn Parser>,
) -> eyre::Result<bool> {
    loop {
        let read_result = reader.read()?;
        match read_result {
            ReadResult::Data(bytes, _) => {
                let parse_result = parser.parse(&bytes);
                if parse_result.is_err() {
                    return Ok(true);
                }
            }
            ReadResult::FinishedSource { .. } => continue,
            ReadResult::NewSource(metadata) => parser.on_new_source_started(metadata.as_ref()),
            ReadResult::Finished => break,
        }
    }
    Ok(false)
}

pub fn assert_error_shown(
    mut reader: Box<dyn Reader>,
    parser: Box<dyn Parser>,
    error_formatted_text: &str,
) {
    let _ = reader
        .read()
        .expect("new data source read event should not fail");
    let row_read_result = reader
        .read()
        .expect("first line read event should not fail");
    match row_read_result {
        ReadResult::Data(bytes, _) => {
            assert_error_shown_for_reader_context(&bytes, parser, error_formatted_text);
        }
        _ => panic!("row_read_result is not Data"),
    }
}

pub fn assert_error_shown_for_reader_context(
    context: &ReaderContext,
    mut parser: Box<dyn Parser>,
    error_formatted_text: &str,
) {
    let row_parse_result = parser.parse(context);
    if let Err(e) = row_parse_result {
        eprintln!("Error details: {e:?}");
        assert_eq!(format!("{e}"), error_formatted_text.to_string());
    } else {
        panic!("We were supposed to get an error, however, the read was successful");
    }
}

pub fn assert_error_shown_for_raw_data(
    raw_data: &[u8],
    parser: Box<dyn Parser>,
    error_formatted_text: &str,
) {
    assert_error_shown_for_reader_context(
        &ReaderContext::from_raw_bytes(DataEventType::Insert, raw_data.to_vec()),
        parser,
        error_formatted_text,
    );
}
