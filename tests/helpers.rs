#![allow(dead_code)]
use std::collections::HashMap;
use std::path::Path;
use std::sync::{mpsc, mpsc::Receiver, Arc, Mutex};
use std::thread;
use std::time::Duration;

use pathway_engine::persistence::storage::{
    filesystem::SaveStatePolicy, FileSystem as FileSystemMetadataStorage,
};
use pathway_engine::persistence::tracker::SimplePersistencyManager;
use pathway_engine::persistence::tracker::{PersistenceManagerConfig, StreamStorageConfig};

use pathway_engine::connectors::data_format::{ParsedEvent, Parser};
use pathway_engine::connectors::data_storage::{ReadResult, Reader, ReaderBuilder, ReaderContext};
use pathway_engine::connectors::snapshot::Event as SnapshotEvent;
use pathway_engine::connectors::{Connector, Entry};
use pathway_engine::engine::Key;
use pathway_engine::persistence::frontier::OffsetAntichain;
use pathway_engine::persistence::tracker::PersistencyManager;

#[derive(Debug)]
pub struct FullReadResult {
    pub raw_entries: Vec<Entry>,
    pub snapshot_entries: Vec<SnapshotEvent>,
    pub new_parsed_entries: Vec<ParsedEvent>,
}

pub fn full_cycle_read(
    reader: Box<dyn ReaderBuilder>,
    parser: &mut dyn Parser,
    persistent_storage: &Option<Arc<Mutex<SimplePersistencyManager>>>,
) -> FullReadResult {
    let maybe_persistent_id = reader.persistent_id();

    let offsets = Arc::new(Mutex::new(HashMap::<u64, OffsetAntichain>::new()));
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
    Connector::<u64>::do_read_updates(reader, persistent_storage, &sender, &main_thread);
    let result = get_entries_in_receiver(receiver);

    let has_persistent_storage = persistent_storage.is_some();
    let mut frontier = OffsetAntichain::new();

    let mut snapshot_writer = {
        if has_persistent_storage {
            maybe_persistent_id.map(|persistent_id| {
                persistent_storage
                    .as_ref()
                    .unwrap()
                    .lock()
                    .unwrap()
                    .create_snapshot_writer(persistent_id)
                    .unwrap()
            })
        } else {
            None
        }
    };

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
            Entry::Realtime(ReadResult::Data(raw_data, Some((offset_key, offset_value)))) => {
                frontier.advance_offset(offset_key.clone(), offset_value.clone());

                let events = parser.parse(raw_data).unwrap();
                for event in events {
                    if let Some(ref mut snapshot_writer) = snapshot_writer {
                        let snapshot_event = match event {
                            ParsedEvent::Insert((_, ref values)) => {
                                let key = Key::random();
                                SnapshotEvent::Insert(key, values.clone())
                            }
                            ParsedEvent::Remove((_, _)) => {
                                todo!("remove isn't supported in this test")
                            }
                            ParsedEvent::AdvanceTime => SnapshotEvent::AdvanceTime(1),
                        };
                        snapshot_writer.write(&snapshot_event).unwrap();
                    }
                    new_parsed_entries.push(event);
                }
            }
            Entry::Realtime(ReadResult::NewSource) => {
                parser.on_new_source_started();
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
        offsets.lock().unwrap().insert(1, frontier);
        persistent_storage
            .clone()
            .unwrap()
            .lock()
            .unwrap()
            .accept_finalized_timestamp(2);
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
            ReadResult::NewSource => parser.on_new_source_started(),
            ReadResult::Finished => break,
        }
    }
    Ok(read_lines)
}

pub fn create_persistency_manager(
    fs_path: &Path,
    recreate: bool,
) -> Arc<Mutex<SimplePersistencyManager>> {
    if recreate {
        let _ = std::fs::remove_dir_all(fs_path);
    }
    let storage = FileSystemMetadataStorage::new(fs_path, 0, SaveStatePolicy::OnUpdate)
        .expect("Storage creation failed");
    Arc::new(Mutex::new(SimplePersistencyManager::new(
        Box::new(storage),
        PersistenceManagerConfig::new(StreamStorageConfig::Filesystem(fs_path.to_path_buf()), 0),
    )))
}

pub fn create_metadata_storage(fs_path: &Path, recreate: bool) -> FileSystemMetadataStorage {
    if recreate {
        let _ = std::fs::remove_dir_all(fs_path);
    }
    FileSystemMetadataStorage::new(fs_path, 0, SaveStatePolicy::OnUpdate)
        .expect("Storage creation failed")
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
            ReadResult::NewSource => parser.on_new_source_started(),
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
        &ReaderContext::from_raw_bytes(raw_data.to_vec()),
        parser,
        error_formatted_text,
    );
}
