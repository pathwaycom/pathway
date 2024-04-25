// Copyright © 2024 Pathway

use super::helpers::create_persistence_manager;
use super::helpers::get_entries_in_receiver;

use assert_matches::assert_matches;
use pathway_engine::engine::Timestamp;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::{mpsc, Arc};

use tempfile::tempdir;

use pathway_engine::connectors::snapshot::Event as SnapshotEvent;
use pathway_engine::connectors::snapshot::{
    LocalBinarySnapshotReader, LocalBinarySnapshotWriter, ReadSnapshotEvent, WriteSnapshotEvent,
};
use pathway_engine::connectors::{Connector, Entry, PersistenceMode};
use pathway_engine::engine::{Key, Value};
use pathway_engine::persistence::frontier::OffsetAntichain;
use pathway_engine::persistence::PersistentId;

fn get_snapshot_reader_entries(
    mut snapshot_reader: Box<dyn ReadSnapshotEvent>,
) -> Vec<SnapshotEvent> {
    let mut entries = Vec::new();
    loop {
        let entry = snapshot_reader
            .read()
            .expect("Read should not terminate with an error");
        if matches!(entry, SnapshotEvent::Finished) {
            break;
        }
        entries.push(entry);
    }

    entries
}

fn read_persistent_buffer(chunks_root: &Path) -> Vec<SnapshotEvent> {
    let snapshot_reader = LocalBinarySnapshotReader::new(chunks_root.to_path_buf())
        .expect("Failed to create reader for test snapshot storage");
    get_snapshot_reader_entries(Box::new(snapshot_reader))
}

fn read_persistent_buffer_full(
    chunks_root: &Path,
    persistent_id: PersistentId,
    persistence_mode: PersistenceMode,
) -> Vec<SnapshotEvent> {
    let tracker = create_persistence_manager(chunks_root, false);
    let (sender, receiver) = mpsc::channel();
    Connector::rewind_from_disk_snapshot(persistent_id, &tracker, &sender, persistence_mode);
    let entries: Vec<Entry> = get_entries_in_receiver(receiver);
    let mut result = Vec::new();
    for entry in entries {
        if let Entry::Snapshot(s) = entry {
            result.push(s);
        } else {
            unreachable!("this part should be unreachable");
        }
    }
    result
}

#[test]
fn test_stream_snapshot_io() -> eyre::Result<()> {
    let event1 = SnapshotEvent::Insert(
        Key::random(),
        vec![
            Value::Int(1),
            Value::Float(1.0.into()),
            Value::String("test string".into()),
        ],
    );
    let event2 = SnapshotEvent::Insert(
        Key::random(),
        vec![
            Value::Bool(false),
            Value::None,
            Value::Tuple(Arc::new([Value::Int(1), Value::Float(1.1.into())])),
        ],
    );

    let test_storage = tempdir()?;
    let test_storage_path = test_storage.path();

    {
        let mut snapshot_writer = LocalBinarySnapshotWriter::new(test_storage_path)
            .expect("Failed to create test snapshot storage");
        snapshot_writer
            .write(&event1)
            .expect("Failed to write event into snapshot file");
        snapshot_writer
            .write(&event2)
            .expect("Failed to write event into snapshot file");
    }

    assert_eq!(
        read_persistent_buffer(test_storage_path),
        vec![event1, event2]
    );

    Ok(())
}

#[test]
fn test_stream_snapshot_io_broken_format() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let test_storage_path = test_storage.path();

    {
        let mut test_file =
            File::create(test_storage_path.join("1")).expect("Test storage creation broken");
        test_file
            .write_all(b"hello world")
            .expect("Failed to write");
    }

    let mut snapshot_reader = LocalBinarySnapshotReader::new(test_storage_path.to_path_buf())
        .expect("Failed to create reader for test snapshot storage");
    let entry = snapshot_reader.read();
    assert_matches!(entry, Err(_));

    Ok(())
}

#[test]
fn test_stream_empty() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let test_storage_path = test_storage.path();

    let mut snapshot_reader = LocalBinarySnapshotReader::new(test_storage_path.to_path_buf())
        .expect("Failed to create reader for test snapshot storage");
    let entry = snapshot_reader.read();
    assert_matches!(entry, Ok(SnapshotEvent::Finished));

    Ok(())
}

#[test]
fn test_stream_not_from_dir() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let test_storage_path = test_storage.path();

    {
        let mut test_file =
            File::create(test_storage_path.join("1")).expect("Test storage creation broken");
        test_file
            .write_all(b"hello world")
            .expect("Failed to write");
    }

    let snapshot_reader = LocalBinarySnapshotReader::new(test_storage_path.join("1"));
    assert!(snapshot_reader.is_err());

    Ok(())
}

#[test]
fn test_buffer_dont_read_beyond_threshold_time() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let test_storage_path = test_storage.path();

    let tracker = create_persistence_manager(test_storage_path, true);
    let buffer = tracker
        .lock()
        .unwrap()
        .create_snapshot_writer(42)
        .expect("Failed to create snapshot writer");

    let mock_sink_id = tracker.lock().unwrap().register_sink();

    tracker
        .lock()
        .unwrap()
        .update_sink_finalized_time(mock_sink_id, Some(Timestamp(1)));

    let event1 =
        SnapshotEvent::Insert(Key::random(), vec![Value::Int(1), Value::Float(2.3.into())]);
    let event2 = SnapshotEvent::Insert(
        Key::random(),
        vec![Value::Int(2), Value::String("abc".into())],
    );

    assert_eq!(
        read_persistent_buffer_full(test_storage_path, 42, PersistenceMode::Batch),
        Vec::new()
    );
    buffer.lock().unwrap().write(&event1).unwrap();
    buffer.lock().unwrap().write(&event2).unwrap();
    buffer
        .lock()
        .unwrap()
        .write(&SnapshotEvent::AdvanceTime(
            Timestamp(2),
            OffsetAntichain::new(),
        ))
        .unwrap();

    tracker
        .lock()
        .unwrap()
        .update_sink_finalized_time(mock_sink_id, Some(Timestamp(2)));

    assert_eq!(
        read_persistent_buffer_full(test_storage_path, 42, PersistenceMode::Batch),
        vec![event1.clone(), event2.clone()]
    );

    buffer
        .lock()
        .unwrap()
        .write(&SnapshotEvent::AdvanceTime(
            Timestamp(10),
            OffsetAntichain::new(),
        ))
        .unwrap();
    let event3 = SnapshotEvent::Insert(Key::random(), vec![Value::Int(3), Value::Bool(true)]);
    let event4 = SnapshotEvent::Insert(Key::random(), vec![Value::Int(4), Value::Bool(false)]);
    buffer.lock().unwrap().write(&event3).unwrap();
    buffer.lock().unwrap().write(&event4).unwrap();
    assert_eq!(
        read_persistent_buffer_full(test_storage_path, 42, PersistenceMode::Batch),
        vec![event1.clone(), event2.clone()]
    );

    Ok(())
}

#[test]
fn test_buffer_scenario_several_writes() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let test_storage_path = test_storage.path();

    let tracker = create_persistence_manager(test_storage_path, true);
    let mock_sink_id = tracker.lock().unwrap().register_sink();

    let event1 =
        SnapshotEvent::Insert(Key::random(), vec![Value::Int(1), Value::Float(2.3.into())]);
    let event2 = SnapshotEvent::Insert(
        Key::random(),
        vec![Value::Int(2), Value::String("abc".into())],
    );

    {
        let buffer = tracker
            .lock()
            .unwrap()
            .create_snapshot_writer(42)
            .expect("Failed to create snapshot writer");

        buffer.lock().unwrap().write(&event1).unwrap();
        buffer
            .lock()
            .unwrap()
            .write(&SnapshotEvent::AdvanceTime(
                Timestamp(1),
                OffsetAntichain::new(),
            ))
            .unwrap();

        tracker
            .lock()
            .unwrap()
            .update_sink_finalized_time(mock_sink_id, Some(Timestamp(2)));

        assert_eq!(
            read_persistent_buffer_full(test_storage_path, 42, PersistenceMode::Batch),
            vec![event1.clone()]
        );
    }

    {
        let buffer = tracker
            .lock()
            .unwrap()
            .create_snapshot_writer(42)
            .expect("Failed to create snapshot writer");

        buffer.lock().unwrap().write(&event2).unwrap();
        buffer
            .lock()
            .unwrap()
            .write(&SnapshotEvent::AdvanceTime(
                Timestamp(2),
                OffsetAntichain::new(),
            ))
            .unwrap();
        futures::executor::block_on(async {
            let flush_future = buffer.lock().unwrap().flush();
            flush_future.await.unwrap().unwrap();
        });

        tracker
            .lock()
            .unwrap()
            .update_sink_finalized_time(mock_sink_id, Some(Timestamp(3)));

        assert_eq!(
            read_persistent_buffer_full(test_storage_path, 42, PersistenceMode::Batch),
            vec![event1, event2]
        );
    }

    Ok(())
}

#[test]
fn test_stream_snapshot_speedrun() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let test_storage_path = test_storage.path();

    let tracker = create_persistence_manager(test_storage_path, true);
    let buffer = tracker
        .lock()
        .unwrap()
        .create_snapshot_writer(42)
        .expect("Failed to create snapshot writer");

    let mock_sink_id = tracker.lock().unwrap().register_sink();

    tracker
        .lock()
        .unwrap()
        .update_sink_finalized_time(mock_sink_id, Some(Timestamp(1)));

    let event1 =
        SnapshotEvent::Insert(Key::random(), vec![Value::Int(1), Value::Float(2.3.into())]);
    let event2 = SnapshotEvent::Insert(
        Key::random(),
        vec![Value::Int(2), Value::String("abc".into())],
    );
    let event3 = SnapshotEvent::AdvanceTime(Timestamp(2), OffsetAntichain::new());
    let event4 = SnapshotEvent::Insert(Key::random(), vec![Value::Int(3), Value::None]);

    assert_eq!(
        read_persistent_buffer_full(test_storage_path, 42, PersistenceMode::SpeedrunReplay),
        Vec::new()
    );
    buffer.lock().unwrap().write(&event1).unwrap();
    buffer.lock().unwrap().write(&event2).unwrap();
    buffer.lock().unwrap().write(&event3).unwrap();
    buffer.lock().unwrap().write(&event4).unwrap();

    tracker
        .lock()
        .unwrap()
        .update_sink_finalized_time(mock_sink_id, Some(Timestamp(3)));

    assert_eq!(
        read_persistent_buffer_full(test_storage_path, 42, PersistenceMode::SpeedrunReplay),
        vec![
            event1.clone(),
            event2.clone(),
            event3.clone(),
            event4.clone()
        ]
    );

    Ok(())
}
