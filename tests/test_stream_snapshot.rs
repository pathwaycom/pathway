mod helpers;
use helpers::create_persistency_manager;
use helpers::get_entries_in_receiver;

use assert_matches::assert_matches;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::{mpsc, Arc};

use tempfile::tempdir;

use pathway_engine::connectors::snapshot::Event as SnapshotEvent;
use pathway_engine::connectors::snapshot::{
    LocalBinarySnapshotReader, LocalBinarySnapshotWriter, SnapshotReader, SnapshotWriter,
};
use pathway_engine::connectors::{Connector, Entry};
use pathway_engine::engine::{Key, Value};
use pathway_engine::persistence::tracker::PersistencyManager;
use pathway_engine::persistence::PersistentId;

fn get_snapshot_reader_entries(mut snapshot_reader: Box<dyn SnapshotReader>) -> Vec<SnapshotEvent> {
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
    let snapshot_reader = LocalBinarySnapshotReader::new(chunks_root)
        .expect("Failed to create reader for test snapshot storage");
    get_snapshot_reader_entries(Box::new(snapshot_reader))
}

fn read_persistent_buffer_full(
    chunks_root: &Path,
    persistent_id: PersistentId,
) -> Vec<SnapshotEvent> {
    let tracker = create_persistency_manager(chunks_root, false);
    let (sender, receiver) = mpsc::channel();
    Connector::<u64>::rewind_from_disk_snapshot(persistent_id, &tracker, &sender);
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
    let test_storage_path = test_storage.into_path();

    {
        let mut snapshot_writer = LocalBinarySnapshotWriter::new(&test_storage_path)
            .expect("Failed to create test snapshot storage");
        snapshot_writer
            .write(&event1)
            .expect("Failed to write event into snapshot file");
        snapshot_writer
            .write(&event2)
            .expect("Failed to write event into snapshot file");
    }

    assert_eq!(
        read_persistent_buffer(&test_storage_path),
        vec![event1, event2]
    );

    Ok(())
}

#[test]
fn test_stream_snapshot_io_broken_format() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let test_storage_path = test_storage.into_path();

    {
        let mut test_file = File::create(test_storage_path.as_path().join("1"))
            .expect("Test storage creation broken");
        test_file
            .write_all(b"hello world")
            .expect("Failed to write");
    }

    let mut snapshot_reader = LocalBinarySnapshotReader::new(&test_storage_path)
        .expect("Failed to create reader for test snapshot storage");
    let entry = snapshot_reader.read();
    assert_matches!(entry, Err(_));

    Ok(())
}

#[test]
fn test_stream_empty() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let test_storage_path = test_storage.into_path();

    let mut snapshot_reader = LocalBinarySnapshotReader::new(Path::new(&test_storage_path))
        .expect("Failed to create reader for test snapshot storage");
    let entry = snapshot_reader.read();
    assert_matches!(entry, Ok(SnapshotEvent::Finished));

    Ok(())
}

#[test]
fn test_stream_not_from_dir() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let test_storage_path = test_storage.into_path();

    {
        let mut test_file = File::create(test_storage_path.as_path().join("1"))
            .expect("Test storage creation broken");
        test_file
            .write_all(b"hello world")
            .expect("Failed to write");
    }

    let snapshot_reader = LocalBinarySnapshotReader::new(&test_storage_path.as_path().join("1"));
    assert!(snapshot_reader.is_err());

    Ok(())
}

#[test]
fn test_buffer_scenario() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let test_storage_path = test_storage.into_path();

    let tracker = create_persistency_manager(&test_storage_path, true);
    let mut buffer = tracker
        .lock()
        .unwrap()
        .create_snapshot_writer(42)
        .expect("Failed to create snapshot writer");

    tracker.lock().unwrap().accept_finalized_timestamp(1);

    let event1 =
        SnapshotEvent::Insert(Key::random(), vec![Value::Int(1), Value::Float(2.3.into())]);
    let event2 = SnapshotEvent::Insert(
        Key::random(),
        vec![Value::Int(2), Value::String("abc".into())],
    );

    assert_eq!(
        read_persistent_buffer_full(&test_storage_path, 42),
        Vec::new()
    );
    buffer.write(&event1).unwrap();
    buffer.write(&event2).unwrap();
    buffer.write(&SnapshotEvent::AdvanceTime(2)).unwrap();
    buffer.flush().unwrap();
    tracker.lock().unwrap().accept_finalized_timestamp(2);

    assert_eq!(
        read_persistent_buffer_full(&test_storage_path, 42),
        vec![event1.clone(), event2.clone()]
    );

    buffer.write(&SnapshotEvent::AdvanceTime(10)).unwrap();
    buffer.flush().unwrap();
    let event3 = SnapshotEvent::Insert(Key::random(), vec![Value::Int(3), Value::Bool(true)]);
    let event4 = SnapshotEvent::Insert(Key::random(), vec![Value::Int(4), Value::Bool(false)]);
    buffer.write(&event3).unwrap();
    buffer.write(&event4).unwrap();
    assert_eq!(
        read_persistent_buffer_full(&test_storage_path, 42),
        vec![event1.clone(), event2.clone()]
    );

    tracker.lock().unwrap().accept_finalized_timestamp(5);
    assert_eq!(
        read_persistent_buffer_full(&test_storage_path, 42),
        vec![event1.clone(), event2.clone()]
    );
    tracker.lock().unwrap().accept_finalized_timestamp(10);
    assert_eq!(
        read_persistent_buffer_full(&test_storage_path, 42),
        vec![event1.clone(), event2.clone()]
    );

    buffer.write(&SnapshotEvent::AdvanceTime(11)).unwrap();
    buffer.flush().unwrap();

    tracker.lock().unwrap().accept_finalized_timestamp(12);
    assert_eq!(
        read_persistent_buffer_full(&test_storage_path, 42),
        vec![
            event1.clone(),
            event2.clone(),
            event3.clone(),
            event4.clone()
        ]
    );
    tracker.lock().unwrap().accept_finalized_timestamp(15);
    assert_eq!(
        read_persistent_buffer_full(&test_storage_path, 42),
        vec![event1, event2, event3, event4]
    );

    Ok(())
}

#[test]
fn test_buffer_scenario_several_writes() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let test_storage_path = test_storage.into_path();

    let tracker = create_persistency_manager(&test_storage_path, true);

    let event1 =
        SnapshotEvent::Insert(Key::random(), vec![Value::Int(1), Value::Float(2.3.into())]);
    let event2 = SnapshotEvent::Insert(
        Key::random(),
        vec![Value::Int(2), Value::String("abc".into())],
    );

    {
        let mut buffer = tracker
            .lock()
            .unwrap()
            .create_snapshot_writer(42)
            .expect("Failed to create snapshot writer");

        buffer.write(&event1).unwrap();
        buffer.write(&SnapshotEvent::AdvanceTime(1)).unwrap();
        buffer.flush().unwrap();

        tracker.lock().unwrap().accept_finalized_timestamp(2);
        assert_eq!(
            read_persistent_buffer_full(&test_storage_path, 42),
            vec![event1.clone()]
        );
    }

    {
        let mut buffer = tracker
            .lock()
            .unwrap()
            .create_snapshot_writer(42)
            .expect("Failed to create snapshot writer");

        buffer.write(&event2).unwrap();
        buffer.write(&SnapshotEvent::AdvanceTime(2)).unwrap();
        buffer.flush().unwrap();

        tracker.lock().unwrap().accept_finalized_timestamp(3);
        assert_eq!(
            read_persistent_buffer_full(&test_storage_path, 42),
            vec![event1, event2]
        );
    }

    Ok(())
}
