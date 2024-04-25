// Copyright © 2024 Pathway

use super::helpers::create_persistence_manager;
use super::helpers::get_entries_in_receiver;

use std::sync::mpsc;
use std::thread::sleep;
use std::time::Duration;

use tempfile::tempdir;

use pathway_engine::connectors::data_storage::StorageType;
use pathway_engine::connectors::{Connector, Entry, PersistenceMode};
use pathway_engine::connectors::{OffsetKey, OffsetValue};
use pathway_engine::engine::{Timestamp, TotalFrontier};
use pathway_engine::persistence::frontier::OffsetAntichain;
use pathway_engine::persistence::metadata_backends::FilesystemKVStorage;
use pathway_engine::persistence::state::MetadataAccessor;

fn assert_frontiers_equal(
    mut lhs: Vec<(OffsetKey, OffsetValue)>,
    mut rhs: Vec<(OffsetKey, OffsetValue)>,
) {
    lhs.sort();
    rhs.sort();
    assert_eq!(lhs, rhs);
}

#[test]
fn test_antichain_simple_storage_offsets() -> eyre::Result<()> {
    let mut x = OffsetAntichain::new();
    assert_frontiers_equal(x.as_vec(), vec![]);

    x.advance_offset(OffsetKey::Empty, OffsetValue::KafkaOffset(1));
    assert_frontiers_equal(
        x.as_vec(),
        vec![(OffsetKey::Empty, OffsetValue::KafkaOffset(1))],
    );

    x.advance_offset(OffsetKey::Empty, OffsetValue::KafkaOffset(3));
    assert_frontiers_equal(
        x.as_vec(),
        vec![(OffsetKey::Empty, OffsetValue::KafkaOffset(3))],
    );

    Ok(())
}

#[test]
fn test_antichain_kafka_offsets() -> eyre::Result<()> {
    let mut x = OffsetAntichain::new();
    assert_frontiers_equal(x.as_vec(), vec![]);

    x.advance_offset(
        OffsetKey::Kafka("test".to_string().into(), 1),
        OffsetValue::KafkaOffset(2),
    );
    assert_frontiers_equal(
        x.as_vec(),
        vec![(
            OffsetKey::Kafka("test".to_string().into(), 1),
            OffsetValue::KafkaOffset(2),
        )],
    );

    x.advance_offset(
        OffsetKey::Kafka("test".to_string().into(), 1),
        OffsetValue::KafkaOffset(3),
    );
    assert_frontiers_equal(
        x.as_vec(),
        vec![(
            OffsetKey::Kafka("test".to_string().into(), 1),
            OffsetValue::KafkaOffset(3),
        )],
    );

    x.advance_offset(
        OffsetKey::Kafka("test".to_string().into(), 2),
        OffsetValue::KafkaOffset(1),
    );
    assert_frontiers_equal(
        x.as_vec(),
        vec![
            (
                OffsetKey::Kafka("test".to_string().into(), 1),
                OffsetValue::KafkaOffset(3),
            ),
            (
                OffsetKey::Kafka("test".to_string().into(), 2),
                OffsetValue::KafkaOffset(1),
            ),
        ],
    );

    x.advance_offset(
        OffsetKey::Kafka("test1".to_string().into(), 5),
        OffsetValue::KafkaOffset(5),
    );
    assert_frontiers_equal(
        x.as_vec(),
        vec![
            (
                OffsetKey::Kafka("test".to_string().into(), 1),
                OffsetValue::KafkaOffset(3),
            ),
            (
                OffsetKey::Kafka("test".to_string().into(), 2),
                OffsetValue::KafkaOffset(1),
            ),
            (
                OffsetKey::Kafka("test1".to_string().into(), 5),
                OffsetValue::KafkaOffset(5),
            ),
        ],
    );

    Ok(())
}

#[test]
fn test_antichain_incomparable_offsets() -> eyre::Result<()> {
    let mut x = OffsetAntichain::new();
    assert_frontiers_equal(x.as_vec(), vec![]);

    x.advance_offset(OffsetKey::Empty, OffsetValue::KafkaOffset(1));
    assert_frontiers_equal(
        x.as_vec(),
        vec![(OffsetKey::Empty, OffsetValue::KafkaOffset(1))],
    );

    x.advance_offset(
        OffsetKey::Kafka("test".to_string().into(), 1),
        OffsetValue::KafkaOffset(2),
    );
    assert_frontiers_equal(
        x.as_vec(),
        vec![
            (OffsetKey::Empty, OffsetValue::KafkaOffset(1)),
            (
                OffsetKey::Kafka("test".to_string().into(), 1),
                OffsetValue::KafkaOffset(2),
            ),
        ],
    );

    Ok(())
}

#[test]
fn test_rewind_for_empty_persistent_storage() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let test_storage_path = test_storage.path();

    let (sender, receiver) = mpsc::channel();
    let tracker = create_persistence_manager(test_storage_path, false);
    Connector::rewind_from_disk_snapshot(1, &tracker, &sender, PersistenceMode::Batch);
    assert_eq!(get_entries_in_receiver::<Entry>(receiver).len(), 0); // We would not even start rewind when there is no frontier

    Ok(())
}

#[test]
fn test_timestamp_advancement_in_tracker() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let test_storage_path = test_storage.path();

    let tracker = create_persistence_manager(test_storage_path, true);

    assert_eq!(
        tracker.lock().unwrap().last_finalized_timestamp(),
        TotalFrontier::At(Timestamp(0))
    );
    let mock_sink_id = tracker.lock().unwrap().register_sink();

    tracker
        .lock()
        .unwrap()
        .update_sink_finalized_time(mock_sink_id, Some(Timestamp(1)));
    assert_eq!(
        tracker.lock().unwrap().last_finalized_timestamp(),
        TotalFrontier::At(Timestamp(1))
    );

    tracker
        .lock()
        .unwrap()
        .update_sink_finalized_time(mock_sink_id, Some(Timestamp(5)));
    assert_eq!(
        tracker.lock().unwrap().last_finalized_timestamp(),
        TotalFrontier::At(Timestamp(5))
    );

    tracker
        .lock()
        .unwrap()
        .update_sink_finalized_time(mock_sink_id, Some(Timestamp(15)));
    assert_eq!(
        tracker.lock().unwrap().last_finalized_timestamp(),
        TotalFrontier::At(Timestamp(15))
    );

    Ok(())
}

#[test]
#[should_panic(expected = "Same persistent_id belongs to more than one data source: 512")]
fn test_unique_persistent_id() {
    let test_storage = tempdir().unwrap();
    let test_storage_path = test_storage.path();

    let tracker = create_persistence_manager(test_storage_path, true);
    let mut tracker = tracker.lock().unwrap();

    tracker.register_input_source(512, &StorageType::FileSystem);
    tracker.register_input_source(512, &StorageType::FileSystem);
}

#[test]
fn test_several_sinks_finalized_timestamp_calculation() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let test_storage_path = test_storage.path();

    let tracker = create_persistence_manager(test_storage_path, true);

    tracker
        .lock()
        .unwrap()
        .register_input_source(512, &StorageType::FileSystem);

    let sink_id_1 = tracker.lock().unwrap().register_sink();
    let sink_id_2 = tracker.lock().unwrap().register_sink();

    tracker
        .lock()
        .unwrap()
        .update_sink_finalized_time(sink_id_1, Some(Timestamp(5)));
    assert_eq!(
        tracker.lock().unwrap().last_finalized_timestamp(),
        TotalFrontier::At(Timestamp(0))
    );
    tracker
        .lock()
        .unwrap()
        .update_sink_finalized_time(sink_id_1, Some(Timestamp(7)));
    assert_eq!(
        tracker.lock().unwrap().last_finalized_timestamp(),
        TotalFrontier::At(Timestamp(0))
    );
    tracker
        .lock()
        .unwrap()
        .update_sink_finalized_time(sink_id_2, Some(Timestamp(4)));
    assert_eq!(
        tracker.lock().unwrap().last_finalized_timestamp(),
        TotalFrontier::At(Timestamp(4))
    );
    tracker
        .lock()
        .unwrap()
        .update_sink_finalized_time(sink_id_2, Some(Timestamp(10)));
    assert_eq!(
        tracker.lock().unwrap().last_finalized_timestamp(),
        TotalFrontier::At(Timestamp(7))
    );
    tracker
        .lock()
        .unwrap()
        .update_sink_finalized_time(sink_id_2, None);
    assert_eq!(
        tracker.lock().unwrap().last_finalized_timestamp(),
        TotalFrontier::At(Timestamp(7))
    );

    /*
        Finalize both sinks, after that the last known time in this test
        equals to 7, so that should be the result here.
    */
    tracker
        .lock()
        .unwrap()
        .update_sink_finalized_time(sink_id_1, None);
    assert_eq!(
        tracker.lock().unwrap().last_finalized_timestamp(),
        TotalFrontier::Done
    );

    Ok(())
}

#[test]
fn test_metadata_files_versioning() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let test_storage_path = test_storage.path();

    // simulating the first run, tracker will be destroyed when the
    // mock run is done
    {
        let tracker = create_persistence_manager(test_storage_path, true);
        tracker
            .lock()
            .unwrap()
            .register_input_source(512, &StorageType::FileSystem);
        let sink_id = tracker.lock().unwrap().register_sink();
        tracker
            .lock()
            .unwrap()
            .update_sink_finalized_time(sink_id, Some(Timestamp(100)));
    }

    {
        let ms = MetadataAccessor::new(Box::new(FilesystemKVStorage::new(test_storage_path)?), 0)?;
        assert_eq!(
            ms.past_runs_threshold_times().values().min().unwrap(),
            &TotalFrontier::At(Timestamp(100))
        );
    }

    // simulating the second run
    // ensure that metadata file will be named differently
    sleep(Duration::from_millis(10));
    {
        let tracker = create_persistence_manager(test_storage_path, false);
        let sink_id = tracker.lock().unwrap().register_sink();
        tracker
            .lock()
            .unwrap()
            .update_sink_finalized_time(sink_id, None);
    }
    {
        let ms = MetadataAccessor::new(Box::new(FilesystemKVStorage::new(test_storage_path)?), 0)?;
        assert_eq!(
            ms.past_runs_threshold_times().values().min().unwrap(),
            &TotalFrontier::Done
        );
    }

    // simulating the third run
    sleep(Duration::from_millis(10));
    {
        let tracker = create_persistence_manager(test_storage_path, false);
        let sink_id = tracker.lock().unwrap().register_sink();
        tracker
            .lock()
            .unwrap()
            .update_sink_finalized_time(sink_id, Some(Timestamp(100)));
    }
    {
        let ms = MetadataAccessor::new(Box::new(FilesystemKVStorage::new(test_storage_path)?), 0)?;
        assert_eq!(
            ms.past_runs_threshold_times().values().min().unwrap(),
            &TotalFrontier::At(Timestamp(100))
        );
    }

    Ok(())
}
