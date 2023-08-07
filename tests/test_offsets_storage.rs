mod helpers;
use helpers::create_metadata_storage;
use helpers::create_persistency_manager;
use helpers::get_entries_in_receiver;

use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;

use tempfile::tempdir;

use pathway_engine::connectors::{Connector, Entry};

use pathway_engine::connectors::data_storage::StorageType;
use pathway_engine::connectors::{OffsetKey, OffsetValue};
use pathway_engine::persistence::frontier::OffsetAntichain;
use pathway_engine::persistence::storage::{
    filesystem::SaveStatePolicy, FileSystem as FileSystemMetadataStorage, Storage,
};
use pathway_engine::persistence::tracker::SimplePersistencyManager;
use pathway_engine::persistence::tracker::{
    PersistenceManagerConfig, PersistencyManager, StreamStorageConfig,
};

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
fn test_state_dump_and_load() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let test_storage_path = test_storage.into_path();

    {
        let mut storage = create_metadata_storage(&test_storage_path, true);
        storage.register_input_source(1, &StorageType::FileSystem);
        storage.save_offset(1, &OffsetKey::Empty, &OffsetValue::KafkaOffset(1));
        storage.save_offset(1, &OffsetKey::Empty, &OffsetValue::KafkaOffset(2));
        storage.save_offset(
            1,
            &OffsetKey::Kafka("test".to_string().into(), 5),
            &OffsetValue::KafkaOffset(5),
        );
    }

    {
        let storage = create_metadata_storage(&test_storage_path, false);
        let antichain = storage.frontier_for(1).as_vec();
        assert_eq!(antichain.len(), 2);
        assert_frontiers_equal(
            antichain,
            vec![
                (OffsetKey::Empty, OffsetValue::KafkaOffset(2)),
                (
                    OffsetKey::Kafka("test".to_string().into(), 5),
                    OffsetValue::KafkaOffset(5),
                ),
            ],
        );
    }

    {
        let mut storage = create_metadata_storage(&test_storage_path, false);
        storage.register_input_source(2, &StorageType::FileSystem);
        storage.save_offset(2, &OffsetKey::Empty, &OffsetValue::KafkaOffset(3));
    }

    {
        let storage = create_metadata_storage(&test_storage_path, false);
        let antichain = storage.frontier_for(1).as_vec();
        assert_eq!(antichain.len(), 2);
        assert_frontiers_equal(
            antichain,
            vec![
                (OffsetKey::Empty, OffsetValue::KafkaOffset(2)),
                (
                    OffsetKey::Kafka("test".to_string().into(), 5),
                    OffsetValue::KafkaOffset(5),
                ),
            ],
        );

        let antichain = storage.frontier_for(2).as_vec();
        assert_eq!(antichain.len(), 1);
        assert_frontiers_equal(
            antichain,
            vec![(OffsetKey::Empty, OffsetValue::KafkaOffset(3))],
        );
    }

    Ok(())
}

#[test]
fn test_rewind_for_empty_persistent_storage() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let test_storage_path = test_storage.into_path();

    let (sender, receiver) = mpsc::channel();
    Connector::<u64>::rewind_from_disk_snapshot(
        1,
        &create_persistency_manager(&test_storage_path, false),
        &sender,
    );
    assert_eq!(get_entries_in_receiver::<Entry>(receiver).len(), 0); // We would not even start rewind when there is no frontier

    Ok(())
}

#[test]
fn test_timestamp_advancement_in_tracker() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let test_storage_path = test_storage.into_path();

    let storage = FileSystemMetadataStorage::new(&test_storage_path, 0, SaveStatePolicy::OnUpdate)
        .expect("Failed to create storage");
    let mut tracker = SimplePersistencyManager::new(
        Box::new(storage),
        PersistenceManagerConfig::new(StreamStorageConfig::Filesystem(test_storage_path), 0, 1),
    );

    assert_eq!(tracker.last_finalized_timestamp(), 0);

    let mock_sink_id = tracker.register_sink();

    tracker.accept_finalized_timestamp(mock_sink_id, Some(1));
    assert_eq!(tracker.last_finalized_timestamp(), 1);

    tracker.accept_finalized_timestamp(mock_sink_id, Some(5));
    assert_eq!(tracker.last_finalized_timestamp(), 5);

    tracker.accept_finalized_timestamp(mock_sink_id, Some(15));
    assert_eq!(tracker.last_finalized_timestamp(), 15);

    Ok(())
}

#[test]
fn test_frontier_dumping_in_tracker() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let test_storage_path = test_storage.into_path();

    let frontier = Arc::new(Mutex::new(HashMap::<u64, OffsetAntichain>::new()));
    let storage = create_metadata_storage(&test_storage_path, true);
    let mut tracker = SimplePersistencyManager::new(
        Box::new(storage),
        PersistenceManagerConfig::new(
            StreamStorageConfig::Filesystem(test_storage_path.clone()),
            0,
            1,
        ),
    );

    tracker.register_input_source(1, &StorageType::FileSystem, frontier.clone());

    assert_eq!(tracker.last_finalized_timestamp(), 0);

    {
        let mut antichain = OffsetAntichain::new();
        antichain.advance_offset(OffsetKey::Empty, OffsetValue::KafkaOffset(42));
        frontier
            .lock()
            .expect("Frontier acquisition failed")
            .insert(2, antichain);
    }

    {
        let storage_reread = create_metadata_storage(&test_storage_path, false);
        let antichain = storage_reread.frontier_for(2).as_vec();
        assert_eq!(antichain.len(), 0);
    }

    {
        let mut antichain = OffsetAntichain::new();
        antichain.advance_offset(OffsetKey::Empty, OffsetValue::KafkaOffset(84));
        frontier
            .lock()
            .expect("Frontier acquisition failed")
            .insert(11, antichain);
    }
    assert_eq!(frontier.lock().expect("Frontier access failed").len(), 2);

    let mock_sink_id = tracker.register_sink();

    tracker.accept_finalized_timestamp(mock_sink_id, Some(3));
    assert_eq!(tracker.last_finalized_timestamp(), 3);
    {
        let storage_reread = create_metadata_storage(&test_storage_path, false);
        let antichain = storage_reread.frontier_for(1).as_vec();
        assert_eq!(antichain.len(), 1);
        assert_frontiers_equal(
            antichain,
            vec![(OffsetKey::Empty, OffsetValue::KafkaOffset(42))],
        );
    }
    assert_eq!(frontier.lock().expect("Frontier access failed").len(), 1);

    tracker.accept_finalized_timestamp(mock_sink_id, Some(10));
    assert_eq!(tracker.last_finalized_timestamp(), 10);
    {
        let storage_reread = create_metadata_storage(&test_storage_path, false);
        let antichain = storage_reread.frontier_for(1).as_vec();
        assert_eq!(antichain.len(), 1);
        assert_frontiers_equal(
            antichain,
            vec![(OffsetKey::Empty, OffsetValue::KafkaOffset(42))],
        );
    }
    assert_eq!(frontier.lock().expect("Frontier access failed").len(), 1);

    tracker.accept_finalized_timestamp(mock_sink_id, Some(15));
    assert_eq!(tracker.last_finalized_timestamp(), 15);
    {
        let storage_reread = create_metadata_storage(&test_storage_path, false);
        let antichain = storage_reread.frontier_for(1).as_vec();
        assert_eq!(antichain.len(), 1);
        assert_frontiers_equal(
            antichain,
            vec![(OffsetKey::Empty, OffsetValue::KafkaOffset(84))],
        );
    }
    assert_eq!(frontier.lock().expect("Frontier access failed").len(), 0);

    Ok(())
}

#[test]
fn test_state_dump_and_load_background() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let test_storage_path = test_storage.into_path();

    {
        let mut storage = FileSystemMetadataStorage::new(
            &test_storage_path,
            0,
            SaveStatePolicy::Background(Duration::from_millis(500)),
        )
        .expect("Storage creation failed");
        storage.register_input_source(1, &StorageType::FileSystem);
        storage.save_offset(1, &OffsetKey::Empty, &OffsetValue::KafkaOffset(1));
        storage.save_offset(1, &OffsetKey::Empty, &OffsetValue::KafkaOffset(2));
        storage.save_offset(
            1,
            &OffsetKey::Kafka("test".to_string().into(), 5),
            &OffsetValue::KafkaOffset(5),
        );
        sleep(Duration::from_secs(1));
    }

    {
        let storage = create_metadata_storage(&test_storage_path, false);
        let antichain = storage.frontier_for(1).as_vec();
        assert_frontiers_equal(
            antichain,
            vec![
                (OffsetKey::Empty, OffsetValue::KafkaOffset(2)),
                (
                    OffsetKey::Kafka("test".to_string().into(), 5),
                    OffsetValue::KafkaOffset(5),
                ),
            ],
        );
    }

    {
        let mut storage = FileSystemMetadataStorage::new(
            &test_storage_path,
            0,
            SaveStatePolicy::Background(Duration::from_millis(500)),
        )
        .expect("Storage creation failed");
        storage.register_input_source(2, &StorageType::FileSystem);
        storage.save_offset(2, &OffsetKey::Empty, &OffsetValue::KafkaOffset(3));
        sleep(Duration::from_secs(1));
    }

    {
        let storage = create_metadata_storage(&test_storage_path, false);
        let antichain = storage.frontier_for(1).as_vec();
        assert_frontiers_equal(
            antichain,
            vec![
                (OffsetKey::Empty, OffsetValue::KafkaOffset(2)),
                (
                    OffsetKey::Kafka("test".to_string().into(), 5),
                    OffsetValue::KafkaOffset(5),
                ),
            ],
        );

        let antichain = storage.frontier_for(2).as_vec();
        assert_frontiers_equal(
            antichain,
            vec![(OffsetKey::Empty, OffsetValue::KafkaOffset(3))],
        );
    }

    Ok(())
}

#[test]
fn test_state_dump_with_newlines_in_offsets() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let test_storage_path = test_storage.into_path();

    {
        let mut storage = FileSystemMetadataStorage::new(
            &test_storage_path,
            0,
            SaveStatePolicy::Background(Duration::from_millis(500)),
        )
        .expect("Storage creation failed");
        storage.register_input_source(1, &StorageType::FileSystem);
        storage.save_offset(
            1,
            &OffsetKey::Kafka("test\n123".to_string().into(), 5),
            &OffsetValue::KafkaOffset(5),
        );
        sleep(Duration::from_secs(1));
    }

    {
        let storage = create_metadata_storage(&test_storage_path, false);
        let antichain = storage.frontier_for(1).as_vec();
        assert_frontiers_equal(
            antichain,
            vec![(
                OffsetKey::Kafka("test\n123".to_string().into(), 5),
                OffsetValue::KafkaOffset(5),
            )],
        );
    }
    Ok(())
}

#[test]
fn test_global_finalized_timestamp() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let test_storage_path = test_storage.into_path();

    let frontiers_by_time = Arc::new(Mutex::new(HashMap::<u64, OffsetAntichain>::new()));
    let storage = create_metadata_storage(&test_storage_path, true);
    let mut tracker = SimplePersistencyManager::new(
        Box::new(storage),
        PersistenceManagerConfig::new(
            StreamStorageConfig::Filesystem(test_storage_path.clone()),
            0,
            1,
        ),
    );

    tracker.register_input_source(1, &StorageType::FileSystem, frontiers_by_time.clone());
    let mock_sink_id = tracker.register_sink();

    let mut frontier = OffsetAntichain::new();
    frontier.advance_offset(OffsetKey::Empty, OffsetValue::KafkaOffset(1));
    frontiers_by_time
        .lock()
        .unwrap()
        .insert(1, frontier.clone());
    frontiers_by_time
        .lock()
        .unwrap()
        .insert(5, frontier.clone());
    frontiers_by_time
        .lock()
        .unwrap()
        .insert(6, frontier.clone());
    frontiers_by_time
        .lock()
        .unwrap()
        .insert(8, frontier.clone());

    assert_eq!(tracker.globally_finalized_timestamp(), 0);
    tracker.accept_finalized_timestamp(mock_sink_id, Some(4));
    assert_eq!(tracker.globally_finalized_timestamp(), 4);
    tracker.accept_finalized_timestamp(mock_sink_id, None);
    assert_eq!(tracker.globally_finalized_timestamp(), 8);

    Ok(())
}

#[test]
#[should_panic(expected = "Same persistent_id belongs to more than one data source: 512")]
fn test_unique_persistent_id() {
    let test_storage = tempdir().unwrap();
    let test_storage_path = test_storage.into_path();

    let frontiers_by_time = Arc::new(Mutex::new(HashMap::<u64, OffsetAntichain>::new()));
    let storage = create_metadata_storage(&test_storage_path, true);
    let mut tracker = SimplePersistencyManager::new(
        Box::new(storage),
        PersistenceManagerConfig::new(
            StreamStorageConfig::Filesystem(test_storage_path.clone()),
            0,
            1,
        ),
    );

    tracker.register_input_source(512, &StorageType::FileSystem, frontiers_by_time.clone());
    tracker.register_input_source(512, &StorageType::FileSystem, frontiers_by_time.clone());
}

#[test]
fn test_several_sinks_finalized_timestamp_calculation() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let test_storage_path = test_storage.into_path();

    let frontiers_by_time = Arc::new(Mutex::new(HashMap::<u64, OffsetAntichain>::new()));
    let storage = create_metadata_storage(&test_storage_path, true);
    let mut tracker = SimplePersistencyManager::new(
        Box::new(storage),
        PersistenceManagerConfig::new(
            StreamStorageConfig::Filesystem(test_storage_path.clone()),
            0,
            1,
        ),
    );

    tracker.register_input_source(512, &StorageType::FileSystem, frontiers_by_time.clone());

    let sink_id_1 = tracker.register_sink();
    let sink_id_2 = tracker.register_sink();

    tracker.accept_finalized_timestamp(sink_id_1, Some(5));
    assert_eq!(tracker.globally_finalized_timestamp(), 0);
    tracker.accept_finalized_timestamp(sink_id_1, Some(7));
    assert_eq!(tracker.globally_finalized_timestamp(), 0);
    tracker.accept_finalized_timestamp(sink_id_2, Some(4));
    assert_eq!(tracker.globally_finalized_timestamp(), 4);
    tracker.accept_finalized_timestamp(sink_id_2, Some(10));
    assert_eq!(tracker.globally_finalized_timestamp(), 7);
    tracker.accept_finalized_timestamp(sink_id_2, None);
    assert_eq!(tracker.globally_finalized_timestamp(), 7);

    /*
        No frontier updates at greater times, so the last finalized time
        should be equal to 7.
    */
    tracker.accept_finalized_timestamp(sink_id_1, None);
    assert_eq!(tracker.globally_finalized_timestamp(), 7);

    Ok(())
}
