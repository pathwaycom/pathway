// Copyright © 2024 Pathway

use tempfile::tempdir;

use pathway_engine::persistence::backends::{FilesystemKVStorage, PersistenceBackend};

#[test]
fn test_simple_kv_operations() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let test_storage_path = test_storage.path();

    let mut storage = FilesystemKVStorage::new(test_storage_path)?;
    assert_eq!(storage.list_keys()?, Vec::<String>::new());

    futures::executor::block_on(async { storage.put_value("1", b"one".to_vec()).await.unwrap() })
        .unwrap();
    assert_eq!(storage.list_keys()?, vec!["1"]);

    futures::executor::block_on(async { storage.put_value("2", b"two".to_vec()).await.unwrap() })
        .unwrap();
    assert_eq!(storage.list_keys()?, vec!["1", "2"]);

    assert_eq!(storage.get_value("1")?, b"one".to_vec());
    assert_eq!(storage.get_value("2")?, b"two".to_vec());

    futures::executor::block_on(async { storage.put_value("1", b"three".to_vec()).await.unwrap() })
        .unwrap();
    assert_eq!(storage.list_keys()?, vec!["1", "2"]);
    assert_eq!(storage.get_value("1")?, b"three".to_vec());

    Ok(())
}
