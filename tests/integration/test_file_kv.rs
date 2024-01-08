// Copyright © 2024 Pathway

use tempfile::tempdir;

use pathway_engine::persistence::metadata_backends::file::FilesystemKVStorage;
use pathway_engine::persistence::metadata_backends::MetadataBackend;

#[test]
fn test_simple_kv_operations() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let test_storage_path = test_storage.path();

    let mut storage = FilesystemKVStorage::new(test_storage_path)?;
    assert_eq!(storage.list_keys()?, Vec::<String>::new());

    storage.put_value("1", "one")?;
    assert_eq!(storage.list_keys()?, vec!["1"]);

    storage.put_value("2", "two")?;
    assert_eq!(storage.list_keys()?, vec!["1", "2"]);

    assert_eq!(storage.get_value("1")?, "one");
    assert_eq!(storage.get_value("2")?, "two");

    storage.put_value("1", "three")?;
    assert_eq!(storage.list_keys()?, vec!["1", "2"]);
    assert_eq!(storage.get_value("1")?, "three");

    Ok(())
}
