// Copyright © 2024 Pathway

use std::path::Path;

use rand::Rng;
use tempfile::{tempdir, tempfile};

use pathway_engine::connectors::metadata::FileLikeMetadata;
use pathway_engine::persistence::backends::FilesystemKVStorage;
use pathway_engine::persistence::cached_object_storage::CachedObjectStorage;

fn create_mock_document() -> Vec<u8> {
    let id: u128 = rand::rng().random();
    id.to_le_bytes().to_vec()
}

fn create_mock_storage_metadata() -> FileLikeMetadata {
    let random_file_id: u128 = rand::rng().random();
    let tempfile = tempfile().unwrap();
    let metadata = tempfile.metadata().unwrap();
    FileLikeMetadata::from_fs_meta(
        Path::new(&format!("/tmp/tempfile/{random_file_id}")),
        &metadata,
    )
}

fn check_storage_has_object(
    storage: &CachedObjectStorage,
    uri: &[u8],
    contents: &[u8],
    metadata: &FileLikeMetadata,
) -> eyre::Result<()> {
    assert!(storage.contains_object(uri));
    assert_eq!(storage.get_object(uri)?, contents);
    assert_eq!(
        storage
            .stored_metadata(uri)
            .expect("Metadata must be present"),
        metadata
    );
    let mut is_uri_found = false;
    for (stored_uri, stored_metadata) in storage.get_iter() {
        if uri == stored_uri {
            assert!(stored_metadata == metadata);
            is_uri_found = true;
        }
    }
    assert!(is_uri_found);
    Ok(())
}

fn check_storage_doesnt_have_object(storage: &CachedObjectStorage, uri: &[u8]) -> eyre::Result<()> {
    assert!(!storage.contains_object(uri));
    assert!(storage.get_object(uri).is_err());
    assert!(storage.stored_metadata(uri).is_none());
    for (stored_uri, _) in storage.get_iter() {
        assert!(uri != stored_uri);
    }
    Ok(())
}

#[test]
fn test_place_access() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let test_storage_path = test_storage.path();
    let backend = FilesystemKVStorage::new(test_storage_path)?;
    let mut storage = CachedObjectStorage::new(Box::new(backend))?;

    let document = create_mock_document();
    let metadata = create_mock_storage_metadata();
    storage.place_object(b"a", &document, metadata.clone())?;
    check_storage_has_object(&storage, b"a", &document, &metadata)?;

    Ok(())
}

#[test]
fn test_place_delete_access() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let test_storage_path = test_storage.path();
    let backend = FilesystemKVStorage::new(test_storage_path)?;
    let mut storage = CachedObjectStorage::new(Box::new(backend))?;

    let document = create_mock_document();
    let metadata = create_mock_storage_metadata();
    storage.place_object(b"a", &document, metadata.clone())?;
    storage.remove_object(b"a")?;
    check_storage_doesnt_have_object(&storage, b"a")?;

    Ok(())
}

#[test]
fn test_place_delete_rewind_access() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let test_storage_path = test_storage.path();
    let backend = FilesystemKVStorage::new(test_storage_path)?;
    let mut storage = CachedObjectStorage::new(Box::new(backend))?;

    let document = create_mock_document();
    let metadata = create_mock_storage_metadata();
    storage.place_object(b"a", &document, metadata.clone())?;
    let rewind_version = storage.actual_version();
    storage.remove_object(b"a")?;
    storage
        .get_external_accessor()
        .lock()
        .unwrap()
        .start_forced_state_upload()?;
    storage
        .get_external_accessor()
        .lock()
        .unwrap()
        .wait_for_all_uploads()?;

    let backend = FilesystemKVStorage::new(test_storage_path)?;
    let mut storage = CachedObjectStorage::new(Box::new(backend))?;
    storage.start_from_stable_version(rewind_version)?;
    check_storage_has_object(&storage, b"a", &document, &metadata)?;

    Ok(())
}

#[test]
fn test_access_latest_version_rewind_clear() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let test_storage_path = test_storage.path();
    let backend = FilesystemKVStorage::new(test_storage_path)?;
    let mut storage = CachedObjectStorage::new(Box::new(backend))?;

    let document_v1 = create_mock_document();
    let metadata_v1 = create_mock_storage_metadata();
    storage.place_object(b"a", &document_v1, metadata_v1.clone())?;
    let rewind_version = storage.actual_version();

    let document_v2 = create_mock_document();
    let metadata_v2 = create_mock_storage_metadata();
    storage.place_object(b"a", &document_v2, metadata_v2.clone())?;
    storage
        .get_external_accessor()
        .lock()
        .unwrap()
        .start_forced_state_upload()?;
    storage
        .get_external_accessor()
        .lock()
        .unwrap()
        .wait_for_all_uploads()?;

    let document_v3 = create_mock_document();
    let metadata_v3 = create_mock_storage_metadata();
    storage.place_object(b"a", &document_v3, metadata_v3.clone())?;
    check_storage_has_object(&storage, b"a", &document_v3, &metadata_v3)?;
    storage
        .get_external_accessor()
        .lock()
        .unwrap()
        .start_forced_state_upload()?;
    storage
        .get_external_accessor()
        .lock()
        .unwrap()
        .wait_for_all_uploads()?;

    let backend = FilesystemKVStorage::new(test_storage_path)?;
    let mut storage = CachedObjectStorage::new(Box::new(backend))?;
    storage.start_from_stable_version(rewind_version)?;
    check_storage_has_object(&storage, b"a", &document_v1, &metadata_v1)?;

    let backend = FilesystemKVStorage::new(test_storage_path)?;
    let mut storage = CachedObjectStorage::new(Box::new(backend))?;
    storage.clear()?;
    check_storage_doesnt_have_object(&storage, b"a")?;

    Ok(())
}

#[test]
fn test_add_version_after_rewind() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let test_storage_path = test_storage.path();
    let backend = FilesystemKVStorage::new(test_storage_path)?;
    let mut storage = CachedObjectStorage::new(Box::new(backend))?;

    let document_v1 = create_mock_document();
    let metadata_v1 = create_mock_storage_metadata();
    storage.place_object(b"a", &document_v1, metadata_v1.clone())?;
    let rewind_version = storage.actual_version();

    let document_v2 = create_mock_document();
    let metadata_v2 = create_mock_storage_metadata();
    storage.place_object(b"a", &document_v2, metadata_v2.clone())?;
    storage
        .get_external_accessor()
        .lock()
        .unwrap()
        .start_forced_state_upload()?;
    storage
        .get_external_accessor()
        .lock()
        .unwrap()
        .wait_for_all_uploads()?;

    let backend = FilesystemKVStorage::new(test_storage_path)?;
    let mut storage = CachedObjectStorage::new(Box::new(backend))?;
    storage.start_from_stable_version(rewind_version)?;
    check_storage_has_object(&storage, b"a", &document_v1, &metadata_v1)?;
    assert_eq!(storage.actual_version(), rewind_version);

    let document_v3 = create_mock_document();
    let metadata_v3 = create_mock_storage_metadata();
    storage.place_object(b"a", &document_v3, metadata_v3.clone())?;
    check_storage_has_object(&storage, b"a", &document_v3, &metadata_v3)?;

    Ok(())
}

#[test]
fn test_rewind_to_removal_then_update() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let test_storage_path = test_storage.path();
    let backend = FilesystemKVStorage::new(test_storage_path)?;
    let mut storage = CachedObjectStorage::new(Box::new(backend))?;

    let document_v1 = create_mock_document();
    let metadata_v1 = create_mock_storage_metadata();
    storage.place_object(b"a", &document_v1, metadata_v1.clone())?;

    storage.remove_object(b"a")?;
    let rewind_version = storage.actual_version();

    let document_v2 = create_mock_document();
    let metadata_v2 = create_mock_storage_metadata();
    storage.place_object(b"a", &document_v2, metadata_v2.clone())?;
    check_storage_has_object(&storage, b"a", &document_v2, &metadata_v2)?;
    storage
        .get_external_accessor()
        .lock()
        .unwrap()
        .start_forced_state_upload()?;
    storage
        .get_external_accessor()
        .lock()
        .unwrap()
        .wait_for_all_uploads()?;

    let backend = FilesystemKVStorage::new(test_storage_path)?;
    let mut storage = CachedObjectStorage::new(Box::new(backend))?;
    storage.start_from_stable_version(rewind_version)?;
    check_storage_doesnt_have_object(&storage, b"a")?;

    let document_v3 = create_mock_document();
    let metadata_v3 = create_mock_storage_metadata();
    storage.place_object(b"a", &document_v3, metadata_v3.clone())?;
    check_storage_has_object(&storage, b"a", &document_v3, &metadata_v3)?;

    Ok(())
}
