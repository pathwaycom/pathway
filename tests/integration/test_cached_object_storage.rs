// Copyright © 2026 Pathway

use std::path::Path;

use rand::Rng;
use tempfile::{tempdir, tempfile};

use pathway_engine::connectors::metadata::FileLikeMetadata;
use pathway_engine::persistence::backends::{FilesystemKVStorage, PersistenceBackend};
use pathway_engine::persistence::cached_object_storage::{
    CachedObjectStorage, CachedObjectsExternalAccessor, METADATA_EXTENSION,
};

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
    let (stored_contents, stored_metadata) = storage.get_object_with_metadata(uri)?;
    assert_eq!(stored_contents, contents);
    assert_eq!(stored_metadata, *metadata);
    let stored_tag = storage.stored_tag(uri).expect("Tag must be present");
    assert!(!storage.is_changed(stored_tag, metadata));
    let mut is_uri_found = false;
    for (stored_uri, _) in storage.get_iter() {
        if uri == stored_uri.as_ref() {
            is_uri_found = true;
        }
    }
    assert!(is_uri_found);
    Ok(())
}

fn check_storage_doesnt_have_object(storage: &CachedObjectStorage, uri: &[u8]) -> eyre::Result<()> {
    assert!(!storage.contains_object(uri));
    assert!(storage.get_object_with_metadata(uri).is_err());
    assert!(storage.stored_tag(uri).is_none());
    for (stored_uri, _) in storage.get_iter() {
        assert!(uri != stored_uri.as_ref());
    }
    Ok(())
}

#[test]
fn test_tag_change_detection_semantics() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let backend = FilesystemKVStorage::new(test_storage.path())?;
    let mut storage = CachedObjectStorage::new(Box::new(backend))?;

    let document = create_mock_document();
    let metadata = create_mock_storage_metadata();
    storage.place_object(b"a", &document, metadata.clone())?;

    let tag = *storage.stored_tag(b"a").expect("Tag must be present");
    assert!(!storage.is_changed(&tag, &metadata));

    let mut size_changed = metadata.clone();
    size_changed.size += 1;
    assert!(storage.is_changed(&tag, &size_changed));

    // Avoid relying on timestamp granularity alone.
    let mut definitely_changed = metadata.clone();
    definitely_changed.size += 1;
    definitely_changed.modified_at = metadata.modified_at.map(|t| t + 1);
    assert!(storage.is_changed(&tag, &definitely_changed));

    // The path is the map key and takes no part in the comparison,
    // exactly as in `FileLikeMetadata::is_changed`.
    let mut path_changed = metadata.clone();
    path_changed.path = "/some/other/path".to_string();
    assert!(!storage.is_changed(&tag, &path_changed));

    Ok(())
}

// The durable snapshot format (metadata event batches + blob files) must stay
// readable across versions: the in-RAM `ScannerTag` snapshot and the ephemeral
// by-uri SQLite store are rebuilt from these batches on every start. The
// fixture was generated at commit e2a12c25f6 (before the `ScannerTag`
// introduction) by the following sequence, with `FileLikeMetadata` taken from
// a tempfile via `from_fs_meta`:
//   place_object(b"/data/fixture/a.txt", b"contents of a v1", ...)  // version 1
//   place_object(b"/data/fixture/b.txt", b"contents of b", ...)     // version 2
//   place_object(b"/data/fixture/a.txt", b"contents of a v2", ...)  // version 3
//   place_object(b"/data/fixture/c.txt", b"contents of c", ...)     // version 4
//   remove_object(b"/data/fixture/c.txt")                           // version 5
// followed by a forced state upload.
#[test]
fn test_reads_snapshot_written_by_older_version() -> eyre::Result<()> {
    // Recovery may repack or delete batches, so operate on a copy of the fixture.
    let fixture_path = Path::new("tests/data/old_format_cached_objects_snapshot");
    let test_storage = tempdir()?;
    for entry in std::fs::read_dir(fixture_path)? {
        let entry = entry?;
        std::fs::copy(entry.path(), test_storage.path().join(entry.file_name()))?;
    }

    let backend = FilesystemKVStorage::new(test_storage.path())?;
    let mut storage = CachedObjectStorage::new(Box::new(backend))?;
    storage.start_from_stable_version(5)?;

    let (contents, metadata) = storage.get_object_with_metadata(b"/data/fixture/a.txt")?;
    assert_eq!(contents, b"contents of a v2");
    assert_eq!(metadata.path, "/data/fixture/a.txt");
    check_storage_has_object(
        &storage,
        b"/data/fixture/a.txt",
        b"contents of a v2",
        &metadata,
    )?;

    let (contents, metadata) = storage.get_object_with_metadata(b"/data/fixture/b.txt")?;
    assert_eq!(contents, b"contents of b");
    assert_eq!(metadata.path, "/data/fixture/b.txt");
    check_storage_has_object(
        &storage,
        b"/data/fixture/b.txt",
        b"contents of b",
        &metadata,
    )?;

    check_storage_doesnt_have_object(&storage, b"/data/fixture/c.txt")?;

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
fn test_large_object_upload_does_not_wait_for_commit() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let test_storage_path = test_storage.path();
    let backend = FilesystemKVStorage::new(test_storage_path)?;
    let mut storage = CachedObjectStorage::new(Box::new(backend))?;

    // The document is larger than the eager upload threshold, so its upload
    // must be started by the placement itself. Otherwise the first commit
    // after the ingestion would have to transfer the whole blob, blocking
    // the advancement of the persistent frontier for the upload duration.
    let document = vec![b'x'; 20_000_000];
    let metadata = create_mock_storage_metadata();
    storage.place_object(b"a", &document, metadata.clone())?;
    let stable_version = storage.actual_version();

    // Note: no forced state upload here. We only await the uploads that the
    // placement has already started.
    storage
        .get_external_accessor()
        .lock()
        .unwrap()
        .wait_for_all_uploads()?;

    let backend = FilesystemKVStorage::new(test_storage_path)?;
    let mut storage = CachedObjectStorage::new(Box::new(backend))?;
    storage.start_from_stable_version(stable_version)?;
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

#[test]
fn test_compression() -> eyre::Result<()> {
    let test_storage = tempdir()?;
    let test_storage_path = test_storage.path();
    let metadata_v1 = create_mock_storage_metadata();
    let metadata_v2 = create_mock_storage_metadata();

    // Run 1
    let rewind_version = {
        let backend = FilesystemKVStorage::new(test_storage_path)?;
        let mut storage = CachedObjectStorage::new(Box::new(backend))?;
        let document_v1 = b"a";
        storage.place_object(b"a", document_v1, metadata_v1.clone())?;

        let document_v2 = b"aaaa";
        storage.place_object(b"b", document_v2, metadata_v2.clone())?;
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
        storage.actual_version()
    };

    // Run 2
    let rewind_version_2 = {
        let backend = FilesystemKVStorage::new(test_storage_path)?;
        let mut storage = CachedObjectStorage::new(Box::new(backend))?;
        storage.start_from_stable_version(rewind_version)?;

        storage.remove_object(b"b")?;
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

        storage.actual_version()
    };

    // Run 3. We expect compression to delete the first batch and create a smaller one.
    {
        let backend = FilesystemKVStorage::new(test_storage_path)?;
        let mut storage = CachedObjectStorage::new(Box::new(backend))?;
        storage.start_from_stable_version(rewind_version_2)?;
    };

    {
        let backend = FilesystemKVStorage::new(test_storage_path)?;
        let keys = backend.list_keys()?;
        let mut total_blobs_length = 0;
        for key in keys {
            if !key.ends_with(METADATA_EXTENSION) {
                continue;
            }
            let number_part = key.split('.').next().unwrap();
            let batch_id: u64 = number_part.parse().unwrap();
            let blobs =
                CachedObjectsExternalAccessor::download_blobs_with_backend(&backend, batch_id)?;
            total_blobs_length += blobs.len();
        }
        assert_eq!(total_blobs_length, 1);
    }

    // Run 4. Check that the objects state is correct.
    {
        let backend = FilesystemKVStorage::new(test_storage_path)?;
        let mut storage = CachedObjectStorage::new(Box::new(backend))?;
        storage.start_from_stable_version(rewind_version_2)?;
        check_storage_has_object(&storage, b"a", b"a", &metadata_v1)?;
        check_storage_doesnt_have_object(&storage, b"b")?;
    };

    Ok(())
}
