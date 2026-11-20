// Copyright © 2026 Pathway

use std::fs;

use pathway_engine::engine::license::read_license_to_string;
use tempfile::tempdir;

#[test]
fn test_license_file_success_is_cached() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("license.txt");
    let key = Some(format!("file://{}", path.display()));
    fs::write(&path, "first contents").unwrap();

    assert_eq!(
        read_license_to_string(key.clone()).unwrap(),
        "first contents"
    );
    fs::write(&path, "changed contents").unwrap();
    assert_eq!(read_license_to_string(key).unwrap(), "first contents");
}

#[test]
fn test_license_file_error_is_cached() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("missing-license.txt");
    let key = Some(format!("file://{}", path.display()));

    let first_error = read_license_to_string(key.clone()).unwrap_err().to_string();
    fs::write(&path, "now available").unwrap();
    assert_eq!(
        read_license_to_string(key).unwrap_err().to_string(),
        first_error
    );
}
