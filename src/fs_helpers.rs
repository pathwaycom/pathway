// Copyright © 2024 Pathway

use std::io::{Error, ErrorKind};
use std::path::Path;

pub fn ensure_directory(fs_path: &Path) -> Result<(), Error> {
    if !fs_path.exists() {
        if let Err(e) = std::fs::create_dir_all(fs_path) {
            if e.kind() == ErrorKind::AlreadyExists {
                return Ok(());
            }
            return Err(e);
        }
    } else if !fs_path.is_dir() {
        // use ErrorKind::NotADirectory when it becomes stable
        return Err(Error::other("target object should be a directory"));
    }
    Ok(())
}
