// Copyright © 2026 Pathway

use serde::Serialize;

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Serialize)]
pub struct MssqlMetadata {
    snapshot_version: u64,
}

impl MssqlMetadata {
    pub fn new(snapshot_version: u64) -> Self {
        Self { snapshot_version }
    }
}
