// Copyright © 2025 Pathway

use serde::Serialize;

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Serialize)]
pub struct IcebergMetadata {
    snapshot_id: i64,
}

impl IcebergMetadata {
    pub fn new(snapshot_id: i64) -> Self {
        Self { snapshot_id }
    }
}
