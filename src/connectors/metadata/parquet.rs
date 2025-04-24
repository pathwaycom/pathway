// Copyright © 2025 Pathway

use serde::Serialize;

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Serialize)]
pub struct ParquetMetadata {
    path: Option<String>,
}

impl ParquetMetadata {
    pub fn new(path: Option<String>) -> Self {
        Self { path }
    }
}
