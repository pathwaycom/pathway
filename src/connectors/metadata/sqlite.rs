// Copyright © 2024 Pathway

use serde::Serialize;

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Serialize)]
pub struct SQLiteMetadata {
    data_version: i64,
}

impl SQLiteMetadata {
    pub fn new(data_version: i64) -> Self {
        Self { data_version }
    }
}
