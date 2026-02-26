// Copyright © 2026 Pathway

use serde::Serialize;

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Serialize)]
pub struct PostgresMetadata {
    txn_id: Option<u32>,
}

impl PostgresMetadata {
    pub fn new(txn_id: Option<u32>) -> Self {
        Self { txn_id }
    }
}
