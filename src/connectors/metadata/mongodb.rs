// Copyright © 2026 Pathway

use serde::Serialize;

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Default, Serialize)]
pub struct MongoDbMetadata {}

impl MongoDbMetadata {
    pub fn new() -> Self {
        Self::default()
    }
}
