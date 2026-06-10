// Copyright © 2026 Pathway

use serde::Serialize;

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Serialize)]
pub struct PollingMetadata {
    poll_sequence: u64,
}

impl PollingMetadata {
    pub fn new(poll_sequence: u64) -> Self {
        Self { poll_sequence }
    }
}
