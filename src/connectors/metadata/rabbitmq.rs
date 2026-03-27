// Copyright © 2026 Pathway

use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct RabbitmqMetadata {
    pub offset: u64,
    pub stream_name: String,
    pub timestamp_millis: Option<i64>,
}

impl RabbitmqMetadata {
    pub fn new(offset: u64, stream_name: String, timestamp_millis: Option<i64>) -> Self {
        Self {
            offset,
            stream_name,
            timestamp_millis,
        }
    }
}
