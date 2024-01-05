// Copyright © 2024 Pathway

use std::time::{SystemTime, UNIX_EPOCH};

pub fn current_unix_timestamp_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Failed to get the current timestamp")
        .as_millis()
}

pub fn current_unix_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Failed to get the current timestamp")
        .as_secs()
}
