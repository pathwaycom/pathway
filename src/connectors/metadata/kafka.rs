// Copyright © 2024 Pathway

use rdkafka::message::{BorrowedMessage as KafkaMessage, Message};
use serde::Serialize;

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Serialize)]
pub struct KafkaMetadata {
    timestamp_millis: Option<i64>,
    topic: String,
    partition: i32,
    offset: i64,
}

impl KafkaMetadata {
    // TODO: Note that if row deletions take place, one needs to ensure
    // that the deletion uses the same metadata entry as the one used
    // during the row insertion.
    pub fn from_rdkafka_message(message: &KafkaMessage) -> Self {
        Self {
            timestamp_millis: message.timestamp().to_millis(),
            topic: message.topic().to_string(),
            partition: message.partition(),
            offset: message.offset(),
        }
    }
}
