// Copyright © 2026 Pathway

use base64::engine::general_purpose;
use base64::Engine;
use rdkafka::message::{BorrowedMessage as KafkaMessage, Headers, Message};
use serde::Serialize;

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Serialize)]
pub struct KafkaMetadata {
    timestamp_millis: Option<i64>,
    topic: String,
    partition: i32,
    offset: i64,
    headers: Vec<(String, Option<String>)>,
}

impl KafkaMetadata {
    // TODO: Note that if row deletions take place, one needs to ensure
    // that the deletion uses the same metadata entry as the one used
    // during the row insertion.
    pub fn from_rdkafka_message(message: &KafkaMessage) -> Self {
        let headers = if let Some(message_headers) = message.headers() {
            let mut headers = Vec::with_capacity(message_headers.count());
            for header in message_headers.iter() {
                headers.push((
                    header.key.to_string(),
                    header.value.map(|v| general_purpose::STANDARD.encode(v)),
                ));
            }
            headers
        } else {
            Vec::with_capacity(0)
        };

        Self {
            timestamp_millis: message.timestamp().to_millis(),
            topic: message.topic().to_string(),
            partition: message.partition(),
            offset: message.offset(),
            headers,
        }
    }
}
