// Copyright © 2026 Pathway

use std::collections::HashMap;

use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct RabbitmqMetadata {
    pub offset: u64,
    pub stream_name: String,
    pub message_id: Option<String>,
    pub correlation_id: Option<String>,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub subject: Option<String>,
    pub reply_to: Option<String>,
    pub priority: Option<u8>,
    pub durable: Option<bool>,
    /// AMQP 1.0 application properties as string key-value pairs.
    pub application_properties: Option<HashMap<String, String>>,
}
