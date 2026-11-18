// Copyright © 2026 Pathway

use std::collections::HashMap;

use base64::engine::general_purpose;
use base64::Engine;
use serde::Serialize;

/// The metadata of one Pulsar message, exposed to the user as the
/// `_metadata` column when `with_metadata=True`.
#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Serialize)]
pub struct PulsarMetadata {
    /// The physical topic the message was read from, including the
    /// `-partition-N` suffix for the partitions of a partitioned topic.
    pub topic: String,
    /// The partition index, `-1` for a non-partitioned topic.
    pub partition: i32,
    /// The components of the message id: together with the partition they
    /// identify the message within the topic.
    pub ledger_id: u64,
    pub entry_id: u64,
    /// The index within the producer batch, `-1` for non-batched messages.
    pub batch_index: i32,
    /// The broker-assigned publish timestamp, in milliseconds since the UNIX
    /// epoch.
    pub publish_time_millis: u64,
    /// The producer-assigned event timestamp, if the producer set one.
    pub event_time_millis: Option<u64>,
    /// The name of the producer that published the message.
    pub producer_name: String,
    /// The ordering key of the message, base64-encoded (it is a byte
    /// sequence, like the Kafka header values).
    pub ordering_key: Option<String>,
    /// The registry version of the schema the message was produced under, or
    /// `None` for the messages produced without a schema. The version is the
    /// big-endian integer the broker stamps into the message metadata.
    pub schema_version: Option<u64>,
    /// The user-defined message properties.
    pub properties: HashMap<String, String>,
}

impl PulsarMetadata {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        topic: String,
        partition: i32,
        ledger_id: u64,
        entry_id: u64,
        batch_index: i32,
        publish_time_millis: u64,
        event_time_millis: Option<u64>,
        producer_name: String,
        ordering_key: Option<&[u8]>,
        schema_version: Option<u64>,
        properties: HashMap<String, String>,
    ) -> Self {
        Self {
            topic,
            partition,
            ledger_id,
            entry_id,
            batch_index,
            publish_time_millis,
            event_time_millis,
            producer_name,
            ordering_key: ordering_key.map(|key| general_purpose::STANDARD.encode(key)),
            schema_version,
            properties,
        }
    }

    /// The schema version in the wire form the registry lookups expect: the
    /// same 8 big-endian bytes the broker stamps into the message metadata.
    pub fn schema_id(&self) -> Option<Vec<u8>> {
        self.schema_version
            .map(|version| version.to_be_bytes().to_vec())
    }
}
