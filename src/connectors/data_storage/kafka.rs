// Copyright © 2026 Pathway

use std::borrow::Cow;
use std::collections::HashMap;
use std::mem::take;
use std::time::Duration;

use arcstr::ArcStr;
use log::{error, info, warn};

use crate::connectors::data_format::FormatterContext;
use crate::connectors::metadata::KafkaMetadata;
use crate::connectors::{OffsetKey, OffsetValue};
use crate::engine::Value;
use crate::persistence::frontier::OffsetAntichain;

use super::{
    ConnectorMode, MessageQueueTopic, ReadError, ReadResult, Reader, ReaderContext, StorageType,
    WriteError, Writer,
};
use rdkafka::consumer::{BaseConsumer, Consumer, DefaultConsumerContext};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::BorrowedMessage;
use rdkafka::producer::{BaseRecord, DefaultProducerContext, Producer, ThreadedProducer};
use rdkafka::topic_partition_list::Offset as KafkaOffset;
use rdkafka::Message;
use rdkafka::TopicPartitionList;

pub struct RdkafkaWatermark {
    pub low: i64,
    pub high: i64,
}

impl RdkafkaWatermark {
    pub fn new(low: i64, high: i64) -> Self {
        Self { low, high }
    }

    /// Checks if the offset is contained within a watermark.
    /// The offset can't be lower than the lower bound of the
    /// watermark, but it can pass beyond the upper bound, if
    /// new data were written after the pipeline start.
    pub fn contains_offset(&self, offset: i64) -> bool {
        offset < self.high
    }

    /// The given watermark has messages if the next used offset is
    /// greater than the beginning of the observed interval.
    /// The beginning of an empty interval can still be non-zero
    /// if topic compaction took place.
    pub fn has_messages(&self) -> bool {
        self.low < self.high
    }

    /// The `self.high` is the offset of the next message that will
    /// be written into partition, therefore the partition has more
    /// messages if the provided offset doesn't correspond to the last
    /// message in the watermark.
    pub fn has_messages_after_offset(&self, offset: i64) -> bool {
        offset < self.high - 1
    }
}

pub struct KafkaReader {
    consumer: BaseConsumer<DefaultConsumerContext>,
    topic: ArcStr,
    positions_for_seek: HashMap<i32, KafkaOffset>,
    watermarks: Vec<RdkafkaWatermark>,
    deferred_read_result: Option<ReadResult>,
    mode: ConnectorMode,
}

impl Reader for KafkaReader {
    fn read(&mut self) -> Result<ReadResult, ReadError> {
        if let Some(deferred_read_result) = take(&mut self.deferred_read_result) {
            return Ok(deferred_read_result);
        }

        loop {
            let kafka_message = match self.mode {
                ConnectorMode::Streaming => self
                    .consumer
                    .poll(None)
                    .expect("poll in streaming mode should never timeout")?,
                ConnectorMode::Static => {
                    if let Some(kafka_message) = self.next_message_in_static_mode()? {
                        kafka_message
                    } else {
                        return Ok(ReadResult::Finished);
                    }
                }
            };
            let message_key = kafka_message.key().map(<[u8]>::to_vec);
            let message_payload = kafka_message.payload().map(<[u8]>::to_vec);

            if let Some(lazy_seek_offset) = self.positions_for_seek.get(&kafka_message.partition())
            {
                info!(
                    "Performing Kafka topic seek for ({}, {}) to {:?}",
                    kafka_message.topic(),
                    kafka_message.partition(),
                    lazy_seek_offset
                );
                // If there is a need for seek, perform it and remove the seek requirement.
                if let Err(e) = self.consumer.seek(
                    kafka_message.topic(),
                    kafka_message.partition(),
                    *lazy_seek_offset,
                    None,
                ) {
                    error!(
                        "Failed to seek topic and partition ({}, {}) to offset {:?}: {e}",
                        kafka_message.topic(),
                        kafka_message.partition(),
                        lazy_seek_offset,
                    );
                } else {
                    self.positions_for_seek.remove(&kafka_message.partition());
                }
                continue;
            }

            let offset = {
                let offset_key = OffsetKey::Kafka(self.topic.clone(), kafka_message.partition());
                let offset_value = OffsetValue::KafkaOffset(kafka_message.offset());
                (offset_key, offset_value)
            };
            let metadata = KafkaMetadata::from_rdkafka_message(&kafka_message);
            let message = ReaderContext::from_key_value(message_key, message_payload);
            self.deferred_read_result = Some(ReadResult::Data(message, offset));

            return Ok(ReadResult::NewSource(metadata.into()));
        }
    }

    fn seek(&mut self, frontier: &OffsetAntichain) -> Result<(), ReadError> {
        // "Lazy" seek implementation
        for (offset_key, offset_value) in frontier {
            let OffsetValue::KafkaOffset(position) = offset_value else {
                warn!("Unexpected type of offset in Kafka frontier: {offset_value:?}");
                continue;
            };
            if let OffsetKey::Kafka(topic, partition) = offset_key {
                if self.topic != *topic {
                    warn!(
                        "Unexpected topic name. Expected: {}, Got: {topic}",
                        self.topic
                    );
                    continue;
                }

                /*
                    Note: we can't do seek straight away, because it works only for
                    assigned partitions.

                    We also don't do any kind of assignment here, because it needs
                    to be done on behalf of rdkafka client, taking account of other
                    members in its' consumer group.
                */
                self.positions_for_seek
                    .insert(*partition, KafkaOffset::Offset(*position + 1));
            } else {
                error!("Unexpected offset in Kafka frontier: ({offset_key:?}, {offset_value:?})");
            }
        }

        Ok(())
    }

    fn short_description(&self) -> Cow<'static, str> {
        format!("Kafka({})", self.topic).into()
    }

    fn storage_type(&self) -> StorageType {
        StorageType::Kafka
    }

    fn max_allowed_consecutive_errors(&self) -> usize {
        32
    }
}

impl KafkaReader {
    pub fn new(
        consumer: BaseConsumer<DefaultConsumerContext>,
        topic: String,
        positions_for_seek: HashMap<i32, KafkaOffset>,
        watermarks: Vec<RdkafkaWatermark>,
        mode: ConnectorMode,
    ) -> KafkaReader {
        KafkaReader {
            consumer,
            topic: topic.into(),
            positions_for_seek,
            watermarks,
            mode,
            deferred_read_result: None,
        }
    }

    fn poll_duration_for_static_mode() -> Duration {
        Duration::from_millis(500)
    }

    /// Default timeout for different broker metadata requests
    pub fn default_timeout() -> Duration {
        Duration::from_secs(30)
    }

    fn polling_attempts_count_for_static_mode() -> usize {
        60
    }

    fn message_matches_static_read_constraints(&self, message: &BorrowedMessage<'_>) -> bool {
        let partition: usize = message
            .partition()
            .try_into()
            .expect("kafka partition can't be negative");
        if partition >= self.watermarks.len() {
            // New partitions have been added after the boundaries for the
            // chunk to be read have been computed. In this case, the message
            // must be skipped.
            return false;
        }
        self.watermarks[partition].contains_offset(message.offset())
    }

    fn static_read_has_finished(&self) -> Result<bool, ReadError> {
        let total_partitions = self.watermarks.len();
        let mut tpl = TopicPartitionList::with_capacity(total_partitions);
        for partition_idx in 0..total_partitions {
            tpl.add_partition(
                self.topic.as_str(),
                partition_idx
                    .try_into()
                    .expect("kafka partition must fit 32-bit signed integer"),
            );
        }
        let committed_offsets = self
            .consumer
            .committed_offsets(tpl, Self::default_timeout())?;
        for committed_offset in committed_offsets.elements() {
            let partition: usize = committed_offset
                .partition()
                .try_into()
                .expect("kafka partition can't be negative");
            let offset = match committed_offset.offset() {
                KafkaOffset::End => {
                    // The exact offset is not reported, but it's greater than the
                    // threshold.
                    continue;
                }
                KafkaOffset::Invalid => {
                    if self.watermarks[partition].has_messages() {
                        return Ok(false);
                    }
                    // It is OK to have unassigned offsets for empty partitions.
                    continue;
                }
                KafkaOffset::Offset(offset) => offset,
                _ => {
                    // There is no way to compare this offset to the desired border.
                    return Ok(false);
                }
            };
            if self.watermarks[partition].has_messages_after_offset(offset) {
                // The committed offset is still smaller than the last offset to be read
                // from this partition.
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn next_message_in_static_mode(&self) -> Result<Option<BorrowedMessage<'_>>, ReadError> {
        let mut result_message = None;
        let mut is_finished = false;
        let n_attempts = Self::polling_attempts_count_for_static_mode();
        for _ in 0..n_attempts {
            let maybe_kafka_message = self.consumer.poll(Self::poll_duration_for_static_mode());
            if let Some(maybe_matching_message) = maybe_kafka_message {
                let maybe_matching_message = maybe_matching_message?;
                if self.message_matches_static_read_constraints(&maybe_matching_message) {
                    result_message = Some(maybe_matching_message);
                    break;
                }

                // The message goes beyond the specified border within the partition or belongs
                // a partition that must not be read at all.
                // Stop reading the further messages from this partition, since they will
                // have greater offsets.
                let mut tpl = TopicPartitionList::with_capacity(1);
                tpl.add_partition(self.topic.as_str(), maybe_matching_message.partition());
                self.consumer.pause(&tpl)?;
            }

            if self.static_read_has_finished()? {
                is_finished = true;
                break;
            }
        }
        if !is_finished && result_message.is_none() {
            warn!("There was no explicit finish detected from Kafka topic '{}', but no matching events were read after {n_attempts} attempts, with {:?} duration each.", self.topic, Self::poll_duration_for_static_mode());
        }
        Ok(result_message)
    }
}

pub struct KafkaWriter {
    producer: ThreadedProducer<DefaultProducerContext>,
    topic: MessageQueueTopic,
    header_fields: Vec<(String, usize)>,
    key_field_index: Option<usize>,
}

impl KafkaWriter {
    pub fn new(
        producer: ThreadedProducer<DefaultProducerContext>,
        topic: MessageQueueTopic,
        header_fields: Vec<(String, usize)>,
        key_field_index: Option<usize>,
    ) -> KafkaWriter {
        KafkaWriter {
            producer,
            topic,
            header_fields,
            key_field_index,
        }
    }
}

impl Drop for KafkaWriter {
    fn drop(&mut self) {
        self.producer.flush(None).expect("kafka commit should work");
    }
}

impl Writer for KafkaWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        let key_as_bytes = match self.key_field_index {
            Some(index) => match &data.values[index] {
                Value::Bytes(bytes) => bytes.to_vec(),
                Value::String(string) => string.as_bytes().to_vec(),
                _ => {
                    return Err(WriteError::IncorrectKeyFieldType(
                        data.values[index].clone(),
                    ))
                }
            },
            None => data.key.0.to_le_bytes().to_vec(),
        };

        let headers = data.construct_kafka_headers(&self.header_fields);
        for payload in data.payloads {
            let payload = payload.into_raw_bytes()?;
            let effective_topic = self.topic.get_for_posting(&data.values)?;
            let mut entry = BaseRecord::<Vec<u8>, Vec<u8>>::to(&effective_topic)
                .payload(&payload)
                .headers(headers.clone())
                .key(&key_as_bytes);
            loop {
                match self.producer.send(entry) {
                    Ok(()) => break,
                    Err((
                        KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull),
                        unsent_entry,
                    )) => {
                        self.producer.poll(Duration::from_millis(10));
                        entry = unsent_entry;
                    }
                    Err((e, _unsent_entry)) => return Err(WriteError::Kafka(e)),
                }
            }
        }
        Ok(())
    }

    fn name(&self) -> String {
        format!("Kafka({})", self.topic)
    }

    fn retriable(&self) -> bool {
        true
    }

    fn single_threaded(&self) -> bool {
        false
    }
}
