// Copyright © 2026 Pathway

use std::borrow::Cow;
use std::collections::HashMap;
use std::mem::take;
use std::thread;
use std::time::{Duration, Instant};

use arcstr::ArcStr;
use log::{error, info, warn};

use crate::connectors::data_format::FormatterContext;
use crate::connectors::metadata::KafkaMetadata;
use crate::connectors::{OffsetKey, OffsetValue};
use crate::engine::Value;
use crate::persistence::frontier::OffsetAntichain;
use crate::timestamp::current_unix_timestamp_ms;

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
}

/// Errors raised while constructing a [`KafkaReader`] (i.e. at pipeline
/// start-up, before any data flows). Surfaced to the Python layer as a
/// start-up error by `construct_kafka_reader`.
#[derive(Debug, thiserror::Error)]
pub enum KafkaReaderError {
    #[error("Failed to fetch topic metadata: {0}")]
    MetadataFetch(KafkaError),

    #[error("Topic '{0}' not found")]
    TopicNotFound(String),

    #[error("Failed to fetch watermarks for ({topic}, {partition}): {source}")]
    WatermarksFetch {
        topic: String,
        partition: usize,
        source: KafkaError,
    },

    #[error("Failed to fetch offsets for the timestamp: {0}")]
    OffsetsForTimestamp(KafkaError),

    #[error("rdkafka returned an invalid offset, details: {0}")]
    InvalidOffset(String),

    #[error("Assigning Kafka partitions failed: {0}")]
    Assign(KafkaError),

    #[error("Subscription to Kafka topic failed: {0}")]
    Subscribe(KafkaError),
}

/// How long the start-up metadata probes keep retrying transient errors before
/// giving up.
const METADATA_PROBE_RETRY_TIMEOUT: Duration = Duration::from_secs(30);
const METADATA_PROBE_RETRY_BACKOFF: Duration = Duration::from_millis(200);

/// Metadata requests against a freshly (re)created topic can transiently fail
/// while the cluster is still electing partition leaders / propagating
/// metadata — most often `NotLeaderForPartition` or `LeaderNotAvailable`, and
/// `UnknownTopicOrPartition` if the create hasn't fully propagated to the
/// queried broker yet. These clear on their own within moments, so callers
/// retry them rather than failing the whole pipeline at start-up.
fn is_transient_metadata_error(err: &KafkaError) -> bool {
    matches!(
        err.rdkafka_error_code(),
        Some(
            RDKafkaErrorCode::NotLeaderForPartition
                | RDKafkaErrorCode::LeaderNotAvailable
                | RDKafkaErrorCode::UnknownTopicOrPartition
                | RDKafkaErrorCode::RequestTimedOut
        )
    )
}

/// Returns the total number of partitions for a Kafka topic.
fn total_partitions_for_topic(
    consumer: &BaseConsumer<DefaultConsumerContext>,
    topic: &str,
) -> Result<usize, KafkaReaderError> {
    let deadline = Instant::now() + METADATA_PROBE_RETRY_TIMEOUT;
    loop {
        match consumer.fetch_metadata(Some(topic), KafkaReader::default_timeout()) {
            Ok(metadata) => {
                if let Some(found) = metadata.topics().iter().find(|t| t.name() == topic) {
                    // A just-created topic can momentarily show up with no
                    // partitions; wait until they're visible.
                    if !found.partitions().is_empty() {
                        return Ok(found.partitions().len());
                    }
                }
                if Instant::now() >= deadline {
                    return Err(KafkaReaderError::TopicNotFound(topic.to_string()));
                }
            }
            Err(e) => {
                if !is_transient_metadata_error(&e) || Instant::now() >= deadline {
                    return Err(KafkaReaderError::MetadataFetch(e));
                }
            }
        }
        thread::sleep(METADATA_PROBE_RETRY_BACKOFF);
    }
}

/// Returns an array of partition watermarks.
/// Used to handle cases where a later call to `offsets_for_times`
/// might return `KafkaOffset::End` for some partitions, allowing for graceful handling.
/// Also used in static mode to identify the boundaries of the data chunk that needs to be read.
fn partition_watermarks(
    consumer: &BaseConsumer<DefaultConsumerContext>,
    topic: &str,
    total_partitions: usize,
) -> Result<Vec<RdkafkaWatermark>, KafkaReaderError> {
    let mut next_used_offset_per_partition = Vec::with_capacity(total_partitions);
    let deadline = Instant::now() + METADATA_PROBE_RETRY_TIMEOUT;
    for partition_idx in 0..total_partitions {
        let partition: i32 = partition_idx.try_into().unwrap();
        let (start_offset, next_offset) = loop {
            match consumer.fetch_watermarks(topic, partition, KafkaReader::default_timeout()) {
                Ok(watermarks) => break watermarks,
                Err(e) => {
                    if !is_transient_metadata_error(&e) || Instant::now() >= deadline {
                        return Err(KafkaReaderError::WatermarksFetch {
                            topic: topic.to_string(),
                            partition: partition_idx,
                            source: e,
                        });
                    }
                    thread::sleep(METADATA_PROBE_RETRY_BACKOFF);
                }
            }
        };
        next_used_offset_per_partition.push(RdkafkaWatermark::new(start_offset, next_offset));
    }
    Ok(next_used_offset_per_partition)
}

fn seek_positions_for_timestamp(
    consumer: &BaseConsumer<DefaultConsumerContext>,
    topic: &str,
    total_partitions: usize,
    start_from_timestamp_ms: i64,
    watermarks: &[RdkafkaWatermark],
) -> Result<HashMap<i32, KafkaOffset>, KafkaReaderError> {
    let mut seek_positions = HashMap::new();
    let mut tpl = TopicPartitionList::new();
    for partition_idx in 0..total_partitions {
        tpl.add_partition_offset(
            topic,
            partition_idx.try_into().unwrap(),
            KafkaOffset::Offset(start_from_timestamp_ms),
        )
        .expect("Failed to add partition offset");
    }

    let offsets = consumer
        .offsets_for_times(tpl, KafkaReader::default_timeout())
        .map_err(KafkaReaderError::OffsetsForTimestamp)?;

    // We could have done a simple `consumer.assign` here, but it would damage the automatic consumer rebalance
    // So we act differently: we pass the seek positions to consumer, and it seeks lazily
    for element in offsets.elements() {
        assert_eq!(element.topic(), topic);
        let offset = match element.offset() {
            KafkaOffset::Invalid => {
                return Err(KafkaReaderError::InvalidOffset(format!("{offsets:?}")));
            }
            KafkaOffset::End => {
                let partition_idx: usize = element.partition().try_into().unwrap();
                warn!("Partition {partition_idx} has no message with a timestamp >= {start_from_timestamp_ms}: the requested start is at or past the end of the partition (offset {}), so none of its already-written data will be read. In static mode this partition yields no rows; in streaming mode only messages produced after the start will be read.", watermarks[partition_idx].high);
                KafkaOffset::Offset(watermarks[partition_idx].high)
            }
            offset => offset,
        };
        info!(
            "Adding a lazy seek position for ({topic}, {}) to ({:?})",
            element.partition(),
            offset
        );
        seek_positions.insert(element.partition(), offset);
    }
    Ok(seek_positions)
}

pub struct KafkaReader {
    consumer: BaseConsumer<DefaultConsumerContext>,
    topic: ArcStr,
    positions_for_seek: HashMap<i32, KafkaOffset>,
    watermarks: Vec<RdkafkaWatermark>,
    deferred_read_result: Option<ReadResult>,
    mode: ConnectorMode,
    // Whether this reader was given any partitions in static mode (see
    // `KafkaReader::build`). False in streaming mode, and for a static reader
    // whose shard is empty (more workers than partitions) — such a reader has
    // nothing to read and finishes immediately. The actual assigned partitions
    // are read back from `consumer.position()` when needed, so only the "owns
    // something" bit is kept here.
    has_assigned_partitions: bool,
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
    fn new(
        consumer: BaseConsumer<DefaultConsumerContext>,
        topic: String,
        positions_for_seek: HashMap<i32, KafkaOffset>,
        watermarks: Vec<RdkafkaWatermark>,
        mode: ConnectorMode,
        has_assigned_partitions: bool,
    ) -> KafkaReader {
        KafkaReader {
            consumer,
            topic: topic.into(),
            positions_for_seek,
            watermarks,
            mode,
            has_assigned_partitions,
            deferred_read_result: None,
        }
    }

    /// Builds a reader from an already-created consumer: fetches the topic's
    /// partitions and watermarks, resolves any timestamp-based start position,
    /// and acquires this worker's partitions (assign for static, subscribe for
    /// streaming). In static mode the partitions are sharded by hand across the
    /// readers: `worker_index` is this worker's index and `reader_count` is how
    /// many workers actually run a reader (the caller computes it — see
    /// `construct_kafka_reader`).
    pub fn build(
        consumer: BaseConsumer<DefaultConsumerContext>,
        topic: String,
        mode: ConnectorMode,
        start_from_timestamp_ms: Option<i64>,
        worker_index: usize,
        reader_count: usize,
    ) -> Result<KafkaReader, KafkaReaderError> {
        let total_partitions = total_partitions_for_topic(&consumer, &topic)?;
        let mut watermarks = partition_watermarks(&consumer, &topic, total_partitions)?;

        let mut seek_positions = HashMap::new();
        if let Some(start_from_timestamp_ms) = start_from_timestamp_ms {
            let current_timestamp = current_unix_timestamp_ms();
            if start_from_timestamp_ms > current_timestamp.try_into().unwrap() {
                warn!("The timestamp {start_from_timestamp_ms} is greater than the current timestamp {current_timestamp}. All new entries will be read.");
            }
            seek_positions = seek_positions_for_timestamp(
                &consumer,
                &topic,
                total_partitions,
                start_from_timestamp_ms,
                &watermarks,
            )?;
            // The lazy seek only fires once the consumer actually receives a
            // message. For a seek target at (or past) the partition's high
            // watermark there's nothing to receive, so no commit ever happens
            // and `static_read_has_finished` would loop until the polling budget
            // runs out. Pre-advance the watermark's low bound to reflect the
            // seek: the range below the seek is logically already consumed, and
            // an empty resulting range marks the partition as "no messages" so
            // static mode exits promptly.
            for (&partition, offset) in &seek_positions {
                if let KafkaOffset::Offset(offset_value) = offset {
                    let partition_idx: usize = partition
                        .try_into()
                        .expect("kafka partition can't be negative");
                    if partition_idx < watermarks.len() {
                        let watermark = &mut watermarks[partition_idx];
                        watermark.low = watermark.low.max(*offset_value);
                    }
                }
            }
        }

        // Partitions are acquired differently depending on the mode:
        //   * Streaming subscribes to the topic, so the consumer group rebalances
        //     partitions across all workers and persists committed offsets for
        //     recovery.
        //   * Static performs a bounded, one-shot read. Going through the consumer
        //     group there is both unnecessary and racy: a fresh group must
        //     complete a JoinGroup/SyncGroup round-trip before the first fetch, and
        //     under load that assignment intermittently fails to deliver within the
        //     polling budget — the reader then finishes having read nothing. So we
        //     assign this worker's shard of the partitions explicitly and start at
        //     each partition's lower boundary. This talks straight to the partition
        //     leaders, with no coordinator dependency.
        let has_assigned_partitions = match mode {
            ConnectorMode::Static => {
                // Static reads don't use a consumer group to spread partitions
                // across workers, so we shard them by hand: reader `worker_index`
                // takes the partitions where `partition % reader_count ==
                // worker_index`. The active readers are exactly the workers with
                // index `0..reader_count`, so this modulo covers every partition
                // exactly once.
                let mut tpl = TopicPartitionList::new();
                for (partition_idx, watermark) in watermarks.iter().enumerate() {
                    if partition_idx % reader_count != worker_index {
                        continue;
                    }
                    let partition: i32 = partition_idx
                        .try_into()
                        .expect("kafka partition must fit 32-bit signed integer");
                    let start_offset = seek_positions
                        .get(&partition)
                        .copied()
                        .unwrap_or(KafkaOffset::Offset(watermark.low));
                    tpl.add_partition_offset(topic.as_str(), partition, start_offset)
                        .expect("adding a partition to the assignment list must not fail");
                }
                consumer.assign(&tpl).map_err(KafkaReaderError::Assign)?;
                // The explicit assignment above already starts each partition at
                // the right offset, so the lazy per-message seek used in
                // streaming mode is not needed.
                seek_positions.clear();
                tpl.count() > 0
            }
            ConnectorMode::Streaming => {
                consumer
                    .subscribe(&[topic.as_str()])
                    .map_err(KafkaReaderError::Subscribe)?;
                false
            }
        };

        Ok(KafkaReader::new(
            consumer,
            topic,
            seek_positions,
            watermarks,
            mode,
            has_assigned_partitions,
        ))
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
        // In static mode this reader owns a fixed shard of partitions (assigned
        // explicitly in `construct_kafka_reader`) and reads each from its lower
        // boundary up to the high watermark captured at construction time. The
        // read is complete once the consume position of every assigned partition
        // has reached that boundary. Positions advance as messages are returned
        // by `poll()`, so — unlike the previous consumer-group-committed-offset
        // approach — this does not depend on any group coordinator round-trip.
        if !self.has_assigned_partitions {
            return Ok(true);
        }
        let positions = self.consumer.position()?;
        for element in positions.elements() {
            let partition: usize = element
                .partition()
                .try_into()
                .expect("kafka partition can't be negative");
            match element.offset() {
                KafkaOffset::Offset(offset) => {
                    if offset < self.watermarks[partition].high {
                        // Not all messages up to the captured boundary have been
                        // consumed from this partition yet.
                        return Ok(false);
                    }
                }
                KafkaOffset::End => {
                    // The position is past the end, hence past the boundary too.
                }
                _ => {
                    // The position is not established yet (no fetch has completed
                    // for this partition). If the partition still holds messages
                    // within the boundary, the read is not finished.
                    if self.watermarks[partition].has_messages() {
                        return Ok(false);
                    }
                }
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
