// Copyright © 2026 Pathway

use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::ffi::{c_int, c_void, CStr, CString};
use std::mem::take;
use std::ptr;
use std::slice;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use arcstr::ArcStr;
use log::{error, info, warn};

use crate::connectors::data_format::{FormatterContext, PathwayHeadersCache};
use crate::connectors::metadata::KafkaMetadata;
use crate::connectors::{OffsetKey, OffsetValue};
use crate::engine::Value;
use crate::persistence::frontier::OffsetAntichain;
use crate::timestamp::current_unix_timestamp_ms;

use super::{
    ConnectorMode, MessageQueueTopic, ReadError, ReadResult, Reader, ReaderContext, StorageType,
    WriteError, Writer,
};
use rdkafka::bindings as rdsys;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header, Headers, OwnedMessage, Timestamp as KafkaTimestamp};
use rdkafka::producer::{BaseRecord, DefaultProducerContext, Producer, ThreadedProducer};
use rdkafka::topic_partition_list::Offset as KafkaOffset;
use rdkafka::ClientContext;
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

    #[error(
        "No Kafka broker could be reached within {timeout_secs}s at \
        bootstrap.servers={bootstrap_servers}: {source}. Check that the address and port \
        are correct, that the broker is running, and that it is reachable from this host \
        (DNS, network, firewall); with a broker behind authentication or TLS, check the \
        security.protocol and sasl.* settings too."
    )]
    BrokerUnreachable {
        bootstrap_servers: String,
        timeout_secs: u64,
        source: KafkaError,
    },

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

/// The consumer context of Pathway's Kafka readers. Its one job: before a
/// partition gets assigned, route the partition's fetch queue to the reader's
/// [`FetchQueue`], so that fetched messages never enter the consumer queue.
///
/// Reading the messages off a queue of their own lets the reader take them in
/// batches (`rd_kafka_consume_batch_queue`), which costs far less per message
/// than rust-rdkafka's event-based `poll`, while the consumer queue keeps
/// carrying only the group events (rebalances, offset commits, errors, logs)
/// that rust-rdkafka's `poll` knows how to dispatch. Batch-consuming the
/// consumer queue itself would not do: librdkafka serves the non-message ops
/// it meets there with its own defaults, and its default for a rebalance
/// event without a rebalance callback is to unassign everything.
#[derive(Default)]
pub struct PathwayConsumerContext {
    fetch_queue: Mutex<Option<Arc<FetchQueue>>>,
}

impl PathwayConsumerContext {
    fn set_fetch_queue(&self, fetch_queue: Option<Arc<FetchQueue>>) {
        *self
            .fetch_queue
            .lock()
            .expect("the fetch queue lock is never poisoned") = fetch_queue;
    }
}

impl ClientContext for PathwayConsumerContext {}

impl ConsumerContext for PathwayConsumerContext {
    fn pre_rebalance(&self, base_consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if let Rebalance::Assign(partitions) = rebalance {
            let fetch_queue = self
                .fetch_queue
                .lock()
                .expect("the fetch queue lock is never poisoned")
                .clone();
            if let Some(fetch_queue) = fetch_queue {
                fetch_queue.forward_partitions(base_consumer, partitions);
            }
        }
    }
}

pub type KafkaConsumer = BaseConsumer<PathwayConsumerContext>;

/// How many already-fetched messages one `rd_kafka_consume_batch_queue` call
/// takes off the fetch queue at most.
const CONSUME_BATCH_SIZE: usize = 256;

/// How long a streaming read waits for a message before serving the consumer
/// queue again (the same cadence rust-rdkafka's `poll` uses internally).
const STREAMING_WAIT: Duration = Duration::from_millis(100);

/// An application-owned librdkafka queue that the fetch queues of the
/// partitions read by a consumer are forwarded to (see
/// [`PathwayConsumerContext`]).
struct FetchQueue {
    ptr: *mut rdsys::rd_kafka_queue_t,
}

// librdkafka queues are thread-safe; the pointer is only ever handed back to
// librdkafka.
unsafe impl Send for FetchQueue {}
unsafe impl Sync for FetchQueue {}

impl FetchQueue {
    fn new(consumer: &KafkaConsumer) -> Self {
        let ptr = unsafe { rdsys::rd_kafka_queue_new(consumer.client().native_ptr()) };
        assert!(!ptr.is_null(), "rd_kafka_queue_new never fails");
        Self { ptr }
    }

    /// Routes the fetch queues of `partitions` here. Must run before the
    /// partitions are assigned: at fetch start librdkafka forwards a fetch
    /// queue to the consumer queue only if the application hasn't forwarded
    /// it elsewhere yet, so forwarding first guarantees that no message of
    /// these partitions ever reaches the consumer queue.
    fn forward_partitions(&self, consumer: &KafkaConsumer, partitions: &TopicPartitionList) {
        let client = consumer.client().native_ptr();
        for element in partitions.elements() {
            let Ok(topic) = CString::new(element.topic()) else {
                error!("Kafka topic name with a NUL byte: {:?}", element.topic());
                continue;
            };
            unsafe {
                let partition_queue = rdsys::rd_kafka_queue_get_partition(
                    client,
                    topic.as_ptr(),
                    element.partition(),
                );
                // Not expected: librdkafka creates the partition object on
                // a miss and returns NULL only for a producer handle. Kept as
                // a loud failure rather than an `expect`, since a partition
                // silently left on the consumer queue would still be read,
                // just without batching.
                if partition_queue.is_null() {
                    error!(
                        "Kafka partition ({}, {}) has no fetch queue, its messages will not be read",
                        element.topic(),
                        element.partition()
                    );
                    continue;
                }
                rdsys::rd_kafka_queue_forward(partition_queue, self.ptr);
                rdsys::rd_kafka_queue_destroy(partition_queue);
            }
        }
    }

    /// Takes the messages already waiting in the queue, up to
    /// `CONSUME_BATCH_SIZE` of them, without waiting for more.
    ///
    /// Caveat: librdkafka advances the partition's application position past
    /// every message it hands out here, at the time of the call, so with
    /// `enable.auto.commit` the offsets of a whole batch may get committed
    /// before its messages are processed. The per-message `poll` did the same
    /// per message; this widens the "committed but unprocessed" window by up
    /// to `CONSUME_BATCH_SIZE` messages. Persistence keeps its own offsets and
    /// is unaffected; a restart from the group's committed offsets alone can
    /// skip that many more messages after a crash.
    fn consume_batch(&self, messages: &mut VecDeque<FetchedMessage>) -> Result<(), KafkaError> {
        let mut raw: Vec<*mut rdsys::rd_kafka_message_t> = Vec::with_capacity(CONSUME_BATCH_SIZE);
        let count = unsafe {
            rdsys::rd_kafka_consume_batch_queue(self.ptr, 0, raw.as_mut_ptr(), CONSUME_BATCH_SIZE)
        };
        let Ok(count) = usize::try_from(count) else {
            let error = unsafe { rdsys::rd_kafka_last_error() };
            return Err(KafkaError::MessageConsumption(error.into()));
        };
        // SAFETY: librdkafka wrote `count` valid message pointers into `raw`.
        unsafe { raw.set_len(count) };
        messages.extend(raw.into_iter().map(|message| FetchedMessage {
            message,
            event: ptr::null_mut(),
        }));
        Ok(())
    }

    /// Waits up to `timeout` for a single message (or error) to arrive.
    fn wait_for_message(&self, timeout: Duration) -> Option<Result<FetchedMessage, KafkaError>> {
        let timeout_ms = c_int::try_from(timeout.as_millis()).unwrap_or(c_int::MAX);
        let event = unsafe { rdsys::rd_kafka_queue_poll(self.ptr, timeout_ms) };
        if event.is_null() {
            return None;
        }
        match unsafe { rdsys::rd_kafka_event_type(event) } {
            rdsys::RD_KAFKA_EVENT_FETCH => {
                let message = unsafe { rdsys::rd_kafka_event_message_next(event) };
                if message.is_null() {
                    unsafe { rdsys::rd_kafka_event_destroy(event) };
                    return None;
                }
                Some(Ok(FetchedMessage {
                    message: message.cast_mut(),
                    event,
                }))
            }
            rdsys::RD_KAFKA_EVENT_ERROR => {
                let code = unsafe { rdsys::rd_kafka_event_error(event) };
                let reason = unsafe { CStr::from_ptr(rdsys::rd_kafka_event_error_string(event)) }
                    .to_string_lossy()
                    .into_owned();
                let is_fatal = unsafe { rdsys::rd_kafka_event_error_is_fatal(event) } != 0;
                unsafe { rdsys::rd_kafka_event_destroy(event) };
                error!("Kafka consumer error: {reason}");
                let code: RDKafkaErrorCode = code.into();
                Some(Err(if is_fatal {
                    KafkaError::MessageConsumptionFatal(code)
                } else {
                    KafkaError::MessageConsumption(code)
                }))
            }
            other => {
                warn!("Unexpected event of type {other} on a Kafka fetch queue, ignoring it");
                unsafe { rdsys::rd_kafka_event_destroy(event) };
                None
            }
        }
    }
}

impl Drop for FetchQueue {
    fn drop(&mut self) {
        unsafe { rdsys::rd_kafka_queue_destroy(self.ptr) };
    }
}

/// A message taken off a [`FetchQueue`]. Owned by the reader until dropped,
/// which hands it back to librdkafka.
struct FetchedMessage {
    message: *mut rdsys::rd_kafka_message_t,
    /// The event a message polled one at a time belongs to (the event owns
    /// the message); null for messages taken by `consume_batch`, which are
    /// destroyed on their own.
    event: *mut rdsys::rd_kafka_event_t,
}

// The message is owned by this struct alone and only read from.
unsafe impl Send for FetchedMessage {}

impl FetchedMessage {
    fn raw(&self) -> &rdsys::rd_kafka_message_t {
        unsafe { &*self.message }
    }

    /// Separates the consumer errors librdkafka delivers as messages from the
    /// real messages.
    fn into_result(self) -> Result<Self, KafkaError> {
        match self.raw().err {
            rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR => Ok(self),
            rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__PARTITION_EOF => {
                Err(KafkaError::PartitionEOF(self.raw().partition))
            }
            err => Err(KafkaError::MessageConsumption(err.into())),
        }
    }
}

impl Drop for FetchedMessage {
    fn drop(&mut self) {
        unsafe {
            if self.event.is_null() {
                rdsys::rd_kafka_message_destroy(self.message);
            } else {
                rdsys::rd_kafka_event_destroy(self.event);
            }
        }
    }
}

/// The headers of a [`FetchedMessage`]: a view over librdkafka's header list,
/// valid as long as the message is. Like rust-rdkafka's `BorrowedHeaders`, a
/// zero-sized type referenced through the native pointer.
struct FetchedHeaders;

impl FetchedHeaders {
    fn as_native_ptr(&self) -> *const rdsys::rd_kafka_headers_t {
        ptr::from_ref(self).cast()
    }
}

impl Headers for FetchedHeaders {
    fn count(&self) -> usize {
        unsafe { rdsys::rd_kafka_header_cnt(self.as_native_ptr()) }
    }

    fn try_get(&self, idx: usize) -> Option<Header<'_, &[u8]>> {
        let mut name = ptr::null();
        let mut value: *const c_void = ptr::null();
        let mut size = 0usize;
        let err = unsafe {
            rdsys::rd_kafka_header_get_all(
                self.as_native_ptr(),
                idx,
                &raw mut name,
                &raw mut value,
                &raw mut size,
            )
        };
        if err != rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
            return None;
        }
        unsafe {
            Some(Header {
                key: CStr::from_ptr(name)
                    .to_str()
                    .expect("Kafka header names are UTF-8"),
                value: (!value.is_null()).then(|| slice::from_raw_parts(value.cast::<u8>(), size)),
            })
        }
    }
}

impl Message for FetchedMessage {
    type Headers = FetchedHeaders;

    fn key(&self) -> Option<&[u8]> {
        let message = self.raw();
        (!message.key.is_null())
            .then(|| unsafe { slice::from_raw_parts(message.key.cast::<u8>(), message.key_len) })
    }

    fn payload(&self) -> Option<&[u8]> {
        let message = self.raw();
        (!message.payload.is_null())
            .then(|| unsafe { slice::from_raw_parts(message.payload.cast::<u8>(), message.len) })
    }

    unsafe fn payload_mut(&mut self) -> Option<&mut [u8]> {
        let message = self.raw();
        (!message.payload.is_null())
            .then(|| slice::from_raw_parts_mut(message.payload.cast::<u8>(), message.len))
    }

    fn topic(&self) -> &str {
        unsafe { CStr::from_ptr(rdsys::rd_kafka_topic_name(self.raw().rkt)) }
            .to_str()
            .expect("Kafka topic names are UTF-8")
    }

    fn partition(&self) -> i32 {
        self.raw().partition
    }

    fn offset(&self) -> i64 {
        self.raw().offset
    }

    fn timestamp(&self) -> KafkaTimestamp {
        let mut timestamp_type = rdsys::rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_NOT_AVAILABLE;
        let timestamp =
            unsafe { rdsys::rd_kafka_message_timestamp(self.message, &raw mut timestamp_type) };
        if timestamp == -1 {
            return KafkaTimestamp::NotAvailable;
        }
        match timestamp_type {
            rdsys::rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_NOT_AVAILABLE => {
                KafkaTimestamp::NotAvailable
            }
            rdsys::rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_CREATE_TIME => {
                KafkaTimestamp::CreateTime(timestamp)
            }
            rdsys::rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME => {
                KafkaTimestamp::LogAppendTime(timestamp)
            }
        }
    }

    fn headers(&self) -> Option<&FetchedHeaders> {
        let mut headers = ptr::null_mut();
        let err = unsafe { rdsys::rd_kafka_message_headers(self.message, &raw mut headers) };
        if err == rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR && !headers.is_null() {
            Some(unsafe { &*headers.cast::<FetchedHeaders>() })
        } else {
            None
        }
    }
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

/// The error codes librdkafka reports when it could not talk to any broker at
/// all - as opposed to a broker answering with a problem about the topic.
fn is_broker_unreachable_error(err: &KafkaError) -> bool {
    matches!(
        err.rdkafka_error_code(),
        Some(
            RDKafkaErrorCode::BrokerTransportFailure
                | RDKafkaErrorCode::AllBrokersDown
                | RDKafkaErrorCode::Resolve
                | RDKafkaErrorCode::OperationTimedOut
        )
    )
}

/// Returns the total number of partitions for a Kafka topic.
fn total_partitions_for_topic(
    consumer: &KafkaConsumer,
    topic: &str,
    bootstrap_servers: &str,
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
                if is_broker_unreachable_error(&e) {
                    return Err(KafkaReaderError::BrokerUnreachable {
                        bootstrap_servers: bootstrap_servers.to_string(),
                        timeout_secs: KafkaReader::default_timeout().as_secs(),
                        source: e,
                    });
                }
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
    consumer: &KafkaConsumer,
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
    consumer: &KafkaConsumer,
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
    // The field order matters for `Drop`: the fetched messages go back to
    // librdkafka first, then the fetch queue, and the consumer last.
    fetched_messages: VecDeque<FetchedMessage>,
    fetch_queue: Arc<FetchQueue>,
    consumer: KafkaConsumer,
    topic: ArcStr,
    positions_for_seek: HashMap<i32, KafkaOffset>,
    watermarks: Vec<RdkafkaWatermark>,
    deferred_read_result: Option<ReadResult>,
    // Whether the per-message `KafkaMetadata` is built and announced through a
    // `NewSource` event before every `Data`. Nobody reads it unless the user
    // asked for the `_metadata` column or the parser needs the metadata itself
    // (see `Parser::needs_source_metadata`), so it is skipped otherwise: the
    // metadata costs an allocation, a headers extraction and a JSON
    // serialization + hash per message, plus a second channel round trip.
    emit_metadata: bool,
    // Streaming mode: partitions are paused while backpressure blocks the
    // delivery of an already-read message (see `keep_alive`); `read` resumes
    // them, since it is only called once the blocked message got through.
    paused_for_backpressure: bool,
    // Messages returned by a keep-alive poll despite the pause. Possible only
    // when a rebalance assigns fresh (unpaused) partitions mid-pause: at most
    // one message slips out before the re-pause, so this holds ≤1 entry per
    // rebalance. The consume position is already past these messages, so they
    // must be delivered, not dropped; `read` drains them before polling.
    pending_messages: VecDeque<OwnedMessage>,
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

        if self.paused_for_backpressure {
            // read() is called only after the blocked message got through,
            // so the backpressure stall is over. On error the connector's
            // retry loop calls read() again, retrying the resume.
            let assignment = self.consumer.assignment()?;
            self.consumer.resume(&assignment)?;
            self.paused_for_backpressure = false;
        }

        loop {
            if let Some(pending_message) = self.pending_messages.pop_front() {
                if let Some(read_result) = self.prepare_read_result_split(&pending_message) {
                    return Ok(read_result);
                }
                continue;
            }
            match self.mode {
                ConnectorMode::Streaming => {
                    let Some(kafka_message) = self.next_fetched_message(STREAMING_WAIT)? else {
                        // Nothing arrived within the wait: go round again (the
                        // consumer queue gets served on every round).
                        continue;
                    };
                    if let Some(read_result) = self.prepare_read_result_split(&kafka_message) {
                        return Ok(read_result);
                    }
                }
                ConnectorMode::Static => {
                    let Some(kafka_message) = self.next_message_in_static_mode()? else {
                        return Ok(ReadResult::Finished);
                    };
                    if let Some(read_result) = self.prepare_read_result_split(&kafka_message) {
                        return Ok(read_result);
                    }
                }
            }
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

    fn keep_alive(&mut self) {
        // Only streaming mode joins a consumer group; static mode uses manual
        // assignment, with no membership to keep alive.
        if !matches!(self.mode, ConnectorMode::Streaming) {
            return;
        }
        if !self.paused_for_backpressure {
            // First tick of a stall: stop fetching. librdkafka drops the
            // prefetch buffers and refetches after resume, so nothing is lost
            // and no memory accumulates while paused.
            match self.pause_assigned_partitions() {
                Ok(()) => self.paused_for_backpressure = true,
                // Best-effort: retried on the next tick. Keep polling anyway —
                // staying in the group matters more, and any message a poll
                // returns is stashed below, not lost.
                Err(error) => warn!("Failed to pause partitions of {}: {error}", self.topic),
            }
        }
        // Poll with the partitions paused: returns no data, but resets the
        // max.poll.interval timer and serves rebalance callbacks. Without
        // this, a stall longer than max.poll.interval.ms gets the consumer
        // evicted from its group, and the rejoin re-delivers (duplicates)
        // everything after the group's committed offset.
        match self.consumer.poll(Duration::ZERO) {
            None => {}
            Some(Ok(message)) => {
                // A rebalance during the pause assigns fresh partitions
                // unpaused, and one message can slip out before the re-pause.
                self.pending_messages.push_back(message.detach());
                if let Err(error) = self.pause_assigned_partitions() {
                    warn!("Failed to re-pause partitions of {}: {error}", self.topic);
                }
            }
            Some(Err(error)) => {
                warn!(
                    "Consumer error in a keep-alive poll of {}: {error}",
                    self.topic
                );
            }
        }
    }
}

impl Drop for KafkaReader {
    fn drop(&mut self) {
        // Drop the context's reference too, so that the fetch queue goes away
        // with this reader, before the consumer, which must outlive it.
        self.consumer.context().set_fetch_queue(None);
    }
}

impl KafkaReader {
    fn new(
        consumer: KafkaConsumer,
        fetch_queue: Arc<FetchQueue>,
        topic: String,
        positions_for_seek: HashMap<i32, KafkaOffset>,
        watermarks: Vec<RdkafkaWatermark>,
        mode: ConnectorMode,
        has_assigned_partitions: bool,
        emit_metadata: bool,
    ) -> KafkaReader {
        KafkaReader {
            fetched_messages: VecDeque::with_capacity(CONSUME_BATCH_SIZE),
            fetch_queue,
            consumer,
            topic: topic.into(),
            emit_metadata,
            positions_for_seek,
            watermarks,
            mode,
            has_assigned_partitions,
            deferred_read_result: None,
            paused_for_backpressure: false,
            pending_messages: VecDeque::new(),
        }
    }

    fn pause_assigned_partitions(&self) -> Result<(), KafkaError> {
        let assignment = self.consumer.assignment()?;
        self.consumer.pause(&assignment)
    }

    /// The next fetched message: from the batch in hand, else from a fresh
    /// batch, else (nothing fetched yet) one polled with `timeout`, `None` if
    /// none arrived in time. The consumer queue is served before every batch
    /// and every wait, which keeps the group events flowing and the
    /// `max.poll.interval.ms` timer reset.
    fn next_fetched_message(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<FetchedMessage>, KafkaError> {
        loop {
            if let Some(message) = self.fetched_messages.pop_front() {
                return message.into_result().map(Some);
            }
            self.serve_consumer_queue()?;
            self.fetch_queue.consume_batch(&mut self.fetched_messages)?;
            if !self.fetched_messages.is_empty() {
                continue;
            }
            return match self.fetch_queue.wait_for_message(timeout) {
                Some(message) => message?.into_result().map(Some),
                None => Ok(None),
            };
        }
    }

    /// One non-blocking poll of the consumer queue: dispatches the group
    /// events (rebalances, offset commits, errors, logs). Messages can't show
    /// up there, as every assigned partition's fetch queue is forwarded to
    /// the reader's own queue, but any that did are kept, not lost.
    ///
    /// This poll is also what keeps the consumer a group member: it resets
    /// the `max.poll.interval.ms` timer. It runs before every batch and every
    /// wait (at most `STREAMING_WAIT` apart), and `keep_alive` covers the
    /// stretches when the reader is blocked on the channel.
    fn serve_consumer_queue(&mut self) -> Result<(), KafkaError> {
        match self.consumer.poll(Duration::ZERO) {
            None => Ok(()),
            Some(Ok(message)) => {
                self.pending_messages.push_back(message.detach());
                Ok(())
            }
            Some(Err(error)) => Err(error),
        }
    }

    /// `prepare_read_result` for a message that does not borrow `self`.
    fn prepare_read_result_split<M: Message>(&mut self, kafka_message: &M) -> Option<ReadResult> {
        let read_result = Self::prepare_read_result(
            &self.consumer,
            &self.topic,
            &mut self.positions_for_seek,
            &mut self.deferred_read_result,
            self.emit_metadata,
            kafka_message,
        );
        if read_result.is_none() {
            // The message triggered the lazy seek of its partition. librdkafka
            // discards what it had fetched before the seek, and so must we:
            // the batch in hand may hold more messages of that partition, all
            // fetched from the pre-seek position.
            let partition = kafka_message.partition();
            self.fetched_messages
                .retain(|message| message.partition() != partition);
        }
        read_result
    }

    /// Shared tail of `read` for freshly-polled and pending messages alike.
    /// Returns `None` if the message was consumed by a lazy seek (the caller
    /// polls again); otherwise stores the `Data` part in
    /// `deferred_read_result` and returns the `NewSource` announcement.
    /// An associated fn over split field borrows, because a freshly-polled
    /// message already borrows `self.consumer`.
    fn prepare_read_result<M: Message>(
        consumer: &KafkaConsumer,
        topic: &ArcStr,
        positions_for_seek: &mut HashMap<i32, KafkaOffset>,
        deferred_read_result: &mut Option<ReadResult>,
        emit_metadata: bool,
        kafka_message: &M,
    ) -> Option<ReadResult> {
        let message_key = kafka_message.key().map(<[u8]>::to_vec);
        let message_payload = kafka_message.payload().map(<[u8]>::to_vec);

        if let Some(lazy_seek_offset) = positions_for_seek.get(&kafka_message.partition()) {
            info!(
                "Performing Kafka topic seek for ({}, {}) to {:?}",
                kafka_message.topic(),
                kafka_message.partition(),
                lazy_seek_offset
            );
            // If there is a need for seek, perform it and remove the seek requirement.
            if let Err(e) = consumer.seek(
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
                positions_for_seek.remove(&kafka_message.partition());
            }
            return None;
        }

        let offset = {
            let offset_key = OffsetKey::Kafka(topic.clone(), kafka_message.partition());
            let offset_value = OffsetValue::KafkaOffset(kafka_message.offset());
            (offset_key, offset_value)
        };
        let message = ReaderContext::from_key_value(message_key, message_payload);
        if !emit_metadata {
            // No consumer for the metadata: hand the data over directly. For
            // Kafka the `NewSource` announcement carries nothing else (commits
            // are always allowed in between messages), so skipping it is safe.
            return Some(ReadResult::Data(message, offset));
        }
        let metadata = KafkaMetadata::from_rdkafka_message(kafka_message);
        *deferred_read_result = Some(ReadResult::Data(message, offset));

        Some(ReadResult::NewSource(metadata.into()))
    }

    /// Builds a reader from an already-created consumer: fetches the topic's
    /// partitions and watermarks, resolves any timestamp-based start position,
    /// and acquires this worker's partitions (assign for static, subscribe for
    /// streaming). In static mode the partitions are sharded by hand across the
    /// readers: `worker_index` is this worker's index and `reader_count` is how
    /// many workers actually run a reader (the caller computes it — see
    /// `construct_kafka_reader`).
    #[allow(clippy::too_many_arguments)] // one flag over the connector's own settings
    pub fn build(
        consumer: KafkaConsumer,
        topic: String,
        bootstrap_servers: &str,
        mode: ConnectorMode,
        start_from_timestamp_ms: Option<i64>,
        worker_index: usize,
        reader_count: usize,
        emit_metadata: bool,
    ) -> Result<KafkaReader, KafkaReaderError> {
        let total_partitions = total_partitions_for_topic(&consumer, &topic, bootstrap_servers)?;
        let mut watermarks = partition_watermarks(&consumer, &topic, total_partitions)?;

        // The reader's own queue for the fetched messages. The consumer context
        // forwards each partition's fetch queue to it right before the partition
        // is assigned by a rebalance; the explicit assignment of the static mode
        // below does the same by hand.
        let fetch_queue = Arc::new(FetchQueue::new(&consumer));
        consumer
            .context()
            .set_fetch_queue(Some(fetch_queue.clone()));

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
                fetch_queue.forward_partitions(&consumer, &tpl);
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
            fetch_queue,
            topic,
            seek_positions,
            watermarks,
            mode,
            has_assigned_partitions,
            emit_metadata,
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

    fn message_matches_static_read_constraints<M: Message>(&self, message: &M) -> bool {
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

    fn next_message_in_static_mode(&mut self) -> Result<Option<FetchedMessage>, ReadError> {
        let n_attempts = Self::polling_attempts_count_for_static_mode();
        for _ in 0..n_attempts {
            if let Some(kafka_message) =
                self.next_fetched_message(Self::poll_duration_for_static_mode())?
            {
                if self.message_matches_static_read_constraints(&kafka_message) {
                    return Ok(Some(kafka_message));
                }

                // The message goes beyond the specified border within the partition or belongs
                // a partition that must not be read at all.
                // Stop reading the further messages from this partition, since they will
                // have greater offsets.
                let mut tpl = TopicPartitionList::with_capacity(1);
                tpl.add_partition(self.topic.as_str(), kafka_message.partition());
                self.consumer.pause(&tpl)?;
            }

            if self.static_read_has_finished()? {
                return Ok(None);
            }
        }
        warn!("There was no explicit finish detected from Kafka topic '{}', but no matching events were read after {n_attempts} attempts, with {:?} duration each.", self.topic, Self::poll_duration_for_static_mode());
        Ok(None)
    }
}

/// How many messages the writer holds back before handing them to librdkafka
/// in one `rd_kafka_produce_batch` call. The sink's flush between output
/// batches hands over whatever is pending, so this only bounds the memory of
/// a large batch.
const PRODUCE_BATCH_SIZE: usize = 512;

/// librdkafka's `RD_KAFKA_PARTITION_UA` (a C macro, absent from the bindings):
/// let the partitioner pick the partition.
const PARTITION_UNASSIGNED: i32 = -1;

/// A message waiting for the next `rd_kafka_produce_batch` call.
struct PendingMessage {
    topic: String,
    key: Vec<u8>,
    payload: Vec<u8>,
}

/// A librdkafka topic handle, the entry point of `rd_kafka_produce_batch`.
struct ProducerTopic(*mut rdsys::rd_kafka_topic_t);

// The handle is only ever handed back to librdkafka, which is thread-safe.
unsafe impl Send for ProducerTopic {}

impl Drop for ProducerTopic {
    fn drop(&mut self) {
        unsafe { rdsys::rd_kafka_topic_destroy(self.0) };
    }
}

pub struct KafkaWriter {
    // The field order matters for `Drop`: the topic handles must be destroyed
    // before the producer.
    topics: HashMap<String, ProducerTopic>,
    pending: Vec<PendingMessage>,
    producer: ThreadedProducer<DefaultProducerContext>,
    topic: MessageQueueTopic,
    header_fields: Vec<(String, usize)>,
    key_field_index: Option<usize>,
    // Whether the `pathway_time`/`pathway_diff` headers are attached to every
    // message. They are part of the documented message layout, but the header
    // set costs a copy per message and a longer wire format, so consumers that
    // never read them can opt out (`with_pathway_headers=False`).
    with_pathway_headers: bool,
    headers_cache: PathwayHeadersCache,
}

impl KafkaWriter {
    pub fn new(
        producer: ThreadedProducer<DefaultProducerContext>,
        topic: MessageQueueTopic,
        header_fields: Vec<(String, usize)>,
        key_field_index: Option<usize>,
        with_pathway_headers: bool,
    ) -> KafkaWriter {
        KafkaWriter {
            topics: HashMap::new(),
            pending: Vec::new(),
            producer,
            topic,
            header_fields,
            key_field_index,
            with_pathway_headers,
            headers_cache: PathwayHeadersCache::default(),
        }
    }

    fn topic_handle(&mut self, name: &str) -> Result<*mut rdsys::rd_kafka_topic_t, WriteError> {
        if let Some(topic) = self.topics.get(name) {
            return Ok(topic.0);
        }
        let c_name = CString::new(name).expect("Kafka topic names have no NUL bytes");
        let handle = unsafe {
            rdsys::rd_kafka_topic_new(
                self.producer.client().native_ptr(),
                c_name.as_ptr(),
                ptr::null_mut(),
            )
        };
        if handle.is_null() {
            let error = unsafe { rdsys::rd_kafka_last_error() };
            return Err(KafkaError::MessageProduction(error.into()).into());
        }
        self.topics.insert(name.to_string(), ProducerTopic(handle));
        Ok(handle)
    }

    /// Hands the pending messages to librdkafka, one `rd_kafka_produce_batch`
    /// call per run of consecutive messages to the same topic.
    fn flush_pending(&mut self) -> Result<(), WriteError> {
        let pending = take(&mut self.pending);
        let mut start = 0;
        while start < pending.len() {
            let topic = &pending[start].topic;
            let run_length = pending[start..]
                .iter()
                .take_while(|message| message.topic == *topic)
                .count();
            let handle = self.topic_handle(topic)?;
            self.produce_batch(handle, &pending[start..start + run_length])?;
            start += run_length;
        }
        Ok(())
    }

    /// Enqueues `messages` for `topic` with a single `rd_kafka_produce_batch`
    /// call, waiting out a full producer queue the way the single-message
    /// path does.
    fn produce_batch(
        &self,
        topic: *mut rdsys::rd_kafka_topic_t,
        messages: &[PendingMessage],
    ) -> Result<(), WriteError> {
        let mut batch: Vec<rdsys::rd_kafka_message_t> = messages
            .iter()
            .map(|message| rdsys::rd_kafka_message_t {
                err: rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR,
                rkt: ptr::null_mut(),
                partition: PARTITION_UNASSIGNED,
                payload: message.payload.as_ptr().cast_mut().cast::<c_void>(),
                len: message.payload.len(),
                key: message.key.as_ptr().cast_mut().cast::<c_void>(),
                key_len: message.key.len(),
                offset: 0,
                _private: ptr::null_mut(),
            })
            .collect();
        loop {
            let batch_size = c_int::try_from(batch.len()).expect("the batch size fits in c_int");
            // RD_KAFKA_MSG_F_COPY: librdkafka copies the payloads and keys, so
            // the pending messages can be dropped right after the call.
            let enqueued = unsafe {
                rdsys::rd_kafka_produce_batch(
                    topic,
                    PARTITION_UNASSIGNED,
                    rdsys::RD_KAFKA_MSG_F_COPY,
                    batch.as_mut_ptr(),
                    batch_size,
                )
            };
            if enqueued == batch_size {
                return Ok(());
            }
            // Retry the messages a full producer queue rejected once it drained
            // a bit; anything else is an error, as in the single-message path.
            // Caveat: on such an error the messages of this batch that were
            // only rejected as `QUEUE_FULL` are not enqueued either, whereas
            // the single-message path failed exactly at the offending message.
            // Either way the error fails the write; if that ever becomes
            // retriable, the rejected remainder must be re-enqueued first.
            let mut retry = Vec::new();
            for message in &batch {
                match message.err {
                    rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR => {}
                    rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__QUEUE_FULL => {
                        let mut message = *message;
                        message.err = rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR;
                        retry.push(message);
                    }
                    error => return Err(KafkaError::MessageProduction(error.into()).into()),
                }
            }
            self.producer.poll(Duration::from_millis(10));
            batch = retry;
        }
    }
}

impl Drop for KafkaWriter {
    fn drop(&mut self) {
        if let Err(error) = self.flush_pending() {
            error!("Failed to hand the pending messages to the Kafka producer: {error}");
        }
        self.producer.flush(None).expect("kafka commit should work");
    }
}

impl Writer for KafkaWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        let row_key_bytes = data.key.to_le_bytes();
        let key: &[u8] = match self.key_field_index {
            Some(index) => match &data.values[index] {
                Value::Bytes(bytes) => bytes,
                Value::String(string) => string.as_bytes(),
                _ => {
                    return Err(WriteError::IncorrectKeyFieldType(
                        data.values[index].clone(),
                    ))
                }
            },
            None => &row_key_bytes,
        };

        let mut headers =
            (self.with_pathway_headers || !self.header_fields.is_empty()).then(|| {
                data.construct_kafka_headers(
                    &self.header_fields,
                    self.with_pathway_headers,
                    &mut self.headers_cache,
                )
            });
        let effective_topic = self.topic.topic_for_posting(&data.values)?;
        if headers.is_none() {
            // Without headers the message can take the batch entry point,
            // `rd_kafka_produce_batch`, which pays the topic lock and the clock
            // read once per batch instead of once per message (and is the
            // only entry point that does not take headers).
            // Ordering relies on the header set being fixed per writer (by its
            // configuration): headerless messages wait in `pending` while
            // headered ones go to `producev` at once, so a writer that mixed
            // the two could reorder messages of one partition.
            for payload in data.payloads {
                let payload = payload.into_raw_bytes()?;
                self.pending.push(PendingMessage {
                    topic: effective_topic.to_string(),
                    key: key.to_vec(),
                    payload,
                });
            }
            if self.pending.len() >= PRODUCE_BATCH_SIZE {
                self.flush_pending()?;
            }
            return Ok(());
        }
        let last_payload_index = data.payloads.len() - 1;
        for (index, payload) in data.payloads.into_iter().enumerate() {
            let payload = payload.into_raw_bytes()?;
            // The headers are handed over to librdkafka with the message, so
            // every payload but the last one gets a copy and the last one
            // takes the original.
            let payload_headers = if index == last_payload_index {
                headers.take()
            } else {
                headers.clone()
            };
            let mut entry = BaseRecord::<[u8], [u8]>::to(&effective_topic)
                .payload(&payload)
                .key(key);
            if let Some(payload_headers) = payload_headers {
                entry = entry.headers(payload_headers);
            }
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

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        self.flush_pending()
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
