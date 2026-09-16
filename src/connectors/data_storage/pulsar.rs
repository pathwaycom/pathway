// Copyright © 2026 Pathway

use log::{debug, error, warn};
use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::mem::take;
use std::str::Utf8Error;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::{FutureExt, StreamExt};
use pulsar::compression::Compression as PulsarCompression;
use pulsar::consumer::{Consumer as PulsarConsumer, ConsumerOptions as PulsarConsumerOptions};
use pulsar::error::ConsumerError as PulsarConsumerError;
use pulsar::message::proto::command_subscribe::SubType as PulsarSubType;
use pulsar::producer::{Message as PulsarProducerMessage, SendFuture};
use pulsar::proto::MessageIdData;
use pulsar::routing_policy::RoutingPolicy as PulsarRoutingPolicy;
use pulsar::{
    consumer::InitialPosition as PulsarInitialPosition, Producer, ProducerOptions, Pulsar,
    TokioExecutor,
};
use tokio::runtime::Runtime as TokioRuntime;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::connectors::data_format::avro::AvroSchemaProvider;
use crate::connectors::data_format::FormatterContext;
use crate::connectors::data_storage::MessageQueueTopic;
use crate::connectors::metadata::PulsarMetadata;
use crate::connectors::offset::{PulsarOffsetKey, PulsarOffsetValue};
use crate::connectors::{
    OffsetKey, OffsetValue, ReadError, ReadResult, Reader, ReaderContext, StorageType, WriteError,
    Writer,
};
use crate::engine::time::DateTime;
use crate::engine::Value;
use crate::persistence::frontier::OffsetAntichain;
use crate::retry::RetryConfig;

// The maximum number of published messages whose broker receipts may be
// outstanding at any given moment. Each publish is enqueued immediately and
// returns a future resolving to the broker's receipt; the writer awaits those
// futures in `flush`. Letting an unbounded number of them accumulate (a large
// minibatch, multiplied by several workers writing to the same topic) would
// grow the internal queues without limit, so once this many are in flight the
// queue is drained to `IN_FLIGHT_DRAIN_TARGET`, bounding the memory usage
// while still allowing aggressive pipelining.
const MAX_IN_FLIGHT_SENDS: usize = 50_000;

// Where a drain triggered by `MAX_IN_FLIGHT_SENDS` stops. Draining begins by
// forcing out the partially filled batches (a receipt of a message sitting in
// an unfilled batch never resolves otherwise), which costs one small publish
// per producer and briefly disables batching for the in-progress batch — so
// the drain must run rarely and release a large portion of the queue at once,
// not one receipt per message. Draining to the half keeps the amortized cost
// of the forced batch flush at one per `MAX_IN_FLIGHT_SENDS / 2` messages.
const IN_FLIGHT_DRAIN_TARGET: usize = MAX_IN_FLIGHT_SENDS / 2;

// The number of messages the producer packs into a single Pulsar batch.
// Batching amortizes the per-message protocol overhead and is the single
// biggest throughput lever for Pulsar producers. Partially filled batches are
// forced out explicitly in `flush`, so no message waits for the batch to fill.
const PRODUCER_BATCH_SIZE: u32 = 1000;

// The payload budget of a single batch. A batch travels to the broker as one
// message, so it is bounded by the broker's `maxMessageSize` (5 MB by
// default) — without a byte budget, `PRODUCER_BATCH_SIZE` rows of a few
// kilobytes each would build a frame far above that limit, and the broker
// answers an oversized frame by dropping the connection, losing the whole
// write. The client library does not expose the limit the broker announces on
// connect, so the budget is a fixed fraction of the default: the same 128 kB
// the Java client uses, which leaves ample headroom for the message metadata
// and for the one message that may cross the budget before the batch is cut.
const PRODUCER_BATCH_MAX_BYTES: usize = 128 * 1024;

// Pulsar's default broker-side limit on a single wire message
// (`maxMessageSize` in broker.conf). The client library performs no size
// check of its own, and the broker reacts to an oversized frame by closing
// the connection — so when a send fails while a payload above this limit is
// in flight, the error is annotated with the likely cause.
const PULSAR_DEFAULT_MAX_MESSAGE_SIZE: usize = 5 * 1024 * 1024;

// How long the writer keeps retrying the sends whose failure is transient
// (a dropped connection, a timed-out request) before giving up and failing
// the pipeline. A broker blip — a failover, a bundle unload, a rolling
// restart — heals well within this budget, so a streaming pipeline rides it
// out instead of dying within a second of the disconnection, the way the
// Kafka writer survives such blips through its client's delivery timeout.
// The broker's definitive refusals (rejected credentials, a deleted topic)
// and the oversized-message diagnosis are never retried.
const SEND_RECOVERY_TOTAL_BUDGET: Duration = Duration::from_mins(2);

// How long a single broker receipt may stay unresolved before its message
// is considered undelivered and republished. A receipt can be lost without
// an error when the connection dies at the wrong moment, and waiting
// forever would hang the flush. A message whose receipt resolves after this
// timeout may be delivered twice — the write contract is at-least-once.
const SEND_RECEIPT_TIMEOUT: Duration = Duration::from_secs(30);

// The cap of the backoff between the send-recovery attempts: the retries
// must probe often enough to notice the broker healing within the budget.
const SEND_RECOVERY_MAX_BACKOFF: Duration = Duration::from_secs(5);

// The byte cap of the writer's in-flight window. The writer retains a copy
// of every unconfirmed message (see `PendingMessage`), so the count cap
// alone would let large payloads hold gigabytes of retained copies.
// Whichever cap is hit first triggers the backpressure drain, which then
// frees down to the byte target below as well as to the count target.
const MAX_IN_FLIGHT_BYTES: usize = 64 * 1024 * 1024;
const IN_FLIGHT_BYTES_DRAIN_TARGET: usize = MAX_IN_FLIGHT_BYTES / 2;

// How long a terminal-class error of the subscription reading mode (a
// poison message, a topic expansion) keeps surfacing within the ordinary
// error budget before turning terminal. The messages acknowledged before
// the error may sit in a minibatch the engine has not committed yet, and a
// pipeline taken down at once drops them — irrecoverably, because the
// broker's cursor is already past them. Every surfacing costs the connector
// one error-backoff sleep, and the grace covers the default autocommit
// interval (1500 ms) many times over; a pipeline configured with an
// autocommit interval above the grace can still lose the tail — the
// inherent risk of the ack-on-read mode.
const TERMINAL_ERROR_COMMIT_GRACE: Duration = Duration::from_secs(10);

// How many messages one runtime entry may take from the subscription
// consumer. Entering the runtime (`block_on`) costs more than the
// per-message processing itself, so the reader drains the consumer's locally
// queued messages in bulk: it waits for the first message, then keeps taking
// the already-delivered ones without waiting, and serves the subsequent
// `read` calls from the local buffer.
const MAX_READ_BATCH_SIZE: usize = 1000;

// The capacity of the channel between the per-partition pump tasks and the
// reader in the partition-reader mode. A full channel suspends the pumps
// (and, through the consumers' flow permits, the broker's dispatch), which
// is the desired backpressure.
const PARTITION_PUMP_CHANNEL_CAPACITY: usize = 4096;

// How often the reader re-checks the number of partitions of its topic.
// Neither reading mechanism can attach to a partition that appeared after
// the reader was positioned — the pumps are started once per partition, and
// the client library's multi-topic consumer refreshes a list that already
// holds the physical partition names — so a topic expanded under a running
// pipeline must be reported instead of silently swallowing everything
// published into the new partitions. The check is one lookup command, and
// it also bounds how long the reader waits for messages, so an expansion is
// noticed on an idle topic too.
const PARTITION_COUNT_CHECK_INTERVAL: Duration = Duration::from_secs(15);

// How long a respawned pump waits before recreating its consumer. A pump is
// only respawned after an error that has already exhausted the client's
// internal reconnection budget, so an immediate retry would most likely fail
// again and spin a hot error loop.
const PUMP_RESPAWN_DELAY: Duration = Duration::from_secs(5);

// The scheme of the topics that store no messages. Such topics ignore the
// delivery schedule: the broker dispatches everything immediately.
const NON_PERSISTENT_TOPIC_PREFIX: &str = "non-persistent://";

#[derive(Debug, thiserror::Error)]
pub enum PulsarError {
    #[error(transparent)]
    Client(#[from] pulsar::Error),

    #[error(transparent)]
    Consumer(#[from] PulsarConsumerError),

    #[error(
        "the consumer stream of the Pulsar topic '{topic}' ended unexpectedly \
         (the client may have exhausted its reconnection attempts)"
    )]
    StreamUnexpectedlyClosed { topic: String },

    #[error(
        "value {0} cannot be used as an event time: only non-negative integers \
         (milliseconds since the UNIX epoch) and UTC datetimes at or after the \
         epoch are supported"
    )]
    IncorrectEventTimeValue(Value),

    #[error(
        "value {0} cannot be used as a delivery time: only non-negative integers \
         (milliseconds since the UNIX epoch) and UTC datetimes at or after the \
         epoch are supported"
    )]
    IncorrectDeliverAtValue(Value),

    #[error(
        "value {0} cannot be used as a delivery delay: only non-negative \
         integers (milliseconds) and non-negative durations are supported"
    )]
    IncorrectDeliverAfterValue(Value),

    #[error(
        "the Pulsar topic '{topic}' is non-persistent, and such topics ignore \
         the delivery schedule (deliver_at / deliver_after): the broker would \
         dispatch the messages immediately. Publish the delayed messages into \
         a persistent topic"
    )]
    DelayedDeliveryOnNonPersistentTopic { topic: String },

    #[error(
        "value {0} can't be used as an ordering key because it's neither \
         'bytes' nor 'string'"
    )]
    IncorrectOrderingKeyValue(Value),

    #[error(
        "the partition key of a message must be valid UTF-8 because Pulsar \
         stores it as a string, but the key column contains bytes that are \
         not: {0}"
    )]
    NonUtf8PartitionKey(Utf8Error),

    #[error(
        "failed to publish a message of {size} bytes: {source}. A message \
         this large exceeds Pulsar's default per-message limit \
         (maxMessageSize, 5242880 bytes), and the broker closes the \
         connection when it receives an oversized frame"
    )]
    OversizedMessage { size: usize, source: pulsar::Error },

    #[error(
        "{undelivered} message(s) could not be delivered to the broker within the \
         {budget:?} recovery budget: the connection kept failing for the whole \
         window. The messages published before the failure may have reached the \
         topic (the delivery is at-least-once)"
    )]
    SendRecoveryBudgetExhausted {
        undelivered: usize,
        budget: Duration,
    },

    #[error(
        "the Pulsar topic '{topic}' contains an end-to-end encrypted message, and this \
         reader cannot decrypt it — it would otherwise deliver the ciphertext as if it \
         were the data. Read the topic with a consumer configured with the decryption \
         keys, or publish without end-to-end encryption"
    )]
    EncryptedMessage { topic: String },

    #[error(
        "the Pulsar topic '{topic}' contains a chunked message (a message split into \
         parts by a producer with chunking enabled), and this reader cannot reassemble \
         the chunks — it would otherwise deliver meaningless fragments as separate \
         rows. Publish without chunking (keep the messages under the broker's \
         maxMessageSize), or read the topic with a chunking-aware consumer"
    )]
    ChunkedMessage { topic: String },

    #[error(
        "the Pulsar topic '{topic}' has been expanded from {old} to {new} partitions \
         while the pipeline was running. The reader is attached to the {old} partitions \
         the topic had at the start and cannot pick up the ones added later, so \
         everything published into them would be skipped unnoticed. Restart the \
         pipeline to read the expanded topic"
    )]
    TopicPartitionsExpanded { topic: String, old: u32, new: u32 },
}

impl PulsarError {
    /// A copy of a data-fatal error, kept by the reader to resurface on the
    /// later reads (the error type as a whole is not `Clone`).
    fn clone_data_fatal(&self) -> PulsarError {
        match self {
            PulsarError::EncryptedMessage { topic } => PulsarError::EncryptedMessage {
                topic: topic.clone(),
            },
            PulsarError::ChunkedMessage { topic } => PulsarError::ChunkedMessage {
                topic: topic.clone(),
            },
            _ => panic!("only the data-fatal errors are kept for resurfacing"),
        }
    }
}

/// The position of a message within one partition: `(ledger_id, entry_id,
/// batch_index)`, ordered lexicographically. All the messages of one producer
/// batch share a single `(ledger_id, entry_id)` pair and differ only in the
/// batch index (`-1` for non-batched messages).
pub type MessagePosition = (u64, u64, i32);

/// A watermark below every real message position: no message compares at or
/// before it (the batch index of a real message is at least `-1`), so a pump
/// started after this watermark delivers the whole partition. Used to encode
/// a resolved `start_from="end"` on a partition that was empty at the
/// resolution moment.
const DELIVER_EVERYTHING: MessagePosition = (0, 0, i32::MIN);

/// The tail of a partition as it was when the read started, resolved by the
/// reader before it spawns the partition's pump.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TailAtStart {
    /// Not resolved: the pump resumes after its watermark or within its
    /// boundary instead (a respawned pump, a restored position).
    Unknown,
    /// The partition held no messages.
    Empty,
    /// The position of the partition's last message.
    Last(MessagePosition),
}

/// A message taken from a consumer, in the form the reader hands over to the
/// engine.
struct PreloadedMessage {
    payload: Vec<u8>,
    partition_key: Option<String>,
    publish_time: u64,
    /// Built only when the user requested the `_metadata` column.
    metadata: Option<Box<PulsarMetadata>>,
}

/// A message delivered by a partition pump task.
struct PumpedMessage {
    partition: i32,
    position: MessagePosition,
    payload: Vec<u8>,
    partition_key: Option<String>,
    /// Built only when the user requested the `_metadata` column.
    metadata: Option<Box<PulsarMetadata>>,
}

/// Builds the user-facing metadata of one message from the message id and
/// the protocol-level metadata the client delivered.
fn build_message_metadata(
    topic: &str,
    id: &MessageIdData,
    proto_metadata: &mut pulsar::message::Metadata,
) -> Box<PulsarMetadata> {
    let properties = take(&mut proto_metadata.properties)
        .into_iter()
        .map(|kv| (kv.key, kv.value))
        .collect();
    // The registry stamps the version as 8 big-endian bytes; anything else
    // would be a non-standard broker, reported as an absent version rather
    // than a bogus number.
    let schema_version = proto_metadata
        .schema_version
        .as_deref()
        .and_then(|bytes| Some(u64::from_be_bytes(bytes.try_into().ok()?)));
    Box::new(PulsarMetadata::new(
        topic.to_string(),
        id.partition.unwrap_or(-1),
        id.ledger_id,
        id.entry_id,
        id.batch_index(),
        proto_metadata.publish_time,
        proto_metadata.event_time,
        take(&mut proto_metadata.producer_name),
        proto_metadata.ordering_key.as_deref(),
        schema_version,
        properties,
    ))
}

/// The admission check of one incoming message, shared by both reading
/// mechanisms so they can never diverge on it. `Ok(true)` — deliver.
/// `Ok(false)` — below a `start_from="timestamp"` position, skipped
/// silently; this deliberately also skips the poison checks, so an
/// undeliverable era of the topic below the threshold does not kill a read
/// positioned above it. `Err` — the message is within the delivered range
/// but cannot be delivered faithfully: a chunked message is a fragment of a
/// larger payload and an encrypted one is ciphertext, so the reader reports
/// a terminal error instead of silently corrupting the data — the topic
/// needs a client that can reassemble or decrypt, which this reader is not.
fn admit_message(
    proto_metadata: &pulsar::message::Metadata,
    min_publish_timestamp_ms: Option<u64>,
    topic: &str,
) -> Result<bool, PulsarError> {
    if min_publish_timestamp_ms.is_some_and(|t| proto_metadata.publish_time < t) {
        return Ok(false);
    }
    if !proto_metadata.encryption_keys.is_empty() {
        return Err(PulsarError::EncryptedMessage {
            topic: topic.to_string(),
        });
    }
    if proto_metadata.num_chunks_from_msg.is_some_and(|n| n > 1)
        || proto_metadata.chunk_id.is_some()
    {
        return Err(PulsarError::ChunkedMessage {
            topic: topic.to_string(),
        });
    }
    Ok(true)
}

/// Whether a read error reports data this reader can never deliver
/// faithfully (see [`admit_message`]). Such an error repeats
/// on every retry, so it must take the pipeline down at once.
fn read_error_is_fatal_for_data(error: &ReadError) -> bool {
    matches!(
        error,
        ReadError::Pulsar(
            PulsarError::EncryptedMessage { .. } | PulsarError::ChunkedMessage { .. }
        )
    )
}

/// What a partition pump task reports to the reader.
enum PumpEvent {
    Message(PumpedMessage),
    /// The partition is read up to its static boundary (static mode only).
    Drained,
    /// The pump died. The reader respawns it: `resume_after` is the pump's
    /// delivery watermark (the respawned pump continues right after it), and
    /// `resolved_boundary` carries the already-resolved static boundary so
    /// that a retry doesn't extend the static message set with the data
    /// published after the original snapshot was taken.
    Failed {
        partition: i32,
        error: PulsarError,
        resume_after: Option<MessagePosition>,
        resolved_boundary: Option<MessagePosition>,
    },
}

/// Everything needed to create the consumer of the subscription mode. The
/// reader keeps it so that it can recreate a consumer whose stream ended:
/// the client library reconnects on its own only within its retry budget,
/// and an outage that outlasts it (a broker restart, most commonly) leaves
/// the stream exhausted for good. A reader attached to such a stream stays
/// alive and delivers nothing, so it must build a new consumer instead.
pub struct PulsarSubscriptionSpec {
    pub topic: String,
    pub subscription_name: String,
    pub subscription_type: PulsarSubType,
    pub consumer_name: String,
    pub options: PulsarConsumerOptions,
}

impl PulsarSubscriptionSpec {
    /// Builds a consumer of the subscription. Both the initial consumer and
    /// the rebuilds after an exhausted stream go through this, so the two
    /// can never diverge in their options.
    pub async fn build_consumer(
        &self,
        client: &Pulsar<TokioExecutor>,
    ) -> Result<PulsarConsumer<Vec<u8>, TokioExecutor>, pulsar::Error> {
        client
            .consumer()
            .with_topic(&self.topic)
            .with_subscription(&self.subscription_name)
            .with_subscription_type(self.subscription_type)
            .with_consumer_name(&self.consumer_name)
            .with_options(self.options.clone())
            .build()
            .await
    }
}

enum PulsarReaderMode {
    /// Streaming through a broker-side subscription. The subscription cursor
    /// is advanced by immediate acknowledgements, so this mode cannot
    /// guarantee lossless recovery and is not allowed with persistence.
    Subscription {
        // `None` between the moment the consumer's stream ends and the
        // rebuild the next read performs.
        consumer: Option<Box<PulsarConsumer<Vec<u8>, TokioExecutor>>>,
        spec: Box<PulsarSubscriptionSpec>,
        preloaded: VecDeque<PreloadedMessage>,
    },
    /// A reader that never consumes anything. The engine constructs a reader
    /// object on every worker, but the single-consumer subscription types
    /// (exclusive, failover) may only connect from one worker — a second
    /// consumer joining the same exclusive subscription would be rejected by
    /// the broker with `ConsumerBusy` and fail the whole pipeline. An idle
    /// reader owns no client at all and reports an immediately finished
    /// source if it is ever polled.
    Idle,
    /// Kafka-like reading: every partition is an independent log consumed by
    /// its own non-durable exclusive consumer from an explicit position, and
    /// the per-partition positions of the delivered messages are the reader's
    /// offsets. Restart recovery replays each partition from the positions of
    /// the last durable checkpoint — nothing depends on broker-side cursors
    /// or acknowledgements, so no message can be lost or double-delivered by
    /// the recovery itself. Used for the static mode and for every persistent
    /// pipeline.
    PartitionReaders {
        partitions: Vec<i32>,
        static_mode: bool,
        start_from_latest: bool,
        positions: HashMap<i32, MessagePosition>,
        // The tail position of every owned partition as it was when the read
        // started (`Empty` for one without messages), resolved by the reader
        // itself before the first pump is spawned and kept across the
        // retries of a failed start. In static mode it is the boundary of
        // the fixed message set; for `start_from_latest` it is the position
        // the delivery starts after. Resolving it in the pumps, at whatever
        // time each of them first managed to connect, tied the message set
        // to the pumps' luck: a pump that died before resolving its boundary
        // (a broker outage right after the read started, while its
        // consumer was still being set up) was respawned without one,
        // resolved a *later* boundary and let the messages published during
        // the outage into a "static" snapshot.
        tails_at_start: HashMap<i32, TailAtStart>,
        pump: Option<PartitionPump>,
    },
}

struct PartitionPump {
    receiver: mpsc::Receiver<PumpEvent>,
    // Kept for respawning the pumps of failed partitions. Because of this
    // sender the channel never reports "closed", so the end of a static
    // read is tracked by counting `Drained` events instead.
    sender: mpsc::Sender<PumpEvent>,
    // Events taken from the channel in bulk (`recv_many`) and not yet
    // consumed: entering the runtime is paid once per batch, not once per
    // message.
    buffered: VecDeque<PumpEvent>,
    // `Some(n)` in static mode: the number of partitions not yet drained.
    // `None` in streaming mode, which never finishes.
    remaining_static_partitions: Option<usize>,
    // Dropped under `runtime.enter()` together with the runtime; the tasks
    // themselves end when their channel sender fails or the runtime dies.
    join_handles: Vec<JoinHandle<()>>,
}

/// The background check of the topic's partition count (see
/// [`PulsarError::TopicPartitionsExpanded`]). The task reports the new count
/// once, when it first sees one above `initial_count`, and then ends.
struct PartitionWatch {
    initial_count: u32,
    receiver: mpsc::Receiver<u32>,
    // Dropped under `runtime.enter()` together with the runtime.
    _handle: JoinHandle<()>,
}

#[allow(clippy::module_name_repetitions)]
pub struct PulsarReader {
    runtime: TokioRuntime,
    // `None` only in the idle mode, which never talks to the broker.
    client: Option<Pulsar<TokioExecutor>>,
    base_topic: arcstr::ArcStr,
    worker_index: usize,
    connector_index: usize,
    total_entries_read: usize,
    // The `start_from="timestamp"` positioning. The broker-side seek of the
    // client library destroys and recreates the consumer and proved to be
    // unreliable, so the reader instead starts from the earliest position and
    // filters out the messages published before this timestamp.
    min_publish_timestamp_ms: Option<u64>,
    // Whether the user requested the `_metadata` column: only then is the
    // per-message metadata collected and reported to the engine.
    with_metadata: bool,
    // A metadata event is emitted *before* the data event of its message, so
    // the data event waits here for the next `read` call — the same pattern
    // the Kafka and RabbitMQ readers use.
    deferred_read_result: Option<ReadResult>,
    // Present in the streaming modes; a static read is defined by the
    // snapshot taken at its start, so the partitions added later are outside
    // of its message set by construction.
    partition_watch: Option<PartitionWatch>,
    // The count the watch reported, once it did. It makes the expansion
    // error repeat on every subsequent read and turns it terminal (see
    // `max_allowed_consecutive_errors`).
    expanded_partition_count: Option<u32>,
    // Set when the topic turns out to contain a message this reader can
    // never deliver faithfully (chunked, encrypted): retrying meets the
    // same message again, so the first such error is terminal.
    fatal_data_error_seen: bool,
    // A fatal data error met behind messages that were already acknowledged
    // but not yet delivered. Failing at once would drop them — and the
    // durable cursor is already past them on the broker — so the error is
    // kept and resurfaces instead, non-terminally for
    // `TERMINAL_ERROR_COMMIT_GRACE` (see `stage_terminal_error`).
    deferred_fatal_error: Option<PulsarError>,
    // When a terminal-class error of the subscription mode first surfaced;
    // it turns terminal once the commit grace has elapsed.
    terminal_error_first_surfaced: Option<Instant>,
    mode: PulsarReaderMode,
}

impl PulsarReader {
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_subscription(
        runtime: TokioRuntime,
        client: Pulsar<TokioExecutor>,
        consumer: PulsarConsumer<Vec<u8>, TokioExecutor>,
        spec: PulsarSubscriptionSpec,
        base_topic: arcstr::ArcStr,
        worker_index: usize,
        connector_index: usize,
        min_publish_timestamp_ms: Option<u64>,
        with_metadata: bool,
    ) -> PulsarReader {
        PulsarReader {
            runtime,
            client: Some(client),
            base_topic,
            worker_index,
            connector_index,
            total_entries_read: 0,
            min_publish_timestamp_ms,
            with_metadata,
            deferred_read_result: None,
            partition_watch: None,
            expanded_partition_count: None,
            fatal_data_error_seen: false,
            deferred_fatal_error: None,
            terminal_error_first_surfaced: None,
            mode: PulsarReaderMode::Subscription {
                consumer: Some(Box::new(consumer)),
                spec: Box::new(spec),
                preloaded: VecDeque::new(),
            },
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_partition_readers(
        runtime: TokioRuntime,
        client: Pulsar<TokioExecutor>,
        base_topic: arcstr::ArcStr,
        partitions: Vec<i32>,
        static_mode: bool,
        start_from_latest: bool,
        worker_index: usize,
        connector_index: usize,
        min_publish_timestamp_ms: Option<u64>,
        with_metadata: bool,
    ) -> PulsarReader {
        PulsarReader {
            runtime,
            client: Some(client),
            base_topic,
            worker_index,
            connector_index,
            total_entries_read: 0,
            min_publish_timestamp_ms,
            with_metadata,
            deferred_read_result: None,
            partition_watch: None,
            expanded_partition_count: None,
            fatal_data_error_seen: false,
            deferred_fatal_error: None,
            terminal_error_first_surfaced: None,
            mode: PulsarReaderMode::PartitionReaders {
                partitions,
                static_mode,
                start_from_latest,
                positions: HashMap::new(),
                tails_at_start: HashMap::new(),
                pump: None,
            },
        }
    }

    /// See [`PulsarReaderMode::Idle`].
    pub fn new_idle(
        runtime: TokioRuntime,
        base_topic: arcstr::ArcStr,
        worker_index: usize,
        connector_index: usize,
    ) -> PulsarReader {
        PulsarReader {
            runtime,
            client: None,
            base_topic,
            worker_index,
            connector_index,
            total_entries_read: 0,
            min_publish_timestamp_ms: None,
            with_metadata: false,
            deferred_read_result: None,
            partition_watch: None,
            expanded_partition_count: None,
            fatal_data_error_seen: false,
            deferred_fatal_error: None,
            terminal_error_first_surfaced: None,
            mode: PulsarReaderMode::Idle,
        }
    }

    /// Starts the background check of the topic's partition count (see
    /// [`PulsarError::TopicPartitionsExpanded`]). Called by the construction
    /// sites of the streaming modes with the count the reader was positioned
    /// on.
    pub fn watch_partition_count(&mut self, initial_count: u32) {
        let Some(client) = self.client.clone() else {
            return; // the idle mode never reads
        };
        let topic = self.base_topic.to_string();
        let (sender, receiver) = mpsc::channel(1);
        let _guard = self.runtime.enter();
        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(PARTITION_COUNT_CHECK_INTERVAL).await;
                match Box::pin(client.lookup_partitioned_topic_number(&topic)).await {
                    Ok(count) if count > initial_count => {
                        let _ = sender.send(count).await;
                        return;
                    }
                    Ok(_) => {}
                    Err(error) => {
                        // A failed lookup says nothing about the topic; the
                        // reading path reports the connection problems.
                        warn!(
                            "failed to check the partition count of the Pulsar topic \
                             '{topic}': {error}"
                        );
                    }
                }
            }
        });
        self.partition_watch = Some(PartitionWatch {
            initial_count,
            receiver,
            _handle: handle,
        });
    }

    /// Marks one surfacing of a terminal-class error of the subscription
    /// mode. The rows acknowledged ahead of the error may sit in a minibatch
    /// the engine has not committed yet, and the broker's cursor is already
    /// past them — so the error stays within the ordinary budget (each
    /// surfacing costing one connector error backoff) until the commit grace
    /// elapses, and only then turns terminal.
    fn stage_terminal_error(&mut self) {
        let first = *self
            .terminal_error_first_surfaced
            .get_or_insert_with(Instant::now);
        if first.elapsed() >= TERMINAL_ERROR_COMMIT_GRACE {
            self.fatal_data_error_seen = true;
        }
    }

    /// Fails the read once the watch has seen the topic grow. Called before
    /// the reader waits for messages — both to notice the expansion on an
    /// idle topic and to keep it off the per-message path.
    fn check_partition_count(&mut self) -> Result<(), ReadError> {
        if let Some(watch) = &mut self.partition_watch {
            if let Ok(new_count) = watch.receiver.try_recv() {
                self.expanded_partition_count = Some(new_count);
            }
        }
        if let Some(new) = self.expanded_partition_count {
            if matches!(self.mode, PulsarReaderMode::Subscription { .. }) {
                // The subscription consumer acknowledges on read, so an
                // instantly terminal error would drop the acknowledged rows
                // of an uncommitted minibatch (see `stage_terminal_error`).
                // The partition-reader mode acknowledges nothing and stays
                // instantly terminal (see
                // `max_allowed_consecutive_errors`).
                self.stage_terminal_error();
            }
            return Err(PulsarError::TopicPartitionsExpanded {
                topic: self.base_topic.to_string(),
                old: self
                    .partition_watch
                    .as_ref()
                    .map_or(0, |watch| watch.initial_count),
                new,
            }
            .into());
        }
        Ok(())
    }

    fn physical_topic(base_topic: &str, partition: i32) -> String {
        if partition < 0 {
            base_topic.to_string()
        } else {
            format!("{base_topic}-partition-{partition}")
        }
    }

    /// Resolves the tail position of every owned partition that still lacks
    /// one (see `PulsarReaderMode::PartitionReaders::tails_at_start`). Only
    /// the modes whose message set depends on it pay the lookup. A failure
    /// leaves the tails resolved so far in place, so the retried start
    /// (paced by the connector's error backoff) does not move them.
    fn resolve_partition_tails(&mut self) -> Result<(), ReadError> {
        let PulsarReaderMode::PartitionReaders {
            partitions,
            static_mode,
            start_from_latest,
            positions,
            tails_at_start,
            ..
        } = &mut self.mode
        else {
            unreachable!("the pump is only started in the partition-reader mode");
        };
        if !*static_mode && !*start_from_latest {
            return Ok(());
        }
        let client = self
            .client
            .as_ref()
            .expect("the partition-reader mode always owns a client");
        for partition in partitions.iter().copied() {
            if tails_at_start.contains_key(&partition) {
                continue;
            }
            if !*static_mode && positions.contains_key(&partition) {
                // A restored position already says where this partition's
                // delivery continues; "latest" only applies to a fresh start.
                tails_at_start.insert(partition, TailAtStart::Unknown);
                continue;
            }
            let physical_topic = Self::physical_topic(&self.base_topic, partition);
            let subscription_name = format!(
                "pathway-reader-{}-{}-p{partition}-tail-{}",
                self.connector_index,
                self.worker_index,
                Self::subscription_nonce()
            );
            let tail = self.runtime.block_on(resolve_partition_tail(
                client,
                &physical_topic,
                subscription_name,
                partition,
            ))?;
            tails_at_start.insert(partition, tail);
        }
        Ok(())
    }

    /// Spawns one pump task per owned partition. Called lazily on the first
    /// `read`, so that `seek` has already restored the per-partition
    /// positions by the time the consumers are positioned. The tails of the
    /// partitions are resolved first; an error here leaves the pump
    /// unstarted, and the next `read` tries again.
    fn start_partition_pump(&mut self) -> Result<PartitionPump, ReadError> {
        self.resolve_partition_tails()?;
        let PulsarReaderMode::PartitionReaders {
            partitions,
            static_mode,
            positions,
            tails_at_start,
            ..
        } = &self.mode
        else {
            unreachable!("the pump is only started in the partition-reader mode");
        };
        let (partitions, static_mode) = (partitions.clone(), *static_mode);
        let starts: Vec<(Option<MessagePosition>, TailAtStart)> = partitions
            .iter()
            .map(|partition| {
                (
                    positions.get(partition).copied(),
                    tails_at_start
                        .get(partition)
                        .copied()
                        .unwrap_or(TailAtStart::Unknown),
                )
            })
            .collect();
        let (sender, receiver) = mpsc::channel(PARTITION_PUMP_CHANNEL_CAPACITY);
        let join_handles = partitions
            .iter()
            .zip(starts)
            .map(|(partition, (start_after, tail_at_start))| {
                self.spawn_pump(
                    *partition,
                    start_after,
                    None,
                    tail_at_start,
                    Duration::ZERO,
                    sender.clone(),
                )
            })
            .collect();
        Ok(PartitionPump {
            receiver,
            sender,
            buffered: VecDeque::new(),
            remaining_static_partitions: static_mode.then_some(partitions.len()),
            join_handles,
        })
    }

    /// The part of a subscription name that makes it unique: the consumers
    /// are exclusive, and a lingering consumer of a previous (possibly
    /// killed) run or of this partition's previous pump would fail the
    /// subscription with `ConsumerBusy`.
    fn subscription_nonce() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time is past the epoch")
            .as_nanos()
    }

    /// Spawns the pump task of one partition.
    fn spawn_pump(
        &self,
        partition: i32,
        start_after: Option<MessagePosition>,
        known_boundary: Option<MessagePosition>,
        tail_at_start: TailAtStart,
        initial_delay: Duration,
        sender: mpsc::Sender<PumpEvent>,
    ) -> JoinHandle<()> {
        let PulsarReaderMode::PartitionReaders {
            static_mode,
            start_from_latest,
            ..
        } = &self.mode
        else {
            unreachable!("pumps only exist in the partition-reader mode");
        };
        let subscription_name = format!(
            "pathway-reader-{}-{}-p{partition}-{}",
            self.connector_index,
            self.worker_index,
            Self::subscription_nonce()
        );
        let _guard = self.runtime.enter();
        tokio::spawn(pump_partition(
            self.client
                .as_ref()
                .expect("the partition-reader mode always owns a client")
                .clone(),
            Self::physical_topic(&self.base_topic, partition),
            subscription_name,
            partition,
            start_after,
            known_boundary,
            tail_at_start,
            *static_mode,
            *start_from_latest,
            self.min_publish_timestamp_ms,
            self.with_metadata,
            initial_delay,
            sender,
        ))
    }

    /// Respawns the pump of a partition whose previous pump task died,
    /// continuing right after the delivery watermark that the dying pump
    /// reported. Transient broker problems therefore auto-heal, while a
    /// persistent one keeps surfacing an error per respawn until the engine
    /// gives up on the connector.
    fn respawn_pump(
        &mut self,
        partition: i32,
        resume_after: Option<MessagePosition>,
        resolved_boundary: Option<MessagePosition>,
    ) {
        let PulsarReaderMode::PartitionReaders {
            pump: Some(pump), ..
        } = &self.mode
        else {
            unreachable!("a pump failure is only observed while the pump exists");
        };
        let sender = pump.sender.clone();
        let handle = self.spawn_pump(
            partition,
            resume_after,
            resolved_boundary,
            TailAtStart::Unknown,
            PUMP_RESPAWN_DELAY,
            sender,
        );
        if let PulsarReaderMode::PartitionReaders {
            pump: Some(pump), ..
        } = &mut self.mode
        {
            pump.join_handles.push(handle);
        }
    }

    /// Waits for pump events and moves them into the pump's local buffer.
    /// `Some(ReadResult)` short-circuits the read (the source is finished);
    /// `None` means the buffer is non-empty and the caller proceeds.
    fn refill_partition_pump(&mut self) -> Result<Option<ReadResult>, ReadError> {
        // The check runs on every refill, i.e. at least once per
        // `MAX_READ_BATCH_SIZE` messages: a busy topic must report its
        // expansion no later than an idle one — the bounded wait below only
        // covers the case of no messages at all.
        self.check_partition_count()?;
        let PulsarReaderMode::PartitionReaders { pump, .. } = &mut self.mode else {
            unreachable!("checked by the caller");
        };
        let pump = pump.as_mut().expect("the pump is started by the caller");
        if pump.remaining_static_partitions == Some(0) {
            // Static mode: every partition is drained up to its boundary.
            return Ok(Some(ReadResult::Finished));
        }
        let mut chunk = Vec::new();
        // The wait is bounded while the partition count is watched, so an
        // expansion is reported even when the partitions this reader owns
        // stay silent. `recv_many` is cancel-safe: a timed-out wait has
        // taken nothing off the channel.
        let received = if self.partition_watch.is_some() {
            // The timer is created inside the runtime: constructing it
            // outside a reactor context panics.
            let waited = self.runtime.block_on(async {
                tokio::time::timeout(
                    PARTITION_COUNT_CHECK_INTERVAL,
                    pump.receiver.recv_many(&mut chunk, MAX_READ_BATCH_SIZE),
                )
                .await
            });
            match waited {
                Ok(received) => received,
                Err(_elapsed) => {
                    self.check_partition_count()?;
                    return Ok(None);
                }
            }
        } else {
            self.runtime
                .block_on(pump.receiver.recv_many(&mut chunk, MAX_READ_BATCH_SIZE))
        };
        if received == 0 {
            // Unreachable while the respawn sender is held; kept as a
            // defensive exit instead of a busy loop.
            return Ok(Some(ReadResult::Finished));
        }
        pump.buffered.extend(chunk);
        Ok(None)
    }

    fn read_from_partition_pump(&mut self) -> Result<ReadResult, ReadError> {
        if let PulsarReaderMode::PartitionReaders {
            partitions, pump, ..
        } = &mut self.mode
        {
            if partitions.is_empty() {
                // A worker that owns no partitions (more readers than
                // partitions) has nothing to do in either mode.
                return Ok(ReadResult::Finished);
            }
            if pump.is_none() {
                let started = self.start_partition_pump()?;
                if let PulsarReaderMode::PartitionReaders { pump, .. } = &mut self.mode {
                    *pump = Some(started);
                }
            }
        }
        loop {
            let refill_needed = {
                let PulsarReaderMode::PartitionReaders { pump, .. } = &mut self.mode else {
                    unreachable!("checked by the caller");
                };
                pump.as_mut()
                    .expect("the pump is started above")
                    .buffered
                    .is_empty()
            };
            if refill_needed {
                if let Some(read_result) = self.refill_partition_pump()? {
                    return Ok(read_result);
                }
            }
            let PulsarReaderMode::PartitionReaders {
                positions, pump, ..
            } = &mut self.mode
            else {
                unreachable!("checked by the caller");
            };
            let pump = pump.as_mut().expect("the pump is started above");
            if pump.buffered.is_empty() {
                // The bounded wait of the refill timed out; try again.
                continue;
            }
            match pump.buffered.pop_front().expect("checked to be non-empty") {
                PumpEvent::Message(message) => {
                    positions.insert(message.partition, message.position);
                    self.total_entries_read += 1;
                    let payload = ReaderContext::from_key_value(
                        message.partition_key.map(String::into_bytes),
                        Some(message.payload),
                    );
                    let (ledger_id, entry_id, batch_index) = message.position;
                    let offset = (
                        OffsetKey::Pulsar(PulsarOffsetKey::Partition(
                            self.base_topic.clone(),
                            message.partition,
                        )),
                        OffsetValue::Pulsar(PulsarOffsetValue::MessagePosition {
                            ledger_id,
                            entry_id,
                            batch_index,
                        }),
                    );
                    if let Some(metadata) = message.metadata {
                        self.deferred_read_result = Some(ReadResult::Data(payload, offset));
                        return Ok(ReadResult::NewSource((*metadata).into()));
                    }
                    return Ok(ReadResult::Data(payload, offset));
                }
                PumpEvent::Drained => {
                    let remaining = pump
                        .remaining_static_partitions
                        .as_mut()
                        .expect("Drained is only sent in static mode");
                    *remaining -= 1;
                    if *remaining == 0 {
                        return Ok(ReadResult::Finished);
                    }
                }
                PumpEvent::Failed {
                    partition,
                    error,
                    resume_after,
                    resolved_boundary,
                } => {
                    let error: ReadError = error.into();
                    if read_error_is_fatal_for_data(&error) {
                        // A respawned pump would only meet the same message
                        // again; the error budget drops to zero instead.
                        self.fatal_data_error_seen = true;
                    } else {
                        self.respawn_pump(partition, resume_after, resolved_boundary);
                    }
                    return Err(error);
                }
            }
        }
    }

    /// Builds the subscription consumer when the reader has none — at the
    /// first read after the previous consumer's stream ended. The connector
    /// loop's error backoff paces the attempts, so a broker that is still
    /// down simply produces another read error.
    fn ensure_subscription_consumer(&mut self) -> Result<(), ReadError> {
        let PulsarReaderMode::Subscription { consumer, spec, .. } = &mut self.mode else {
            unreachable!("checked by the caller");
        };
        if consumer.is_some() {
            return Ok(());
        }
        let client = self
            .client
            .as_ref()
            .expect("the subscription mode always owns a client");
        let consequence = if spec.options.durable == Some(true) {
            "a durable subscription resumes from its broker-side cursor, \
             re-delivering at most the messages consumed since the cursor \
             was last saved"
        } else if matches!(spec.options.initial_position, PulsarInitialPosition::Latest) {
            "a non-durable subscription has no cursor to resume from, and \
             this one starts at the end of the topic: the messages published \
             while the connection was down are SKIPPED"
        } else {
            "a non-durable subscription has no cursor to resume from, so it \
             restarts at the beginning position and the already processed \
             messages arrive again"
        };
        warn!(
            "recreating the Pulsar consumer of the topic '{}': the previous one \
             gave up reconnecting. Note that {consequence}",
            spec.topic
        );
        let rebuilt = self
            .runtime
            .block_on(spec.build_consumer(client))
            .map_err(PulsarError::from)?;
        *consumer = Some(Box::new(rebuilt));
        Ok(())
    }

    /// Detaches the exhausted consumer so that the next read builds a fresh
    /// one, and reports the failure the connector's error budget accounts
    /// for.
    fn drop_exhausted_subscription_consumer(&mut self) -> ReadError {
        {
            // The consumer interacts with the async runtime when dropped.
            let _guard = self.runtime.enter();
            if let PulsarReaderMode::Subscription { consumer, .. } = &mut self.mode {
                consumer.take();
            }
        }
        PulsarError::StreamUnexpectedlyClosed {
            topic: self.base_topic.to_string(),
        }
        .into()
    }

    /// Takes up to `MAX_READ_BATCH_SIZE` messages from the subscription
    /// consumer in one runtime entry: waits for the first message, then keeps
    /// taking the already-delivered ones without waiting. Every message is
    /// acknowledged immediately: this mode is never used with persistence,
    /// so there is no reason to defer the cursor advancement.
    fn refill_subscription_preloaded(&mut self) -> Result<(), ReadError> {
        if let Some(error) = &self.deferred_fatal_error {
            // Everything acknowledged ahead of the poison message — in this
            // refill or the earlier ones — has been delivered by now. The
            // broker will not redeliver the unacknowledged poison message to
            // this live consumer, so the kept error resurfaces here instead —
            // terminally, once the commit grace has passed (see
            // `stage_terminal_error`).
            let error = error.clone_data_fatal();
            self.stage_terminal_error();
            return Err(error.into());
        }
        self.ensure_subscription_consumer()?;
        let PulsarReaderMode::Subscription {
            consumer,
            preloaded,
            ..
        } = &mut self.mode
        else {
            unreachable!("checked by the caller");
        };
        let consumer = consumer.as_mut().expect("built above");
        let base_topic = self.base_topic.to_string();
        let min_publish_timestamp_ms = self.min_publish_timestamp_ms;
        let with_metadata = self.with_metadata;
        let mut deferred: Option<PulsarError> = None;
        let preload_message = |message: pulsar::consumer::Message<Vec<u8>>| -> PreloadedMessage {
            let mut proto_metadata = message.payload.metadata;
            let metadata = with_metadata.then(|| {
                build_message_metadata(&message.topic, &message.message_id.id, &mut proto_metadata)
            });
            PreloadedMessage {
                publish_time: proto_metadata.publish_time,
                partition_key: proto_metadata.partition_key,
                payload: message.payload.data,
                metadata,
            }
        };
        // The wait for the first message is bounded while the partition
        // count is watched, so an expansion is noticed on an idle topic too;
        // the caller re-checks it and returns without progress.
        let watching = self.partition_watch.is_some();
        // An exhausted consumer stream is never a normal end of data here:
        // the topic is unbounded, so it means the client gave up. `false`
        // reports it, and the consumer is replaced outside of this borrow —
        // reporting `Finished` instead would let a streaming pipeline
        // silently "complete" and ignore everything published afterwards.
        let stream_alive = self.runtime.block_on(async {
            let first_message = if watching {
                match tokio::time::timeout(PARTITION_COUNT_CHECK_INTERVAL, consumer.next()).await {
                    Ok(message) => message,
                    Err(_elapsed) => return Ok(true),
                }
            } else {
                consumer.next().await
            };
            let Some(first_message) = first_message else {
                return Ok(false);
            };
            let first_message = first_message.map_err(PulsarError::from)?;
            if let Err(error) = admit_message(
                &first_message.payload.metadata,
                min_publish_timestamp_ms,
                &base_topic,
            ) {
                // Deferred rather than returned (see below): the messages
                // acknowledged by the earlier refills may still sit in an
                // uncommitted minibatch, which an instantly terminal error
                // would drop.
                deferred = Some(error);
                return Ok(true);
            }
            consumer
                .ack(&first_message)
                .await
                .map_err(PulsarError::from)?;
            preloaded.push_back(preload_message(first_message));
            while preloaded.len() < MAX_READ_BATCH_SIZE {
                let message = match consumer.next().now_or_never() {
                    None => break,
                    Some(None) => return Ok(false),
                    Some(Some(message)) => message.map_err(PulsarError::from)?,
                };
                if let Err(error) = admit_message(
                    &message.payload.metadata,
                    min_publish_timestamp_ms,
                    &base_topic,
                ) {
                    // The messages preloaded above are already acknowledged:
                    // the broker's cursor is past them, so they must reach
                    // the engine before this error kills the pipeline. The
                    // poison message itself is left unacknowledged.
                    deferred = Some(error);
                    break;
                }
                consumer.ack(&message).await.map_err(PulsarError::from)?;
                preloaded.push_back(preload_message(message));
            }
            Ok::<bool, ReadError>(true)
        });
        let stream_alive = match stream_alive {
            Ok(alive) => alive,
            Err(error) => {
                if read_error_is_fatal_for_data(&error) {
                    self.fatal_data_error_seen = true;
                }
                return Err(error);
            }
        };
        if let Some(error) = deferred {
            self.deferred_fatal_error = Some(error);
        }
        if stream_alive {
            Ok(())
        } else {
            Err(self.drop_exhausted_subscription_consumer())
        }
    }

    fn read_from_subscription(&mut self) -> Result<ReadResult, ReadError> {
        loop {
            let PulsarReaderMode::Subscription { preloaded, .. } = &mut self.mode else {
                unreachable!("checked by the caller");
            };
            let Some(message) = preloaded.pop_front() else {
                self.check_partition_count()?;
                self.refill_subscription_preloaded()?;
                continue;
            };
            if let Some(threshold) = self.min_publish_timestamp_ms {
                if message.publish_time < threshold {
                    continue;
                }
            }
            self.total_entries_read += 1;
            let payload = ReaderContext::from_key_value(
                message.partition_key.map(String::into_bytes),
                Some(message.payload),
            );
            let offset = (
                OffsetKey::Pulsar(PulsarOffsetKey::Worker(self.worker_index)),
                OffsetValue::Pulsar(PulsarOffsetValue::EntriesCount(self.total_entries_read)),
            );
            if let Some(metadata) = message.metadata {
                self.deferred_read_result = Some(ReadResult::Data(payload, offset));
                return Ok(ReadResult::NewSource((*metadata).into()));
            }
            return Ok(ReadResult::Data(payload, offset));
        }
    }
}

/// The pump task of one partition in the partition-reader mode: an exclusive
/// non-durable consumer created directly at the requested position, whose
/// messages are forwarded into the reader's channel. The task ends when the
/// static boundary is reached, when the reader goes away, or on an error
/// (reported through the same channel together with the resume state, so
/// the reader can respawn the pump without losing or repeating messages).
#[allow(clippy::too_many_arguments)]
async fn pump_partition(
    client: Pulsar<TokioExecutor>,
    physical_topic: String,
    subscription_name: String,
    partition: i32,
    start_after: Option<MessagePosition>,
    known_boundary: Option<MessagePosition>,
    tail_at_start: TailAtStart,
    static_mode: bool,
    start_from_latest: bool,
    min_publish_timestamp_ms: Option<u64>,
    with_metadata: bool,
    initial_delay: Duration,
    sender: mpsc::Sender<PumpEvent>,
) {
    if !initial_delay.is_zero() {
        tokio::time::sleep(initial_delay).await;
    }
    // The delivery watermark: everything at or before it has already been
    // handed to the reader (or deterministically skipped). It both drives
    // the deduplication inside the pump and, on a failure, tells the reader
    // where the respawned pump must resume.
    let mut watermark = start_after;
    let mut boundary = known_boundary;
    let result = pump_partition_inner(
        client,
        &physical_topic,
        subscription_name,
        partition,
        tail_at_start,
        static_mode,
        start_from_latest,
        min_publish_timestamp_ms,
        with_metadata,
        &mut watermark,
        &mut boundary,
        &sender,
    )
    .await;
    // The reader may already be gone; then the events are irrelevant.
    match result {
        Ok(()) => {
            if static_mode {
                let _ = sender.send(PumpEvent::Drained).await;
            }
        }
        Err(error) => {
            let _ = sender
                .send(PumpEvent::Failed {
                    partition,
                    error,
                    resume_after: watermark,
                    resolved_boundary: boundary,
                })
                .await;
        }
    }
}

/// Builds the exclusive non-durable consumer of one partition pump,
/// positioned right after `start_after` when a delivery watermark exists.
async fn build_pump_consumer(
    client: &Pulsar<TokioExecutor>,
    physical_topic: &str,
    subscription_name: String,
    partition: i32,
    start_from_latest: bool,
    start_after: Option<MessagePosition>,
) -> Result<PulsarConsumer<Vec<u8>, TokioExecutor>, PulsarError> {
    let mut options = PulsarConsumerOptions::default()
        .durable(false)
        .with_initial_position(if start_from_latest {
            PulsarInitialPosition::Latest
        } else {
            PulsarInitialPosition::Earliest
        });
    if let Some((ledger_id, entry_id, batch_index)) = start_after {
        options = options.starting_on_message(MessageIdData {
            ledger_id,
            entry_id,
            batch_index: (batch_index >= 0).then_some(batch_index),
            partition: (partition >= 0).then_some(partition),
            ..MessageIdData::default()
        });
    }
    Ok(client
        .consumer()
        .with_topic(physical_topic)
        .with_subscription(subscription_name)
        .with_subscription_type(PulsarSubType::Exclusive)
        .with_options(options)
        .build()
        .await?)
}

/// The position of the last existing message of the partition, or `None`
/// if the partition is empty (`entry_id == u64::MAX` is the broker's
/// encoding of `entryId = -1`).
async fn partition_tail_position(
    consumer: &mut PulsarConsumer<Vec<u8>, TokioExecutor>,
) -> Result<Option<MessagePosition>, PulsarError> {
    let last_message_ids = Box::pin(consumer.get_last_message_id()).await?;
    Ok(last_message_ids
        .into_iter()
        .next()
        .filter(|id| id.entry_id != u64::MAX && id.ledger_id != u64::MAX)
        .map(|id| (id.ledger_id, id.entry_id, id.batch_index())))
}

/// The tail position of a partition, looked up through a short-lived
/// consumer of its own (the only way the client exposes `getLastMessageId`).
async fn resolve_partition_tail(
    client: &Pulsar<TokioExecutor>,
    physical_topic: &str,
    subscription_name: String,
    partition: i32,
) -> Result<TailAtStart, PulsarError> {
    let mut consumer = Box::pin(build_pump_consumer(
        client,
        physical_topic,
        subscription_name,
        partition,
        true,
        None,
    ))
    .await?;
    let tail = partition_tail_position(&mut consumer).await;
    if let Err(error) = Box::pin(consumer.close()).await {
        debug!("could not close the tail-lookup consumer of '{physical_topic}': {error}");
    }
    Ok(tail?.map_or(TailAtStart::Empty, TailAtStart::Last))
}

#[allow(clippy::too_many_arguments)]
async fn pump_partition_inner(
    client: Pulsar<TokioExecutor>,
    physical_topic: &str,
    subscription_name: String,
    partition: i32,
    tail_at_start: TailAtStart,
    static_mode: bool,
    start_from_latest: bool,
    min_publish_timestamp_ms: Option<u64>,
    with_metadata: bool,
    watermark: &mut Option<MessagePosition>,
    boundary_slot: &mut Option<MessagePosition>,
    sender: &mpsc::Sender<PumpEvent>,
) -> Result<(), PulsarError> {
    // In static mode, starting from the latest position means the fixed
    // message set of the run is empty by definition.
    if static_mode && start_from_latest {
        return Ok(());
    }
    // The tail the reader resolved when the read started defines the
    // message set before this pump even connects, so a pump that dies
    // during its own setup (a broker outage right after the start) is
    // respawned with the same "end"/boundary instead of a later one.
    let empty_at_start = tail_at_start == TailAtStart::Empty;
    if tail_at_start != TailAtStart::Unknown {
        let tail = match tail_at_start {
            TailAtStart::Last(position) => Some(position),
            TailAtStart::Empty | TailAtStart::Unknown => None,
        };
        if start_from_latest && watermark.is_none() {
            *watermark = Some(tail.unwrap_or(DELIVER_EVERYTHING));
        }
        if static_mode && boundary_slot.is_none() {
            let Some(boundary) = tail else {
                return Ok(()); // the partition was empty when the read started
            };
            *boundary_slot = Some(boundary);
        }
    }
    let mut consumer = build_pump_consumer(
        &client,
        physical_topic,
        subscription_name,
        partition,
        // A partition that was empty at the start has nothing to skip: its
        // whole content is "after the end", so the consumer starts at the
        // beginning rather than at the latest position of the connection
        // moment, which could already be past a few messages.
        start_from_latest && !empty_at_start,
        if empty_at_start { None } else { *watermark },
    )
    .await?;

    if start_from_latest && watermark.is_none() {
        // A pump spawned without a resolved tail (the reader resolves them
        // up front, so only as a fallback): "end" becomes a concrete
        // position exactly once, at the first pump start. A respawned pump
        // continues from where the end *was*, not from the latest position
        // at the respawn moment.
        let tail = partition_tail_position(&mut consumer).await?;
        *watermark = Some(tail.unwrap_or(DELIVER_EVERYTHING));
    }

    if static_mode && boundary_slot.is_none() {
        // The same fallback for the static boundary.
        let Some(boundary) = partition_tail_position(&mut consumer).await? else {
            return Ok(()); // the partition is empty
        };
        *boundary_slot = Some(boundary);
    }
    if let Some(boundary) = *boundary_slot {
        // A boundary reported without a batch index (-1) while the watermark
        // sits *inside* that entry's producer batch is not conclusive: the
        // true end of the static set is the batch's last message, which is
        // only learned from the entry's metadata once it arrives. Skipping
        // the early exit in that case lets the loop below extend the
        // boundary and deliver the batch tail instead of dropping it.
        let boundary_may_extend = boundary.2 < 0
            && watermark.is_some_and(|watermark| {
                (watermark.0, watermark.1) == (boundary.0, boundary.1) && watermark.2 >= 0
            });
        if !boundary_may_extend && watermark.is_some_and(|watermark| watermark >= boundary) {
            // Everything was already read in the previous runs or by this
            // partition's previous pump.
            return Ok(());
        }
    }

    loop {
        let Some(message) = consumer.next().await else {
            // The stream may only end this way when the client has given up
            // (e.g. it exhausted its reconnection attempts during a broker
            // outage): a streaming partition is unbounded, and a static one
            // returns above once its boundary is reached. Surfacing an error
            // instead of finishing prevents the pipeline from silently
            // "completing" and ignoring the data published after recovery.
            return Err(PulsarError::StreamUnexpectedlyClosed {
                topic: physical_topic.to_string(),
            });
        };
        let message = message?;
        let id = &message.message_id.id;
        let position = (id.ledger_id, id.entry_id, id.batch_index());
        if let Some(boundary) = boundary_slot.as_mut() {
            // The broker may report the last message id of a batched entry
            // without a batch index. Taken literally, such a boundary would
            // exclude the whole final producer batch from the static set
            // (every message of the batch compares above it), so once the
            // boundary entry itself arrives, the boundary is extended to the
            // batch's last message using the batch size from the metadata.
            if boundary.2 < 0 && (position.0, position.1) == (boundary.0, boundary.1) {
                if let Some(batch_size) = message.payload.metadata.num_messages_in_batch {
                    boundary.2 = batch_size - 1;
                }
            }
        }
        let boundary = *boundary_slot;
        if watermark.is_some_and(|watermark| position <= watermark) {
            // Either the broker delivered the position the consumer started
            // on (whether the requested start is inclusive is a broker-side
            // detail), or the client reconnected mid-run: on a reconnection
            // it recreates the consumer at the *original* starting position,
            // redelivering everything the pump has already handled. The
            // moving watermark drops both kinds of duplicates.
            continue;
        }
        if boundary.is_some_and(|boundary| position > boundary) {
            // Published after the static snapshot was taken: not a part of
            // the fixed message set of this run.
            return Ok(());
        }
        if admit_message(
            &message.payload.metadata,
            min_publish_timestamp_ms,
            physical_topic,
        )? {
            let mut proto_metadata = message.payload.metadata;
            let metadata = with_metadata.then(|| {
                build_message_metadata(physical_topic, &message.message_id.id, &mut proto_metadata)
            });
            let sent = sender
                .send(PumpEvent::Message(PumpedMessage {
                    partition,
                    position,
                    payload: message.payload.data,
                    partition_key: proto_metadata.partition_key,
                    metadata,
                }))
                .await;
            if sent.is_err() {
                return Ok(()); // the reader is gone
            }
        }
        *watermark = Some(position);
        if boundary.is_some_and(|boundary| position >= boundary) {
            return Ok(()); // the partition is drained up to its boundary
        }
    }
}

impl Reader for PulsarReader {
    fn read(&mut self) -> Result<ReadResult, ReadError> {
        if let Some(deferred_read_result) = self.deferred_read_result.take() {
            return Ok(deferred_read_result);
        }
        match &self.mode {
            PulsarReaderMode::Subscription { .. } => self.read_from_subscription(),
            PulsarReaderMode::PartitionReaders { .. } => self.read_from_partition_pump(),
            PulsarReaderMode::Idle => Ok(ReadResult::Finished),
        }
    }

    fn seek(&mut self, frontier: &OffsetAntichain) -> Result<(), ReadError> {
        match &mut self.mode {
            PulsarReaderMode::Subscription { .. } => {
                // The subscription mode is not allowed with persistence, so
                // there is nothing to seek; keep the counter monotonic if a
                // frontier is provided anyway.
                if let Some(OffsetValue::Pulsar(PulsarOffsetValue::EntriesCount(entries_read))) =
                    frontier.get_offset(&OffsetKey::Pulsar(PulsarOffsetKey::Worker(
                        self.worker_index,
                    )))
                {
                    self.total_entries_read = *entries_read;
                }
                Ok(())
            }
            PulsarReaderMode::PartitionReaders {
                partitions,
                positions,
                pump,
                ..
            } => {
                assert!(
                    pump.is_none(),
                    "seek must precede the first read of the Pulsar reader"
                );
                for partition in partitions {
                    let key = OffsetKey::Pulsar(PulsarOffsetKey::Partition(
                        self.base_topic.clone(),
                        *partition,
                    ));
                    match frontier.get_offset(&key) {
                        Some(OffsetValue::Pulsar(PulsarOffsetValue::MessagePosition {
                            ledger_id,
                            entry_id,
                            batch_index,
                        })) => {
                            positions.insert(*partition, (*ledger_id, *entry_id, *batch_index));
                        }
                        Some(other) => {
                            error!("Unexpected offset type for Pulsar reader: {other:?}");
                        }
                        None => {}
                    }
                }
                Ok(())
            }
            PulsarReaderMode::Idle => Ok(()),
        }
    }

    fn storage_type(&self) -> StorageType {
        StorageType::Pulsar
    }

    fn max_allowed_consecutive_errors(&self) -> usize {
        // A topic expanded under a running pipeline never heals by itself,
        // and every retry is a stretch of time during which the messages of
        // the new partitions are dropped: it takes the pipeline down at
        // once in the partition-reader mode, unlike the transient broker
        // problems the budget is for. In the subscription mode both the
        // expansion and the poison-message errors pass through the commit
        // grace first (see `stage_terminal_error`), which sets
        // `fatal_data_error_seen` when the grace is over.
        let expansion_instantly_terminal = self.expanded_partition_count.is_some()
            && !matches!(self.mode, PulsarReaderMode::Subscription { .. });
        if self.fatal_data_error_seen || expansion_instantly_terminal {
            return 0;
        }
        32
    }

    fn short_description(&self) -> Cow<'static, str> {
        format!("Pulsar({})", self.base_topic).into()
    }
}

impl Drop for PulsarReader {
    fn drop(&mut self) {
        // The consumers and the pump tasks interact with the async runtime
        // when dropped, so they are dropped explicitly under the runtime
        // context.
        let _guard = self.runtime.enter();
        self.partition_watch.take();
        match &mut self.mode {
            PulsarReaderMode::Subscription { consumer, .. } => {
                consumer.take();
            }
            PulsarReaderMode::PartitionReaders { pump, .. } => {
                pump.take();
            }
            PulsarReaderMode::Idle => {}
        }
    }
}

/// The retained copy of one in-flight message: everything needed to publish
/// it again when its receipt reports a transient failure. The client library
/// consumes the message on send and drops its internal copy with the broken
/// connection, so without this copy the only possible answer to such a
/// failure is failing the whole pipeline (see `SEND_RECOVERY_TOTAL_BUDGET`).
#[derive(Clone)]
struct PendingMessage {
    // Shared, not owned: one minibatch retains tens of thousands of copies
    // of the same topic name.
    topic: arcstr::ArcStr,
    payload: Vec<u8>,
    properties: HashMap<String, String>,
    partition_key: Option<String>,
    ordering_key: Option<Vec<u8>>,
    event_time: Option<u64>,
    deliver_at_time: Option<i64>,
}

impl PendingMessage {
    fn to_message(&self) -> PulsarProducerMessage {
        PulsarProducerMessage {
            payload: self.payload.clone(),
            properties: self.properties.clone(),
            partition_key: self.partition_key.clone(),
            ordering_key: self.ordering_key.clone(),
            event_time: self.event_time,
            deliver_at_time: self.deliver_at_time,
            ..PulsarProducerMessage::default()
        }
    }
}

/// Retries `operation` on transient failures with a backoff capped at
/// [`SEND_RECOVERY_MAX_BACKOFF`] until `deadline`. The broker's definitive
/// refusals ([`pulsar_error_is_permanent`]) and the deadline end the retries
/// with the last error; `describe` names the operation in the warnings.
async fn retry_transient_pulsar_errors<T>(
    deadline: Instant,
    describe: impl Fn() -> String,
    mut operation: impl AsyncFnMut() -> Result<T, pulsar::Error>,
) -> Result<T, pulsar::Error> {
    let mut backoff = RetryConfig::default();
    loop {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(error) => {
                if pulsar_error_is_permanent(&error) || Instant::now() >= deadline {
                    return Err(error);
                }
                let delay = backoff.next_delay().min(SEND_RECOVERY_MAX_BACKOFF);
                warn!("{}, retrying in {delay:?}: {error}", describe());
                tokio::time::sleep(delay).await;
            }
        }
    }
}

/// When the broker delivers the produced messages to the consumers of the
/// shared and key-shared subscriptions (the other subscription types and the
/// partition readers ignore the schedule, as the protocol defines).
#[derive(Clone, Debug)]
pub enum DeliverySchedule {
    /// No schedule: the messages are delivered as soon as they are published.
    Immediate,
    /// The column carrying the absolute delivery time of each message.
    AtColumn(usize),
    /// The column carrying the delay of each message, counted from the
    /// moment it is published.
    AfterColumn(usize),
    /// The same delay for every message, in milliseconds.
    AfterMillis(u64),
}

#[allow(clippy::module_name_repetitions)]
pub struct PulsarWriter {
    client: Pulsar<TokioExecutor>,
    // A Pulsar producer is bound to a single topic at creation time. To
    // support dynamic topics (where each row can target a different topic),
    // producers are cached per topic name and created on demand. Dropping a
    // producer spawns its close command onto the current tokio runtime and
    // panics if the thread is not inside one, so the map is dropped
    // explicitly under `runtime.enter()` (see the `Drop` impl below).
    producers: Option<HashMap<String, Producer<TokioExecutor>>>,
    in_flight: VecDeque<(SendFuture, PendingMessage)>,
    // The payload bytes of the in-flight queue, so the backpressure can cap
    // the retained copies by size as well as by count (see
    // `MAX_IN_FLIGHT_BYTES`).
    in_flight_bytes: usize,
    // The retained copies a failed recovery could not confirm. The engine
    // retries a failed `write` per row, so these must be republished by the
    // retried call before anything else — dropping them would silently lose
    // every message of the failed recovery except the retried row itself.
    unconfirmed: VecDeque<PendingMessage>,
    // The topics whose producer failed to build with a permanent error,
    // mapped to the error text. The engine retries a failed `write` a few
    // times; without this cache every retry would sit through the client's
    // full internal retry budget (about two minutes for a taken producer
    // name) before repeating the same verdict.
    permanent_producer_failures: HashMap<String, String>,
    // The largest payload submitted since the in-flight queue was last
    // empty. Used to annotate a failed send with the oversized-message
    // diagnosis (see `PULSAR_DEFAULT_MAX_MESSAGE_SIZE`).
    max_pending_payload_bytes: usize,
    runtime: TokioRuntime,
    topic: MessageQueueTopic,
    header_fields: Vec<(String, usize)>,
    key_field_index: Option<usize>,
    // The column whose value becomes the ordering key of the messages: the
    // key the broker hashes when distributing a key_shared subscription, used
    // when the ordering entity differs from the partition-routing key. `None`
    // leaves the ordering key unset, and key_shared falls back to the
    // partition key.
    ordering_key_field_index: Option<usize>,
    // Where the `event_time` of the messages comes from: a column of the
    // table, the engine (minibatch) time — the same value the messages carry
    // in the `pathway_time` property — or nowhere (the field is left unset).
    // At most one of the two options is set; the caller validates that.
    event_time_field_index: Option<usize>,
    event_time_from_engine: bool,
    delivery_schedule: DeliverySchedule,
    // The codec the producers compress the outgoing messages with. `None`
    // sends the payloads uncompressed. The reading side needs no matching
    // setting: the codec travels in the message metadata and the consumers
    // decompress transparently.
    compression: Option<PulsarCompression>,
    // The name the producers register themselves under, already made unique
    // per worker by the caller (Pulsar rejects two producers with one name on
    // one topic). `None` lets the broker assign a generated name. One writer
    // may own several producers (dynamic topics); the name is shared, which
    // is fine because the uniqueness is per topic.
    producer_name: Option<String>,
    // The JSON of the Avro schema the producers declare to the broker's
    // schema registry, set when the payloads are Avro-encoded. The broker
    // checks it against the topic's current schema (per the namespace
    // compatibility policy) and stamps the resulting schema version into the
    // metadata of every published message, which is what the schema-aware
    // consumers decode by.
    declared_avro_schema: Option<String>,
}

impl PulsarWriter {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        runtime: TokioRuntime,
        client: Pulsar<TokioExecutor>,
        topic: MessageQueueTopic,
        header_fields: Vec<(String, usize)>,
        key_field_index: Option<usize>,
        ordering_key_field_index: Option<usize>,
        event_time_field_index: Option<usize>,
        event_time_from_engine: bool,
        delivery_schedule: DeliverySchedule,
        compression: Option<PulsarCompression>,
        producer_name: Option<String>,
        declared_avro_schema: Option<String>,
    ) -> Self {
        PulsarWriter {
            runtime,
            client,
            producers: Some(HashMap::new()),
            in_flight: VecDeque::new(),
            in_flight_bytes: 0,
            unconfirmed: VecDeque::new(),
            permanent_producer_failures: HashMap::new(),
            max_pending_payload_bytes: 0,
            topic,
            header_fields,
            key_field_index,
            ordering_key_field_index,
            event_time_field_index,
            event_time_from_engine,
            delivery_schedule,
            compression,
            producer_name,
            declared_avro_schema,
        }
    }

    fn ensure_producer(&mut self, topic: &str) -> Result<(), WriteError> {
        if let Some(reason) = self.permanent_producer_failures.get(topic) {
            return Err(PulsarError::Client(pulsar::Error::Custom(reason.clone())).into());
        }
        let producers = self
            .producers
            .as_mut()
            .expect("producers are set until drop");
        if !producers.contains_key(topic) {
            let batching_allowed = matches!(self.delivery_schedule, DeliverySchedule::Immediate);
            let schema = self.declared_avro_schema.as_ref().map(|schema_json| {
                pulsar::message::proto::Schema {
                    r#type: pulsar::message::proto::schema::Type::Avro as i32,
                    schema_data: schema_json.as_bytes().to_vec(),
                    ..pulsar::message::proto::Schema::default()
                }
            });
            let mut builder =
                self.client
                    .producer()
                    .with_topic(topic)
                    .with_options(ProducerOptions {
                        // A batch carries a single delivery time in its
                        // envelope and the client library drops the
                        // per-message ones, so the scheduled messages
                        // must be published one by one (the Java client
                        // bypasses batching for them the same way).
                        batch_size: batching_allowed.then_some(PRODUCER_BATCH_SIZE),
                        batch_byte_size: batching_allowed.then_some(PRODUCER_BATCH_MAX_BYTES),
                        // Await queue space instead of failing with
                        // `SlowDown` when the client's outbound channel
                        // is full.
                        block_queue_if_full: true,
                        // Route by the hash of the partition key, the way
                        // the other Pulsar clients do by default. Without
                        // an explicit policy the library round-robins every
                        // message across the partitions and ignores the key
                        // entirely, so the updates of one row would scatter
                        // over the topic and lose their order.
                        routing_policy: Some(PulsarRoutingPolicy::RoundRobin),
                        compression: self.compression.clone(),
                        schema,
                        ..ProducerOptions::default()
                    });
            if let Some(producer_name) = &self.producer_name {
                builder = builder.with_name(producer_name);
            }
            // A transient failure to build the producer (the broker is
            // momentarily unreachable) is retried within the same budget as
            // the sends, so a blip at the first write of a topic does not
            // take the pipeline down either.
            let producer = self.runtime.block_on(retry_transient_pulsar_errors(
                Instant::now() + SEND_RECOVERY_TOTAL_BUDGET,
                || format!("transient failure to create a Pulsar producer for the topic '{topic}'"),
                async || Box::pin(builder.clone().build()).await,
            ));
            let producer = match producer {
                Ok(producer) => producer,
                Err(error) => {
                    if pulsar_error_is_permanent(&error) {
                        self.permanent_producer_failures
                            .insert(topic.to_string(), error.to_string());
                    }
                    return Err(PulsarError::from(error).into());
                }
            };
            producers.insert(topic.to_string(), producer);
        }
        Ok(())
    }

    /// Publishes one retained message, retrying the transient failures with
    /// backoff until `deadline`, and returns its receipt future.
    async fn send_pending_message(
        producers: &mut HashMap<String, Producer<TokioExecutor>>,
        message: &PendingMessage,
        deadline: Instant,
        max_pending_payload_bytes: usize,
    ) -> Result<SendFuture, WriteError> {
        retry_transient_pulsar_errors(
            deadline,
            || {
                format!(
                    "transient failure to publish a message to the Pulsar topic '{}'",
                    message.topic
                )
            },
            async || {
                let producer = producers
                    .get_mut(message.topic.as_str())
                    .expect("the producer of an in-flight topic exists");
                Box::pin(producer.send_non_blocking(message.to_message())).await
            },
        )
        .await
        .map_err(|error| Self::annotate_send_error(error, max_pending_payload_bytes).into())
    }

    // Awaits broker receipts, oldest first, until at most `limit` sends
    // remain in flight. Used both to apply backpressure in `write` and to
    // drain everything in `flush`.
    //
    // A receipt that reports a transient failure (or does not arrive within
    // `SEND_RECEIPT_TIMEOUT`) does not fail the pipeline: the message is
    // republished from its retained copy and the drain restarts, for up to
    // `SEND_RECOVERY_TOTAL_BUDGET` — a broker blip mid-write heals instead
    // of killing the run. When the recovery fails anyway, the retained
    // copies stay in `unconfirmed` and the next call republishes them
    // first: the engine retries a failed `write` per row, and without the
    // retained copies the retried call would confirm only its own row,
    // silently losing the rest of the failed recovery's messages. A message
    // whose dropped receipt would still have resolved may be delivered
    // twice this way — the write contract is at-least-once.
    async fn drain_in_flight(
        producers: &mut HashMap<String, Producer<TokioExecutor>>,
        in_flight: &mut VecDeque<(SendFuture, PendingMessage)>,
        unconfirmed: &mut VecDeque<PendingMessage>,
        in_flight_bytes: &mut usize,
        max_pending_payload_bytes: &mut usize,
        limit: usize,
        byte_limit: usize,
    ) -> Result<(), WriteError> {
        let result = if !unconfirmed.is_empty()
            || in_flight.len() > limit
            || *in_flight_bytes > byte_limit
        {
            let result = Self::drain_in_flight_inner(
                producers,
                in_flight,
                unconfirmed,
                in_flight_bytes,
                *max_pending_payload_bytes,
                limit,
                byte_limit,
            )
            .await;
            if result.is_err() {
                unconfirmed.extend(in_flight.drain(..).map(|(_, message)| message));
                *in_flight_bytes = 0;
            }
            result
        } else {
            Ok(())
        };
        if in_flight.is_empty() && unconfirmed.is_empty() {
            *max_pending_payload_bytes = 0;
        }
        result
    }

    // The rounds of the drain: republish the retained copies, force the
    // partial batches out (a receipt for a message sitting in an unfilled
    // batch would otherwise never resolve), await the receipts. Once any
    // trouble is seen, the drain empties the whole queue rather than
    // stopping at `limit`, so the recovery ends with every republished
    // message confirmed.
    async fn drain_in_flight_inner(
        producers: &mut HashMap<String, Producer<TokioExecutor>>,
        in_flight: &mut VecDeque<(SendFuture, PendingMessage)>,
        unconfirmed: &mut VecDeque<PendingMessage>,
        in_flight_bytes: &mut usize,
        max_pending_payload_bytes: usize,
        limit: usize,
        byte_limit: usize,
    ) -> Result<(), WriteError> {
        let deadline = Instant::now() + SEND_RECOVERY_TOTAL_BUDGET;
        let mut backoff = RetryConfig::default();
        let mut ever_troubled = !unconfirmed.is_empty();
        loop {
            while let Some(message) = unconfirmed.pop_front() {
                match Self::send_pending_message(
                    producers,
                    &message,
                    deadline,
                    max_pending_payload_bytes,
                )
                .await
                {
                    Ok(send_future) => {
                        *in_flight_bytes += message.payload.len();
                        in_flight.push_back((send_future, message));
                    }
                    Err(error) => {
                        unconfirmed.push_front(message);
                        return Err(error);
                    }
                }
            }
            let mut round_troubled = false;
            // A transient failure to force a batch out is not final: the
            // receipts of the affected messages fail or time out below, and
            // the messages are republished.
            for producer in producers.values_mut() {
                // The scheduled messages are published one by one (see
                // `ensure_producer`), and the client library reports the
                // batch flush of such a producer as an error.
                if producer.options().batch_size.is_none() {
                    continue;
                }
                if let Err(error) = producer.send_batch().await {
                    if pulsar_error_is_permanent(&error) {
                        return Err(
                            Self::annotate_send_error(error, max_pending_payload_bytes).into()
                        );
                    }
                    warn!("failed to flush a Pulsar producer batch, recovering: {error}");
                    round_troubled = true;
                }
            }
            loop {
                let (count_target, byte_target) = if ever_troubled || round_troubled {
                    (0, 0)
                } else {
                    (limit, byte_limit)
                };
                if in_flight.len() <= count_target && *in_flight_bytes <= byte_target {
                    break;
                }
                let (send_future, message) = in_flight
                    .pop_front()
                    .expect("in_flight is non-empty while a target is exceeded");
                *in_flight_bytes -= message.payload.len();
                // Capped by the recovery deadline: the receipt waits are
                // sequential, and without the cap a queue of receipts lost
                // to one dead connection would hold the flush far beyond
                // the budget — thousands of thirty-second waits.
                let wait =
                    SEND_RECEIPT_TIMEOUT.min(deadline.saturating_duration_since(Instant::now()));
                match tokio::time::timeout(wait, send_future).await {
                    Ok(Ok(_receipt)) => {}
                    Ok(Err(error)) => {
                        if pulsar_error_is_permanent(&error) {
                            unconfirmed.push_back(message);
                            return Err(Self::annotate_send_error(
                                error,
                                max_pending_payload_bytes,
                            )
                            .into());
                        }
                        warn!(
                            "a Pulsar broker receipt reported a transient failure, \
                             republishing the message: {error}"
                        );
                        round_troubled = true;
                        unconfirmed.push_back(message);
                    }
                    Err(_elapsed) => {
                        // Most likely lost together with a dropped
                        // connection. If it was in fact delivered, the
                        // republication duplicates the message — the write
                        // contract is at-least-once.
                        warn!(
                            "a Pulsar broker receipt did not arrive within {wait:?}, \
                             republishing the message"
                        );
                        round_troubled = true;
                        unconfirmed.push_back(message);
                    }
                }
            }
            if !round_troubled {
                // A full round without a new failure: everything above the
                // limit (or, after any trouble, everything at all) is
                // confirmed by the broker.
                return Ok(());
            }
            ever_troubled = true;
            // An oversized payload in flight never shortens the recovery:
            // the broker's message-size limit is configurable and the client
            // library does not report it, so the suspicion cannot be told
            // from a transient blip on a permissive broker. A genuinely
            // oversized payload burns the whole budget and gets the
            // diagnosis attached to the terminal error (see
            // `recovery_gave_up_error`).
            if Instant::now() >= deadline {
                return Err(Self::recovery_gave_up_error(
                    unconfirmed.len() + in_flight.len(),
                    max_pending_payload_bytes,
                )
                .into());
            }
            tokio::time::sleep(backoff.next_delay().min(SEND_RECOVERY_MAX_BACKOFF)).await;
        }
    }

    /// The terminal error of a recovery that ends with unconfirmed messages:
    /// the oversized-payload diagnosis when it applies (see
    /// `annotate_send_error`), the plain budget report otherwise.
    fn recovery_gave_up_error(undelivered: usize, max_pending_payload_bytes: usize) -> PulsarError {
        if max_pending_payload_bytes > PULSAR_DEFAULT_MAX_MESSAGE_SIZE {
            return Self::annotate_send_error(
                pulsar::Error::Custom(format!(
                    "{undelivered} message(s) could not be delivered to the broker"
                )),
                max_pending_payload_bytes,
            );
        }
        PulsarError::SendRecoveryBudgetExhausted {
            undelivered,
            budget: SEND_RECOVERY_TOTAL_BUDGET,
        }
    }

    // A send failure while an oversized payload is in flight is almost
    // certainly the broker dropping the connection over its per-message
    // limit; the raw client error ("Connection error: Disconnected") does
    // not say so, hence the annotation. The limit is broker-configurable,
    // so the check is applied to the diagnosis, not to the send itself: a
    // broker configured with a higher limit accepts such messages normally.
    fn annotate_send_error(error: pulsar::Error, max_pending_payload_bytes: usize) -> PulsarError {
        if max_pending_payload_bytes > PULSAR_DEFAULT_MAX_MESSAGE_SIZE {
            PulsarError::OversizedMessage {
                size: max_pending_payload_bytes,
                source: error,
            }
        } else {
            PulsarError::from(error)
        }
    }
}

impl PulsarWriter {
    fn row_partition_key(&self, data: &FormatterContext) -> Result<String, WriteError> {
        match self.key_field_index {
            Some(index) => match &data.values[index] {
                Value::String(string) => Ok(string.to_string()),
                Value::Bytes(bytes) => Ok(std::str::from_utf8(bytes)
                    .map_err(PulsarError::NonUtf8PartitionKey)?
                    .to_string()),
                _ => Err(WriteError::IncorrectKeyFieldType(
                    data.values[index].clone(),
                )),
            },
            None => Ok(format!("{:x}", data.key.0)),
        }
    }

    /// The ordering key is raw bytes on the wire, so a bytes column is passed
    /// through as is, without the UTF-8 requirement the partition key has.
    fn row_ordering_key(&self, data: &FormatterContext) -> Result<Option<Vec<u8>>, WriteError> {
        match self.ordering_key_field_index {
            Some(index) => match &data.values[index] {
                Value::String(string) => Ok(Some(string.as_bytes().to_vec())),
                Value::Bytes(bytes) => Ok(Some(bytes.to_vec())),
                other => Err(PulsarError::IncorrectOrderingKeyValue(other.clone()).into()),
            },
            None => Ok(None),
        }
    }

    /// The `event_time` of the messages, in milliseconds since the UNIX
    /// epoch, as Pulsar stores it.
    fn row_event_time(&self, data: &FormatterContext) -> Result<Option<u64>, WriteError> {
        if let Some(index) = self.event_time_field_index {
            let millis = match &data.values[index] {
                Value::Int(millis) if *millis >= 0 => {
                    u64::try_from(*millis).expect("checked to be non-negative")
                }
                Value::DateTimeUtc(datetime) if datetime.timestamp_milliseconds() >= 0 => {
                    u64::try_from(datetime.timestamp_milliseconds())
                        .expect("checked to be non-negative")
                }
                other => {
                    return Err(PulsarError::IncorrectEventTimeValue(other.clone()).into());
                }
            };
            return Ok(Some(millis));
        }
        if self.event_time_from_engine {
            // The engine time is the UNIX timestamp of the minibatch in
            // milliseconds — the same value the messages carry in the
            // `pathway_time` property.
            return Ok(Some(data.time.0));
        }
        Ok(None)
    }

    /// The `deliver_at_time` of the messages, in milliseconds since the UNIX
    /// epoch, as the protocol carries it. The delays are counted from now:
    /// the message is published right after this is computed.
    fn row_deliver_at_time(&self, data: &FormatterContext) -> Result<Option<i64>, WriteError> {
        let delay_millis = match self.delivery_schedule {
            DeliverySchedule::Immediate => return Ok(None),
            DeliverySchedule::AtColumn(index) => {
                let millis = match &data.values[index] {
                    Value::Int(millis) if *millis >= 0 => *millis,
                    Value::DateTimeUtc(datetime) if datetime.timestamp_milliseconds() >= 0 => {
                        datetime.timestamp_milliseconds()
                    }
                    other => {
                        return Err(PulsarError::IncorrectDeliverAtValue(other.clone()).into());
                    }
                };
                return Ok(Some(millis));
            }
            DeliverySchedule::AfterColumn(index) => match &data.values[index] {
                Value::Int(millis) if *millis >= 0 => *millis,
                Value::Duration(duration) if duration.milliseconds() >= 0 => {
                    duration.milliseconds()
                }
                other => {
                    return Err(PulsarError::IncorrectDeliverAfterValue(other.clone()).into());
                }
            },
            DeliverySchedule::AfterMillis(millis) => {
                i64::try_from(millis).expect("the delay is validated by the caller")
            }
        };
        let now_millis = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("the system clock is set after the UNIX epoch")
                .as_millis(),
        )
        .expect("the current time fits into i64 milliseconds");
        Ok(Some(now_millis.saturating_add(delay_millis)))
    }
}

impl Writer for PulsarWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        let effective_topic = self.topic.get_for_posting(&data.values)?;
        if !matches!(self.delivery_schedule, DeliverySchedule::Immediate)
            && effective_topic.starts_with(NON_PERSISTENT_TOPIC_PREFIX)
        {
            return Err(PulsarError::DelayedDeliveryOnNonPersistentTopic {
                topic: effective_topic,
            }
            .into());
        }
        self.ensure_producer(&effective_topic)?;

        let partition_key = self.row_partition_key(&data)?;
        let ordering_key = self.row_ordering_key(&data)?;
        let event_time = self.row_event_time(&data)?;
        let deliver_at_time = self.row_deliver_at_time(&data)?;

        // User-defined header values are serialized to JSON strings because
        // Pulsar message properties are string-to-string pairs. pathway_time
        // and pathway_diff are always added, consistent with the Kafka, NATS
        // and RabbitMQ writers.
        let mut properties: HashMap<String, String> = data
            .construct_string_properties(&self.header_fields)
            .into_iter()
            .collect();

        let Self {
            runtime,
            producers,
            in_flight,
            in_flight_bytes,
            unconfirmed,
            max_pending_payload_bytes,
            ..
        } = self;
        let producers = producers.as_mut().expect("producers are set until drop");
        let effective_topic = arcstr::ArcStr::from(effective_topic);

        runtime.block_on(async {
            let deadline = Instant::now() + SEND_RECOVERY_TOTAL_BUDGET;
            let last_payload_index = data.payloads.len() - 1;
            for (index, payload) in data.payloads.into_iter().enumerate() {
                // Avoid copying data on the last iteration, reuse the existing properties
                let properties = {
                    if index == last_payload_index {
                        take(&mut properties)
                    } else {
                        properties.clone()
                    }
                };
                if in_flight.len() >= MAX_IN_FLIGHT_SENDS
                    || *in_flight_bytes >= MAX_IN_FLIGHT_BYTES
                    || !unconfirmed.is_empty()
                {
                    Self::drain_in_flight(
                        producers,
                        in_flight,
                        unconfirmed,
                        in_flight_bytes,
                        max_pending_payload_bytes,
                        IN_FLIGHT_DRAIN_TARGET,
                        IN_FLIGHT_BYTES_DRAIN_TARGET,
                    )
                    .await?;
                }
                let payload = payload.into_raw_bytes()?;
                *max_pending_payload_bytes = (*max_pending_payload_bytes).max(payload.len());
                let message = PendingMessage {
                    topic: effective_topic.clone(),
                    payload,
                    properties,
                    partition_key: Some(partition_key.clone()),
                    ordering_key: ordering_key.clone(),
                    event_time,
                    deliver_at_time,
                };
                let send_future = Self::send_pending_message(
                    producers,
                    &message,
                    deadline,
                    *max_pending_payload_bytes,
                )
                .await?;
                *in_flight_bytes += message.payload.len();
                in_flight.push_back((send_future, message));
            }
            Ok(())
        })
    }

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        let Self {
            runtime,
            producers,
            in_flight,
            in_flight_bytes,
            unconfirmed,
            max_pending_payload_bytes,
            ..
        } = self;
        let producers = producers.as_mut().expect("producers are set until drop");
        // Every flush drains the queue completely, like the RabbitMQ and
        // NATS writers do. This is a correctness requirement, not a place
        // for pipelining: with persistence enabled the engine records the
        // sink's committed time *before* calling flush, so any receipt left
        // unawaited here may correspond to a message the recovery will
        // consider already written — a crash would then lose it forever.
        // The send pipelining happens inside `write` instead (see
        // MAX_IN_FLIGHT_SENDS), where no commit point can interleave.
        runtime.block_on(async {
            Self::drain_in_flight(
                producers,
                in_flight,
                unconfirmed,
                in_flight_bytes,
                max_pending_payload_bytes,
                0,
                0,
            )
            .await
        })
    }

    fn retriable(&self) -> bool {
        true
    }

    fn single_threaded(&self) -> bool {
        false
    }

    fn name(&self) -> String {
        format!("Pulsar({})", self.topic)
    }
}

/// The human name of a registry schema type, for the error messages: the
/// protocol enum value alone ("type 1") would send the user to the protocol
/// definition to learn what their topic is typed with.
fn registry_schema_type_name(type_: i32) -> String {
    pulsar::message::proto::schema::Type::try_from(type_).map_or_else(
        |_| format!("unknown ({type_})"),
        |schema_type| schema_type.as_str_name().to_string(),
    )
}

/// Whether the client error carries the broker's definitive refusal —
/// rejected credentials, missing permissions, a deleted topic, and the like.
/// Such failures do not heal on retry and must be classified as permanent;
/// everything else (timeouts, dropped connections) is worth retrying.
fn pulsar_error_is_permanent(error: &pulsar::Error) -> bool {
    use pulsar::error::{ConnectionError, ConsumerError as ClientConsumerError};
    use pulsar::message::proto::ServerError;
    fn server_error_is_permanent(server_error: ServerError) -> bool {
        matches!(
            server_error,
            ServerError::AuthenticationError
                | ServerError::AuthorizationError
                | ServerError::TopicNotFound
                | ServerError::NotAllowedError
                | ServerError::IncompatibleSchema
                | ServerError::TopicTerminatedError
                // Reaches this classification only after the client has
                // already retried it for its whole internal budget (see
                // `build_pulsar_client`), so by now the name is genuinely
                // held by a live producer of another pipeline.
                | ServerError::ProducerBusy
        )
    }
    fn connection_error_is_permanent(error: &ConnectionError) -> bool {
        matches!(error, ConnectionError::PulsarError(Some(server_error), _)
            if server_error_is_permanent(*server_error))
    }
    match error {
        pulsar::Error::Authentication(_) => true,
        pulsar::Error::Connection(connection_error)
        | pulsar::Error::Consumer(ClientConsumerError::Connection(connection_error)) => {
            connection_error_is_permanent(connection_error)
        }
        _ => false,
    }
}

/// Wraps a client error of a schema lookup into its retry classification.
fn classify_lookup_error(error: &pulsar::Error, context: &str) -> SchemaLookupError {
    let message = format!("{context}: {error}");
    if pulsar_error_is_permanent(error) {
        SchemaLookupError::Permanent(message)
    } else {
        SchemaLookupError::Transient(message)
    }
}

/// Fetches a schema from the broker's registry over a short-lived probe
/// consumer: the lookup is a consumer-scoped command of the binary protocol.
/// `version: None` requests the latest version. The consumer exists only for
/// the duration of the call — the lookups are rare (once per schema version,
/// or once per run for the deduction), and a long-lived probe would sit on an
/// exclusive subscription and buffer flow-permit messages nobody reads.
///
/// For a partitioned topic the consumer expands into per-partition consumers
/// keyed by the physical `...-partition-K` names, and its `get_schema` looks
/// the consumer up by that exact key — so the query must use one of the
/// consumer's own topic names, never the base name the caller subscribed
/// with. The broker resolves a partition name to its base topic when serving
/// the schema, so any partition works.
fn fetch_registry_schema(
    runtime: &TokioRuntime,
    client: &Pulsar<TokioExecutor>,
    topic: &str,
    version: Option<Vec<u8>>,
) -> Result<Option<pulsar::message::proto::Schema>, SchemaLookupError> {
    // The probe subscription is non-durable and uniquely named: it exists
    // only to scope the schema lookup and never consumes anything.
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time is past the epoch")
        .as_nanos();
    let mut consumer: PulsarConsumer<Vec<u8>, TokioExecutor> = runtime
        .block_on(
            client
                .consumer()
                .with_topic(topic)
                .with_subscription(format!("pathway-schema-probe-{nonce}"))
                .with_subscription_type(PulsarSubType::Exclusive)
                .with_options(PulsarConsumerOptions::default().durable(false))
                .build(),
        )
        .map_err(|e| classify_lookup_error(&e, "failed to connect to the topic"))?;
    let query_topic = consumer.topics().into_iter().next().ok_or_else(|| {
        SchemaLookupError::Permanent(
            "the topic has no partitions to query the schema through".to_string(),
        )
    })?;
    let registry_schema = runtime
        .block_on(consumer.get_schema(&query_topic, version))
        .map_err(|e| classify_lookup_error(&e, "failed to fetch the schema of the topic"));
    // The probe consumer interacts with the async runtime when dropped.
    {
        let _guard = runtime.enter();
        drop(consumer);
    }
    registry_schema
}

/// The failure classification of a schema lookup, private to this storage:
/// a transient failure (the registry was unreachable) says nothing about the
/// message, while a permanent one is the registry's own verdict — no such
/// version, an unusable schema, a refused connection.
#[derive(Debug)]
pub enum SchemaLookupError {
    Transient(String),
    Permanent(String),
}

// The retry schedule of the transient schema-lookup failures: the shared
// `RetryConfig` backoff (with jitter, so the parallel workers do not retry
// in lockstep), capped per delay. The retries continue indefinitely,
// because this reader acknowledges a message before its payload is parsed:
// giving up would lose the row for good, so the reading thread blocks until
// the registry answers. This is the intended backpressure, and it stalls
// the pipeline (including its shutdown) for as long as the registry stays
// unreachable; the operator sees the repeated warnings and decides.
const SCHEMA_LOOKUP_MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Runs `attempt` until it yields a schema or fails permanently, sleeping
/// with backoff between the transient failures. The returned error message
/// is what the parser reports for the row.
fn retry_transient_lookups<T>(
    mut attempt: impl FnMut() -> Result<T, SchemaLookupError>,
) -> Result<T, String> {
    let mut retry_config = RetryConfig::default();
    loop {
        match attempt() {
            Ok(value) => return Ok(value),
            Err(SchemaLookupError::Permanent(message)) => return Err(message),
            Err(SchemaLookupError::Transient(message)) => {
                let delay = retry_config.next_delay().min(SCHEMA_LOOKUP_MAX_BACKOFF);
                warn!(
                    "transient failure to obtain a writer schema from the registry, \
                     retrying in {delay:?}: {message}"
                );
                std::thread::sleep(delay);
            }
        }
    }
}

// How long a registry verdict stays in the negative cache. The verdicts
// are final for a fixed version as far as the registry's answer goes, but
// the conditions around them heal: an operator grants the missing
// permissions, a geo-replicated registry catches up. A bounded cooldown
// keeps the per-message lookup storm away while letting the recovery
// through without a pipeline restart.
const NEGATIVE_CACHE_TTL: Duration = Duration::from_mins(1);

/// The factory of the provider's connection, invoked on the first lookup:
/// the workers whose parser never meets an unseen schema version (idle ones
/// included) then never open a client of their own. The factory classifies
/// its own failures: a broken client configuration (an unreadable
/// certificate file and the like) is permanent, while a failed connection
/// attempt is transient — nothing definitive is known before the broker
/// answers.
pub type PulsarConnectionFactory =
    Box<dyn Fn() -> Result<(TokioRuntime, Pulsar<TokioExecutor>), SchemaLookupError> + Send>;

/// The Pulsar-backed source of Avro writer schemas: looks the schemas up in
/// the broker's registry by the version stamped into the message metadata.
/// Both the successful lookups and the registry's permanent verdicts are
/// cached (a permanent verdict is final for a fixed version — without the
/// negative cache, a stretch of messages under an undecodable version would
/// open a fresh probe consumer per message). The connection is established
/// lazily, on the first cache miss, and the probe consumer of a lookup
/// lives only for the duration of that miss: a topic accumulates a handful
/// of schema versions over its lifetime, so the lookups are rare.
pub struct PulsarSchemaProvider {
    connection: Option<(TokioRuntime, Pulsar<TokioExecutor>)>,
    connection_factory: PulsarConnectionFactory,
    topic: String,
    cache: HashMap<Vec<u8>, Arc<apache_avro::schema::Schema>>,
    permanent_failures: HashMap<Vec<u8>, (String, std::time::Instant)>,
}

impl PulsarSchemaProvider {
    pub fn new(connection_factory: PulsarConnectionFactory, topic: &str) -> PulsarSchemaProvider {
        PulsarSchemaProvider {
            connection: None,
            connection_factory,
            topic: topic.to_string(),
            cache: HashMap::new(),
            permanent_failures: HashMap::new(),
        }
    }

    fn connection(&mut self) -> Result<&(TokioRuntime, Pulsar<TokioExecutor>), SchemaLookupError> {
        if self.connection.is_none() {
            // The factory classifies its failures itself (see
            // `PulsarConnectionFactory`).
            let connection = (self.connection_factory)()?;
            self.connection = Some(connection);
        }
        Ok(self.connection.as_ref().expect("just set"))
    }
}

impl AvroSchemaProvider for PulsarSchemaProvider {
    fn get_schema(&mut self, schema_id: &[u8]) -> Result<Arc<apache_avro::schema::Schema>, String> {
        retry_transient_lookups(|| self.lookup_once(schema_id))
    }
}

impl PulsarSchemaProvider {
    /// One lookup attempt, answering with the classification the retry
    /// policy above acts on.
    fn lookup_once(
        &mut self,
        schema_id: &[u8],
    ) -> Result<Arc<apache_avro::schema::Schema>, SchemaLookupError> {
        if let Some(schema) = self.cache.get(schema_id) {
            return Ok(schema.clone());
        }
        if let Some((message, expires_at)) = self.permanent_failures.get(schema_id) {
            if std::time::Instant::now() < *expires_at {
                return Err(SchemaLookupError::Permanent(message.clone()));
            }
            // The cooldown has passed: forget the verdict and ask the
            // registry anew — the conditions around it may have healed.
            self.permanent_failures.remove(schema_id);
        }
        let topic = self.topic.clone();
        let (runtime, client) = self.connection()?;
        // A failed fetch is classified by `fetch_registry_schema`: the
        // broker's definitive refusals are permanent, the transport-level
        // failures are transient and the parser retries them. Everything
        // below is the registry's own verdict about the schema — permanent.
        let lookup_result =
            fetch_registry_schema(runtime, client, &topic, Some(schema_id.to_vec()))
                .and_then(|registry_schema| {
                    registry_schema.ok_or_else(|| {
                        // Transient: the version was stamped by a broker, so
                        // its schema exists somewhere — most likely the
                        // local registry lags behind a geo-replicated one.
                        // The parser retries until it catches up.
                        SchemaLookupError::Transient(
                            "the registry holds no schema under this version (yet)".to_string(),
                        )
                    })
                })
                .and_then(|registry_schema| {
                    let avro_type = pulsar::message::proto::schema::Type::Avro as i32;
                    if registry_schema.r#type != avro_type {
                        return Err(SchemaLookupError::Permanent(format!(
                            "the schema version of the message is of the {} type, not AVRO: \
                         the topic mixes schema types, and only AVRO payloads can be \
                         decoded by this reader",
                            registry_schema_type_name(registry_schema.r#type),
                        )));
                    }
                    Ok(registry_schema)
                })
                .and_then(|registry_schema| {
                    let schema_json =
                        std::str::from_utf8(&registry_schema.schema_data).map_err(|e| {
                            SchemaLookupError::Permanent(format!(
                                "the registry schema is not valid UTF-8: {e}"
                            ))
                        })?;
                    apache_avro::schema::Schema::parse_str(schema_json).map_err(|e| {
                        SchemaLookupError::Permanent(format!(
                            "the registry schema does not parse as Avro: {e}"
                        ))
                    })
                });
        match lookup_result {
            Ok(schema) => {
                let schema = Arc::new(schema);
                self.cache.insert(schema_id.to_vec(), schema.clone());
                Ok(schema)
            }
            Err(SchemaLookupError::Permanent(message)) => {
                // Remember the verdict for the cooldown, so a stretch of
                // undecodable messages does not turn into a lookup per
                // message — while an operator-side fix (granted
                // permissions, a caught-up registry) still gets through
                // without a restart.
                self.permanent_failures.insert(
                    schema_id.to_vec(),
                    (
                        message.clone(),
                        std::time::Instant::now() + NEGATIVE_CACHE_TTL,
                    ),
                );
                Err(SchemaLookupError::Permanent(message))
            }
            Err(transient) => Err(transient),
        }
    }
}

impl Drop for PulsarWriter {
    fn drop(&mut self) {
        if let Err(e) = self.flush(true) {
            error!("Pulsar flush failed on drop: {e}");
        }
        // See the comment on the `producers` field.
        let _guard = self.runtime.enter();
        self.producers.take();
    }
}

/// How long the deduction keeps re-checking a topic that appears to have no
/// schema before believing it (see `fetch_latest_registry_schema`).
const SCHEMA_DEDUCTION_RECHECK_BUDGET: Duration = Duration::from_secs(10);

/// Fetches the latest registry schema of the topic for the deduction.
///
/// An empty lookup answer is not conclusive. The client library drops the
/// error code of the `GetSchema` response, so a lookup the broker could not
/// serve — a schema ledger read that timed out under load, a registry
/// momentarily unavailable — looks exactly like a topic without a schema. The
/// deduction runs once, at construction time, so it cannot afford to be
/// misled: an empty answer (and a transient lookup failure) is re-checked
/// over fresh probe connections with backoff for a bounded time, and only a
/// consistently empty answer is reported as "no schema". The price of the
/// caution is paid by a genuinely schema-less topic: its error takes
/// `SCHEMA_DEDUCTION_RECHECK_BUDGET` longer to surface. Permanent failures
/// (the registry's own verdict) surface right away.
fn fetch_latest_registry_schema(
    runtime: &TokioRuntime,
    client: &Pulsar<TokioExecutor>,
    topic: &str,
) -> Result<Option<pulsar::message::proto::Schema>, String> {
    let started = Instant::now();
    let mut backoff = RetryConfig::default();
    loop {
        let outcome = match fetch_registry_schema(runtime, client, topic, None) {
            Ok(Some(schema)) => return Ok(Some(schema)),
            Ok(None) => Ok(()),
            Err(SchemaLookupError::Permanent(message)) => return Err(message),
            Err(SchemaLookupError::Transient(message)) => Err(message),
        };
        let delay = backoff.next_delay();
        if started.elapsed() + delay >= SCHEMA_DEDUCTION_RECHECK_BUDGET {
            return outcome.map(|()| None);
        }
        match &outcome {
            Ok(()) => warn!(
                "the schema lookup of the topic {topic:?} answered with no schema, re-checking \
                 in {delay:?} in case the registry could not serve it"
            ),
            Err(message) => warn!(
                "transient failure to look the schema of the topic {topic:?} up, retrying in \
                 {delay:?}: {message}"
            ),
        }
        std::thread::sleep(delay);
    }
}

/// Deduces the columns of a table from the current schema of the topic in the
/// broker's registry: the Pulsar half of `schema=None`. Both the AVRO and the
/// JSON registry types are accepted — Pulsar describes JSON-typed topics with
/// the same Avro record grammar. When the payload `format` of the reading
/// connector is provided, the registry type must agree with it: the type is
/// known up front, so a mismatch fails here instead of on every message at
/// runtime.
pub fn explore_schema(
    runtime: &TokioRuntime,
    client: &Pulsar<TokioExecutor>,
    topic: &str,
    format: Option<&str>,
) -> Result<Vec<crate::connectors::exploration::ExploredField>, String> {
    let Some(registry_schema) = fetch_latest_registry_schema(runtime, client, topic)? else {
        return Err(
            "the topic has no schema registered, so there is nothing to deduce the table \
             columns from. Pass an explicit schema instead. Note: if the topic name is \
             mistyped, the lookup itself may have just created an empty topic under that \
             name — the broker's allowAutoTopicCreation is enabled by default"
                .to_string(),
        );
    };
    let avro_type = pulsar::message::proto::schema::Type::Avro as i32;
    let json_type = pulsar::message::proto::schema::Type::Json as i32;
    if registry_schema.r#type != avro_type && registry_schema.r#type != json_type {
        return Err(format!(
            "the schema of the topic has the {} type in the registry, while only the AVRO \
             and JSON schema types describe table columns. Pass an explicit schema instead",
            registry_schema_type_name(registry_schema.r#type),
        ));
    }
    let format_matches = match format {
        Some("avro") => registry_schema.r#type == avro_type,
        Some("json") => registry_schema.r#type == json_type,
        _ => true,
    };
    if !format_matches {
        let (actual, suggested) = if registry_schema.r#type == avro_type {
            ("AVRO", "avro")
        } else {
            ("JSON", "json")
        };
        return Err(format!(
            "the topic's registry schema has the {actual} type, while format={:?} expects \
             differently encoded payloads — the read would fail on every message. Use \
             format={suggested:?} instead, or pass an explicit schema",
            format.unwrap_or_default(),
        ));
    }
    let schema_json = std::str::from_utf8(&registry_schema.schema_data)
        .map_err(|e| format!("the registered schema is not valid UTF-8: {e}"))?;
    // The registry type decides how the payloads are encoded, and with it
    // the deduction table: the JSON payloads are read by the JSON parser,
    // whose representations differ from the Avro ones.
    let encoding = if registry_schema.r#type == json_type {
        crate::connectors::data_format::avro::PayloadEncoding::Json
    } else {
        crate::connectors::data_format::avro::PayloadEncoding::AvroDatum
    };
    crate::connectors::data_format::avro::avro_schema_to_explored_fields(schema_json, encoding)
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::Reader;
    use arcstr::ArcStr;

    #[test]
    fn transient_lookup_failures_are_retried_until_they_pass() {
        // A registry that is momentarily unreachable must not condemn the
        // message: this reader acknowledges before parsing, so a failed
        // lookup would lose the row for good.
        let mut attempts = 0;
        let result = retry_transient_lookups(|| {
            attempts += 1;
            if attempts < 3 {
                Err(SchemaLookupError::Transient("unreachable".to_string()))
            } else {
                Ok("schema")
            }
        });
        assert_eq!(result, Ok("schema"));
        assert_eq!(attempts, 3);
    }

    #[test]
    fn permanent_lookup_failures_are_reported_at_once() {
        // The registry's own verdict is final: retrying it would only stall
        // the pipeline, so it travels to the parser after a single attempt.
        let mut attempts = 0;
        let result: Result<&str, String> = retry_transient_lookups(|| {
            attempts += 1;
            Err(SchemaLookupError::Permanent("no such version".to_string()))
        });
        assert_eq!(result, Err("no such version".to_string()));
        assert_eq!(attempts, 1);
    }

    fn position_frontier(
        topic: &str,
        partition: i32,
        position: MessagePosition,
    ) -> OffsetAntichain {
        let mut frontier = OffsetAntichain::new();
        let (ledger_id, entry_id, batch_index) = position;
        frontier.advance_offset(
            OffsetKey::Pulsar(PulsarOffsetKey::Partition(ArcStr::from(topic), partition)),
            OffsetValue::Pulsar(PulsarOffsetValue::MessagePosition {
                ledger_id,
                entry_id,
                batch_index,
            }),
        );
        frontier
    }

    #[test]
    fn merge_frontiers_keeps_the_furthest_position_per_partition() {
        // Recovering a Pulsar reader's frontier from several persisted
        // snapshots merges their offsets pairwise. The merged frontier must
        // keep the furthest position of each partition, so a restart resumes
        // after everything that was already processed rather than replaying
        // from an earlier snapshot. The batch index participates in the
        // ordering: all the messages of one producer batch share a ledger
        // and an entry.
        let behind = position_frontier("topic", 0, (12, 7, 0));
        let ahead = position_frontier("topic", 0, (12, 7, 3));

        let expected = OffsetValue::Pulsar(PulsarOffsetValue::MessagePosition {
            ledger_id: 12,
            entry_id: 7,
            batch_index: 3,
        });
        let key = OffsetKey::Pulsar(PulsarOffsetKey::Partition(ArcStr::from("topic"), 0));

        let merged = PulsarReader::merge_two_frontiers(&behind, &ahead);
        assert_eq!(merged.get_offset(&key), Some(&expected));

        // The result must not depend on the argument order.
        let merged_swapped = PulsarReader::merge_two_frontiers(&ahead, &behind);
        assert_eq!(merged_swapped.get_offset(&key), Some(&expected));
    }

    #[test]
    fn merge_frontiers_keeps_the_furthest_entries_count() {
        // The subscription mode tracks a monotonic per-worker delivery
        // counter; merging two frontiers must keep the larger one.
        let key = OffsetKey::Pulsar(PulsarOffsetKey::Worker(0));
        let mut behind = OffsetAntichain::new();
        behind.advance_offset(
            key.clone(),
            OffsetValue::Pulsar(PulsarOffsetValue::EntriesCount(10)),
        );
        let mut ahead = OffsetAntichain::new();
        ahead.advance_offset(
            key.clone(),
            OffsetValue::Pulsar(PulsarOffsetValue::EntriesCount(25)),
        );

        let expected = OffsetValue::Pulsar(PulsarOffsetValue::EntriesCount(25));
        let merged = PulsarReader::merge_two_frontiers(&behind, &ahead);
        assert_eq!(merged.get_offset(&key), Some(&expected));
    }
}
