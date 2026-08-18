// Copyright © 2026 Pathway

use log::error;
use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::mem::take;
use std::str::Utf8Error;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::{FutureExt, StreamExt};
use pulsar::compression::Compression as PulsarCompression;
use pulsar::consumer::{Consumer as PulsarConsumer, ConsumerOptions as PulsarConsumerOptions};
use pulsar::error::ConsumerError as PulsarConsumerError;
use pulsar::message::proto::command_subscribe::SubType as PulsarSubType;
use pulsar::producer::{Message as PulsarProducerMessage, SendFuture};
use pulsar::proto::MessageIdData;
use pulsar::{
    consumer::InitialPosition as PulsarInitialPosition, Producer, ProducerOptions, Pulsar,
    TokioExecutor,
};
use tokio::runtime::Runtime as TokioRuntime;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

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

// How long a respawned pump waits before recreating its consumer. A pump is
// only respawned after an error that has already exhausted the client's
// internal reconnection budget, so an immediate retry would most likely fail
// again and spin a hot error loop.
const PUMP_RESPAWN_DELAY: Duration = Duration::from_secs(5);

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
        properties,
    ))
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

enum PulsarReaderMode {
    /// Streaming through a broker-side subscription. The subscription cursor
    /// is advanced by immediate acknowledgements, so this mode cannot
    /// guarantee lossless recovery and is not allowed with persistence.
    Subscription {
        consumer: Option<Box<PulsarConsumer<Vec<u8>, TokioExecutor>>>,
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
    mode: PulsarReaderMode,
}

impl PulsarReader {
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_subscription(
        runtime: TokioRuntime,
        client: Pulsar<TokioExecutor>,
        consumer: PulsarConsumer<Vec<u8>, TokioExecutor>,
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
            mode: PulsarReaderMode::Subscription {
                consumer: Some(Box::new(consumer)),
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
            mode: PulsarReaderMode::PartitionReaders {
                partitions,
                static_mode,
                start_from_latest,
                positions: HashMap::new(),
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
            mode: PulsarReaderMode::Idle,
        }
    }

    fn physical_topic(base_topic: &str, partition: i32) -> String {
        if partition < 0 {
            base_topic.to_string()
        } else {
            format!("{base_topic}-partition-{partition}")
        }
    }

    /// Spawns one pump task per owned partition. Called lazily on the first
    /// `read`, so that `seek` has already restored the per-partition
    /// positions by the time the consumers are positioned.
    fn start_partition_pump(&mut self) -> PartitionPump {
        let PulsarReaderMode::PartitionReaders {
            partitions,
            static_mode,
            positions,
            ..
        } = &self.mode
        else {
            unreachable!("the pump is only started in the partition-reader mode");
        };
        let (partitions, static_mode) = (partitions.clone(), *static_mode);
        let start_positions: Vec<Option<MessagePosition>> = partitions
            .iter()
            .map(|partition| positions.get(partition).copied())
            .collect();
        let (sender, receiver) = mpsc::channel(PARTITION_PUMP_CHANNEL_CAPACITY);
        let join_handles = partitions
            .iter()
            .zip(start_positions)
            .map(|(partition, start_after)| {
                self.spawn_pump(
                    *partition,
                    start_after,
                    None,
                    Duration::ZERO,
                    sender.clone(),
                )
            })
            .collect();
        PartitionPump {
            receiver,
            sender,
            buffered: VecDeque::new(),
            remaining_static_partitions: static_mode.then_some(partitions.len()),
            join_handles,
        }
    }

    /// Spawns the pump task of one partition.
    fn spawn_pump(
        &self,
        partition: i32,
        start_after: Option<MessagePosition>,
        known_boundary: Option<MessagePosition>,
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
        // The subscription names must be unique: the consumers are exclusive,
        // and a lingering consumer of a previous (possibly killed) run or of
        // this partition's previous pump would fail the subscription with
        // ConsumerBusy.
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time is past the epoch")
            .as_nanos();
        let subscription_name = format!(
            "pathway-reader-{}-{}-p{partition}-{nonce}",
            self.connector_index, self.worker_index
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
                let started = self.start_partition_pump();
                if let PulsarReaderMode::PartitionReaders { pump, .. } = &mut self.mode {
                    *pump = Some(started);
                }
            }
        }
        loop {
            let PulsarReaderMode::PartitionReaders {
                positions, pump, ..
            } = &mut self.mode
            else {
                unreachable!("checked by the caller");
            };
            let pump = pump.as_mut().expect("the pump is started above");
            if pump.buffered.is_empty() {
                if pump.remaining_static_partitions == Some(0) {
                    // Static mode: every partition is drained up to its
                    // boundary.
                    return Ok(ReadResult::Finished);
                }
                let mut chunk = Vec::new();
                let received = self
                    .runtime
                    .block_on(pump.receiver.recv_many(&mut chunk, MAX_READ_BATCH_SIZE));
                if received == 0 {
                    // Unreachable while the respawn sender is held; kept as
                    // a defensive exit instead of a busy loop.
                    return Ok(ReadResult::Finished);
                }
                pump.buffered.extend(chunk);
            }
            match pump.buffered.pop_front().expect("refilled above") {
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
                    self.respawn_pump(partition, resume_after, resolved_boundary);
                    return Err(error.into());
                }
            }
        }
    }

    /// Takes up to `MAX_READ_BATCH_SIZE` messages from the subscription
    /// consumer in one runtime entry: waits for the first message, then keeps
    /// taking the already-delivered ones without waiting. Every message is
    /// acknowledged immediately: this mode is never used with persistence,
    /// so there is no reason to defer the cursor advancement.
    fn refill_subscription_preloaded(&mut self) -> Result<(), ReadError> {
        let PulsarReaderMode::Subscription {
            consumer,
            preloaded,
        } = &mut self.mode
        else {
            unreachable!("checked by the caller");
        };
        let consumer = consumer.as_mut().expect("consumer is set until drop");
        let base_topic = &self.base_topic;
        let with_metadata = self.with_metadata;
        // An exhausted consumer stream is never a normal end of data in the
        // subscription mode: the topic is unbounded. It means the client gave
        // up — most likely it exhausted its reconnection attempts during a
        // broker outage — so it must surface as an error, not as `Finished`:
        // otherwise a streaming pipeline would silently "complete" and ignore
        // all the data published after the broker recovers.
        let stream_closed = || {
            ReadError::from(PulsarError::StreamUnexpectedlyClosed {
                topic: base_topic.to_string(),
            })
        };
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
        self.runtime.block_on(async {
            let Some(first_message) = consumer.next().await else {
                return Err(stream_closed());
            };
            let first_message = first_message.map_err(PulsarError::from)?;
            consumer
                .ack(&first_message)
                .await
                .map_err(PulsarError::from)?;
            preloaded.push_back(preload_message(first_message));
            while preloaded.len() < MAX_READ_BATCH_SIZE {
                let message = match consumer.next().now_or_never() {
                    None => break,
                    Some(None) => return Err(stream_closed()),
                    Some(Some(message)) => message.map_err(PulsarError::from)?,
                };
                consumer.ack(&message).await.map_err(PulsarError::from)?;
                preloaded.push_back(preload_message(message));
            }
            Ok(())
        })
    }

    fn read_from_subscription(&mut self) -> Result<ReadResult, ReadError> {
        loop {
            let PulsarReaderMode::Subscription { preloaded, .. } = &mut self.mode else {
                unreachable!("checked by the caller");
            };
            let Some(message) = preloaded.pop_front() else {
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

#[allow(clippy::too_many_arguments)]
async fn pump_partition_inner(
    client: Pulsar<TokioExecutor>,
    physical_topic: &str,
    subscription_name: String,
    partition: i32,
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
    let mut options = PulsarConsumerOptions::default()
        .durable(false)
        .with_initial_position(if start_from_latest {
            PulsarInitialPosition::Latest
        } else {
            PulsarInitialPosition::Earliest
        });
    if let Some((ledger_id, entry_id, batch_index)) = *watermark {
        options = options.starting_on_message(MessageIdData {
            ledger_id,
            entry_id,
            batch_index: (batch_index >= 0).then_some(batch_index),
            partition: (partition >= 0).then_some(partition),
            ..MessageIdData::default()
        });
    }
    let mut consumer: PulsarConsumer<Vec<u8>, TokioExecutor> = client
        .consumer()
        .with_topic(physical_topic)
        .with_subscription(subscription_name)
        .with_subscription_type(PulsarSubType::Exclusive)
        .with_options(options)
        .build()
        .await?;

    if start_from_latest && watermark.is_none() {
        // Resolve "end" into a concrete position exactly once, at the first
        // pump start. A respawned pump must continue from where the end
        // *was*, not from the latest position at the respawn moment —
        // otherwise everything published between the failure and the
        // respawn would be silently skipped.
        let tail = partition_tail_position(&mut consumer).await?;
        *watermark = Some(tail.unwrap_or(DELIVER_EVERYTHING));
    }

    if static_mode && boundary_slot.is_none() {
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
        let filtered_out =
            min_publish_timestamp_ms.is_some_and(|t| message.payload.metadata.publish_time < t);
        if !filtered_out {
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
    in_flight: VecDeque<SendFuture>,
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
        compression: Option<PulsarCompression>,
        producer_name: Option<String>,
    ) -> Self {
        PulsarWriter {
            runtime,
            client,
            producers: Some(HashMap::new()),
            in_flight: VecDeque::new(),
            max_pending_payload_bytes: 0,
            topic,
            header_fields,
            key_field_index,
            ordering_key_field_index,
            event_time_field_index,
            event_time_from_engine,
            compression,
            producer_name,
        }
    }

    fn ensure_producer(&mut self, topic: &str) -> Result<(), WriteError> {
        let producers = self
            .producers
            .as_mut()
            .expect("producers are set until drop");
        if !producers.contains_key(topic) {
            let mut builder =
                self.client
                    .producer()
                    .with_topic(topic)
                    .with_options(ProducerOptions {
                        batch_size: Some(PRODUCER_BATCH_SIZE),
                        batch_byte_size: Some(PRODUCER_BATCH_MAX_BYTES),
                        // Await queue space instead of failing with
                        // `SlowDown` when the client's outbound channel
                        // is full.
                        block_queue_if_full: true,
                        compression: self.compression.clone(),
                        ..ProducerOptions::default()
                    });
            if let Some(producer_name) = &self.producer_name {
                builder = builder.with_name(producer_name);
            }
            let producer = self
                .runtime
                .block_on(builder.build())
                .map_err(PulsarError::from)?;
            producers.insert(topic.to_string(), producer);
        }
        Ok(())
    }

    // Awaits broker receipts, oldest first, until at most `limit` sends
    // remain in flight. Used both to apply backpressure in `write` and to
    // drain everything in `flush`. Partially filled batches are forced out
    // first: a receipt for a message sitting in an unfilled batch would
    // otherwise never resolve.
    async fn drain_in_flight(
        producers: &mut HashMap<String, Producer<TokioExecutor>>,
        in_flight: &mut VecDeque<SendFuture>,
        max_pending_payload_bytes: &mut usize,
        limit: usize,
    ) -> Result<(), WriteError> {
        if in_flight.len() > limit {
            let annotate =
                |error: pulsar::Error| Self::annotate_send_error(error, *max_pending_payload_bytes);
            for producer in producers.values_mut() {
                producer.send_batch().await.map_err(annotate)?;
            }
            while in_flight.len() > limit {
                let send_future = in_flight
                    .pop_front()
                    .expect("in_flight is non-empty while its length exceeds the limit");
                send_future.await.map_err(annotate)?;
            }
        }
        if in_flight.is_empty() {
            *max_pending_payload_bytes = 0;
        }
        Ok(())
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
}

impl Writer for PulsarWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        let effective_topic = self.topic.get_for_posting(&data.values)?;
        self.ensure_producer(&effective_topic)?;

        let partition_key = self.row_partition_key(&data)?;
        let ordering_key = self.row_ordering_key(&data)?;
        let event_time = self.row_event_time(&data)?;

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
            max_pending_payload_bytes,
            ..
        } = self;
        let producers = producers.as_mut().expect("producers are set until drop");

        runtime.block_on(async {
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
                if in_flight.len() >= MAX_IN_FLIGHT_SENDS {
                    Self::drain_in_flight(
                        producers,
                        in_flight,
                        max_pending_payload_bytes,
                        IN_FLIGHT_DRAIN_TARGET,
                    )
                    .await?;
                }
                let payload = payload.into_raw_bytes()?;
                *max_pending_payload_bytes = (*max_pending_payload_bytes).max(payload.len());
                let message = PulsarProducerMessage {
                    payload,
                    properties,
                    partition_key: Some(partition_key.clone()),
                    ordering_key: ordering_key.clone(),
                    event_time,
                    ..PulsarProducerMessage::default()
                };
                let producer = producers
                    .get_mut(&effective_topic)
                    .expect("the producer is created by ensure_producer above");
                let send_future = Box::pin(producer.send_non_blocking(message))
                    .await
                    .map_err(|e| Self::annotate_send_error(e, *max_pending_payload_bytes))?;
                in_flight.push_back(send_future);
            }
            Ok(())
        })
    }

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        let Self {
            runtime,
            producers,
            in_flight,
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
            Self::drain_in_flight(producers, in_flight, max_pending_payload_bytes, 0).await
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::Reader;
    use arcstr::ArcStr;

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
