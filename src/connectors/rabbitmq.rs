// Copyright © 2026 Pathway

use arcstr::ArcStr;
use log::error;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::StreamExt;
use rabbitmq_stream_client::error::{
    ConsumerCreateError, ConsumerDeliveryError, ProducerCreateError, ProducerPublishError,
};
use rabbitmq_stream_client::types::{Message, OffsetSpecification, SimpleValue};
use rabbitmq_stream_client::{Consumer as RmqConsumer, Environment, Producer as RmqProducer};
use tokio::runtime::Runtime as TokioRuntime;

use crate::connectors::data_format::{serialize_value_to_json, FormatterContext};
use crate::connectors::data_storage::MessageQueueTopic;
use crate::connectors::metadata::RabbitmqMetadata;
use crate::connectors::offset::RabbitmqStreamType;
use crate::connectors::{
    DataEventType, OffsetKey, OffsetValue, ReadError, ReadResult, Reader, ReaderContext,
    StorageType, WriteError, Writer,
};
use crate::persistence::frontier::OffsetAntichain;

#[derive(Debug, thiserror::Error)]
pub enum RabbitmqError {
    #[error(transparent)]
    ConsumerDelivery(#[from] ConsumerDeliveryError),

    #[error(transparent)]
    ConsumerCreate(#[from] ConsumerCreateError),

    #[error(transparent)]
    ProducerCreate(#[from] ProducerCreateError),

    #[error(transparent)]
    Publish(#[from] ProducerPublishError),

    #[error("message not confirmed (publishing_id={publishing_id})")]
    NotConfirmed { publishing_id: u64 },

    #[error("flush timed out after {timeout_secs}s with {pending} messages still pending")]
    FlushTimeout { timeout_secs: u64, pending: usize },

    #[error("offset overflow: cannot seek past u64::MAX")]
    OffsetOverflow,

    #[error(
        "persisted offset {saved} exceeds stream's last offset {stream_last}; \
         persistence data may be corrupted or from a different stream"
    )]
    InvalidPersistedOffset { saved: u64, stream_last: u64 },
}

/// Probe a `RabbitMQ` stream to find the offset of the last message.
///
/// Positions at the last chunk via `OffsetSpecification::Last` and drains it
/// to find the true tail offset. Returns `None` for empty streams.
pub async fn probe_last_offset(environment: &Environment, stream_name: &str) -> Option<u64> {
    let mut probe = environment
        .consumer()
        .offset(OffsetSpecification::Last)
        .build(stream_name)
        .await
        .ok()?;

    let mut last_seen: Option<u64> = None;
    let first_timeout = Duration::from_secs(5);
    let drain_timeout = Duration::from_millis(100);
    if let Ok(Some(Ok(delivery))) = tokio::time::timeout(first_timeout, probe.next()).await {
        last_seen = Some(delivery.offset());
        while let Ok(Some(Ok(delivery))) = tokio::time::timeout(drain_timeout, probe.next()).await {
            last_seen = Some(delivery.offset());
        }
    }
    last_seen
}

const FLUSH_TIMEOUT: Duration = Duration::from_secs(30);
const FLUSH_POLL_INTERVAL: Duration = Duration::from_millis(10);

// --- Writer ---

/// A `RabbitMQ` Streams producer is bound to a single stream at creation time.
/// To support dynamic topics (where each row can target a different stream),
/// we cache producers per stream name and create them on demand.
#[allow(clippy::module_name_repetitions)]
pub struct RabbitmqWriter {
    runtime: TokioRuntime,
    environment: Environment,
    producers: HashMap<String, RmqProducer<rabbitmq_stream_client::NoDedup>>,
    topic: MessageQueueTopic,
    header_fields: Vec<(String, usize)>,
    pending_confirms: Arc<AtomicUsize>,
    send_errors: Arc<Mutex<Vec<RabbitmqError>>>,
}

impl Writer for RabbitmqWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        self.check_send_errors()?;

        let effective_topic = self.topic.get_for_posting(&data.values)?;
        self.ensure_producer(&effective_topic)?;

        // User-defined header values are serialized as AMQP strings using JSON
        // encoding because RabbitMQ Streams does not reliably confirm messages
        // with non-string application property values.
        let mut header_props: Vec<(String, String)> =
            Vec::with_capacity(self.header_fields.len() + 2);
        // pathway_time and pathway_diff are always added, consistent with
        // Kafka and NATS writers.
        header_props.push(("pathway_time".to_string(), data.time.to_string()));
        header_props.push(("pathway_diff".to_string(), data.diff.to_string()));
        for (name, idx) in &self.header_fields {
            let json = serialize_value_to_json(&data.values[*idx])
                .map(|v| v.to_string())
                .unwrap_or_default();
            header_props.push((name.clone(), json));
        }
        let pending = self.pending_confirms.clone();
        let errs = self.send_errors.clone();

        let Self {
            runtime, producers, ..
        } = self;
        let producer = producers.get_mut(&effective_topic).unwrap();

        runtime.block_on(async {
            let last_idx = data.payloads.len() - 1;
            for (idx, payload) in data.payloads.into_iter().enumerate() {
                let payload_bytes = payload.into_raw_bytes()?;
                let message = {
                    let mut app_props = Message::builder().application_properties();
                    if idx == last_idx {
                        for (name, value) in std::mem::take(&mut header_props) {
                            app_props = app_props.insert(name.as_str(), value.as_str());
                        }
                    } else {
                        for (name, value) in &header_props {
                            app_props = app_props.insert(name.as_str(), value.as_str());
                        }
                    }
                    app_props.message_builder().body(payload_bytes).build()
                };

                pending.fetch_add(1, Ordering::Release);
                let counter = pending.clone();
                let err_sink = errs.clone();
                producer
                    .send(message, move |result| {
                        let c = counter;
                        let e = err_sink;
                        async move {
                            c.fetch_sub(1, Ordering::Release);
                            match result {
                                Ok(confirm) if !confirm.confirmed() => {
                                    e.lock().unwrap().push(RabbitmqError::NotConfirmed {
                                        publishing_id: confirm.publishing_id(),
                                    });
                                }
                                Err(err) => {
                                    e.lock().unwrap().push(RabbitmqError::Publish(err));
                                }
                                _ => {}
                            }
                        }
                    })
                    .await
                    .map_err(RabbitmqError::from)?;
            }
            Ok(())
        })
    }

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        let pending = self.pending_confirms.clone();
        let start = Instant::now();
        // Use tokio::time::sleep so the runtime can process confirmation callbacks
        // between polls. std::thread::sleep would starve the async tasks.
        self.runtime.block_on(async {
            while pending.load(Ordering::Acquire) > 0 {
                if start.elapsed() > FLUSH_TIMEOUT {
                    return Err(WriteError::Rabbitmq(RabbitmqError::FlushTimeout {
                        timeout_secs: FLUSH_TIMEOUT.as_secs(),
                        pending: pending.load(Ordering::Acquire),
                    }));
                }
                tokio::time::sleep(FLUSH_POLL_INTERVAL).await;
            }
            Ok(())
        })?;
        self.check_send_errors()
    }

    fn retriable(&self) -> bool {
        true
    }

    fn single_threaded(&self) -> bool {
        false
    }

    fn name(&self) -> String {
        format!("RabbitMQ({})", self.topic)
    }
}

impl Drop for RabbitmqWriter {
    fn drop(&mut self) {
        if let Err(e) = self.flush(true) {
            error!("RabbitMQ flush failed on drop: {e}");
        }
    }
}

impl RabbitmqWriter {
    pub fn new(
        runtime: TokioRuntime,
        environment: Environment,
        topic: MessageQueueTopic,
        header_fields: Vec<(String, usize)>,
    ) -> Self {
        RabbitmqWriter {
            runtime,
            environment,
            producers: HashMap::new(),
            topic,
            header_fields,
            pending_confirms: Arc::new(AtomicUsize::new(0)),
            send_errors: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn ensure_producer(&mut self, stream_name: &str) -> Result<(), WriteError> {
        if !self.producers.contains_key(stream_name) {
            let producer = self
                .runtime
                .block_on(self.environment.producer().build(stream_name))
                .map_err(RabbitmqError::from)?;
            self.producers.insert(stream_name.to_string(), producer);
        }
        Ok(())
    }

    fn check_send_errors(&self) -> Result<(), WriteError> {
        let mut errs = self.send_errors.lock().unwrap();
        if let Some(err) = errs.drain(..).next() {
            return Err(err.into());
        }
        Ok(())
    }
}

// --- Reader ---

#[allow(clippy::module_name_repetitions)]
pub struct RabbitmqReader {
    runtime: TokioRuntime,
    consumer: RmqConsumer,
    environment: Environment,
    stream_name: ArcStr,
    /// When set, the reader stops after processing a message with this offset.
    /// Used for static mode. If the consumer starts past this offset (e.g.
    /// `start_from="end"`), the reader returns `Finished` immediately.
    end_offset: Option<u64>,
    /// Tracks the last offset we returned, so we can detect when we've read
    /// past `end_offset` without waiting for another message.
    last_read_offset: Option<u64>,
    with_metadata: bool,
    deferred_read_result: Option<ReadResult>,
}

impl Reader for RabbitmqReader {
    fn read(&mut self) -> Result<ReadResult, ReadError> {
        // Return deferred result from previous call (metadata pattern)
        if let Some(deferred) = self.deferred_read_result.take() {
            return Ok(deferred);
        }

        // Static mode: if we already read the last expected message, stop
        // without blocking on the next consumer.next().
        if let (Some(end), Some(last)) = (self.end_offset, self.last_read_offset) {
            if last >= end {
                return Ok(ReadResult::Finished);
            }
        }

        let delivery = self.runtime.block_on(async { self.consumer.next().await });
        match delivery {
            Some(Ok(delivery)) => {
                let stream_offset = delivery.offset();
                self.last_read_offset = Some(stream_offset);

                // Static mode: offsets are sequential, so once we pass end_offset we're done
                if let Some(end) = self.end_offset {
                    if stream_offset > end {
                        return Ok(ReadResult::Finished);
                    }
                }

                let message = delivery.message();
                let body = message.data().map(<[u8]>::to_vec);
                let payload =
                    ReaderContext::from_raw_bytes(DataEventType::Insert, body.unwrap_or_default());

                let offset = (
                    OffsetKey::Rabbitmq(RabbitmqStreamType::Stream(self.stream_name.clone())),
                    OffsetValue::RabbitmqOffset(stream_offset),
                );

                if self.with_metadata {
                    let metadata =
                        Self::extract_metadata(message, stream_offset, &self.stream_name);
                    self.deferred_read_result = Some(ReadResult::Data(payload, offset));
                    Ok(ReadResult::NewSource(metadata.into()))
                } else {
                    Ok(ReadResult::Data(payload, offset))
                }
            }
            Some(Err(e)) => Err(RabbitmqError::from(e).into()),
            None => Ok(ReadResult::Finished),
        }
    }

    fn seek(&mut self, frontier: &OffsetAntichain) -> Result<(), ReadError> {
        let key = OffsetKey::Rabbitmq(RabbitmqStreamType::Stream(self.stream_name.clone()));
        let offset_value = frontier.get_offset(&key);
        if let Some(offset) = offset_value {
            if let OffsetValue::RabbitmqOffset(saved_offset) = offset {
                let next_offset = saved_offset
                    .checked_add(1)
                    .ok_or(RabbitmqError::OffsetOverflow)?;

                // Validate: the saved offset must not exceed the stream's
                // current last offset. A larger value indicates corrupted
                // persistence data or a mismatched stream.
                //
                // If `probe_last_offset` returns `None` the stream is either empty
                // or didn't deliver within the probe timeout; in both cases we
                // can't positively confirm a message at `saved_offset` exists.
                // Treating the tail as 0 means any `saved_offset > 0` fails fast
                // here instead of letting the main consumer block forever on a
                // seek to an out-of-range offset (static mode hangs otherwise).
                let stream_last = self
                    .runtime
                    .block_on(probe_last_offset(&self.environment, &self.stream_name))
                    .unwrap_or(0);
                if *saved_offset > stream_last {
                    return Err(RabbitmqError::InvalidPersistedOffset {
                        saved: *saved_offset,
                        stream_last,
                    }
                    .into());
                }

                let new_consumer = self
                    .runtime
                    .block_on(async {
                        self.environment
                            .consumer()
                            .offset(OffsetSpecification::Offset(next_offset))
                            .build(&self.stream_name)
                            .await
                    })
                    .map_err(RabbitmqError::from)?;
                self.consumer = new_consumer;
            } else {
                error!("Unexpected offset type for RabbitMQ reader: {offset:?}");
            }
        }
        Ok(())
    }

    fn storage_type(&self) -> StorageType {
        StorageType::Rabbitmq
    }

    fn max_allowed_consecutive_errors(&self) -> usize {
        32
    }

    fn short_description(&self) -> Cow<'static, str> {
        format!("RabbitMQ({})", self.stream_name).into()
    }
}

impl RabbitmqReader {
    pub fn new(
        runtime: TokioRuntime,
        consumer: RmqConsumer,
        environment: Environment,
        stream_name: ArcStr,
        end_offset: Option<u64>,
        already_at_end: bool,
        with_metadata: bool,
    ) -> RabbitmqReader {
        // If already_at_end is true (`start_from="end"` in static mode), the
        // consumer is positioned past all existing data. Set last_read_offset
        // so the first read() returns Finished without blocking.
        let last_read_offset = if already_at_end { end_offset } else { None };
        RabbitmqReader {
            runtime,
            consumer,
            environment,
            stream_name,
            end_offset,
            last_read_offset,
            with_metadata,
            deferred_read_result: None,
        }
    }

    fn extract_metadata(message: &Message, offset: u64, stream_name: &str) -> RabbitmqMetadata {
        let props = message.properties();
        let header = message.header();
        let app_props = message.application_properties().map(|ap| {
            ap.iter()
                .map(|(k, v)| {
                    let value_str = match v {
                        SimpleValue::String(s) => s.clone(),
                        SimpleValue::Null => "null".to_string(),
                        SimpleValue::Boolean(b) => b.to_string(),
                        SimpleValue::Long(i) => i.to_string(),
                        SimpleValue::Int(i) => i.to_string(),
                        SimpleValue::Short(i) => i.to_string(),
                        SimpleValue::Byte(i) => i.to_string(),
                        SimpleValue::Ulong(u) => u.to_string(),
                        SimpleValue::Uint(u) => u.to_string(),
                        SimpleValue::Ushort(u) => u.to_string(),
                        SimpleValue::Ubyte(u) => u.to_string(),
                        SimpleValue::Float(f) => format!("{f:?}"),
                        SimpleValue::Double(f) => format!("{f:?}"),
                        other => format!("{other:?}"),
                    };
                    (k.clone(), value_str)
                })
                .collect()
        });
        RabbitmqMetadata {
            offset,
            stream_name: stream_name.to_string(),
            message_id: props
                .and_then(|p| p.message_id.as_ref())
                .map(|id| format!("{id:?}")),
            correlation_id: props
                .and_then(|p| p.correlation_id.as_ref())
                .map(|id| format!("{id:?}")),
            content_type: props
                .and_then(|p| p.content_type.as_ref())
                .map(|s| s.to_string()),
            content_encoding: props
                .and_then(|p| p.content_encoding.as_ref())
                .map(|s| s.to_string()),
            subject: props.and_then(|p| p.subject.clone()),
            reply_to: props
                .and_then(|p| p.reply_to.as_ref())
                .map(ToString::to_string),
            priority: header.map(|h| h.priority),
            durable: header.map(|h| h.durable),
            application_properties: app_props,
        }
    }
}
