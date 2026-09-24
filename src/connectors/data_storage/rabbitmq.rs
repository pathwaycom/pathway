// Copyright © 2026 Pathway

use arcstr::ArcStr;
use log::{error, warn};
use std::borrow::Cow;
use std::collections::HashMap;
use std::future::Future;
use std::io::ErrorKind;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::StreamExt;
use rabbitmq_stream_client::error::{
    ClientError, ConsumerCreateError, ConsumerDeliveryError, ProducerCreateError,
    ProducerPublishError,
};
use rabbitmq_stream_client::types::{Message, OffsetSpecification, ResponseCode, SimpleValue};
use rabbitmq_stream_client::{Consumer as RmqConsumer, Environment, Producer as RmqProducer};
use tokio::runtime::Runtime as TokioRuntime;

use crate::connectors::data_format::{FormatterContext, PathwayHeadersCache};
use crate::connectors::data_storage::MessageQueueTopic;
use crate::connectors::metadata::RabbitmqMetadata;
use crate::connectors::offset::RabbitmqStreamType;
use crate::connectors::socket_guard::{SilenceProfile, TCP_CONNECT_TIMEOUT};
use crate::connectors::{
    DataEventType, OffsetKey, OffsetValue, ReadError, ReadResult, Reader, ReaderContext,
    StorageType, WriteError, Writer,
};
use crate::persistence::frontier::OffsetAntichain;
use crate::retry::{execute_with_retries_if_async, RetryConfig};

#[derive(Debug, thiserror::Error)]
pub enum RabbitmqError {
    #[error(transparent)]
    Client(#[from] ClientError),

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

    #[error("{operation} did not complete within {}s", timeout.as_secs())]
    ConnectTimeout {
        operation: String,
        timeout: Duration,
    },
}

/// The broker interactions that set a connection up — the TCP connect with
/// the stream-protocol handshake, the metadata query and the subscribe of a
/// new consumer, the declaration of a new producer — are all awaited without
/// a timeout by the client library. Should the broker accept the TCP
/// connection but never finish the handshake (a broker still starting up, or
/// one overloaded enough to drop the connection process mid-handshake: the
/// `Tune` frame that the client waits for is then never sent, and the wait
/// is not woken up by the connection closing either), such an interaction
/// blocks forever. The pipeline then never starts, and because the
/// connectors are constructed under the GIL, it cannot even be interrupted
/// by a signal.
///
/// The library owns its sockets, so the stream-level guards of
/// `crate::connectors::socket_guard` cannot wrap them; the shared policy is
/// applied through what the library exposes instead: every such interaction
/// runs under the shared `TCP_CONNECT_TIMEOUT`, its transient failures are
/// retried with the shared backoff — on a fresh connection, since a
/// half-established one cannot be reused — and the liveness of an
/// established connection is left to the protocol heartbeat, see
/// `heartbeat_secs`.
const MAX_CONNECT_RETRIES: usize = 3;

/// The stream protocol's heartbeat, in seconds: the client sends one every
/// half of this, and closes a connection over which it received nothing for
/// four of them. That is the library's counterpart of the stall guard of
/// `crate::connectors::socket_guard`, and the value is derived from the same
/// silence profile so a dead flow is declared within the writer's bound
/// (4 × 11 s = 44 s) instead of the library's default 4 × 60 s. A closed
/// connection ends the consumer's stream, which the reader answers with a
/// reconnect (see `RabbitmqReader::read`).
#[allow(clippy::cast_possible_truncation)]
pub fn heartbeat_secs() -> u32 {
    (SilenceProfile::Writer.max_silence().as_secs() / 4) as u32
}

fn client_error_is_transient(error: &ClientError) -> bool {
    match error {
        // The broker dropped the connection in the middle of the exchange.
        ClientError::ConnectionClosed | ClientError::AlreadyClosed => true,
        ClientError::Io(e) => matches!(
            e.kind(),
            ErrorKind::ConnectionReset
                | ErrorKind::ConnectionAborted
                | ErrorKind::BrokenPipe
                | ErrorKind::UnexpectedEof
                | ErrorKind::TimedOut
                | ErrorKind::Interrupted
        ),
        _ => false,
    }
}

/// The stream is known to the broker, but its resources are not up yet
/// (a leader still being elected, the coordinator momentarily failing).
fn response_code_is_transient(code: &ResponseCode) -> bool {
    matches!(
        code,
        ResponseCode::StreamNotAvailable | ResponseCode::InternalError
    )
}

impl RabbitmqError {
    /// Whether a failure of a connection-time interaction is worth retrying
    /// from scratch: a timeout, a connection dropped by the broker, or a
    /// stream whose resources are still starting. A refused connection, a
    /// rejected login or a missing stream are reported right away.
    fn is_transient_at_connect(&self) -> bool {
        match self {
            RabbitmqError::ConnectTimeout { .. } => true,
            RabbitmqError::Client(e) => client_error_is_transient(e),
            RabbitmqError::ConsumerCreate(ConsumerCreateError::Client(e))
            | RabbitmqError::ProducerCreate(ProducerCreateError::Client(e)) => {
                client_error_is_transient(e)
            }
            RabbitmqError::ConsumerCreate(ConsumerCreateError::Create { status, .. })
            | RabbitmqError::ProducerCreate(ProducerCreateError::Create { status, .. }) => {
                response_code_is_transient(status)
            }
            _ => false,
        }
    }
}

/// Runs a connection-time broker interaction (`operation` names it for the
/// logs and the errors) under the shared `TCP_CONNECT_TIMEOUT`, retrying the
/// attempts that fail transiently with the shared backoff, up to
/// `MAX_CONNECT_RETRIES` times. Every attempt starts from scratch: `attempt`
/// must produce a fresh future (and with it a fresh connection) each time it
/// is called.
pub async fn connect_with_retries<T, E, F, Fut>(
    operation: &str,
    mut attempt: F,
) -> Result<T, RabbitmqError>
where
    E: Into<RabbitmqError>,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    execute_with_retries_if_async(
        async || match tokio::time::timeout(TCP_CONNECT_TIMEOUT, attempt()).await {
            Ok(result) => result.map_err(Into::into),
            Err(_elapsed) => Err(RabbitmqError::ConnectTimeout {
                operation: operation.to_string(),
                timeout: TCP_CONNECT_TIMEOUT,
            }),
        },
        RabbitmqError::is_transient_at_connect,
        RetryConfig::default(),
        MAX_CONNECT_RETRIES,
    )
    .await
}

/// Probe a `RabbitMQ` stream to find the offset of the last message.
///
/// Positions at the last chunk via `OffsetSpecification::Last` and drains it
/// to find the true tail offset. Returns `None` for empty streams. A failure
/// to subscribe the probe is an error, not an empty stream: a static read
/// that took it for one would silently produce an empty table.
pub async fn probe_last_offset(
    environment: &Environment,
    stream_name: &str,
) -> Result<Option<u64>, RabbitmqError> {
    let mut probe = connect_with_retries(
        &format!("subscribing the tail probe to the RabbitMQ stream {stream_name:?}"),
        || {
            environment
                .consumer()
                .offset(OffsetSpecification::Last)
                .build(stream_name)
        },
    )
    .await?;

    let mut last_seen: Option<u64> = None;
    // On an empty stream a `Last` subscription never delivers, and on a loaded
    // broker the last chunk of a non-empty stream can take a while to be read
    // back; a static read that mistakes the latter for an empty stream returns
    // nothing, so the wait is generous.
    let first_timeout = Duration::from_secs(15);
    let drain_timeout = Duration::from_millis(100);
    if let Ok(Some(Ok(delivery))) = tokio::time::timeout(first_timeout, probe.next()).await {
        last_seen = Some(delivery.offset());
        while let Ok(Some(Ok(delivery))) = tokio::time::timeout(drain_timeout, probe.next()).await {
            last_seen = Some(delivery.offset());
        }
    }
    Ok(last_seen)
}

/// Subscribes a consumer to the stream at the given position, retrying the
/// transient failures of the subscription (see `connect_with_retries`).
pub async fn build_consumer(
    environment: &Environment,
    stream_name: &str,
    offset: OffsetSpecification,
) -> Result<RmqConsumer, RabbitmqError> {
    connect_with_retries(
        &format!("subscribing to the RabbitMQ stream {stream_name:?}"),
        || {
            environment
                .consumer()
                .offset(offset.clone())
                .build(stream_name)
        },
    )
    .await
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
    headers_cache: PathwayHeadersCache,
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
        let mut header_props =
            data.construct_string_properties(&self.header_fields, &mut self.headers_cache);
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
            headers_cache: PathwayHeadersCache::default(),
            pending_confirms: Arc::new(AtomicUsize::new(0)),
            send_errors: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn ensure_producer(&mut self, stream_name: &str) -> Result<(), WriteError> {
        if !self.producers.contains_key(stream_name) {
            let Self {
                runtime,
                environment,
                ..
            } = self;
            let producer = runtime.block_on(connect_with_retries(
                &format!("declaring a producer for the RabbitMQ stream {stream_name:?}"),
                || environment.producer().build(stream_name),
            ))?;
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
    /// Static mode: the reader delivers the messages that existed when it was
    /// created (up to `end_offset`) and then finishes.
    is_static: bool,
    /// Where a rebuilt consumer starts when nothing has been read yet.
    initial_offset: OffsetSpecification,
    with_metadata: bool,
    deferred_read_result: Option<ReadResult>,
}

impl Reader for RabbitmqReader {
    fn read(&mut self) -> Result<ReadResult, ReadError> {
        // Return deferred result from previous call (metadata pattern)
        if let Some(deferred) = self.deferred_read_result.take() {
            return Ok(deferred);
        }

        // Static mode: stop without blocking on the next `consumer.next()` once
        // the last expected message has been read, or right away when the tail
        // probe saw nothing (an empty stream). Waiting on the consumer instead
        // would block the reader forever: a static read has no other way of
        // ending.
        if self.is_static {
            match (self.end_offset, self.last_read_offset) {
                (None, _) => return Ok(ReadResult::Finished),
                (Some(end), Some(last)) if last >= end => return Ok(ReadResult::Finished),
                _ => {}
            }
        }

        let delivery = loop {
            match self.runtime.block_on(async { self.consumer.next().await }) {
                // The consumer's stream ends only when its connection was
                // closed: by the broker going away, or by the client's own
                // heartbeat guard declaring the flow dead (see
                // `heartbeat_secs`). Neither is the end of the data, so the
                // reader reconnects where it left off instead of finishing a
                // pipeline that would then silently never see a message again.
                None => {
                    warn!(
                        "RabbitMQ({}): the connection of the consumer was closed, reconnecting",
                        self.stream_name
                    );
                    let resume_from = match self.last_read_offset {
                        Some(last) => OffsetSpecification::Offset(
                            last.checked_add(1).ok_or(RabbitmqError::OffsetOverflow)?,
                        ),
                        None => self.initial_offset.clone(),
                    };
                    self.consumer = self.runtime.block_on(build_consumer(
                        &self.environment,
                        &self.stream_name,
                        resume_from,
                    ))?;
                }
                Some(delivery) => break delivery,
            }
        };
        match delivery {
            Ok(delivery) => {
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
            Err(e) => Err(RabbitmqError::from(e).into()),
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
                    .block_on(probe_last_offset(&self.environment, &self.stream_name))?
                    .unwrap_or(0);
                if *saved_offset > stream_last {
                    return Err(RabbitmqError::InvalidPersistedOffset {
                        saved: *saved_offset,
                        stream_last,
                    }
                    .into());
                }

                let new_consumer = self.runtime.block_on(build_consumer(
                    &self.environment,
                    &self.stream_name,
                    OffsetSpecification::Offset(next_offset),
                ))?;
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
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        runtime: TokioRuntime,
        consumer: RmqConsumer,
        environment: Environment,
        stream_name: ArcStr,
        is_static: bool,
        initial_offset: OffsetSpecification,
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
            is_static,
            initial_offset,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    // Paused time: the connect timeout and the backoff sleeps resolve as soon
    // as the runtime is idle, so these run in milliseconds.

    #[tokio::test(start_paused = true)]
    async fn stalled_attempt_is_timed_out_and_the_next_one_succeeds() {
        // The failure mode of a broker that accepts the TCP connection but
        // never finishes the handshake: the attempt's future stays pending.
        let attempts = Cell::new(0);
        let result = connect_with_retries("connecting", || {
            attempts.set(attempts.get() + 1);
            let stalled = attempts.get() == 1;
            async move {
                if stalled {
                    std::future::pending::<()>().await;
                }
                Ok::<_, ClientError>("connected")
            }
        })
        .await;
        assert_eq!(result.unwrap(), "connected");
        assert_eq!(attempts.get(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn dropped_connection_is_retried() {
        let attempts = Cell::new(0);
        let result = connect_with_retries("connecting", || {
            attempts.set(attempts.get() + 1);
            let outcome = if attempts.get() < 3 {
                Err(ClientError::ConnectionClosed)
            } else {
                Ok(())
            };
            async move { outcome }
        })
        .await;
        assert!(result.is_ok());
        assert_eq!(attempts.get(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn rejected_login_is_reported_at_once() {
        let attempts = Cell::new(0);
        let result = connect_with_retries("connecting", || {
            attempts.set(attempts.get() + 1);
            async {
                Err::<(), _>(ClientError::RequestError(
                    ResponseCode::AuthenticationFailure,
                ))
            }
        })
        .await;
        assert!(matches!(
            result,
            Err(RabbitmqError::Client(ClientError::RequestError(
                ResponseCode::AuthenticationFailure
            )))
        ));
        assert_eq!(attempts.get(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_broker_that_never_answers_is_given_up_on() {
        let attempts = Cell::new(0);
        let result = connect_with_retries("connecting", || {
            attempts.set(attempts.get() + 1);
            async {
                std::future::pending::<()>().await;
                Ok::<_, ClientError>(())
            }
        })
        .await;
        let error = result.unwrap_err();
        assert!(
            matches!(error, RabbitmqError::ConnectTimeout { .. }),
            "unexpected error: {error}"
        );
        assert_eq!(attempts.get(), MAX_CONNECT_RETRIES + 1);
    }

    #[test]
    fn heartbeat_declares_a_dead_flow_within_the_shared_silence_bound() {
        let heartbeat = heartbeat_secs();
        assert!(heartbeat > 0);
        assert!(u64::from(heartbeat) * 4 <= SilenceProfile::Writer.max_silence().as_secs());
    }
}
