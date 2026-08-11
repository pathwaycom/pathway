use log::{error, warn};
use std::borrow::Cow;
use std::collections::VecDeque;
use std::future::{Future, IntoFuture};
use std::mem::take;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use async_nats::header::HeaderMap as NatsHeaders;
use async_nats::jetstream;
use async_nats::jetstream::consumer::pull::Stream as NatsPullStream;
use async_nats::jetstream::context::Context as JetStream;
use async_nats::jetstream::context::PublishAckFuture as JetStreamAckFuture;
use async_nats::jetstream::context::PublishErrorKind;
use async_nats::jetstream::message::Acker as NatsAcker;
use async_nats::Client as NatsClient;
use async_nats::Subscriber as NatsSubscriber;
use bytes::Bytes;
use futures::StreamExt;
use tokio::runtime::Handle as TokioHandle;
use tokio::runtime::Runtime as TokioRuntime;

use crate::connectors::data_format::FormatterContext;
use crate::connectors::data_storage::{DeferredAckWorker, MessageQueueTopic};
use crate::connectors::{
    DataEventType, OffsetKey, OffsetValue, ReadError, ReadResult, Reader, ReaderContext,
    StorageType, WriteError, Writer,
};
use crate::persistence::frontier::OffsetAntichain;
use crate::retry::{execute_with_retries_if_async, RetryConfig};

// The maximum number of JetStream publishes whose acknowledgements may be
// outstanding at any given moment. Each publish is sent immediately and returns
// a future resolving to the server's acknowledgement; the writer only awaits
// those futures in `flush`. Letting an unbounded number of them accumulate
// (a large minibatch, multiplied by several workers writing to the same stream)
// overwhelms the server, which then rejects publishes with a "too many requests"
// error. Once this many are in flight, the oldest acknowledgement is awaited
// before sending more, bounding the pressure on the server while still allowing
// aggressive pipelining.
const MAX_IN_FLIGHT_ACKS: usize = 1000;

// How many times a single publish whose acknowledgement fails with a transient
// error (server overload or acknowledgement timeout) is re-sent before giving
// up. Retries use exponential backoff.
const PUBLISH_ACK_RETRIES: usize = 8;

pub enum NatsPoller {
    Simple(NatsSubscriber),
    JetStream(Box<NatsPullStream>),
}

impl NatsPoller {
    /// Returns the message payload along with the ack handle when the
    /// underlying consumer expects an explicit acknowledgement (`JetStream`).
    /// The caller decides when to acknowledge: right away, or - with
    /// persistence - once a durable checkpoint covers the message.
    async fn poll(&mut self) -> Result<Option<(Vec<u8>, Option<NatsAcker>)>, ReadError> {
        match self {
            Self::Simple(subscriber) => Ok(subscriber
                .next()
                .await
                .map(|message| (message.payload.to_vec(), None))),
            Self::JetStream(messages) => {
                let mut next_message = messages.take(1);
                if let Some(message) = next_message.next().await {
                    let (message, acker) = message?.split();
                    Ok(Some((message.payload.to_vec(), Some(acker))))
                } else {
                    Ok(None)
                }
            }
        }
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct NatsReader {
    // Dropping a NATS subscriber spawns its unsubscribe command onto the
    // current tokio runtime and panics if the thread is not inside one, so
    // the poller is dropped explicitly under `runtime.enter()` (see the
    // `Drop` impl below).
    poller: Option<NatsPoller>,
    runtime: TokioRuntime,
    worker_index: usize,
    total_entries_read: usize,
    topic: String,
    deferred_mode: bool,
    // Entries are pushed in the reading order, so the sequence numbers grow
    // monotonically and every acknowledged frontier cuts off a queue prefix.
    pending_acks: Arc<Mutex<VecDeque<(usize, NatsAcker)>>>,
}

impl Reader for NatsReader {
    fn read(&mut self) -> Result<ReadResult, ReadError> {
        let poller = self.poller.as_mut().expect("poller is set until drop");
        if let Some((message, acker)) = self.runtime.block_on(async { poller.poll().await })? {
            self.total_entries_read += 1;
            if let Some(acker) = acker {
                if self.deferred_mode {
                    // Must happen before the message is returned to the
                    // engine, so that no checkpoint can cover an offset whose
                    // ack handle is not in the queue yet.
                    self.pending_acks
                        .lock()
                        .unwrap()
                        .push_back((self.total_entries_read, acker));
                } else {
                    self.runtime
                        .block_on(async { acker.ack().await })
                        .map_err(ReadError::NatsMessageAck)?;
                }
            }
            let payload = ReaderContext::from_raw_bytes(DataEventType::Insert, message);
            let offset = (
                OffsetKey::Nats(self.worker_index),
                OffsetValue::NatsReadEntriesCount(self.total_entries_read),
            );
            Ok(ReadResult::Data(payload, offset))
        } else {
            Ok(ReadResult::Finished)
        }
    }

    fn take_deferred_ack_worker(&mut self) -> Option<Box<dyn DeferredAckWorker>> {
        if !matches!(self.poller, Some(NatsPoller::JetStream(_))) {
            // Core NATS is fire-and-forget: there is nothing to acknowledge,
            // and nothing the broker would redeliver after a crash.
            return None;
        }
        self.deferred_mode = true;
        Some(Box::new(NatsDeferredAckWorker {
            runtime_handle: self.runtime.handle().clone(),
            worker_index: self.worker_index,
            pending_acks: self.pending_acks.clone(),
        }))
    }

    fn seek(&mut self, frontier: &OffsetAntichain) -> Result<(), ReadError> {
        let offset_value = frontier.get_offset(&OffsetKey::Nats(self.worker_index));
        if let Some(offset) = offset_value {
            if let OffsetValue::NatsReadEntriesCount(last_run_entries_read) = offset {
                self.total_entries_read = *last_run_entries_read;
            } else {
                error!("Unexpected offset type for NATS reader: {offset:?}");
            }
        }
        Ok(())
    }

    fn storage_type(&self) -> StorageType {
        StorageType::Nats
    }

    fn max_allowed_consecutive_errors(&self) -> usize {
        32
    }

    fn short_description(&self) -> Cow<'static, str> {
        format!("NATS({})", self.topic).into()
    }
}

impl NatsReader {
    pub fn new(
        runtime: TokioRuntime,
        poller: NatsPoller,
        worker_index: usize,
        topic: String,
    ) -> NatsReader {
        NatsReader {
            runtime,
            poller: Some(poller),
            worker_index,
            topic,
            total_entries_read: 0,
            deferred_mode: false,
            pending_acks: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
}

pub struct NatsDeferredAckWorker {
    runtime_handle: TokioHandle,
    worker_index: usize,
    pending_acks: Arc<Mutex<VecDeque<(usize, NatsAcker)>>>,
}

impl DeferredAckWorker for NatsDeferredAckWorker {
    fn ack_up_to(&mut self, frontier: &OffsetAntichain) {
        let threshold = match frontier.get_offset(&OffsetKey::Nats(self.worker_index)) {
            Some(OffsetValue::NatsReadEntriesCount(entries_read)) => *entries_read,
            Some(other) => {
                error!("Unexpected offset type in the NATS ack frontier: {other:?}");
                return;
            }
            None => return,
        };
        let mut to_ack = Vec::new();
        {
            let mut pending_acks = self.pending_acks.lock().unwrap();
            loop {
                match pending_acks.front() {
                    Some((sequence_number, _)) if *sequence_number <= threshold => {}
                    _ => break,
                }
                to_ack.push(pending_acks.pop_front().unwrap().1);
            }
        }
        if to_ack.is_empty() {
            return;
        }
        // The acknowledgements are sent from the reader's runtime instead of
        // the checkpoint-committing thread, which must not block. JetStream
        // acknowledgements are per-message, so a failed one doesn't have to
        // stop the batch: the server redelivers the message after `ack_wait`,
        // which is a duplicate, never a loss.
        self.runtime_handle.spawn(async move {
            for acker in to_ack {
                if let Err(e) = acker.ack().await {
                    warn!(
                        "Failed to acknowledge a JetStream message, \
                         it will be redelivered by the server: {e}"
                    );
                }
            }
        });
    }
}

impl Drop for NatsReader {
    fn drop(&mut self) {
        // See the comment on the `poller` field.
        let _guard = self.runtime.enter();
        self.poller.take();
    }
}

pub type AccessorResult<'a> = Pin<Box<dyn Future<Output = Result<(), WriteError>> + 'a>>;
pub trait WriteAccessor: Send {
    fn publish_with_headers(
        &mut self,
        topic: String,
        headers: NatsHeaders,
        payload: Vec<u8>,
    ) -> AccessorResult<'_>;

    fn flush(&mut self) -> AccessorResult<'_>;
}

pub struct SimpleWriteAccessor {
    client: NatsClient,
}

impl SimpleWriteAccessor {
    pub fn new(client: NatsClient) -> Self {
        Self { client }
    }
}

impl WriteAccessor for SimpleWriteAccessor {
    fn publish_with_headers(
        &mut self,
        topic: String,
        headers: NatsHeaders,
        payload: Vec<u8>,
    ) -> AccessorResult<'_> {
        Box::pin(async {
            self.client
                .publish_with_headers(topic, headers, payload.into())
                .await
                .map_err(WriteError::NatsPublish)
        })
    }

    fn flush(&mut self) -> AccessorResult<'_> {
        Box::pin(async { self.client.flush().await.map_err(WriteError::NatsFlush) })
    }
}

// A JetStream publish whose acknowledgement has not been awaited yet. The
// message payload and headers are kept so the publish can be retried if its
// acknowledgement fails with a transient error.
struct InFlightPublish {
    subject: String,
    headers: NatsHeaders,
    payload: Bytes,
    ack: <JetStreamAckFuture as IntoFuture>::IntoFuture,
}

// A publish acknowledgement error is worth retrying only when it reflects a
// transient server condition: an overloaded server (mapped to `Other`, e.g. a
// "too many requests" response) or an acknowledgement that did not arrive in
// time. Permanent errors (missing stream, wrong expected sequence) fail fast.
fn is_retriable_publish_error(error: &WriteError) -> bool {
    match error {
        WriteError::JetStream(error) => {
            matches!(
                error.kind(),
                PublishErrorKind::TimedOut | PublishErrorKind::Other
            )
        }
        _ => false,
    }
}

pub struct JetStreamWriteAccessor {
    jetstream: JetStream,
    in_flight: VecDeque<InFlightPublish>,
    max_in_flight: usize,
}

impl JetStreamWriteAccessor {
    pub fn new(client: NatsClient) -> Self {
        Self {
            jetstream: jetstream::new(client),
            in_flight: VecDeque::new(),
            max_in_flight: MAX_IN_FLIGHT_ACKS,
        }
    }

    // Awaits a single publish acknowledgement, re-sending the message with
    // exponential backoff if the server reports a transient failure. The first
    // attempt reuses the acknowledgement future of the publish that was already
    // sent; only retries re-publish the message (which is safe under Pathway's
    // at-least-once output semantics).
    async fn await_ack(&self, publish: InFlightPublish) -> Result<(), WriteError> {
        let InFlightPublish {
            subject,
            headers,
            payload,
            ack,
        } = publish;
        let mut ack = Some(ack);
        execute_with_retries_if_async(
            async || {
                let ack_future = match ack.take() {
                    Some(existing) => existing,
                    None => self
                        .jetstream
                        .publish_with_headers(subject.clone(), headers.clone(), payload.clone())
                        .await?
                        .into_future(),
                };
                ack_future.await?;
                Ok(())
            },
            is_retriable_publish_error,
            RetryConfig::default(),
            PUBLISH_ACK_RETRIES,
        )
        .await
    }

    // Awaits acknowledgements, oldest first, until at most `limit` publishes
    // remain in flight. Used both to apply backpressure before sending a new
    // publish and to drain everything on `flush`.
    async fn drain_until(&mut self, limit: usize) -> Result<(), WriteError> {
        while self.in_flight.len() > limit {
            let publish = self
                .in_flight
                .pop_front()
                .expect("in_flight is non-empty while its length exceeds the limit");
            self.await_ack(publish).await?;
        }
        Ok(())
    }
}

impl WriteAccessor for JetStreamWriteAccessor {
    fn publish_with_headers(
        &mut self,
        topic: String,
        headers: NatsHeaders,
        payload: Vec<u8>,
    ) -> AccessorResult<'_> {
        Box::pin(async move {
            // Apply backpressure so we never have more than `max_in_flight`
            // un-acknowledged publishes outstanding at once.
            self.drain_until(self.max_in_flight.saturating_sub(1))
                .await?;
            let payload: Bytes = payload.into();
            let ack = self
                .jetstream
                .publish_with_headers(topic.clone(), headers.clone(), payload.clone())
                .await
                .map_err(WriteError::JetStream)?
                .into_future();
            self.in_flight.push_back(InFlightPublish {
                subject: topic,
                headers,
                payload,
                ack,
            });
            Ok(())
        })
    }

    fn flush(&mut self) -> AccessorResult<'_> {
        Box::pin(async move { self.drain_until(0).await })
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct NatsWriter {
    // The JetStream accessor owns subscriptions whose drop spawns onto the
    // current tokio runtime and panics if the thread is not inside one, so
    // the accessor is dropped explicitly under `runtime.enter()` (see the
    // `Drop` impl below).
    accessor: Option<Box<dyn WriteAccessor>>,
    runtime: TokioRuntime,
    topic: MessageQueueTopic,
    header_fields: Vec<(String, usize)>,
}

impl Writer for NatsWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        let accessor = self.accessor.as_mut().expect("accessor is set until drop");
        let topic = &self.topic;
        let header_fields = &self.header_fields;
        self.runtime.block_on(async {
            let last_payload_index = data.payloads.len() - 1;
            let mut common_headers = data.construct_nats_headers(header_fields);
            for (index, payload) in data.payloads.into_iter().enumerate() {
                // Avoid copying data on the last iteration, reuse the existing headers
                let headers = {
                    if index == last_payload_index {
                        take(&mut common_headers)
                    } else {
                        common_headers.clone()
                    }
                };
                let payload = payload.into_raw_bytes()?;
                let effective_topic = topic.get_for_posting(&data.values)?;
                accessor
                    .publish_with_headers(effective_topic, headers, payload)
                    .await?;
            }
            Ok(())
        })
    }

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        let accessor = self.accessor.as_mut().expect("accessor is set until drop");
        self.runtime.block_on(async { accessor.flush().await })
    }

    fn retriable(&self) -> bool {
        true
    }

    fn single_threaded(&self) -> bool {
        false
    }

    fn name(&self) -> String {
        format!("NATS({})", self.topic)
    }
}

impl Drop for NatsWriter {
    fn drop(&mut self) {
        self.flush(true).expect("failed to send the final messages");
        // See the comment on the `accessor` field.
        let _guard = self.runtime.enter();
        self.accessor.take();
    }
}

impl NatsWriter {
    pub fn new(
        runtime: TokioRuntime,
        accessor: Box<dyn WriteAccessor>,
        topic: MessageQueueTopic,
        header_fields: Vec<(String, usize)>,
    ) -> Self {
        NatsWriter {
            runtime,
            accessor: Some(accessor),
            topic,
            header_fields,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::Reader;

    fn nats_frontier(worker_index: usize, entries_read: usize) -> OffsetAntichain {
        let mut frontier = OffsetAntichain::new();
        frontier.advance_offset(
            OffsetKey::Nats(worker_index),
            OffsetValue::NatsReadEntriesCount(entries_read),
        );
        frontier
    }

    #[test]
    fn merge_frontiers_keeps_the_furthest_read_position() {
        // Recovering a NATS reader's frontier from several persisted snapshots
        // merges their offsets pairwise. The merged frontier must keep the
        // furthest-read position (the largest number of entries read), so a
        // restart resumes after everything that was already processed rather
        // than replaying from an earlier snapshot.
        let behind = nats_frontier(0, 10);
        let ahead = nats_frontier(0, 25);

        let expected = OffsetValue::NatsReadEntriesCount(25);

        let merged = NatsReader::merge_two_frontiers(&behind, &ahead);
        assert_eq!(merged.get_offset(&OffsetKey::Nats(0)), Some(&expected));

        // The result must not depend on the argument order.
        let merged_swapped = NatsReader::merge_two_frontiers(&ahead, &behind);
        assert_eq!(
            merged_swapped.get_offset(&OffsetKey::Nats(0)),
            Some(&expected)
        );
    }
}
