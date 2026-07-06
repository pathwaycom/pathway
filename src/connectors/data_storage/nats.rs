use log::error;
use std::borrow::Cow;
use std::collections::VecDeque;
use std::future::{Future, IntoFuture};
use std::mem::take;
use std::pin::Pin;

use async_nats::header::HeaderMap as NatsHeaders;
use async_nats::jetstream;
use async_nats::jetstream::consumer::pull::Stream as NatsPullStream;
use async_nats::jetstream::context::Context as JetStream;
use async_nats::jetstream::context::PublishAckFuture as JetStreamAckFuture;
use async_nats::jetstream::context::PublishErrorKind;
use async_nats::Client as NatsClient;
use async_nats::Subscriber as NatsSubscriber;
use bytes::Bytes;
use futures::StreamExt;
use tokio::runtime::Runtime as TokioRuntime;

use crate::connectors::data_format::FormatterContext;
use crate::connectors::data_storage::MessageQueueTopic;
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
    async fn poll(&mut self) -> Result<Option<Vec<u8>>, ReadError> {
        match self {
            Self::Simple(subscriber) => Ok(subscriber
                .next()
                .await
                .map(|message| message.payload.to_vec())),
            Self::JetStream(messages) => {
                let mut next_message = messages.take(1);
                if let Some(message) = next_message.next().await {
                    let message = message?;
                    let message_contents = message.payload.to_vec();
                    message.ack().await.map_err(ReadError::NatsMessageAck)?;
                    Ok(Some(message_contents))
                } else {
                    Ok(None)
                }
            }
        }
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct NatsReader {
    runtime: TokioRuntime,
    poller: NatsPoller,
    worker_index: usize,
    total_entries_read: usize,
    topic: String,
}

impl Reader for NatsReader {
    fn read(&mut self) -> Result<ReadResult, ReadError> {
        if let Some(message) = self.runtime.block_on(async { self.poller.poll().await })? {
            let payload = ReaderContext::from_raw_bytes(DataEventType::Insert, message);
            self.total_entries_read += 1;
            let offset = (
                OffsetKey::Nats(self.worker_index),
                OffsetValue::NatsReadEntriesCount(self.total_entries_read),
            );
            Ok(ReadResult::Data(payload, offset))
        } else {
            Ok(ReadResult::Finished)
        }
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
            poller,
            worker_index,
            topic,
            total_entries_read: 0,
        }
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
    runtime: TokioRuntime,
    accessor: Box<dyn WriteAccessor>,
    topic: MessageQueueTopic,
    header_fields: Vec<(String, usize)>,
}

impl Writer for NatsWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        self.runtime.block_on(async {
            let last_payload_index = data.payloads.len() - 1;
            let mut common_headers = data.construct_nats_headers(&self.header_fields);
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
                let effective_topic = self.topic.get_for_posting(&data.values)?;
                self.accessor
                    .publish_with_headers(effective_topic, headers, payload)
                    .await?;
            }
            Ok(())
        })
    }

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        self.runtime.block_on(async { self.accessor.flush().await })
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
            accessor,
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
