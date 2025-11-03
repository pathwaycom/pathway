use log::{error, info};
use std::borrow::Cow;
use std::future::{Future, IntoFuture};
use std::mem::take;
use std::pin::Pin;

use async_nats::header::HeaderMap as NatsHeaders;
use async_nats::jetstream;
use async_nats::jetstream::consumer::pull::Stream as NatsPullStream;
use async_nats::jetstream::context::Context as JetStream;
use async_nats::jetstream::context::PublishAckFuture as JetStreamAckFuture;
use async_nats::Client as NatsClient;
use async_nats::Subscriber as NatsSubscriber;
use futures::future::join_all;
use futures::StreamExt;
use tokio::runtime::Runtime as TokioRuntime;

use crate::connectors::data_format::FormatterContext;
use crate::connectors::data_storage::MessageQueueTopic;
use crate::connectors::{
    DataEventType, OffsetKey, OffsetValue, ReadError, ReadResult, Reader, ReaderContext,
    StorageType, WriteError, Writer,
};
use crate::persistence::frontier::OffsetAntichain;

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
    ) -> AccessorResult;

    fn flush(&mut self) -> AccessorResult;
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
    ) -> AccessorResult {
        Box::pin(async {
            self.client
                .publish_with_headers(topic, headers, payload.into())
                .await
                .map_err(WriteError::NatsPublish)
        })
    }

    fn flush(&mut self) -> AccessorResult {
        Box::pin(async { self.client.flush().await.map_err(WriteError::NatsFlush) })
    }
}

pub struct JetStreamWriteAccessor {
    jetstream: JetStream,
    ack_futures: Vec<<JetStreamAckFuture as IntoFuture>::IntoFuture>,
}

impl JetStreamWriteAccessor {
    pub fn new(client: NatsClient) -> Self {
        Self {
            jetstream: jetstream::new(client),
            ack_futures: Vec::new(),
        }
    }
}

impl WriteAccessor for JetStreamWriteAccessor {
    fn publish_with_headers(
        &mut self,
        topic: String,
        headers: NatsHeaders,
        payload: Vec<u8>,
    ) -> AccessorResult {
        Box::pin(async {
            let ack_future = self
                .jetstream
                .publish_with_headers(topic, headers, payload.into())
                .await
                .map_err(WriteError::JetStream)?;
            self.ack_futures.push(ack_future.into_future());
            Ok(())
        })
    }

    fn flush(&mut self) -> AccessorResult {
        Box::pin(async {
            for result in join_all(take(&mut self.ack_futures)).await {
                let _ = result?;
            }
            Ok(())
        })
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
                info!("Before publishing");
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
