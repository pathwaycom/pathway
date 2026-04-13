// Copyright © 2026 Pathway

use log::error;
use std::borrow::Cow;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::StreamExt;
use rabbitmq_stream_client::types::{Message, OffsetSpecification, SimpleValue};
use rabbitmq_stream_client::{Consumer as RmqConsumer, Environment, Producer as RmqProducer};
use tokio::runtime::Runtime as TokioRuntime;

use crate::connectors::data_format::FormatterContext;
use crate::connectors::data_storage::MessageQueueTopic;
use crate::connectors::metadata::RabbitmqMetadata;
use crate::connectors::{
    OffsetKey, OffsetValue, ReadError, ReadResult, Reader, ReaderContext, StorageType, WriteError,
    Writer,
};
use crate::engine::Value;
use crate::persistence::frontier::OffsetAntichain;

/// AMQP 1.0 application property name used to carry the message key.
/// Writer stores the key here; reader extracts it from this property.
const RABBITMQ_KEY_PROPERTY: &str = "key";

const FLUSH_TIMEOUT: Duration = Duration::from_secs(30);
const FLUSH_POLL_INTERVAL: Duration = Duration::from_millis(10);

// --- Writer ---

#[allow(clippy::module_name_repetitions)]
pub struct RabbitmqWriter {
    runtime: TokioRuntime,
    producer: RmqProducer<rabbitmq_stream_client::NoDedup>,
    topic: MessageQueueTopic,
    header_fields: Vec<(String, usize)>,
    key_field_index: Option<usize>,
    pending_confirms: Arc<AtomicUsize>,
    send_errors: Arc<Mutex<Vec<String>>>,
}

impl Writer for RabbitmqWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        // Check for errors accumulated from previous async sends
        {
            let errs = self.send_errors.lock().unwrap();
            if !errs.is_empty() {
                return Err(WriteError::RabbitmqPublish(errs.join("; ")));
            }
        }

        self.runtime.block_on(async {
            let properties = data.construct_rabbitmq_properties(&self.header_fields);

            // Extract key from data values if key_field_index is set
            let key_value: Option<String> =
                self.key_field_index.map(|idx| match &data.values[idx] {
                    Value::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
                    Value::String(s) => s.to_string(),
                    other => other.to_string(),
                });

            let has_app_props = !properties.is_empty() || key_value.is_some();

            for payload in data.payloads {
                let payload_bytes = payload.into_raw_bytes()?;
                let message = if has_app_props {
                    let mut app_props = Message::builder().application_properties();
                    if let Some(ref key) = key_value {
                        app_props = app_props.insert(RABBITMQ_KEY_PROPERTY, key.as_str());
                    }
                    for (name, value) in &properties {
                        app_props = app_props.insert(name.as_str(), value.as_str());
                    }
                    app_props.message_builder().body(payload_bytes).build()
                } else {
                    Message::builder().body(payload_bytes).build()
                };

                self.pending_confirms.fetch_add(1, Ordering::Release);
                let counter = self.pending_confirms.clone();
                let errs = self.send_errors.clone();
                self.producer
                    .send(message, move |result| {
                        let c = counter;
                        let e = errs;
                        async move {
                            c.fetch_sub(1, Ordering::Release);
                            match result {
                                Ok(confirm) if !confirm.confirmed() => {
                                    e.lock().unwrap().push(format!(
                                        "message not confirmed (publishing_id={})",
                                        confirm.publishing_id()
                                    ));
                                }
                                Err(err) => {
                                    e.lock().unwrap().push(format!("{err}"));
                                }
                                _ => {}
                            }
                        }
                    })
                    .await
                    .map_err(|e| WriteError::RabbitmqPublish(e.to_string()))?;
            }
            Ok(())
        })
    }

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        let start = Instant::now();
        while self.pending_confirms.load(Ordering::Acquire) > 0 {
            if start.elapsed() > FLUSH_TIMEOUT {
                return Err(WriteError::RabbitmqPublish(format!(
                    "flush timed out after {}s with {} messages still pending",
                    FLUSH_TIMEOUT.as_secs(),
                    self.pending_confirms.load(Ordering::Acquire)
                )));
            }
            std::thread::sleep(FLUSH_POLL_INTERVAL);
        }
        let errs = self.send_errors.lock().unwrap();
        if !errs.is_empty() {
            return Err(WriteError::RabbitmqPublish(errs.join("; ")));
        }
        Ok(())
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
        producer: RmqProducer<rabbitmq_stream_client::NoDedup>,
        topic: MessageQueueTopic,
        header_fields: Vec<(String, usize)>,
        key_field_index: Option<usize>,
    ) -> Self {
        RabbitmqWriter {
            runtime,
            producer,
            topic,
            header_fields,
            key_field_index,
            pending_confirms: Arc::new(AtomicUsize::new(0)),
            send_errors: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

// --- Reader ---

#[allow(clippy::module_name_repetitions)]
pub struct RabbitmqReader {
    runtime: TokioRuntime,
    consumer: RmqConsumer,
    environment: Environment,
    worker_index: usize,
    stream_name: String,
    end_offset: Option<u64>,
    deferred_read_result: Option<ReadResult>,
}

impl Reader for RabbitmqReader {
    fn read(&mut self) -> Result<ReadResult, ReadError> {
        // Return deferred result from previous call (metadata pattern)
        if let Some(deferred) = self.deferred_read_result.take() {
            return Ok(deferred);
        }

        let delivery = self.runtime.block_on(async { self.consumer.next().await });
        match delivery {
            Some(Ok(delivery)) => {
                let stream_offset = delivery.offset();

                // Static mode: check if we've reached the end
                if let Some(end) = self.end_offset {
                    if stream_offset > end {
                        return Ok(ReadResult::Finished);
                    }
                }

                let message = delivery.message();

                // Extract key from application_properties if present
                let key: Option<Vec<u8>> = message
                    .application_properties()
                    .and_then(|props| props.get(RABBITMQ_KEY_PROPERTY))
                    .map(|v| match v {
                        SimpleValue::String(s) => s.as_bytes().to_vec(),
                        SimpleValue::Binary(b) => b.clone(),
                        other => format!("{other:?}").into_bytes(),
                    });
                let body = message.data().map(<[u8]>::to_vec);

                // Use key-value context to support native record keys
                let payload = ReaderContext::from_key_value(key, body);

                let offset = (
                    OffsetKey::Rabbitmq(self.worker_index),
                    OffsetValue::RabbitmqOffset(stream_offset),
                );

                // Create metadata and use deferred pattern (like Kafka).
                // Timestamp extraction is not possible due to private field in
                // rabbitmq_stream_protocol::Timestamp.
                let metadata = RabbitmqMetadata::new(stream_offset, self.stream_name.clone(), None);
                self.deferred_read_result = Some(ReadResult::Data(payload, offset));
                Ok(ReadResult::NewSource(metadata.into()))
            }
            Some(Err(e)) => Err(ReadError::Rabbitmq(e.to_string())),
            None => Ok(ReadResult::Finished),
        }
    }

    fn seek(&mut self, frontier: &OffsetAntichain) -> Result<(), ReadError> {
        let offset_value = frontier.get_offset(&OffsetKey::Rabbitmq(self.worker_index));
        if let Some(offset) = offset_value {
            if let OffsetValue::RabbitmqOffset(last_offset) = offset {
                // Rebuild consumer starting from the next offset after the persisted one
                let next_offset = last_offset.checked_add(1).ok_or_else(|| {
                    ReadError::Rabbitmq("Offset overflow: cannot seek past u64::MAX".to_string())
                })?;
                let new_consumer = self
                    .runtime
                    .block_on(async {
                        self.environment
                            .consumer()
                            .offset(OffsetSpecification::Offset(next_offset))
                            .build(&self.stream_name)
                            .await
                    })
                    .map_err(|e| {
                        ReadError::Rabbitmq(format!("Failed to rebuild consumer for seek: {e}"))
                    })?;
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
        worker_index: usize,
        stream_name: String,
        end_offset: Option<u64>,
    ) -> RabbitmqReader {
        RabbitmqReader {
            runtime,
            consumer,
            environment,
            worker_index,
            stream_name,
            end_offset,
            deferred_read_result: None,
        }
    }
}
