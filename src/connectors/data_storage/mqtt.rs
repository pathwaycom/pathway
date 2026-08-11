// Copyright © 2026 Pathway

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use log::{error, info, warn};
use rumqttc::{
    mqttbytes::QoS as MqttQoS, Client as MqttClient, Connection as MqttConnection,
    Event as MqttEvent, Incoming as MqttIncoming, Outgoing as MqttOutgoing, Packet as MqttPacket,
    Publish as MqttPublish,
};

use crate::connectors::data_format::FormatterContext;
use crate::connectors::{OffsetKey, OffsetValue};
use crate::persistence::frontier::OffsetAntichain;

use super::{
    DataEventType, DeferredAckWorker, MessageQueueTopic, ReadError, ReadResult, Reader,
    ReaderContext, StorageType, WriteError, Writer,
};

pub const MQTT_MAX_MESSAGES_IN_QUEUE: usize = 1024;
pub const MQTT_CLIENT_MAX_CHANNEL_SIZE: usize = 1024 * 1024;

// If the broker keeps the connection alive (i.e. it still answers keep-alive
// pings) but never acknowledges the messages we publish, the delivery-draining
// loop below would spin on keep-alive traffic forever, so `pw.run()` would never
// terminate on a bounded input. To keep the connector usable for terminating
// batch pipelines we give up after this many consecutive keep-alive pings during
// which not a single in-flight message was confirmed, and surface an error
// instead of blocking the pipeline indefinitely.
pub const MQTT_MAX_PINGS_WITHOUT_DELIVERY: usize = 3;

// A transient broker outage (broker restart, network blip) surfaces as a burst
// of connection errors while `rumqttc` reconnects. We tolerate this many
// consecutive errors before giving up, matching the resilience of the NATS and
// Kafka readers, so that a short outage doesn't tear down a streaming pipeline.
pub const MQTT_MAX_CONSECUTIVE_ERRORS: usize = 32;

/// The state shared between the reader (which buffers the packets to
/// acknowledge) and the deferred ack worker (which acknowledges them once a
/// durable checkpoint covers them).
struct MqttPendingAcks {
    // Entries are pushed in the reading order, so the sequence numbers grow
    // monotonically and every acknowledged frontier cuts off a queue prefix.
    queue: VecDeque<(usize, u64, MqttPublish)>,
    // Packet ids are only valid within one network connection. Bumped by the
    // reader on every reconnect; the worker drops the handles of older epochs
    // without acknowledging - the broker redelivers their messages on the new
    // connection anyway, and those copies carry fresh sequence numbers.
    connection_epoch: u64,
}

pub struct MqttReader {
    client: MqttClient,
    connection: MqttConnection,
    topic: String,
    qos: MqttQoS,
    total_entries_read: usize,
    // Whether the connection was opened with `manual_acks`: with persistence
    // the acknowledgements are our responsibility - either inline right after
    // the read (until `take_deferred_ack_worker` is called) or deferred until
    // a durable checkpoint covers the message.
    manual_acks: bool,
    deferred_mode: bool,
    pending_acks: Arc<Mutex<MqttPendingAcks>>,
    // Messages the broker redelivered during the construction handshake,
    // before the subscription was confirmed. Served before any live traffic.
    preloaded: VecDeque<MqttPublish>,
}

impl MqttReader {
    pub fn new(
        client: MqttClient,
        connection: MqttConnection,
        topic: String,
        qos: MqttQoS,
        manual_acks: bool,
        preloaded: Vec<MqttPublish>,
    ) -> Self {
        Self {
            client,
            connection,
            topic,
            qos,
            total_entries_read: 0,
            manual_acks,
            deferred_mode: false,
            pending_acks: Arc::new(Mutex::new(MqttPendingAcks {
                queue: VecDeque::new(),
                connection_epoch: 0,
            })),
            preloaded: preloaded.into(),
        }
    }

    fn on_publish(&mut self, message: MqttPublish) -> ReadResult {
        self.total_entries_read += 1;
        let payload = message.payload.to_vec();
        if self.manual_acks {
            if self.deferred_mode {
                // Must happen before the message is returned to the engine, so
                // that no checkpoint can cover an offset whose ack handle is
                // not in the queue yet.
                let mut pending_acks = self.pending_acks.lock().unwrap();
                let connection_epoch = pending_acks.connection_epoch;
                pending_acks
                    .queue
                    .push_back((self.total_entries_read, connection_epoch, message));
            } else if let Err(e) = self.client.ack(&message) {
                // The requests channel is either closed or overflowed; both
                // mean the connection is going down, and the broker will
                // redeliver the unacknowledged message after the reconnect.
                warn!("Failed to acknowledge an MQTT message: {e}");
            }
        }
        let offset = (
            OffsetKey::Empty,
            OffsetValue::MqttReadEntriesCount(self.total_entries_read),
        );
        ReadResult::Data(
            ReaderContext::from_raw_bytes(DataEventType::Insert, payload),
            offset,
        )
    }
}

impl Reader for MqttReader {
    fn read(&mut self) -> Result<ReadResult, ReadError> {
        if let Some(message) = self.preloaded.pop_front() {
            return Ok(self.on_publish(message));
        }
        loop {
            let event = match self.connection.recv() {
                Ok(event) => event?,
                Err(e) => {
                    warn!("Source channel has been closed: {e:?}");
                    break;
                }
            };
            match event {
                MqttEvent::Incoming(MqttPacket::Publish(message)) => {
                    return Ok(self.on_publish(message));
                }
                MqttEvent::Incoming(MqttPacket::ConnAck(_)) => {
                    // A `ConnAck` here means the connection was re-established
                    // after a disconnect. The packet ids of the not yet
                    // acknowledged messages died with the old connection: open
                    // a new epoch so that the ack worker doesn't send their
                    // acks into the new one.
                    if self.manual_acks {
                        self.pending_acks.lock().unwrap().connection_epoch += 1;
                    }
                    // With a clean session the broker forgets our subscription
                    // on reconnect, so we must re-subscribe or we would
                    // silently stop receiving messages. With a persistent
                    // session re-subscribing is a no-op.
                    if let Err(e) = self.client.subscribe(self.topic.clone(), self.qos) {
                        warn!("Failed to re-subscribe to MQTT topic after reconnect: {e}");
                    }
                }
                other => {
                    info!("Received metadata event from MQTT reader: {other:?}");
                }
            }
        }

        // The broker has closed the connection, no new messages are expected
        Ok(ReadResult::Finished)
    }

    fn take_deferred_ack_worker(&mut self) -> Option<Box<dyn DeferredAckWorker>> {
        if !self.manual_acks {
            return None;
        }
        self.deferred_mode = true;
        Some(Box::new(MqttDeferredAckWorker {
            client: self.client.clone(),
            pending_acks: self.pending_acks.clone(),
        }))
    }

    fn max_allowed_consecutive_errors(&self) -> usize {
        MQTT_MAX_CONSECUTIVE_ERRORS
    }

    fn seek(&mut self, frontier: &OffsetAntichain) -> Result<(), ReadError> {
        let offset_value = frontier.get_offset(&OffsetKey::Empty);
        if let Some(offset) = offset_value {
            if let OffsetValue::MqttReadEntriesCount(last_run_entries_read) = offset {
                self.total_entries_read = *last_run_entries_read;
            } else {
                error!("Unexpected offset type for MQTT reader: {offset:?}");
            }
        }

        Ok(())
    }

    fn storage_type(&self) -> StorageType {
        StorageType::Mqtt
    }
}

pub struct MqttDeferredAckWorker {
    client: MqttClient,
    pending_acks: Arc<Mutex<MqttPendingAcks>>,
}

impl DeferredAckWorker for MqttDeferredAckWorker {
    fn ack_up_to(&mut self, frontier: &OffsetAntichain) {
        let threshold = match frontier.get_offset(&OffsetKey::Empty) {
            Some(OffsetValue::MqttReadEntriesCount(entries_read)) => *entries_read,
            Some(other) => {
                error!("Unexpected offset type in the MQTT ack frontier: {other:?}");
                return;
            }
            None => return,
        };
        let mut pending_acks = self.pending_acks.lock().unwrap();
        let current_epoch = pending_acks.connection_epoch;
        loop {
            match pending_acks.queue.front() {
                Some((sequence_number, _, _)) if *sequence_number <= threshold => {}
                _ => break,
            }
            let (sequence_number, epoch, message) = pending_acks.queue.pop_front().unwrap();
            if epoch != current_epoch {
                // The connection the packet id belongs to is gone; the broker
                // redelivers the message on the current one, and that copy is
                // acknowledged under its own sequence number.
                continue;
            }
            if let Err(e) = self.client.try_ack(&message) {
                // The requests channel is full or closed. Put the handle back
                // and retry when the next checkpoint is committed; until then
                // the broker simply treats the message as still in flight.
                warn!("Failed to enqueue an MQTT ack, will retry at the next checkpoint: {e}");
                pending_acks
                    .queue
                    .push_front((sequence_number, epoch, message));
                break;
            }
        }
    }
}

pub struct MqttWriter {
    client: MqttClient,
    topic: MessageQueueTopic,
    qos: MqttQoS,
    retain: bool,
    connection: MqttConnection,
    packets_in_queue: usize,
    packet_id_waits_for_confirmation: Vec<bool>,
}

impl MqttWriter {
    pub fn new(
        client: MqttClient,
        connection: MqttConnection,
        topic: MessageQueueTopic,
        qos: MqttQoS,
        retain: bool,
    ) -> Self {
        Self {
            client,
            topic,
            qos,
            retain,
            connection,
            packets_in_queue: 0,
            packet_id_waits_for_confirmation: vec![false; u16::MAX as usize + 1],
        }
    }

    fn on_packet_acked(&mut self, id: u16) {
        let id = id as usize;
        if self.packet_id_waits_for_confirmation[id] {
            self.packet_id_waits_for_confirmation[id] = false;
            self.packets_in_queue -= 1;
        } else {
            warn!("Unexpected message confirmation: id = {id}");
        }
    }

    fn ensure_max_packets_in_queue(&mut self, max_in_queue: usize) -> Result<(), WriteError> {
        // Counts keep-alive pings observed since the last time a message was
        // actually confirmed. It is reset to zero on every delivery confirmation
        // and lets us detect a broker that keeps the connection alive but never
        // acknowledges our publishes (which would otherwise loop forever).
        let mut pings_without_delivery = 0;
        while self.packets_in_queue > max_in_queue {
            let packets_in_queue_before = self.packets_in_queue;
            let packet = match self.connection.recv() {
                Ok(Ok(event)) => event,
                Ok(Err(event_error)) => {
                    error!("Failed to communicate with MQTT broker: {event_error}");
                    return Err(WriteError::MqttPoll(event_error));
                }
                Err(e) => {
                    // Nobody can accept events or respond
                    warn!("All clients have closed the requests channel: {e:?}");
                    return Ok(());
                }
            };
            match packet {
                MqttEvent::Outgoing(MqttOutgoing::Publish(id)) => {
                    if id == 0 {
                        // ID = 0 implies that QoS is 0.
                        // The message was sent with this outgoing packet,
                        // and no acknowledgment is expected.
                        self.packets_in_queue -= 1;
                    } else {
                        self.packet_id_waits_for_confirmation[id as usize] = true;
                    }
                }
                MqttEvent::Incoming(MqttIncoming::PubAck(id)) => {
                    // A `PubAck` message implies QoS = 1.
                    // Communication works as follows:
                    // 1. An outgoing `Publish` packet is sent from Pathway to the broker.
                    // 2. When the broker receives the packet, it sends a `PubAck` message
                    //    back to Pathway with the packet's identifier.
                    //    If no `PubAck` is received within a certain time frame,
                    //    the client retries sending the `Publish` packet.
                    self.on_packet_acked(id.pkid);
                }
                MqttEvent::Incoming(MqttIncoming::PubComp(id)) => {
                    // A `PubComp` message implies QoS = 2.
                    // The communication sequence works as follows:
                    // 1. An outgoing `Publish` packet is sent from Pathway to the broker.
                    // 2. When the broker receives the packet, it sends a `PubRec` message
                    //    back to Pathway with the packet's identifier.
                    // 3. Client reads the identifier and sends a `PubRel` message to release the message.
                    // 4. The broker completes the flow by sending a `PubComp` message to Pathway.
                    // If any expected message is not received within a timeout,
                    // the MQTT client retries sending the last message with the DUP flag set.
                    self.on_packet_acked(id.pkid);
                }
                MqttEvent::Outgoing(MqttOutgoing::PingReq) => {
                    // A keep-alive ping means a whole keep-alive interval elapsed
                    // with the connection alive but no message got confirmed. If
                    // this keeps happening, the broker is silently dropping our
                    // publishes and we must not block the pipeline forever.
                    pings_without_delivery += 1;
                    if pings_without_delivery >= MQTT_MAX_PINGS_WITHOUT_DELIVERY {
                        error!(
                            "MQTT broker did not confirm delivery of {} in-flight message(s) \
                             over {MQTT_MAX_PINGS_WITHOUT_DELIVERY} keep-alive intervals",
                            self.packets_in_queue
                        );
                        return Err(WriteError::MqttDeliveryConfirmationTimeout(
                            self.packets_in_queue,
                        ));
                    }
                }
                other => {
                    info!("Auxiliary information packet, unused in submission tracking: {other:?}");
                }
            }
            if self.packets_in_queue < packets_in_queue_before {
                // A message was confirmed since the last iteration: the broker is
                // making progress, so reset the keep-alive stall detector.
                pings_without_delivery = 0;
            }
        }
        Ok(())
    }
}

impl Writer for MqttWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        for payload in data.payloads {
            self.packets_in_queue += 1;
            let payload = payload.into_raw_bytes()?;
            let effective_topic = self.topic.get_for_posting(&data.values)?;
            self.client
                .publish(effective_topic, self.qos, self.retain, payload)
                .map_err(WriteError::MqttPublish)?;
        }

        // The message identifier is a 16-bit integer, hence we don't want
        // to keep the big amounts of messages in-fly.
        self.ensure_max_packets_in_queue(MQTT_MAX_MESSAGES_IN_QUEUE)
    }

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        self.ensure_max_packets_in_queue(0)
    }

    fn name(&self) -> String {
        format!("MQTT({})", self.topic)
    }

    fn retriable(&self) -> bool {
        true
    }

    fn single_threaded(&self) -> bool {
        false
    }
}
