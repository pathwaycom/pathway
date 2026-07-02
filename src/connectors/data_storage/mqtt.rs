// Copyright © 2026 Pathway

use log::{error, info, warn};
use rumqttc::{
    mqttbytes::QoS as MqttQoS, Client as MqttClient, Connection as MqttConnection,
    Event as MqttEvent, Incoming as MqttIncoming, Outgoing as MqttOutgoing, Packet as MqttPacket,
};

use crate::connectors::data_format::FormatterContext;
use crate::connectors::{OffsetKey, OffsetValue};
use crate::persistence::frontier::OffsetAntichain;

use super::{
    DataEventType, MessageQueueTopic, ReadError, ReadResult, Reader, ReaderContext, StorageType,
    WriteError, Writer,
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

pub struct MqttReader {
    client: MqttClient,
    connection: MqttConnection,
    topic: String,
    qos: MqttQoS,
    total_entries_read: usize,
}

impl MqttReader {
    pub fn new(
        client: MqttClient,
        connection: MqttConnection,
        topic: String,
        qos: MqttQoS,
    ) -> Self {
        Self {
            client,
            connection,
            topic,
            qos,
            total_entries_read: 0,
        }
    }
}

impl Reader for MqttReader {
    fn read(&mut self) -> Result<ReadResult, ReadError> {
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
                    self.total_entries_read += 1;
                    let offset = (
                        OffsetKey::Empty,
                        OffsetValue::MqttReadEntriesCount(self.total_entries_read),
                    );
                    return Ok(ReadResult::Data(
                        ReaderContext::from_raw_bytes(
                            DataEventType::Insert,
                            message.payload.to_vec(),
                        ),
                        offset,
                    ));
                }
                MqttEvent::Incoming(MqttPacket::ConnAck(_)) => {
                    // A `ConnAck` here means the connection was re-established after
                    // a disconnect. With a clean session the broker forgets our
                    // subscription on reconnect, so we must re-subscribe or we would
                    // silently stop receiving messages.
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
