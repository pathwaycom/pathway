import json
import socket
import struct
import threading
import time
from uuid import uuid4

import pytest

import pathway as pw
from pathway.internals.parse_graph import G
from pathway.tests.utils import (
    CsvLinesNumberChecker,
    FileLinesNumberChecker,
    wait_result_with_checker,
)

from .utils import MQTT_BASE_ROUTE, check_keys_in_file


class NonConfirmingMqttBroker:
    """A minimal MQTT v3.1.1 broker that completes the CONNECT handshake and keeps
    the connection alive by answering keep-alive ``PINGREQ`` packets, but never
    acknowledges published messages (it silently drops every ``PUBLISH``).

    It is used to check that the writer does not block the pipeline forever while
    waiting for delivery confirmations that never arrive.
    """

    def __init__(self):
        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server.bind(("127.0.0.1", 0))
        self.port = self._server.getsockname()[1]
        self._server.listen(8)
        self._thread = threading.Thread(target=self._serve, daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        try:
            self._server.close()
        except OSError:
            pass

    @staticmethod
    def _read_remaining_length(conn):
        multiplier = 1
        value = 0
        while True:
            chunk = conn.recv(1)
            if not chunk:
                return None
            byte = chunk[0]
            value += (byte & 0x7F) * multiplier
            if (byte & 0x80) == 0:
                return value
            multiplier *= 128

    def _handle(self, conn):
        conn.settimeout(120)
        while True:
            try:
                header = conn.recv(1)
            except OSError:
                return
            if not header:
                return
            packet_type = header[0] >> 4
            remaining = self._read_remaining_length(conn)
            if remaining is None:
                return
            body = b""
            while len(body) < remaining:
                chunk = conn.recv(remaining - len(body))
                if not chunk:
                    return
                body += chunk
            if packet_type == 1:  # CONNECT
                conn.sendall(bytes([0x20, 0x02, 0x00, 0x00]))  # CONNACK, success
            elif packet_type == 12:  # PINGREQ
                conn.sendall(bytes([0xD0, 0x00]))  # PINGRESP -> connection stays alive
            elif packet_type == 8:  # SUBSCRIBE
                packet_id = body[0:2]
                conn.sendall(bytes([0x90, 0x03]) + packet_id + bytes([0x00]))  # SUBACK
            elif packet_type == 14:  # DISCONNECT
                return
            # PUBLISH (3) and every other packet: intentionally left unacknowledged.

    def _serve(self):
        while True:
            try:
                conn, _ = self._server.accept()
            except OSError:
                return
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()


def _mqtt_host_port():
    location = MQTT_BASE_ROUTE.split("://", 1)[1].split("?", 1)[0]
    host, port = location.split(":")
    return host, int(port)


def _encode_remaining_length(length):
    encoded = bytearray()
    while True:
        byte = length % 128
        length //= 128
        if length > 0:
            byte |= 0x80
        encoded.append(byte)
        if length == 0:
            return bytes(encoded)


def _mqtt_raw_connect(client_id):
    """Open a minimal MQTT v3.1.1 connection with the given client id."""
    host, port = _mqtt_host_port()
    sock = socket.create_connection((host, port))
    cid = client_id.encode()
    variable_header = (
        struct.pack("!H", 4) + b"MQTT" + bytes([0x04, 0x02]) + struct.pack("!H", 60)
    )
    payload = struct.pack("!H", len(cid)) + cid
    body = variable_header + payload
    sock.sendall(bytes([0x10]) + _encode_remaining_length(len(body)) + body)
    sock.recv(4)  # CONNACK
    return sock


def _mqtt_raw_publish(sock, topic, payload):
    """Publish a QoS 0 message on an already-connected raw MQTT socket."""
    topic_bytes = topic.encode()
    body = struct.pack("!H", len(topic_bytes)) + topic_bytes + payload.encode()
    sock.sendall(bytes([0x30]) + _encode_remaining_length(len(body)) + body)


def run_identity_program(
    input_file,
    output_file,
    qos,
    mqtt_topic,
    mqtt_reader_uri,
    mqtt_writer_uri,
    new_entries,
    persistence_config=None,
):
    with open(input_file, "a") as f:
        for entry in new_entries:
            f.write(entry)
            f.write("\n")

    G.clear()
    table = pw.io.plaintext.read(input_file, mode="static")
    pw.io.mqtt.write(
        table,
        uri=mqtt_writer_uri,
        topic=mqtt_topic,
        format="json",
        qos=qos,
    )

    class InputSchema(pw.Schema):
        data: str

    table_reread = pw.io.mqtt.read(
        uri=mqtt_reader_uri,
        topic=mqtt_topic,
        schema=InputSchema,
        format="json",
        qos=qos,
    )
    pw.io.csv.write(table_reread, output_file)

    wait_result_with_checker(
        CsvLinesNumberChecker(output_file, len(new_entries)),
        30,
        kwargs={"persistence_config": persistence_config},
    )


@pytest.mark.parametrize("qos", [0, 1, 2])
def test_mqtt_simple(tmp_path, mqtt_context, qos):
    input_path = tmp_path / "input.txt"
    output_path = tmp_path / "output.txt"
    run_identity_program(
        input_path,
        output_path,
        qos,
        mqtt_context.topic,
        mqtt_context.reader_connection_string,
        mqtt_context.writer_connection_string,
        ["one", "two", "three", "four", "five"],
    )


@pytest.mark.parametrize("qos", [0, 1, 2])
def test_mqtt_large_message_roundtrip(tmp_path, mqtt_context, qos):
    """Payloads larger than a few kilobytes must be written and read back intact.

    MQTT routinely carries payloads well above 10 KB (JSON documents, batched sensor
    readings, small binary blobs), and the connector must transport them without
    truncating them, dropping them, or failing the pipeline.
    """
    output_path = tmp_path / "output.jsonl"
    payload = "A" * 100_000

    G.clear()
    table = pw.debug.table_from_rows(
        schema=pw.schema_from_types(data=str),
        rows=[(payload,)],
    )
    pw.io.mqtt.write(
        table,
        uri=mqtt_context.writer_connection_string,
        topic=mqtt_context.topic,
        format="plaintext",
        value=table.data,
        qos=qos,
        retain=True,
    )
    reread = pw.io.mqtt.read(
        uri=mqtt_context.reader_connection_string,
        topic=mqtt_context.topic,
        format="plaintext",
        qos=qos,
    )
    pw.io.jsonlines.write(reread, output_path)

    wait_result_with_checker(FileLinesNumberChecker(output_path, 1), 30)

    with open(output_path) as f:
        received = json.loads(f.readline())
    assert received["data"] == payload


@pytest.mark.parametrize("qos", [0, 1, 2])
def test_mqtt_dynamic_topic(tmp_path, qos):
    input_path = tmp_path / "input.txt"
    dynamic_topic_1 = str(uuid4())
    dynamic_topic_2 = str(uuid4())
    with open(input_path, "w") as f:
        f.write(json.dumps({"k": "0", "v": "foo", "t": dynamic_topic_1}))
        f.write("\n")
        f.write(json.dumps({"k": "1", "v": "bar", "t": dynamic_topic_2}))
        f.write("\n")
        f.write(json.dumps({"k": "2", "v": "baz", "t": dynamic_topic_1}))
        f.write("\n")

    class InputSchema(pw.Schema):
        k: str = pw.column_definition(primary_key=True)
        v: str
        t: str

    writer_uri = MQTT_BASE_ROUTE.replace("$CLIENT_ID", f"writer-{str(uuid4())}")
    table = pw.io.jsonlines.read(input_path, schema=InputSchema)
    pw.io.mqtt.write(
        table,
        uri=writer_uri,
        topic=table.select(topic="MqttTopic-" + pw.this.t).topic,
        format="json",
        qos=qos,
    )

    output_path_1 = tmp_path / "output_1.txt"
    output_path_2 = tmp_path / "output_2.txt"
    reader_uri_1 = MQTT_BASE_ROUTE.replace("$CLIENT_ID", f"reader-{str(uuid4())}")
    reader_uri_2 = MQTT_BASE_ROUTE.replace("$CLIENT_ID", f"reader-{str(uuid4())}")
    stream_1 = pw.io.mqtt.read(
        uri=reader_uri_1,
        topic=f"MqttTopic-{dynamic_topic_1}",
        format="plaintext",
        qos=qos,
    )
    stream_2 = pw.io.mqtt.read(
        uri=reader_uri_2,
        topic=f"MqttTopic-{dynamic_topic_2}",
        format="plaintext",
        qos=qos,
    )
    pw.io.jsonlines.write(stream_1, output_path_1)
    pw.io.jsonlines.write(stream_2, output_path_2)
    wait_result_with_checker(
        FileLinesNumberChecker(output_path_1, 2).add_path(output_path_2, 1), 30
    )
    check_keys_in_file(
        output_path_1,
        "json",
        {"0", "2"},
        {"k", "v", "t", "time", "diff", "topic"},
    )
    check_keys_in_file(
        output_path_2, "json", {"1"}, {"k", "v", "t", "time", "diff", "topic"}
    )


def test_mqtt_read_rejects_empty_topic():
    """Subscribing to an empty topic must fail with a clear validation error.

    An empty topic filter is not a valid MQTT subscription; passing one must be
    reported immediately instead of letting the broker abort the connection and the
    pipeline crash with an obscure "subscription confirmation" error.
    """
    with pytest.raises(ValueError):
        pw.io.mqtt.read(
            uri=MQTT_BASE_ROUTE.replace("$CLIENT_ID", "reader"),
            topic="",
            format="plaintext",
        )


@pytest.mark.parametrize("bad_topic", ["", "sensors/#", "sensors/+/temp", "#", "+"])
def test_mqtt_write_rejects_invalid_publish_topic(bad_topic):
    """A static publish topic that MQTT forbids must be rejected with a clear error.

    MQTT publish topics cannot be empty and cannot contain the wildcard characters
    ``+`` or ``#`` (those are only meaningful when subscribing). Passing such a topic
    must fail immediately with a descriptive validation error rather than letting the
    pipeline start and then crash with an obscure error once it tries to publish.
    """
    table = pw.debug.table_from_markdown(
        """
        data
        hello
        """
    )
    with pytest.raises(ValueError):
        pw.io.mqtt.write(
            table,
            uri=MQTT_BASE_ROUTE.replace("$CLIENT_ID", "writer"),
            topic=bad_topic,
            format="plaintext",
            value=table.data,
        )


def test_mqtt_reader_recovers_after_disconnect(tmp_path):
    """A streaming reader must survive a transient disconnect and keep receiving.

    Broker restarts and network blips are routine in production. When the connection
    to the broker drops, the reader must reconnect, re-subscribe to its topic, and go
    on delivering messages published afterwards — not crash the pipeline and not
    silently stop receiving.
    """
    output_path = tmp_path / "output.jsonl"
    topic = f"recovery/{uuid4()}"
    reader_client_id = f"reader-{uuid4()}"
    reader_uri = MQTT_BASE_ROUTE.replace("$CLIENT_ID", reader_client_id)

    G.clear()
    reread = pw.io.mqtt.read(
        uri=reader_uri,
        topic=topic,
        format="plaintext",
        qos=1,
        autocommit_duration_ms=200,
    )
    pw.io.jsonlines.write(reread, output_path)

    runner = threading.Thread(
        target=lambda: pw.run(monitoring_level=pw.MonitoringLevel.NONE),
        daemon=True,
    )
    runner.start()

    def received(marker):
        if not output_path.exists():
            return False
        with open(output_path) as f:
            return any(json.loads(line)["data"] == marker for line in f if line.strip())

    def wait_for(marker, timeout):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if received(marker):
                return True
            time.sleep(0.2)
        return False

    # Give the reader time to subscribe, then confirm it is receiving messages.
    time.sleep(4)
    publisher = _mqtt_raw_connect(f"publisher-{uuid4()}")
    _mqtt_raw_publish(publisher, topic, "before")
    assert wait_for("before", 15), "reader did not receive the initial message"

    # Force a disconnect: connecting with the reader's client id makes the broker
    # drop the reader's existing connection (MQTT session takeover).
    _mqtt_raw_connect(reader_client_id).close()
    time.sleep(1)

    # After the disconnect the reader must reconnect, re-subscribe and keep working.
    recovered = False
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        _mqtt_raw_publish(publisher, topic, "after")
        if wait_for("after", 1):
            recovered = True
            break
    publisher.close()

    assert (
        runner.is_alive()
    ), "the reader crashed the pipeline on a transient disconnect"
    assert recovered, "the reader did not resume receiving messages after reconnecting"


def test_mqtt_writer_terminates_when_broker_never_confirms():
    """Writing to a broker that stays connected but never confirms deliveries must
    not block the pipeline forever.

    A finite/batch input written to an MQTT sink must let ``pw.run()`` terminate even
    if the broker keeps the connection alive (answering keep-alive pings) yet never
    acknowledges the published messages. The connector must give up waiting for the
    missing confirmations and surface an error instead of hanging indefinitely.
    """
    broker = NonConfirmingMqttBroker()
    broker.start()
    try:
        G.clear()
        table = pw.debug.table_from_markdown(
            """
            data
            one
            two
            three
            """
        )
        pw.io.mqtt.write(
            table,
            uri=f"mqtt://127.0.0.1:{broker.port}/?client_id=writer&keep_alive_secs=1",
            topic="test/topic",
            format="plaintext",
            value=table.data,
            qos=2,
        )

        run_error: list[BaseException] = []

        def run_pipeline():
            try:
                pw.run(monitoring_level=pw.MonitoringLevel.NONE)
            except (
                BaseException
            ) as e:  # noqa: BLE001 - recorded for the assertion below
                run_error.append(e)

        runner = threading.Thread(target=run_pipeline, daemon=True)
        runner.start()
        runner.join(timeout=30)

        assert not runner.is_alive(), (
            "pw.run() did not terminate: the MQTT writer blocked forever waiting for "
            "delivery confirmations that never arrived"
        )
        assert run_error, (
            "expected pw.run() to surface an error when the broker never confirms "
            "deliveries"
        )
    finally:
        broker.stop()
