import json
import logging
import time

from pathway.io._utils import STATIC_MODE_NAME
from pathway.io.python import ConnectorSubject
from pathway.third_party.airbyte_serverless.destinations import (
    BaseDestination as BaseAirbyteDestination,
)
from pathway.third_party.airbyte_serverless.sources import AbstractAirbyteSource

MAX_RETRIES = 5
INCREMENTAL_SYNC_MODE = "incremental"

AIRBYTE_STREAM_RECORD_PREFIX = "_airbyte_raw_"
AIRBYTE_DATA_RECORD_FIELD = "_airbyte_data"
AIRBYTE_STATE_RECORD_TYPE = "_airbyte_states"


class _PathwayAirbyteDestination(BaseAirbyteDestination):
    def __init__(self, on_event, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.on_event = on_event
        self._state = {}

    def get_state(self):
        return self._state

    def _write(self, record_type, records):
        if record_type.startswith(AIRBYTE_STREAM_RECORD_PREFIX):
            for record in records:
                payload = record.pop(AIRBYTE_DATA_RECORD_FIELD)
                self.on_event(payload)
        elif record_type == AIRBYTE_STATE_RECORD_TYPE:
            # Parse state according to the schema described in the protocol
            # https://docs.airbyte.com/understanding-airbyte/airbyte-protocol#airbytestatemessage
            for record in records:
                full_state = json.loads(record[AIRBYTE_DATA_RECORD_FIELD])
                state_type = full_state.get("type", "LEGACY")
                if state_type == "LEGACY":
                    self._handle_legacy_state(full_state)
                elif state_type == "GLOBAL":
                    self._handle_global_state(full_state)
                elif state_type == "STREAM" or state_type == "PER_STREAM":
                    # two different names in the docs, hence two clauses
                    self._handle_stream_state(full_state)
                else:
                    logging.warning(
                        f"Unknown state type: {state_type}. Ignoring state: {full_state}"
                    )

    def _handle_stream_state(self, full_state):
        stream = full_state.get("stream")
        if stream is None:
            logging.warning("Stream state doesn't contain 'stream' section")
            return
        self._handle_stream_state_inner(stream)

    def _handle_stream_state_inner(self, stream):
        descriptor = stream.get("stream_descriptor")
        if descriptor is None:
            logging.warning(
                "Stream state doesn't contain 'stream.stream_descriptor' section"
            )
            return
        stream_name = descriptor.get("name")
        if stream_name is None:
            logging.warning(
                "Stream state doesn't contain 'stream.stream_descriptor.name' field"
            )
            return
        state_blob = stream.get("stream_state")
        if state_blob is not None:
            self._state.update({stream_name: state_blob})
        else:
            logging.warning("Stream state doesn't contain 'stream.stream_state' field")

    def _handle_legacy_state(self, full_state):
        state_blob = full_state.get("data")
        if state_blob is not None:
            self._state = state_blob
        else:
            logging.warning("Legacy state doesn't contain 'data' field")

    def _handle_global_state(self, full_state):
        stream_states = full_state.get("stream_states")
        if stream_states is None:
            logging.warning("Global state doesn't contain 'stream_states' section")
            return
        for stream_state in stream_states:
            self._handle_stream_state_inner(stream_state)


class _PathwayAirbyteSubject(ConnectorSubject):

    def __init__(
        self,
        source: AbstractAirbyteSource,
        mode: str,
        refresh_interval_ms: int,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.source = source
        self.mode = mode
        self.refresh_interval = refresh_interval_ms / 1000.0

    def run(self):
        destination = _PathwayAirbyteDestination(
            on_event=lambda payload: self.next_json({"data": payload})
        )
        n_times_failed = 0
        while True:
            time_before_start = time.time()
            try:
                messages = self.source.extract(destination.get_state())
            except Exception:
                logging.exception(
                    "Failed to query airbyte-serverless source, retrying..."
                )
                n_times_failed += 1
                if n_times_failed == MAX_RETRIES:
                    raise
                time_elapsed = time.time() - time_before_start
                time_between_retries = 1.5**n_times_failed
                if time_elapsed < time_between_retries:
                    time.sleep(time_between_retries - time_elapsed)
                continue

            n_times_failed = 0
            destination.load(messages)

            if self.mode == STATIC_MODE_NAME:
                break

            time_elapsed = time.time() - time_before_start
            if time_elapsed < self.refresh_interval:
                time.sleep(self.refresh_interval - time_elapsed)

    def on_stop(self):
        self.source.on_stop()
