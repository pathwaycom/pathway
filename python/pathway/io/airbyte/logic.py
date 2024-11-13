import json
import logging
import time
from collections.abc import Sequence

from pathway.internals import api
from pathway.io._utils import STATIC_MODE_NAME
from pathway.io.python import ConnectorSubject
from pathway.third_party.airbyte_serverless.destinations import (
    BaseDestination as BaseAirbyteDestination,
)
from pathway.third_party.airbyte_serverless.sources import AbstractAirbyteSource

MAX_RETRIES = 5
INCREMENTAL_SYNC_MODE = "incremental"
FULL_REFRESH_SYNC_MODE = "full_refresh"

AIRBYTE_STREAM_RECORD_PREFIX = "_airbyte_raw_"
AIRBYTE_DATA_RECORD_FIELD = "_airbyte_data"
AIRBYTE_STATE_RECORD_TYPE = "_airbyte_states"


class _PathwayAirbyteDestination(BaseAirbyteDestination):
    def __init__(self, on_event, on_state, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.on_event = on_event
        self.on_state = on_state
        self._state = {}
        self._shared_state = None

    def get_state(self):
        stream_states = []
        for stream_name, stream_state in self._state.items():
            stream_states.append(
                {
                    "stream_descriptor": {
                        "name": stream_name,
                    },
                    "stream_state": stream_state,
                }
            )
        global_state = {
            "stream_states": stream_states,
        }
        if self._shared_state is not None:
            global_state["shared_state"] = self._shared_state
        result = {
            "type": "GLOBAL",
            "global": global_state,
        }
        return result

    def set_state(self, state):
        self._state = {}
        self._shared_state = None
        self._handle_global_state(state)

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
            self.on_state(self.get_state())

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
        global_ = full_state.get("global")
        if global_ is None:
            logging.warning("Global state doesn't contain 'global' section")
            return
        stream_states = global_.get("stream_states")
        if stream_states is None:
            logging.warning("Global state doesn't contain 'stream_states' section")
            return
        for stream_state in stream_states:
            self._handle_stream_state_inner(stream_state)
        shared_state = global_.get("shared_state")
        if shared_state is not None:
            self._shared_state = shared_state
        else:
            self._shared_state = None


class _PathwayAirbyteSubject(ConnectorSubject):

    def __init__(
        self,
        source: AbstractAirbyteSource,
        mode: str,
        refresh_interval_ms: int,
        streams: Sequence[str],
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.source = source
        self.mode = mode
        self.refresh_interval = refresh_interval_ms / 1000.0
        self.destination = _PathwayAirbyteDestination(
            on_event=self.on_event,
            on_state=self.on_state,
        )
        self.streams = streams
        self.sync_mode = self._sync_mode()
        self._cache: dict[api.Pointer, bytes] = {}
        self._present_keys: set[api.Pointer] = set()

    def on_event(self, payload):
        if self.sync_mode == INCREMENTAL_SYNC_MODE:
            self.next_json({"data": payload})
        elif self.sync_mode == FULL_REFRESH_SYNC_MODE:
            message = json.dumps(
                {"data": payload}, ensure_ascii=False, sort_keys=True
            ).encode("utf-8")
            key = api.ref_scalar(message)
            if self._cache.get(key) != message:
                self._cache[key] = message
                self._add(key, message)
            self._present_keys.add(key)
        else:
            raise RuntimeError(f"Unknown sync_mode: {self.sync_mode}")

    def on_state(self, state):
        self._report_offset(json.dumps(state).encode("utf-8"))
        self._enable_commits()  # A commit is done here
        self._disable_commits()  # Disable commits till the next state

    def run(self):
        self._disable_commits()
        n_times_failed = 0
        while True:
            time_before_start = time.time()
            try:
                messages = self.source.extract([self.destination.get_state()])
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
            self.destination.load(messages)

            if self.mode == STATIC_MODE_NAME:
                break
            if self.sync_mode == FULL_REFRESH_SYNC_MODE:
                absent_keys = set()
                for key, message in self._cache.items():
                    if key not in self._present_keys:
                        self._remove(key, message)
                        absent_keys.add(key)
                for key in absent_keys:
                    self._cache.pop(key)
                self._present_keys.clear()
            self._enable_commits()
            self._disable_commits()

            time_elapsed = time.time() - time_before_start
            if time_elapsed < self.refresh_interval:
                time.sleep(self.refresh_interval - time_elapsed)

    def on_stop(self):
        self.source.on_stop()

    def _sync_mode(self):
        stream = self.source.configured_catalog["streams"][0]
        sync_mode = stream["sync_mode"]
        return sync_mode

    def _seek(self, state):
        self.destination.set_state(json.loads(state.decode("utf-8")))

    def on_persisted_run(self):
        if len(self.streams) != 1:
            raise RuntimeError(
                "Persistence in airbyte connector is supported only for the case of a single stream. "
                "Please use several airbyte connectors with one stream per connector to persist the state."
            )
