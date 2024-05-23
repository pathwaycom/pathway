import json
import datetime
import uuid


class BaseDestination:

    destination_columns = [
        ("_airbyte_raw_id", "string", "Record uuid generated at ingestion"),
        ("_airbyte_job_started_at", "timestamp", "Extract-load job start timestamp"),
        (
            "_airbyte_slice_started_at",
            "timestamp",
            "When incremental mode is used, data records are emitted by chunks a.k.a. slices. "
            "At the end of each slice, a state record is emitted to store a checkpoint. "
            "This column stores the timestamp when the slice started",
        ),
        ("_airbyte_extracted_at", "timestamp", "Record extract timestamp from source"),
        ("_airbyte_loaded_at", "timestamp", "Record ingestion timestamp"),
        ("_airbyte_data", "json", "Record data as json"),
    ]

    yaml_definition_example = "\n".join(
        [
            "buffer_size_max: 10000 # OPTIONAL | integer | maximum number of records in buffer "
            "before writing to destination (defaults to 10000 when not specified)",
        ]
    )

    def __init__(self, buffer_size_max=10000):
        self.buffer_size_max = buffer_size_max

    def get_state(self):
        raise NotImplementedError()

    def get_logs(self):
        raise NotImplementedError()

    def load(self, messages):
        self.job_started_at = datetime.datetime.utcnow().isoformat()
        self.slice_started_at = self.job_started_at
        buffer = []
        stream = None
        for message in messages:
            if message["type"] == "RECORD":
                new_stream = message["record"]["stream"]
                if new_stream != stream and stream is not None:
                    self._format_and_write(f"_airbyte_raw_{stream}", buffer)
                    buffer = []
                    self.slice_started_at = datetime.datetime.utcnow().isoformat()
                stream = new_stream
                buffer.append(message["record"])
                if len(buffer) > self.buffer_size_max:
                    self._format_and_write(f"_airbyte_raw_{stream}", buffer)
                    buffer = []
            elif message["type"] == "STATE":
                self._format_and_write(f"_airbyte_raw_{stream}", buffer)
                buffer = []
                self._format_and_write("_airbyte_states", [message["state"]])
                self.slice_started_at = datetime.datetime.utcnow().isoformat()
            elif message["type"] == "LOG":
                self._format_and_write("_airbyte_logs", [message["log"]])
            elif message["type"] == "TRACE":
                self._format_and_write("_airbyte_logs", [message["trace"]])
            else:
                raise NotImplementedError(
                    f'message type {message["type"]} is not managed yet'
                )
        self._format_and_write(f"_airbyte_raw_{stream}", buffer)

    def _format_and_write(self, record_type, records):
        if not records:
            return
        records = self._format(record_type, records)
        self._write(record_type, records)

    def _format(self, record_type, records):
        now = datetime.datetime.utcnow().isoformat()
        return [
            {
                "_airbyte_raw_id": str(uuid.uuid4()),
                "_airbyte_job_started_at": self.job_started_at,
                "_airbyte_slice_started_at": self.slice_started_at,
                "_airbyte_extracted_at": (
                    datetime.datetime.fromtimestamp(
                        record["emitted_at"] / 1000
                    ).isoformat()
                    if "emitted_at" in record
                    else None
                ),
                "_airbyte_loaded_at": now,
                "_airbyte_data": json.dumps(
                    (
                        record["data"]
                        if record_type.startswith("_airbyte_raw")
                        else record
                    ),
                    ensure_ascii=False,
                ),
            }
            for record in records
        ]

    def _write(self, record_type, records):
        raise NotImplementedError()


class PrintDestination(BaseDestination):

    def get_state(self):
        return {}

    def _write(self, record_type, records):
        print("\n", "-" * 100)
        print(record_type.upper())
        for record in records:
            print(json.dumps(record))


DESTINATION_CLASS_MAP = {
    "print": PrintDestination,
}


class Destination:

    def __init__(self, connector=None, config=None):
        assert (
            connector and connector in DESTINATION_CLASS_MAP
        ), f"destination should be among {list(DESTINATION_CLASS_MAP.keys())}"
        self.destination_class = DESTINATION_CLASS_MAP[connector]
        self.connector = connector
        self.config = config
        self._destination = None

    @property
    def yaml_definition_example(self):
        return "\n".join(
            [
                f'connector: "{self.connector}" # GENERATED | string | An AirbyteServerless Destination Connector. '
                f"Must be one of {list(DESTINATION_CLASS_MAP.keys())}",
                "config: # PREGENERATED | object | PLEASE UPDATE this pre-generated config",
                "  "
                + self.destination_class.yaml_definition_example.replace("\n", "\n  "),
            ]
        )

    def __getattr__(self, name):
        if self._destination is None:
            self._destination = self.destination_class(**self.config)
        return getattr(self._destination, name)
