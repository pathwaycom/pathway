# This script is delivered to the remote instance where Airbyte connector's Docker container run.
# Then it's executed as simple as `python <script_name.py>`
# Please don't import anything that would make this script impossible to run on its own.

import base64
import copy
import json
import os
import re
import subprocess
import tempfile
import zlib
from abc import ABC, abstractmethod
from functools import cache

import yaml


MAX_GCP_ENV_VAR_LENGTH = 32768


def get_configured_catalog(catalog, streams):
    configured_catalog = catalog
    configured_catalog["streams"] = [
        {
            "stream": stream,
            "sync_mode": (
                "incremental"
                if "incremental" in stream["supported_sync_modes"]
                else "full_refresh"
            ),
            "destination_sync_mode": "append",
            "cursor_field": stream.get("default_cursor_field", []),
        }
        for stream in configured_catalog["streams"]
        if not streams or stream["name"] in streams
    ]
    return configured_catalog


class AbstractAirbyteSource(ABC):
    @abstractmethod
    def extract(self, state=None): ...

    @property
    def configured_catalog(self): ...

    def on_stop(self):
        pass


class ConnectorResultProcessor:
    CHUNK_ENTRY_TYPE_FIELD = "__entry_type"
    CHUNK_INDEX_FIELD = "index"
    CHUNK_PAYLOAD_FIELD = "payload"
    GLOBAL_MESSAGES_FIELD = "messages"
    GLOBAL_CATALOG_FIELD = "catalog"

    CHUNK_METADATA_ENTRY_TYPE = "metadata"
    CHUNK_PAYLOAD_ENTRY_TYPE = "chunk"

    N_CHUNKS_FIELD_NAME = "n_chunks"

    # https://cloud.google.com/logging/quotas#log-limits
    # note that it's an approximate value, so a margin is needed
    MAX_LOG_ENTRY_LENGTH = 262144
    CHUNK_METADATA_LENGTH = 256

    def __init__(self):
        self._n_chunks_expected = None
        self._chunks = []

    @classmethod
    def serialize_to_logs(cls, messages, catalog):
        catalog_compressed = zlib.compress(
            json.dumps(catalog, ensure_ascii=False).encode("utf-8"),
            level=zlib.Z_BEST_COMPRESSION,
        )
        catalog_b64: str | None
        catalog_b64 = base64.b64encode(catalog_compressed).decode("utf-8")
        if len(catalog_b64) > MAX_GCP_ENV_VAR_LENGTH:
            catalog_b64 = None

        result = json.dumps(
            {
                cls.GLOBAL_MESSAGES_FIELD: [x for x in messages],
                cls.GLOBAL_CATALOG_FIELD: catalog_b64,
            },
            ensure_ascii=False,
        )

        # 256 Kb is an approximate value given by Google Cloud, so we allow some gap
        # to be on the safe side
        chunk_size_bytes = int(cls.MAX_LOG_ENTRY_LENGTH * 0.9)

        # Since we don't ensure ASCII and a Unicode character may take up to 4 bytes,
        # we introduce the chunk length limit in chars as the size in bytes divided by 4.
        # We additionally subtract the metadata overhead length.
        chunk_size_chars = int(chunk_size_bytes / 4) - cls.CHUNK_METADATA_LENGTH

        # We additionally divide it by two, since after repeated json.dumps some chars
        # may be escaped for the second time
        chunk_size_chars = int(chunk_size_chars / 2)
        chunks = [
            result[i : i + chunk_size_chars]
            for i in range(0, len(result), chunk_size_chars)
        ]

        print(
            json.dumps(
                {
                    cls.CHUNK_ENTRY_TYPE_FIELD: cls.CHUNK_METADATA_ENTRY_TYPE,
                    cls.N_CHUNKS_FIELD_NAME: len(chunks),
                }
            )
        )
        for index, chunk in enumerate(chunks):
            payload = {
                cls.CHUNK_ENTRY_TYPE_FIELD: cls.CHUNK_PAYLOAD_ENTRY_TYPE,
                cls.CHUNK_INDEX_FIELD: index,
                cls.CHUNK_PAYLOAD_FIELD: chunk,
            }
            print(json.dumps(payload, ensure_ascii=False))

    def append_chunk(self, payload):
        if not isinstance(payload, dict):
            return

        entry_type = payload.get(self.CHUNK_ENTRY_TYPE_FIELD)
        if not entry_type:
            # Not a serialized chunk
            return

        if entry_type == self.CHUNK_METADATA_ENTRY_TYPE:
            self._n_chunks_expected = payload[self.N_CHUNKS_FIELD_NAME]
        elif entry_type == self.CHUNK_PAYLOAD_ENTRY_TYPE:
            self._chunks.append(payload)
        else:
            raise ValueError(f"Unknown block type: {entry_type}")

    @cache
    def restore_received_state(self):
        if self._n_chunks_expected != len(self._chunks):
            return None
        self._chunks.sort(key=lambda chunk: int(chunk[self.CHUNK_INDEX_FIELD]))
        full_state = "".join(chunk[self.CHUNK_PAYLOAD_FIELD] for chunk in self._chunks)
        return json.loads(full_state)

    def get_messages(self):
        received_state = self.restore_received_state()
        if received_state is None:
            return None
        return received_state[self.GLOBAL_MESSAGES_FIELD]

    def get_catalog(self):
        received_state = self.restore_received_state()
        if received_state is None:
            return None
        return received_state[self.GLOBAL_CATALOG_FIELD]


def replace_secrets(yaml_config):
    google_secrets = re.findall(r"GCP_SECRET\([^\)]*\)", yaml_config)
    if google_secrets:
        import google.cloud.secretmanager

        secrets = {}
        secret_manager = google.cloud.secretmanager.SecretManagerServiceClient()
        for google_secret in google_secrets:
            if google_secret not in secrets:
                secret_name = (
                    google_secret.replace("GCP_SECRET(", "")
                    .replace(")", "")
                    .replace('"', "")
                    .replace("'", "")
                )
                secret = secret_manager.access_secret_version(name=secret_name)
                secret_decoded = secret.payload.data.decode("utf-8")
                secrets[google_secret] = secret_decoded
            yaml_config = yaml_config.replace(google_secret, secrets[google_secret])
    return yaml_config


class AirbyteSourceException(Exception):
    pass


class ExecutableAirbyteSource(AbstractAirbyteSource):

    def __init__(self, executable=None, config=None, streams=None, env_vars=None):
        self.env_vars = env_vars
        self.executable = executable
        self.config = config
        self.streams = (
            [stream.strip() for stream in streams.split(",")]
            if isinstance(streams, str)
            else streams
        )
        self.temp_dir_obj = (
            tempfile.TemporaryDirectory()
        )  # Used to dump config as files used by airbyte connector
        self.temp_dir = self.temp_dir_obj.name
        self.temp_dir_for_executable = (
            self.temp_dir
        )  # May be different if executable is a docker image where temp dir is mounted elsewhere
        self._cached_catalog = None

    def _run(self, action, state=None):
        assert self.executable, "`executable` attribute should be set"
        command = f"{self.executable} {action}"

        def add_argument(name, value):
            with open(f"{self.temp_dir}/{name}.json", "w", encoding="utf-8") as file:
                json.dump(value, file)
            return f" --{name} {self.temp_dir_for_executable}/{name}.json"

        needs_config = action != "spec"
        if needs_config:
            assert self.config, "config attribute is not defined"
            command += add_argument("config", self.config)

        needs_configured_catalog = action == "read"
        if needs_configured_catalog:
            command += add_argument("catalog", self.configured_catalog)

        if state:
            command += add_argument("state", state)

        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            shell=True,
            env=self.env_vars,
        )
        if process.stdout is not None:
            for line in iter(process.stdout.readline, b""):
                content = line.decode().strip()
                try:
                    message = json.loads(content)
                except ValueError:
                    print("NOT JSON:", content)
                    continue
                if message.get("trace", {}).get("error"):
                    raise AirbyteSourceException(json.dumps(message["trace"]["error"]))
                yield message

    def _run_and_return_first_message(self, action):
        messages = self._run(action)
        message = next(
            (
                message
                for message in messages
                if message["type"] not in ["LOG", "TRACE"]
            ),
            None,
        )
        assert (
            message is not None
        ), f"No message returned by AirbyteSource with action `{action}`"
        return message

    @property
    def spec(self):
        message = self._run_and_return_first_message("spec")
        return message["spec"]

    @property
    def catalog(self):
        if self._cached_catalog is None:
            message = self._run_and_return_first_message("discover")
            self._cached_catalog = message["catalog"]
        return copy.deepcopy(self._cached_catalog)

    @property
    def configured_catalog(self):
        return get_configured_catalog(self.catalog, self.streams)

    def load_cached_catalog(self, cached_catalog):
        self._cached_catalog = cached_catalog

    def extract(self, state=None):
        return self._run("read", state=state)


class Connection:
    def __init__(self, yaml_config):
        self.yaml_config = yaml_config
        self.source = ExecutableAirbyteSource(
            executable=self.config["source"]["executable"],
            config=self.config["source"]["config"],
            streams=self.config["source"]["streams"],
        )

    @property
    def config(self):
        yaml_config = self.yaml_config
        yaml_config = replace_secrets(yaml_config)
        assert (
            yaml_config
        ), "connection `yaml_config` does not exist. Please re-create connection"
        return yaml.safe_load(yaml_config)

    def run(self):
        state = os.environ.get("AIRBYTE_STATE")
        if state:
            state = json.loads(state)
        cached_catalog = os.environ.get("CACHED_CATALOG")
        if cached_catalog:
            compressed_catalog = base64.b64decode(cached_catalog)
            catalog_raw = zlib.decompress(compressed_catalog)
            catalog_decoded = json.loads(catalog_raw)
            self.source.load_cached_catalog(catalog_decoded)
        messages = self.source.extract(state=state)
        ConnectorResultProcessor.serialize_to_logs(messages, self.source.catalog)


class ConnectionFromEnvironmentVariables(Connection):

    def __init__(self):
        executable = os.environ.get("AIRBYTE_ENTRYPOINT")
        yaml_config_b64 = os.environ.get("YAML_CONFIG")
        assert executable, "AIRBYTE_ENTRYPOINT environment variable is not set"
        assert yaml_config_b64, "YAML_CONFIG environment variable is not set"
        yaml_config = base64.b64decode(yaml_config_b64.encode("utf-8")).decode("utf-8")
        yaml_config = yaml.safe_load(yaml_config)
        yaml_config["source"]["executable"] = executable  # type: ignore
        super().__init__(yaml.dump(yaml_config))


if __name__ == "__main__":
    connection = ConnectionFromEnvironmentVariables()
    connection.run()
