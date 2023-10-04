import os
from dataclasses import KW_ONLY, dataclass

from pathway.internals import api
from pathway.internals._io_helpers import AwsS3Settings


class PersistentStorageBackend:
    """
    This class works as a part of a high-level persistence config. User specifies
    the persistent storage parameters using one classmethod-marked methods.

    In order to configure persistence in Pathway, you will need two settings of this
    kind: one for stream storage and one for snapshot storage.
    """

    def __init__(
        self,
        engine_data_storage: api.DataStorage,
        fs_path: str | os.PathLike[str] | None = None,
    ):
        self._engine_data_storage = engine_data_storage
        self._fs_path = fs_path

    @classmethod
    def filesystem(cls, path: str | os.PathLike[str]):
        return cls(
            api.DataStorage(
                storage_type="fs",
                path=os.fspath(path),
            ),
            fs_path=path,
        )

    @classmethod
    def s3(cls, root_path: str, bucket_settings: AwsS3Settings):
        return cls(
            api.DataStorage(
                storage_type="s3",
                aws_s3_settings=bucket_settings.settings,
                path=root_path,
            ),
        )

    @property
    def engine_data_storage(self):
        return self._engine_data_storage

    def store_path_in_env_variable(self):
        if self._fs_path:
            os.environ["PATHWAY_PERSISTENT_STORAGE"] = os.fspath(self._fs_path)


@dataclass(frozen=True)
class PersistenceConfig:
    """
    This class aggregates the metadata and stream storage settings. This is the entry
    point for persistence configuration and should be used as ``persistence_config``
    parameter in ``pw.run(...)`` command.
    """

    _: KW_ONLY
    refresh_duration_ms: int = 0
    metadata_storage: PersistentStorageBackend
    snapshot_storage: PersistentStorageBackend

    @classmethod
    def single_backend(cls, backend: PersistentStorageBackend, refresh_duration_ms=0):
        return cls(
            refresh_duration_ms=refresh_duration_ms,
            metadata_storage=backend,
            snapshot_storage=backend,
        )

    @property
    def engine_config(self):
        return api.PersistenceConfig(
            refresh_duration_ms=self.refresh_duration_ms,
            metadata_storage=self.metadata_storage.engine_data_storage,
            stream_storage=self.snapshot_storage.engine_data_storage,
        )

    def on_before_run(self):
        self.snapshot_storage.store_path_in_env_variable()
