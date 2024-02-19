# Copyright © 2024 Pathway

import contextlib
import os
from collections.abc import Generator
from dataclasses import KW_ONLY, dataclass

from pathway.internals import api
from pathway.internals._io_helpers import AwsS3Settings


class Backend:
    """
    The settings of a backend, which is used to persist the computation state. There
    are two kinds of data backends: metadata backend and snapshot backend. Both are
    configurable via this class.
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
        """
        Configure the filesystem backend.

        Args:
            path: the path to the root directory in the file system, which will be used \
to store the persisted data.

        Returns:
            Class instance denoting the filesystem storage backend with root directory \
at ``path``.
        """
        return cls(
            api.DataStorage(
                storage_type="fs",
                path=os.fspath(path),
            ),
            fs_path=path,
        )

    @classmethod
    def s3(cls, root_path: str, bucket_settings: AwsS3Settings):
        """
        Configure the S3 backend.

        Args:
            root_path: path to the root in the S3 storage, which will be used to \
store persisted data;
            bucket_settings: the settings for S3 bucket connection in the same format \
as they are used by S3 connectors.

        Returns:
            Class instance denoting the S3 storage backend with root directory as
            ``root_path`` and connection settings given by ``bucket_settings``.
        """
        return cls(
            api.DataStorage(
                storage_type="s3",
                aws_s3_settings=bucket_settings.settings,
                path=root_path,
            ),
        )

    @classmethod
    def mock(cls, events: dict[tuple[str, int], list[api.SnapshotEvent]]):
        return cls(api.DataStorage(storage_type="mock", mock_events=events))

    @property
    def engine_data_storage(self):
        return self._engine_data_storage

    def store_path_in_env_variable(self):
        if self._fs_path:
            os.environ["PATHWAY_PERSISTENT_STORAGE"] = os.fspath(self._fs_path)

    def remove_path_from_env_variable(self):
        if self._fs_path:
            del os.environ["PATHWAY_PERSISTENT_STORAGE"]


@dataclass(frozen=True)
class Config:
    """
    Configure the data persistence. An instance of this class should be passed as a
    parameter to pw.run in case persistence is enabled.

    Please note that if you'd like to use the same backend for both metadata and
    snapshot storages, you can use the convenience method ``simple_config``.

    Args:
        metadata_storage: metadata backend configuration;
        snapshot_storage: snapshots backend configuration;
        snapshot_interval_ms: the desired duration between snapshot updates in \
milliseconds;
    """

    _: KW_ONLY
    snapshot_interval_ms: int = 0
    metadata_storage: Backend
    snapshot_storage: Backend
    snapshot_access: api.SnapshotAccess
    persistence_mode: api.PersistenceMode
    continue_after_replay: bool

    @classmethod
    def simple_config(
        cls,
        backend: Backend,
        snapshot_interval_ms=0,
        snapshot_access=api.SnapshotAccess.FULL,
        persistence_mode=api.PersistenceMode.PERSISTING,
        continue_after_replay=True,
    ):
        """
        Construct config from a single instance of the \
``Backend`` class, using this backend to persist metadata and \
snapshot.

        Args:
            backend: storage backend settings;
            snapshot_interval_ms: the desired freshness of the persisted snapshot in \
milliseconds. The greater the value is, the more the amount of time that the snapshot \
may fall behind, and the less computational resources are required.

        Returns:
            Persistence config.
        """

        return cls(
            snapshot_interval_ms=snapshot_interval_ms,
            metadata_storage=backend,
            snapshot_storage=backend,
            snapshot_access=snapshot_access,
            persistence_mode=persistence_mode,
            continue_after_replay=continue_after_replay,
        )

    @property
    def engine_config(self):
        return api.PersistenceConfig(
            snapshot_interval_ms=self.snapshot_interval_ms,
            metadata_storage=self.metadata_storage.engine_data_storage,
            stream_storage=self.snapshot_storage.engine_data_storage,
            snapshot_access=self.snapshot_access,
            persistence_mode=self.persistence_mode,
            continue_after_replay=self.continue_after_replay,
        )

    def on_before_run(self):
        self.snapshot_storage.store_path_in_env_variable()

    def on_after_run(self):
        self.snapshot_storage.remove_path_from_env_variable()


@contextlib.contextmanager
def get_persistence_engine_config(
    persistence_config: Config | None,
) -> Generator[api.PersistenceConfig | None, None, None]:
    if persistence_config is None:
        yield None
        return

    persistence_config.on_before_run()
    try:
        yield persistence_config.engine_config
    finally:
        persistence_config.on_after_run()
