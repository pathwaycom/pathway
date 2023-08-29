import os
from dataclasses import KW_ONLY, dataclass
from typing import Optional, Union

from pathway.internals import api


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
        fs_path: Optional[Union[str, os.PathLike[str]]],
    ):
        self._engine_data_storage = engine_data_storage
        self._fs_path = fs_path

    @classmethod
    def filesystem(cls, path: Union[str, os.PathLike[str]]):
        return cls(
            api.DataStorage(
                storage_type="fs",
                path=os.fspath(path),
            ),
            fs_path=path,
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
    metadata_storage: PersistentStorageBackend
    snapshot_storage: PersistentStorageBackend

    @classmethod
    def single_backend(cls, backend: PersistentStorageBackend):
        return cls(
            metadata_storage=backend,
            snapshot_storage=backend,
        )

    @property
    def engine_config(self):
        return api.PersistenceConfig(
            metadata_storage=self.metadata_storage.engine_data_storage,
            stream_storage=self.snapshot_storage.engine_data_storage,
        )

    def on_before_run(self):
        self.snapshot_storage.store_path_in_env_variable()
