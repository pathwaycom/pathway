# Copyright © 2023 Pathway

import os

from pathway.internals import api
from pathway.persistence import (
    Backend as PersistentStorageBackend,
    Config as PersistenceConfig,
)

ignore_asserts = os.environ.get("PATHWAY_IGNORE_ASSERTS", "false").lower() in (
    "1",
    "true",
    "yes",
)

runtime_typechecking = os.environ.get(
    "PATHWAY_RUNTIME_TYPECHECKING", "false"
).lower() in (
    "1",
    "true",
    "yes",
)


def get_replay_config():
    if replay_storage := os.environ.get("PATHWAY_REPLAY_STORAGE"):
        fallback_mode = os.environ.get("PATHWAY_REPLAY_MODE", "")
        match os.environ.get("PATHWAY_PERSISTENCE_MODE", fallback_mode).lower():
            case "speedrun":
                persistence_mode = api.PersistenceMode.SPEEDRUN_REPLAY
            case "batch":
                persistence_mode = api.PersistenceMode.BATCH
            case _:
                persistence_mode = api.PersistenceMode.BATCH
        match os.environ.get("PATHWAY_SNAPSHOT_ACCESS", "").lower():
            case "record":
                snapshot_access = api.SnapshotAccess.RECORD
            case "replay":
                snapshot_access = api.SnapshotAccess.REPLAY
            case _:
                raise ValueError(
                    """unexpected value of PATHWAY_SNAPSHOT_ACCESS environment variable """
                    """- when PATHWAY_REPLAY_STORAGE is set, PATHWAY_SNAPSHOT_ACCESS """
                    """needs to be set to either "record" or "replay" """
                )

        continue_after_replay = bool(os.environ.get("PATHWAY_CONTINUE_AFTER_REPLAY"))

        data_storage = PersistentStorageBackend.filesystem(replay_storage)
        persistence_config = PersistenceConfig.simple_config(
            data_storage,
            persistence_mode=persistence_mode,
            snapshot_access=snapshot_access,
            continue_after_replay=continue_after_replay,
        )
        return persistence_config
    else:
        return None
