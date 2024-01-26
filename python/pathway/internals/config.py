import os
from dataclasses import dataclass, field

from pathway import persistence
from pathway.internals import api


def _env_field(name: str, default: str | None = None):
    def factory():
        return os.environ.get(name, default)

    return field(default_factory=factory)


def _env_bool_field(name: str):
    def factory():
        value = os.environ.get(name, "false").lower()
        if value in ("1", "true", "yes"):
            return True
        elif value in ("0", "false", "no"):
            return False
        else:
            raise ValueError(
                f"Unexpected value for {name!r} environment variable: {value!r}"
            )

    return field(default_factory=factory)


def _snapshot_access() -> api.SnapshotAccess | None:
    match os.environ.get("PATHWAY_SNAPSHOT_ACCESS", "").lower():
        case "record":
            return api.SnapshotAccess.RECORD
        case "replay":
            return api.SnapshotAccess.REPLAY
        case _:
            return None


def _persistence_mode() -> api.PersistenceMode:
    match os.environ.get(
        "PATHWAY_PERSISTENCE_MODE", os.environ.get("PATHWAY_REPLAY_MODE", "")
    ).lower():
        case "speedrun":
            return api.PersistenceMode.SPEEDRUN_REPLAY
        case "batch":
            return api.PersistenceMode.BATCH
        case _:
            return api.PersistenceMode.BATCH


@dataclass
class PathwayConfig:
    continue_after_replay: bool = _env_bool_field("PATHWAY_CONTINUE_AFTER_REPLAY")
    ignore_asserts: bool = _env_bool_field("PATHWAY_IGNORE_ASSERTS")
    runtime_typechecking: bool = _env_bool_field("PATHWAY_RUNTIME_TYPECHECKING")
    persistence_mode: api.PersistenceMode = field(default_factory=_persistence_mode)
    snapshot_access: api.SnapshotAccess | None = field(default_factory=_snapshot_access)
    replay_storage: str | None = _env_field("PATHWAY_REPLAY_STORAGE")

    @property
    def replay_config(
        self,
    ) -> persistence.Config | None:
        if self.replay_storage:
            if self.snapshot_access not in (
                api.SnapshotAccess.RECORD,
                api.SnapshotAccess.REPLAY,
            ):
                raise ValueError(
                    "unexpected value of PATHWAY_SNAPSHOT_ACCESS environment variable "
                    + "- when PATHWAY_REPLAY_STORAGE is set, PATHWAY_SNAPSHOT_ACCESS "
                    + "needs to be set to either 'record' or 'replay'"
                )
            data_storage = persistence.Backend.filesystem(self.replay_storage)
            persistence_config = persistence.Config.simple_config(
                data_storage,
                persistence_mode=self.persistence_mode,
                snapshot_access=self.snapshot_access,
                continue_after_replay=self.continue_after_replay,
            )
            return persistence_config
        else:
            return None


pathway_config = PathwayConfig()

__all__ = ["PathwayConfig", "pathway_config"]
