import os
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field

from pathway import persistence
from pathway.internals import api


def _env_field(name: str, default: str | None = None, default_if_empty: bool = False):
    def factory():
        value = os.environ.get(name, default)
        if default_if_empty and value == "":
            value = default
        return value

    return field(default_factory=factory)


def _env_bool_field(name: str, *, default: str = "false"):
    def factory():
        value = os.environ.get(name, default).lower()
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
    license_key: str | None = _env_field("PATHWAY_LICENSE_KEY")
    monitoring_server: str | None = _env_field(
        "PATHWAY_MONITORING_SERVER", default_if_empty=True
    )
    terminate_on_error: bool = _env_bool_field(
        "PATHWAY_TERMINATE_ON_ERROR", default="true"
    )
    process_id: str = _env_field("PATHWAY_PROCESS_ID", default="0")
    suppress_other_worker_errors: bool = _env_bool_field(
        "PATHWAY_SUPPRESS_OTHER_WORKER_ERRORS"
    )

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
            persistence_config = persistence.Config(
                data_storage,
                persistence_mode=self.persistence_mode,
                snapshot_access=self.snapshot_access,
                continue_after_replay=self.continue_after_replay,
            )
            return persistence_config
        else:
            return None


_pathway_config: ContextVar[PathwayConfig] = ContextVar(
    "pathway_config", default=PathwayConfig()
)


def _check_entitlements(*args: str):
    return api.check_entitlements(
        license_key=get_pathway_config().license_key, entitlements=list(args)
    )


@contextmanager
def local_pathway_config():
    config = PathwayConfig()
    token = _pathway_config.set(config)
    try:
        yield config
    finally:
        _pathway_config.reset(token)


def get_pathway_config() -> PathwayConfig:
    return _pathway_config.get()


def set_license_key(key: str | None) -> None:
    """Sets Pathway license key.
    License key can be obtained from the
    `official website <https://pathway.com/get-license/>`_.

    Args:
        key: The license key to be set. If None, any existing license key will be cleared.

    Returns:
        None

    Example:

    >>> import pathway as pw
    >>> pw.set_license_key("demo-license-key-with-telemetry")
    """
    get_pathway_config().license_key = key


def set_monitoring_config(*, server_endpoint: str | None) -> None:
    """Sets the monitoring server endpoint.
    Requires a valid Pathway Scale license key.

    Args:
        server_endpoint: The server endpoint URL for monitoring,
            or None to clear the existing configuration.
            The endpoint should be
            `OTLP <https://opentelemetry.io/docs/specs/otlp/>`_ compatible
            and support gRPC protocol.

    Returns:
        None

    Example:

    >>> import pathway as pw
    >>> pw.set_license_key("YOUR_LICENSE_KEY")
    >>> pw.set_monitoring_config(server_endpoint="https://example.com:4317")
    """
    get_pathway_config().monitoring_server = server_endpoint


__all__ = [
    "PathwayConfig",
    "get_pathway_config",
    "local_pathway_config",
    "set_license_key",
    "set_monitoring_config",
]
