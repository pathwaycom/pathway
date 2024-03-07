# Copyright © 2024 Pathway

# mypy: disallow-untyped-defs, extra-checks, disallow-any-generics, warn-return-any

from __future__ import annotations

import builtins
import logging
import sys
import warnings
from collections.abc import Callable
from dataclasses import dataclass
from threading import Event, Lock, Thread
from typing import Any, TypeVar

from pathway.internals import (
    api,
    datasink as datasinks,
    datasource as datasources,
    graph_runner as graph_runners,
    monitoring,
    operator as operators,
    parse_graph as parse_graphs,
    schema as schemas,
    table as tables,
    table_io,
)

S = TypeVar("S", bound=schemas.Schema)


class DisplayAsStr:
    def _repr_pretty_(self, p: Any, cycle: bool) -> None:
        p.text(str(self))


class LiveTableState:
    exception: BaseException | None
    lock: Lock
    tables: dict[int, api.ExportedTable]
    all_initialized: Event

    def __init__(self) -> None:
        self.exception = None
        self.lock = Lock()
        self.tables = {}
        self.all_initialized = Event()

    def export_callback(
        self, scope: api.Scope, exported_table: api.ExportedTable
    ) -> None:
        assert scope.process_count == 1

        done = False
        with self.lock:
            assert scope.worker_index not in self.tables
            self.tables[scope.worker_index] = exported_table
            if len(self.tables) == scope.worker_count:
                done = True
        if done:
            self.all_initialized.set()

    def import_callback(self, scope: api.Scope) -> api.ExportedTable:
        assert scope.process_count == 1
        with self.lock:
            assert scope.worker_count == len(self.tables)
            return self.tables[scope.worker_index]

    def frontier(self) -> api.Frontier:
        return min(exported.frontier() for exported in self.tables.values())

    def snapshot_at(
        self, frontier: api.Frontier
    ) -> list[tuple[api.Pointer[Any], list[api.Value]]]:
        res = []
        for exported in self.tables.values():
            res.extend(exported.snapshot_at(frontier))
        return res

    def failed(self) -> bool:
        if self.exception is not None:
            return True

        return any(table.failed() for table in self.tables.values())


class LiveTableThread(Thread):
    state: LiveTableState
    operator: operators.Operator
    logger: logging.Logger

    def __init__(
        self, state: LiveTableState, operator: operators.Operator, *, name: str
    ):
        super().__init__(name=name)
        self.state = state
        self.operator = operator
        self.logger = logging.getLogger(__name__)

    def run(self) -> None:
        try:
            self.logger.info(f"{self.name} started")
            runner = graph_runners.GraphRunner(
                self.operator.graph,
                debug=False,
                with_http_server=False,
                monitoring_level=monitoring.MonitoringLevel.NONE,
            )  # TODO: disable more options?
            runner.run_nodes([self.operator])
        except BaseException as e:
            self.state.exception = e
            self.state.all_initialized.set()
        finally:
            self.logger.info(f"{self.name} finishing")


@dataclass(frozen=True)
class LiveTableSnapshot(DisplayAsStr):
    frontier: api.Frontier
    data: list[tuple[api.Pointer[Any], list[api.Value]]]

    def __str__(self) -> str:
        if self.frontier is api.DONE:
            header = "final snapshot"
        else:
            header = f"snapshot at time {self.frontier}"
        return header + "\n" + str(self.data)


class LiveTable(DisplayAsStr, tables.Table[S]):
    _state: LiveTableState
    _thread: LiveTableThread

    @classmethod
    def _create(cls, origin: tables.Table[S]) -> LiveTable[S]:
        state = LiveTableState()
        datasink = datasinks.ExportDataSink(state.export_callback)
        operator = table_io.table_to_datasink(origin, datasink, special=True)
        thread = LiveTableThread(
            state, operator, name=f"live thread for table {origin!r}"
        )
        thread.start()
        state.all_initialized.wait()

        datasource = datasources.ImportDataSource(
            state.import_callback, schema=origin.schema
        )
        result = table_io.table_from_datasource(datasource, table_cls=cls)

        result._state = state
        result._thread = thread

        return result

    def __str__(self) -> str:
        return str(self.snapshot())

    def live(self) -> LiveTable[S]:
        return self

    def failed(self) -> bool:
        return self._state.failed()

    def frontier(self) -> api.Frontier:
        return self._state.frontier()

    def snapshot_at(self, frontier: api.Frontier) -> LiveTableSnapshot:
        data = self._state.snapshot_at(frontier)
        return LiveTableSnapshot(frontier, data)

    def snapshot(self) -> LiveTableSnapshot:
        return self.snapshot_at(self.frontier())

    def subscribe(self, callback: Any) -> None:
        raise NotImplementedError("TODO")

    def __del__(self) -> None:
        print(f"Live table {id(self)} no longer needed")


class InteractiveModeController:
    _orig_displayhook: Callable[[object], None]

    def __init__(self, _pathway_internal: bool = False) -> None:
        assert _pathway_internal, "InteractiveModeController is an internal class"
        self._orig_displayhook = sys.displayhook
        sys.displayhook = self._displayhook

    def _displayhook(self, value: object) -> None:
        if isinstance(value, DisplayAsStr):
            builtins._ = value  # type: ignore [attr-defined]
            print(str(value))
        else:
            self._orig_displayhook(value)


def is_interactive_mode_enabled() -> bool:
    graph = parse_graphs.G

    return graph.interactive_mode_controller is not None


def enable_interactive_mode() -> InteractiveModeController:
    warnings.warn("interactive mode is experimental", stacklevel=2)

    graph = parse_graphs.G

    if graph.interactive_mode_controller is not None:
        return graph.interactive_mode_controller

    if not graph.global_scope.is_empty():
        # XXX: is this a correct test?
        raise ValueError("Cannot enable interactive mode")

    import logging

    logging.basicConfig(level=logging.INFO)

    graph.interactive_mode_controller = InteractiveModeController(
        _pathway_internal=True
    )
    return graph.interactive_mode_controller
