# Copyright © 2024 Pathway

import contextlib
import logging
import os
from enum import Enum
from typing import Any

from rich import box
from rich.align import Align
from rich.console import Console, ConsoleOptions, Group, RenderResult
from rich.layout import Layout
from rich.live import Live
from rich.logging import RichHandler
from rich.panel import Panel
from rich.segment import Segment
from rich.table import Table

from pathway.internals import api


class ConsolePrintingToBuffer(Console):
    def __init__(self) -> None:
        super().__init__()
        self.logs: list[Table] = []

    def print(self, *records, **kwargs) -> None:
        self.logs.extend(records)

    def forget(self, num_records_to_remember: int) -> None:
        self.logs = self.logs[-num_records_to_remember:]


class LogsOutput:
    def __init__(self, console: ConsolePrintingToBuffer) -> None:
        self.console = console

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:
        suffix_len = options.max_height - 1
        self.console.forget(options.max_height - 1)
        group = Group(*self.console.logs)
        panel = Panel(group, title="LOGS", expand=True, box=box.MINIMAL)

        # if the terminal is too narrow, print only suffix of lines
        rendered_lines = console.render_lines(panel)
        lines = [rendered_lines[0]] + rendered_lines[1:][-suffix_len:]
        new_line = Segment.line()
        for i, line in enumerate(lines):
            yield from line
            if i + 1 < len(lines):
                yield new_line


class MonitoringOutput:
    def __init__(
        self,
        node_names: list[tuple[int, str]],
        data: Any,
        now: int,
    ) -> None:
        self.node_names = node_names
        self.data = data
        self.now = now

    def get_latency(self, time: int | None) -> int | None:
        if time is not None:
            return max(0, self.now - time)
        return None

    def log_line(
        self, table: Table, node_name: str, stats: Any, skip_lag: bool = False
    ) -> None:
        if stats is None:
            table.add_row(node_name, "no info", "")
        elif stats.done:
            table.add_row(node_name, "finished", "")
        else:
            latency = self.get_latency(stats.time)
            table.add_row(
                node_name,
                "initializing" if latency is None else f"{latency}",
                "" if skip_lag else f"{stats.lag}",
            )

    def get_connectors_table(self) -> Table:
        table = Table(
            box=box.SIMPLE,
        )
        table.add_column("connector", justify="left")
        table.add_column("no. messages in the last minibatch", justify="right")
        table.add_column("in the last minute", justify="right")
        table.add_column("since start", justify="right")

        for name, entry in self.data.connector_stats:
            table.add_row(
                name,
                (
                    "finished"
                    if entry.finished
                    else f"{entry.num_messages_recently_committed}"
                ),
                f"{entry.num_messages_in_last_minute}",
                f"{entry.num_messages_from_start}",
            )
        return table

    def get_operators_table(self, max_height) -> Table:
        if len(self.node_names) == 0:
            caption = (
                "Above you can see the latency of input and output operators. The latency "
                + "is measured as the difference between the time when the operator "
                + "processed the data and the time when pathway acquired the data."
            )
        else:
            caption = (
                "You can find the latency for each operator in the computation graph"
                + " displayed above. The latency is measured as the difference between "
                + "the time when the operator processed the data and the time when "
                + "pathway acquired the data."
            )
        table = Table(caption=caption, box=box.SIMPLE)
        table.add_column("operator", justify="left")
        table.add_column(
            r"latency to wall clock \[ms]",
            justify="right",
        )
        table.add_column(
            r"lag to input \[ms]",
            justify="right",
        )

        self.log_line(table, "input", self.data.input_stats, skip_lag=True)
        max_operator_rows_to_print = max_height - 4
        # 2 lines for header, 1 line for input, 1 line for output

        for i, (id_, node_name) in enumerate(self.node_names):
            if (
                max_operator_rows_to_print < len(self.node_names)
                and i + 1 == max_operator_rows_to_print
            ):
                break
            self.log_line(table, node_name, self.data.operators_stats.get(id_))
        if max_operator_rows_to_print < len(self.node_names):
            table.add_row("...", "...", "...")

        self.log_line(table, "output", self.data.output_stats)

        return table

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:
        layout = Layout(name="monitoring_inner")
        layout.split_row(Layout(name="connectors"), Layout(name="operators"))
        layout["connectors"].update(Align.center(self.get_connectors_table()))
        layout["operators"].update(
            Align.center(self.get_operators_table(options.max_height - 2))
        )
        # options.max_height-2 because a title of the dashboard takes 2 lines
        yield Panel(layout, title="PATHWAY PROGRESS DASHBOARD", box=box.MINIMAL)


class StatsMonitor:
    def __init__(self, node_names: list[tuple[int, str]]) -> None:
        self.layout = Layout(name="root")
        if len(node_names) > 0:
            ratio = 2
        else:
            ratio = 1
        self.layout.split(
            Layout(name="monitoring", ratio=ratio),
            Layout(name="logs"),
        )
        self.layout["monitoring"].update("")
        console = ConsolePrintingToBuffer()
        self.handler = RichHandler(console=console, show_path=False)
        self.layout["logs"].update(LogsOutput(console))
        self.node_names = node_names

    def get_logging_handler(self) -> RichHandler:
        return self.handler

    def update_monitoring(self, data: Any, now: int) -> None:
        self.layout["monitoring"].update(MonitoringOutput(self.node_names, data, now))


@contextlib.contextmanager
def monitor_stats(
    monitoring_level: api.MonitoringLevel,
    node_names: list[tuple[int, str]],
    *,
    default_logging: bool,
    process_id: str,
    refresh_per_second: int = 4,
):
    if monitoring_level != api.MonitoringLevel.ALL:
        node_names = []
    if monitoring_level != api.MonitoringLevel.NONE:
        if process_id == "0":
            stats_monitor = StatsMonitor(node_names)
            handler = stats_monitor.get_logging_handler()
            logging.basicConfig(level=logging.INFO, handlers=[])
            logging.getLogger().addHandler(handler)
            with Live(
                stats_monitor.layout, refresh_per_second=refresh_per_second, screen=True
            ):
                yield stats_monitor
            logging.getLogger().removeHandler(handler)
        else:
            with open(os.devnull, "w") as devnull:
                with (
                    contextlib.redirect_stdout(devnull),
                    contextlib.redirect_stderr(devnull),
                ):
                    yield None
    else:
        if default_logging:
            logging.basicConfig(
                level=logging.INFO,
                format="[%(asctime)s]:%(levelname)s:%(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
        yield None


class MonitoringLevel(Enum):
    """Specifies a verbosity of Pathway monitoring mechanism."""

    AUTO = 0
    """
    Automatically sets IN_OUT in an interactive terminal and jupyter notebook.
    Sets NONE otherwise.
    """
    AUTO_ALL = 1
    """
    Automatically sets ALL in an interactive terminal and jupyter notebook.
    Sets NONE otherwise.
    """
    NONE = 2
    """No monitoring."""
    IN_OUT = 3
    """
    Monitor input connectors and input and output latency. The latency is measured as
    the difference between the time when the operator processed the data and the time
    when pathway acquired the data.
    """
    ALL = 4
    """
    Monitor input connectors and latency for each operator in the execution graph. The
    latency is measured as the difference between the time when the operator processed
    the data and the time when pathway acquired the data.
    """

    def to_internal(self) -> api.MonitoringLevel:
        if (
            self in {MonitoringLevel.AUTO, MonitoringLevel.AUTO_ALL}
            and _disable_monitoring_when_auto()
        ):
            return api.MonitoringLevel.NONE
        return {
            MonitoringLevel.AUTO: api.MonitoringLevel.IN_OUT,
            MonitoringLevel.AUTO_ALL: api.MonitoringLevel.ALL,
            MonitoringLevel.NONE: api.MonitoringLevel.NONE,
            MonitoringLevel.IN_OUT: api.MonitoringLevel.IN_OUT,
            MonitoringLevel.ALL: api.MonitoringLevel.ALL,
        }[self]


def _disable_monitoring_when_auto() -> bool:
    console = Console()
    return not (console.is_interactive or console.is_jupyter)
