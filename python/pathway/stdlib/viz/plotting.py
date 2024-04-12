# Copyright © 2024 Pathway

from collections.abc import Callable
from typing import Any

import pandas as pd
import panel as pn
from bokeh.models import ColumnDataSource, Plot

import pathway as pw
from pathway.internals import api, parse_graph
from pathway.internals.graph_runner import GraphRunner
from pathway.internals.monitoring import MonitoringLevel
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.table_subscription import subscribe as internal_subscribe
from pathway.internals.trace import trace_user_frame


# after: https://stackoverflow.com/questions/15411967/how-can-i-check-if-code-is-executed-in-the-ipython-notebook
def _in_notebook():
    try:
        from IPython import get_ipython  # noqa

        if "IPKernelApp" not in get_ipython().config:  # noqa
            return False
    except ImportError:
        return False
    except AttributeError:
        return False
    return True


@check_arg_types
@trace_user_frame
def plot(
    self: pw.Table,
    plotting_function: Callable[[ColumnDataSource], Plot],
    sorting_col=None,
) -> pn.Column:
    """
    Allows for plotting contents of the table visually in e.g. jupyter. If the table
    depends only on the bounded data sources, the plot will be generated right away.
    Otherwise (in streaming scenario), the plot will be auto-updating after running pw.run()

    Args:
        self (pw.Table): a table serving as a source of data
        plotting_function (Callable[[ColumnDataSource], Plot]): function for creating plot
            from ColumnDataSource

    Returns:
        pn.Column: visualization which can be displayed immediately or passed as a dashboard widget

    Example:

    >>> import pathway as pw
    >>> from bokeh.plotting import figure
    >>> def func(source):
    ...     plot = figure(height=400, width=400, title="CPU usage over time")
    ...     plot.scatter('a', 'b', source=source, line_width=3, line_alpha=0.6)
    ...     return plot
    >>> viz = pw.debug.table_from_pandas(pd.DataFrame({"a":[1,2,3],"b":[3,1,2]})).plot(func)
    >>> type(viz)
    <class 'panel.layout.base.Column'>
    """

    col_names = self.schema.column_names()

    gr = GraphRunner(parse_graph.G, debug=False, monitoring_level=MonitoringLevel.NONE)
    bounded = gr.has_bounded_input(self)

    source = ColumnDataSource(data={colname: [] for colname in col_names})

    plot = plotting_function(source)
    viz = pn.Column(
        pn.Row(
            "Static preview" if bounded else "Streaming mode",
            pn.widgets.TooltipIcon(
                value=(
                    "Immediate table preview is possible as the table depends only on static inputs"
                    if bounded
                    else "Table depends on streaming inputs. Please run pw.run()"
                )
            ),
        ),
        plot,
    )

    if bounded:
        [captured] = gr.run_tables(self)
        output_data = api.squash_updates(captured)
        keys = list(output_data.keys())
        if sorting_col:
            sorting_i = list(self._columns.keys()).index(sorting_col)
            keys.sort(key=lambda k: output_data[k][sorting_i])  # type: ignore
        dict_data = {
            name: [output_data[key][index] for key in keys]
            for index, name in enumerate(self._columns.keys())
        }
        source.stream(dict_data, rollover=len(output_data))  # type: ignore
    else:
        integrated: dict[api.Pointer, Any] = {}

        in_notebook = _in_notebook()

        def stream_updates():
            df = pd.DataFrame.from_dict(integrated, orient="index", columns=col_names)
            if sorting_col:
                df = df.sort_values(sorting_col)
            else:
                df = df.sort_index()
            df = df.reset_index(drop=True)

            source.stream(
                df.to_dict("list"), rollover=len(df)  # type:ignore[arg-type]
            )

            if in_notebook:
                pn.io.push_notebook(viz)

        def _update(key, row, time, is_addition):
            if is_addition:
                integrated[key] = row
            else:
                del integrated[key]

        def on_time_end(time):
            if plot.document is not None:
                if plot.document.session_context:
                    plot.document.add_next_tick_callback(stream_updates)
                else:
                    stream_updates()

        internal_subscribe(
            self, on_change=_update, on_time_end=on_time_end, skip_persisted_batch=True
        )
        pn.state.on_session_created(lambda _: stream_updates())

    return viz
