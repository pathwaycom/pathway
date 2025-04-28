# Copyright © 2024 Pathway

import os

import pandas as pd
import panel as pn

import pathway as pw
from pathway.internals import api, parse_graph
from pathway.internals.graph_runner import GraphRunner
from pathway.internals.monitoring import MonitoringLevel
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.table_subscription import subscribe as internal_subscribe
from pathway.internals.trace import trace_user_frame

if os.environ.get("PATHWAY_INTERACTIVE", "1") == "1":
    pn.extension("tabulator")


def _repr_mimebundle_(self: pw.Table, include, exclude):
    return self.show(snapshot=True)._repr_mimebundle_(include, exclude)


@check_arg_types
@trace_user_frame
def show(
    self: pw.Table, *, snapshot=True, include_id=True, short_pointers=True, sorters=None
) -> pn.Column:
    """
    Allows for displaying table visually in e.g. jupyter. If the table
    depends only on the bounded data sources, the table preview will be generated right away.
    Otherwise (in streaming scenario), the table will be auto-updating after running pw.run()

    Args:
        self (pw.Table): a table to be displayed
        snapshot (bool, optional): whether only current snapshot or all changes to the table should be displayed.
            Defaults to True.
        include_id (bool, optional): whether to show ids of rows. Defaults to True.
        short_pointers (bool, optional): whether to shorten printed ids. Defaults to True.

    Returns:
        pn.Column: visualization which can be displayed immediately or passed as a dashboard widget

    Example:

    >>> import pathway as pw
    >>> table_viz = pw.debug.table_from_pandas(pd.DataFrame({"a":[1,2,3],"b":[3,1,2]})).show()
    >>> type(table_viz)
    <class 'panel.layout.base.Column'>
    """

    col_names = []
    if include_id:
        col_names.append("id")
    col_names += self.schema.column_names()
    if not snapshot:
        col_names.append("time")
        col_names.append("diff")

    def _format_types(x):
        if pd.isna(x):
            return None
        if isinstance(x, api.Pointer):
            s = str(x)
            if len(s) > 8 and short_pointers:
                s = s[:8] + "..."
            return s
        if isinstance(x, pd.Timestamp):
            return x.strftime("%Y-%m-%d %H:%M:%S%z")
        if isinstance(x, pw.Json):
            s = str(x)
            if len(s) > 64:
                s = s[:64] + " ..."
            return s
        return x

    gr = GraphRunner(parse_graph.G, debug=False, monitoring_level=MonitoringLevel.NONE)
    bounded = gr.has_bounded_input(self)

    if sorters is None:
        sorters = []
    sorters += (
        []
        if bounded or snapshot
        else [
            {"field": "time", "dir": "desc"},
            {"field": "diff", "dir": "desc"},
        ]
    )

    dynamic_table = pn.widgets.Tabulator(
        pd.DataFrame(columns=col_names),
        disabled=True,
        show_index=False,
        height=400,
        sorters=sorters,
        pagination="local",
        page_size=10,
        sizing_mode="stretch_width",
    )

    def color_negative_red(row):
        color = "red" if row["diff"] < 0 else "green"
        return [f"color: {color}" for _ in row]

    if not snapshot:
        dynamic_table.style.apply(color_negative_red, axis=1)  # type:ignore[union-attr]

    if bounded:
        if not snapshot:
            raise NotImplementedError(
                "Cannot plot diff table in a static mode"
            )  # todo: implement with support for _time and _diff
        [captured] = gr.run_tables(self)
        output_data = api.squash_updates(captured)
        keys = list(output_data.keys())
        dict_data = {
            name: [_format_types(output_data[key][index]) for key in keys]
            for index, name in enumerate(self._columns.keys())
        }
        dynamic_table.value = pd.DataFrame(dict_data)
    else:
        integrated = {}

        def update(key, row, time, is_addition):
            row["id"] = key
            if not snapshot:
                row["time"] = time
                row["diff"] = is_addition * 2 - 1
                row = {k: _format_types(v) for k, v in row.items()}
                dynamic_table.stream(row, follow=False)
            else:
                if is_addition:
                    integrated[key] = row
                else:
                    del integrated[key]

                # todo: use async transformer to throttle updates
                # dynamic_table.stream(
                #     df.to_dict("list"), rollover=len(df)
                # )  # alternative update method

        def on_time_end(time):
            df = (
                pd.DataFrame.from_dict(integrated, orient="index", columns=col_names)
                .sort_index()
                .reset_index(drop=True)
            )
            df = df[col_names]

            df = df.map(_format_types)

            dynamic_table.value = df

        internal_subscribe(
            self, on_change=update, on_time_end=on_time_end, skip_persisted_batch=True
        )

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
        dynamic_table,
    )
    return viz
