# ---
# title: "Part 1: Static data exploration in Jupyter"
# description: ''
# notebook_export_path: projects/from_jupyter_to_deploy/part1_jupyter_exploration.ipynb
# author: pathway
# article:
#   date: '2023-11-29'
#   thumbnail: ''
#   tags: []
# jupyter:
#   jupytext:
#     formats: py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Part 1: Static data exploration in Jupyter
# This notebook is part of the first part of the tutorial [From interactive data exploration to deployment](/developers/user-guide/exploring-pathway/from-jupyter-to-deploy#part-1-static-data-exploration-in-jupyter).


# %%
# Download CSV file
# !wget -nc https://gist.githubusercontent.com/janchorowski/e351af72ecd8d206a34763a428826ab7/raw/ticker.csv

# %% [markdown]
# ## Loading data

# %%
import datetime

import pathway as pw

fname = "ticker.csv"
schema = pw.schema_from_csv(fname)
data = pw.io.csv.read(fname, schema=schema, mode="static")
# _MD_SHOW_data

# %%
data = data.with_columns(t=data.t.dt.utc_from_timestamp(unit="ms"))
# _MD_SHOW_data

# %%
import bokeh.plotting


def vwap_history(src):
    fig = bokeh.plotting.figure(
        height=400,
        width=600,
        title="Volume-weighted average price",
        x_axis_type="datetime",
    )
    fig.line("t", "vwap", source=src)
    return fig


# %%
data.plot(vwap_history, sorting_col="t")

# %% [markdown]
# ## Designing the algorithm

# %%
minute_20_stats = (
    data.windowby(
        pw.this.t,
        window=pw.temporal.sliding(
            hop=datetime.timedelta(minutes=1), duration=datetime.timedelta(minutes=20)
        ),
        instance=pw.this.ticker,
    )
    .reduce(
        ticker=pw.this._pw_instance,
        t=pw.this._pw_window_end,
        volume=pw.reducers.sum(pw.this.volume),
        transact_total=pw.reducers.sum(pw.this.volume * pw.this.vwap),
        transact_total2=pw.reducers.sum(pw.this.volume * pw.this.vwap**2),
    )
    .with_columns(vwap=pw.this.transact_total / pw.this.volume)
    .with_columns(
        vwstd=(pw.this.transact_total2 / pw.this.volume - pw.this.vwap**2) ** 0.5
    )
    .with_columns(
        bollinger_upper=pw.this.vwap + 2 * pw.this.vwstd,
        bollinger_lower=pw.this.vwap - 2 * pw.this.vwstd,
    )
)
# _MD_SHOW_minute_20_stats

# %%
minute_1_stats = (
    data.windowby(
        pw.this.t,
        window=pw.temporal.tumbling(datetime.timedelta(minutes=1)),
        instance=pw.this.ticker,
    )
    .reduce(
        ticker=pw.this._pw_instance,
        t=pw.this._pw_window_end,
        volume=pw.reducers.sum(pw.this.volume),
        transact_total=pw.reducers.sum(pw.this.volume * pw.this.vwap),
    )
    .with_columns(vwap=pw.this.transact_total / pw.this.volume)
)
# _MD_SHOW_minute_1_stats

# %%
joint_stats = (
    minute_1_stats.join(
        minute_20_stats, pw.left.t == pw.right.t, pw.left.ticker == pw.right.ticker
    )
    .select(
        *pw.left,
        bollinger_lower=pw.right.bollinger_lower,
        bollinger_upper=pw.right.bollinger_upper,
    )
    .with_columns(
        is_alert=(
            (pw.this.volume > 10000)
            & (
                (pw.this.vwap > pw.this.bollinger_upper)
                | (pw.this.vwap < pw.this.bollinger_lower)
            )
        )
    )
    .with_columns(
        action=pw.if_else(
            pw.this.is_alert,
            pw.if_else(pw.this.vwap > pw.this.bollinger_upper, "sell", "buy"),
            "hodl",
        )
    )
)
# _MD_SHOW_joint_stats

# %%
alerts = joint_stats.filter(pw.this.is_alert).select(
    pw.this.ticker, pw.this.t, pw.this.vwap, pw.this.action
)
# _MD_SHOW_alerts

# %% [markdown]
# ## Plotting Bollinger Bands

# %%
import bokeh.models


def stats_plotter(src):
    actions = ["buy", "sell", "hodl"]
    color_map = bokeh.models.CategoricalColorMapper(
        factors=actions, palette=("#00ff00", "#ff0000", "#00000000")
    )

    fig = bokeh.plotting.figure(
        height=400,
        width=600,
        title="20 minutes Bollinger bands with last 1 minute average",
        x_axis_type="datetime",
    )

    fig.line("t", "vwap", source=src)

    fig.line("t", "bollinger_lower", source=src, line_alpha=0.3)
    fig.line("t", "bollinger_upper", source=src, line_alpha=0.3)
    fig.varea(
        x="t",
        y1="bollinger_lower",
        y2="bollinger_upper",
        fill_alpha=0.3,
        fill_color="gray",
        source=src,
    )

    fig.scatter(
        "t",
        "vwap",
        size=10,
        marker="circle",
        color={"field": "action", "transform": color_map},
        source=src,
    )

    return fig


# _MD_SHOW_joint_stats.plot(stats_plotter, sorting_col="t")
