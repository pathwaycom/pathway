# ---
# title: "Jupyter / Colab: visualizing and transforming live data streams in Python notebooks with Pathway"
# description: ''
# aside: true
# author:
#   name: Avril Aysha
#   description: Developer Advocate
#   img: /assets/pictures/image_avril.png
#   linkedin: https://www.linkedin.com/in/avrilaysha/
# article:
#   date: '2023-11-29'
#   thumbnail: ''
#   tags: ['tutorial', 'data-pipeline', 'showcase']
# keywords: ['Jupyter notebook', 'interactivity', 'bokeh', 'panel', 'Bollinger bands', 'vizualization', 'Weighted Average', 'rolling statistics', 'dashboard', 'notebook', 'Docker']
# notebook_export_path: notebooks/showcases/live-data-jupyter.ipynb
# docker_github_link: "https://github.com/pathwaycom/pathway/tree/main/examples/projects/from_jupyter_to_deploy/part4_deployment"
# popular: true
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
# # Jupyter / Colab: visualizing and transforming live data streams in Python notebooks with Pathway
#
# <b> 💡 This notebook is one part of a [full-length tutorial](/developers/user-guide/deployment/from-jupyter-to-deploy/) depicting a production-grade data science scenario from data exploration to interactive dashboarding and containerized deploy.</b>
#
# ## Jupyter notebooks & streaming data
# Jupyter notebooks are a great tool for working with data. They give you a friendly interactive interface in which you can process data comfortably and visualise it in intuitive, human-readable ways.
#
# Most data analysts use Jupyter notebooks with static (or "batch") workflows. Using Jupyter with streaming workflows can feel intimidating or have mixed results due to inadequate tooling.
#
# In this notebook you will learn how to visualise a live data stream in real-time from the familiar context of a Jupyter Notebook. You will use Pathway, [Bokeh](https://bokeh.org/) and [Panel](https://panel.holoviz.org/) to build a real-time data visualisation that will alert you when the data hits a critical threshold. You can follow along on this page or run the code for yourself [in Colab](https://colab.research.google.com/github/pathwaycom/pathway/blob/main/examples/notebooks/showcases/live-data-jupyter.ipynb) or [from Github](https://github.com/pathwaycom/pathway/blob/main/examples/notebooks/showcases/live-data-jupyter.ipynb).
#
# Because Pathway [unites static and stream processing](/developers/user-guide/connect/switch-from-batch-to-streaming#switching-from-batch-to-streaming) in a single syntax, you can use the exact same code for both batch and streaming. This way you can test your workflow with a static snapshot and then simply switch out the data source for a live stream when you want to visualise the full data flow in real-time.
#
# Let's jump in! 🪂
#
# ![image](https://github.com/pathwaycom/pathway-examples/blob/c20cd69a6c9c87fc70a9082de57666c50f2ab3c2/documentation/from_jupyter_to_deploy/jupyter-demo-final-smallest-compressed.gif?raw=true)
#
# ## What we'll do
#
# In this notebook, we will take a stream of financial data and implement a simple trading algorithm using [Bollinger Bands](https://en.wikipedia.org/wiki/Bollinger_Bands). This will be a helpful example of visualizing streaming data and will also show the usefulness of getting critical alerts in real-time.

# You don't need to fully understand the algorithm or the financial terminology. Focus on the steps you will to take a live data stream, perform some computation on it in real-time, and output a data visualisation in your Jupyter Notebook that updates on-the-fly. These steps are generalizable to any data streaming use case.
#
# Concretely, we will compute the 1-minute running mean of Volume Weighted Average Price (`vwap`) and the 20-minute volatility, the Volume Weighted Standard Deviation (`wvstd`) on the price time series. This creates two bands around the mean price, with most price movements happening between the bands. Intuitively, when a price approaches the upper band, it is abnormally high and may likely drop - it is a good moment to _SELL_ and we'll raise an alert. Likewise, when the price approaches the lower band, it indicates it is low and may grow to revert to the mean - we'll raise an alert that it's a good moment to _BUY_. For further reliability, the BUY/SELL actions are performed only when there is a significant volume of trades, indicating that the outlying price is not a one-off event.
# %% [markdown]
# ## Imports and setup
#
# First, import the necessary libraries:
#
# - [`pathway`](/developers/user-guide/introduction/welcome/) for data processing,
# - `datetime` for date manipulation,
# - `panel` and `bokeh` for dashboarding.

# %%
import datetime

import bokeh.models
import bokeh.plotting
import panel

import pathway as pw

# %% [markdown]
# And then fetch the sample data from Github. This data was generated using [polygon.io](https://polygon.io).

# %%
%%capture --no-display
!wget -nc https://gist.githubusercontent.com/janchorowski/e351af72ecd8d206a34763a428826ab7/raw/ticker.csv

# %% [markdown]
# ## Data source setup
#
# Create a streaming data source that replays data in a CSV file. This is an easy way to simulate a live data stream without any infrastructure hassle. You can of course use Pathway with a real, production-grade data stream, for example [from Kafka](/developers/user-guide/deployment/from-jupyter-to-deploy#kafka-integration) or [Redpanda](/developers/user-guide/connect/connectors/switching-to-redpanda/).
#
# The `input_rate` parameter controls how fast the data is replayed.
#
# 💡 No data processing actually happens when you run this cell. We are building a computational graph that will only be executed at the end of the notebook. This allows Pathway to optimise the computations and perform them as fast as possible when the data starts streaming.

# %%
fname = "ticker.csv"
schema = pw.schema_from_csv(fname)
data = pw.demo.replay_csv(fname, schema=schema, input_rate=1000)

# # For static data exploration use
# data = pw.io.csv.read(fname, schema=schema, mode="static")

# Parse the timestamps
data = data.with_columns(t=data.t.dt.utc_from_timestamp(unit="ms"))

# %% [markdown]
# ## Switching between static and stream processing
#
# As you probably noticed, the code block above also includes a commented-out section. You can use this line instead of `data = pw.demo.replay_csv(...)` to test the workflow with static data. This is the only change you need to make in your code to switch between static and stream processing. Learn more about Pathway's unified engine and single syntax in [our User Guide](/developers/user-guide/connect/switch-from-batch-to-streaming#switching-from-batch-to-streaming).
#
# ## 20-minute rolling statistics
#
# Now it's time to build your trading algorithm. There is no need to fully understand the terminology or the math here. What's most important to grasp is how you are taking a stream of data and performing a windowing transformation to get more analytical value out of the raw data.
#
# Start by creating the first of our two Bollinger Bands: the 20-minute volatility measured as the Volume Weighted Standard Deviation. Use a [`sliding window`](/developers/user-guide/temporal-data/windows-manual#temporal-sliding-windowing) to compute at every minute the volume weighted price mean and standard deviation aggregate on the past 20 minutes of data. The `behavior` option tells Pathway that the window should emit the statistics only when it is finished - we do not want to see incomplete results.
#
# To compute the standard deviation, use the identity:
#
# $$
# \sigma(X) = \sqrt{\operatorname E\left[(X - \operatorname E[X])^2\right]} = \sqrt{\operatorname E\left[X^2\right] - (\operatorname E[X])^2},
# $$
#
# which is easily expressible using [Pathway reducers](/developers/api-docs/reducers/): we first compute the total $\mathrm{volume}$, $\mathrm{price}$, and $\mathrm{price}^2$. We then postprocess them to obtain the mean ($\mathrm{vwap}$), standard deviation ($\mathrm{vwstd}$), and Bollinger Bands places at $\mathrm{vwap} \pm 2\cdot \mathrm{vwstd}$.
#
# <b>Or in simpler terms: the code block below takes your incoming data stream and calculates important statistics in real-time. These statistics are continually updated as the data comes in so that you can identify critical moments as they happen.</b>

# %%
minute_20_stats = (
    data.windowby(
        pw.this.t,
        window=pw.temporal.sliding(
            hop=datetime.timedelta(minutes=1), duration=datetime.timedelta(minutes=20)
        ),
        behavior=pw.temporal.exactly_once_behavior(),
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

# %% [markdown]
# ## 1-minute rolling statistics
#
# Now it's time to compute the second Bollinger Bands: the 1-minute running mean. You will need to compute the mean price over the last minute of trades.
#
# The code is analogous to the 20-minute statistics but simpler: you can use a [`tumbling window`](/glossary/tumbling-window) and don't have to compute the standard deviation.

# %%
minute_1_stats = (
    data.windowby(
        pw.this.t,
        window=pw.temporal.tumbling(datetime.timedelta(minutes=1)),
        behavior=pw.temporal.exactly_once_behavior(),
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

# %% [markdown]
# ## Joining the statistics
#
# Now join the 20-minute and 1-minute statistics, gathering all the information needed for alerting in one place. Alert triggering is now a breeze.

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
        is_alert=(pw.this.volume > 10000)
        & (
            (pw.this.vwap > pw.this.bollinger_upper)
            | (pw.this.vwap < pw.this.bollinger_lower)
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
alerts = joint_stats.filter(pw.this.is_alert)


# %% [markdown]
# ## Dashboard creation
#
# Now create a `Bokeh` plot and `Panel` table visualization: the plot shows the Bollinger Bands along with the price running mean and indicates the price of buy and sell decisions. The table gathers all the decisions conveniently for further processing, such as reducing it to compute a historical evaluation of the gains of the strategy.
#
# When the cell is executed, placeholder containers are created for the plot and table visualization. They will be populated with live data when the computation is started (i.e. when running the final `pw.run()` at the end of this notebook).


# %%
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
        y_range=(188.5, 191),
    )
    fig.line("t", "vwap", source=src)
    band = bokeh.models.Band(
        base="t",
        lower="bollinger_lower",
        upper="bollinger_upper",
        source=src,
        fill_alpha=0.3,
        fill_color="gray",
        line_color="black",
    )

    fig.scatter(
        "t",
        "vwap",
        color={"field": "action", "transform": color_map},
        size=10,
        marker="circle",
        source=src,
    )

    fig.add_layout(band)
    return fig


viz = panel.Row(
    joint_stats.plot(stats_plotter, sorting_col="t"),
    alerts.select(pw.this.ticker, pw.this.t, pw.this.vwap, pw.this.action).show(
        include_id=False, sorters=[{"field": "t", "dir": "desc"}]
    ),
)
# _MD_SHOW_viz

# %% [markdown]
# ## Running the computation
#
# All the hard work is done! The final step is to start the Pathway data processing engine using the `pw.run()` command:
#
# %%
%%capture --no-display
pw.run()
# %% [markdown]
# Watch how the dashboard is updated in realtime! The basic Bollinger Bands action trigger seems to be working - the green buy decision markers are frequently followed by the red sell markers at a slightly higher price.
#
# While the computation is running, `pathway` prints important statistics such as message processing latency.
#
# Successful evaluation of the code should result in the animation:
# ![image](https://github.com/pathwaycom/pathway-examples/blob/c20cd69a6c9c87fc70a9082de57666c50f2ab3c2/documentation/from_jupyter_to_deploy/jupyter-demo-final-smallest-compressed.gif?raw=true)

# %%
# _MD_COMMENT_START_
# The code below is managed using the _posthook.sh please change/delete it accordingly
# Do not change the `# Due to ..` line!!!!
# _MD_COMMENT_END_
# Due to the bug described in https://github.com/jupyter/notebook/issues/1622
# Please execute `pw.run()` manually, after the widget above is initialized and visible.

# pw.run()

# %% [markdown]
# ## Jupyter Notebooks & Streaming Data in Production
#
# Congratulations! You have succesfully built a live data streaming pipeline with useful data visualisations and real-time alerts, right from a Jupyter notebook 😄
#
# This is just a taste of what is possible. If you're interested in diving deeper and building a production-grade data science pipeline all the way from data exploration to deployment, you may want to check out the full-length [From Jupyter to Deploy](/developers/user-guide/deployment/from-jupyter-to-deploy/) tutorial.
#
# ## What else can you do with Pathway?
#
# * Perform machine learning in real time. e.g. [ real-time
# Classification](/developers/templates/etl/lsh/lsh_chapter1/) , [real-time fuzzy joins](/developers/templates/etl/fuzzy_join/fuzzy_join_chapter2/)
#
# * Transform unstructured data to structured data using [live LLM pipelines](https://github.com/pathwaycom/llm-app)
#
# * Making [joins](/developers/user-guide/temporal-data/interval-join) simple with timeseries data
#
# And so much more... Read more about what we can do in the [developer docs](/developers/user-guide/introduction/welcome/).
#
# We would love to have you try out [Pathway on GitHub](https://github.com/pathwaycom/pathway).
