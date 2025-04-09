# ---
# title: "Sensor Fusion in real-time: combining time series data with Pathway"
# description: "Learn how to combine between two time series with different timestamps in Pathway."
# aside: true
# article:
#   date: '2023-04-28'
#   thumbnail: '/assets/content/tutorials/time_series/thumbnail-time-series.png'
#   tags: ['Time Series']
# keywords: ['time series', 'multiple data sources', 'interpolation', 'connectors', 'notebook']
# author: 'olivier'
# notebook_export_path: notebooks/tutorials/combining-time-series.ipynb
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
# # Sensor Fusion in real-time: combining time series data with Pathway
# In this article, you will learn how to combine time series in Pathway.
#
# With the emergence of IoT technology, we now have a wide range of sensor devices at our disposal that can measure almost anything, from GPS positions to humidity and temperature.
# Since each measurement is timestamped, this generated data are time series: the data can be analyzed and modeled as a sequence of values that change over time.
#
# ![Example of combination of two time series](/assets/content/tutorials/time_series/time_series_join_process.svg)
#
# While combining time series data can be challenging due to the lack of matching timestamps between different datasets, it is a crucial operation.
# Combining values from multiple sensors measuring the same metric can improve measurement accuracy.
# Also, combining various metrics on a common time index allows in-depth analysis and modeling.
#
# ::article-img
# ---
# src: '/assets/content/tutorials/time_series/time_series_1.svg'
# alt: 'Example of two time series with different timestamps'
# width: '500px'
# class: 'mx-auto'
# ---
# ::
#
# In this article, we will explore how to combine time series data using Pathway by calculating the average speed of a fleet of cars.
# So, fasten your seatbelt and get ready to explore the world of time series!
#
# ## Time series
#
# A time series is a type of data that records observations or measurements over time. It can be anything from stock prices, weather data, or the number of customers visiting a store to more complex measures like physiological signals from medical devices.
# Consider a time series as a sequence of data points collected at regular or irregular intervals over time. Each data point in a time series represents a measurement or observation made at a specific point in time.
#
# ::inline
#
# ::article-img
# ---
# src: '/assets/content/tutorials/time_series/single_time_series.svg'
# alt: 'Example of a time series represented as an event stream'
# width: '75px'
# ---
# ::
#
# ::article-img
# ---
# src: '/assets/content/tutorials/time_series/time_series_graph.svg'
# alt: 'Example of a time series represented as a graph'
# width: '350px'
# ---
# ::
#
# ::article-img
# ---
# src: '/assets/content/tutorials/time_series/time_series_table-cropped.svg'
# alt: 'Example of a time series represented as a table'
# width: '150px'
# ---
# ::
#
# ::
#
# <!---
# ![Example of a time series represented as an event stream](/assets/content/tutorials/time_series/single_time_series.svg)
# ![Example of a time series represented as a graph](/assets/content/tutorials/time_series/time_series_graph.svg)
# ![Example of a time series represented as a table](/assets/content/tutorials/time_series/time_series_table-cropped.svg)
# -->
#
# The concept of time series is critical in many real-world applications, from finance and economics to healthcare and meteorology. Analyzing time series data allows us to gain insights into patterns, trends, and relationships between variables over time. For example, we can use time series data to forecast future values, identify anomalies, or monitor changes in a system.
# Time series data can be represented in various formats, from simple spreadsheets to complex databases. However, analyzing and modeling time series data can be challenging due to its complex nature and different types of noise and anomalies.
#
#
# ### Combining time series
#
# Combining time series is the process of combining different time series into a single time series based on a common timestamp or index. In other words, combining time series consists in merging data from various sources into a comprehensive time series, allowing for deeper analysis and modeling.
#
# ![Example of combination of two time series](/assets/content/tutorials/time_series/time_series_join_process.svg)
#
# Combining time series are essential for several reasons. Firstly, it can improve the accuracy of the measurements by combining the values of several sensors measuring the same metric. For example, imagine you have multiple sensors measuring the temperature of a room. By combining the time series data from these sensors, you can get a more accurate representation of the temperature in the room.
#
# Secondly, by combining and analyzing various time series data streams, valuable insights can be derived across different domains, enabling performance optimization, predictive maintenance, resource management, and strategic decision-making:
#  - Website analytics 💻: Combining time series data on website traffic, user engagement, conversion rates, and marketing campaign metrics can provide insights into user behavior, measure the effectiveness of marketing efforts, and optimize website performance.
#  - Health monitoring 🩺: Combining time series data from wearable devices, such as heart rate, sleep patterns, and physical activity, can help track and analyze individuals' health and wellness trends, enabling personalized healthcare interventions.
#  - Environmental monitoring 🌡️: Combining time series data from air quality sensors, weather stations, and pollutant levels can provide a comprehensive understanding of the environment's condition, aid in pollution control efforts, and support urban planning initiatives.
#  - Supply chain management 📦: Combining time series data on inventory levels, production rates, transportation delays, and customer demand can optimize inventory management, improve delivery schedules, and enhance overall supply chain efficiency.
#  - Analyzing stock market data 📈: Combining time series of stock prices, trading volumes, and financial indicators can provide a comprehensive view of the market's behavior and aid in identifying trends, correlations, and investment opportunities.
#  - Technical analysis and development 🏎️: Combining time series data on car sensors readings, engine performance, aerodynamic data, and telemetry can aid in the technical analysis and development of Formula 1 cars, leading to improvements in speed, reliability, and overall performance.
#
# Combining time series is an essential operation in time series analysis, but it can be challenging due to the need for matching timestamps between different datasets. Nonetheless, various techniques and tools can help us merge time series data effectively, such as interpolation or merging on the closest timestamp.
# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## The average speed of a taxi and its passenger
# Imagine you are working for a taxi company, and people are using your app to book a ride.
# You can access the GPS traces of the taxi and the passengers.
# By using those two traces jointly, you can better approximate the average speed of the ride.
# This allows you to identify potential bottlenecks or inefficiencies in the system and take corrective measures to improve traffic flow and reduce travel time.
#
# %% [markdown]
# ## Creating the dataset
#
# Unfortunately, it is hard to find publicly available datasets with the characteristics we are interested in.
# Existing publicly available GPS datasets contain individual taxi traces without the passengers' GPS traces.
# You will generate our pairs of traces from one of such individual traces.
#
# ### The dataset: GeoLife
# You will use one trace of the [GeoLife dataset](https://www.microsoft.com/en-us/research/publication/geolife-gps-trajectory-dataset-user-guide/) from Microsoft Research.
# GeoLife is a GPS trajectory dataset containing 17,621 trajectories obtained using different GPS loggers and phones.
#
# ### Generation of a fleet of two cars
# Let's generate two traces by splitting the trace: each GPS data point is alternatively assigned to one of the new traces.
# This ensures that the two traces have the same trajectory while having different timestamps.
#
# You can download the trace of this example by uncommenting and executing the following command:

# %%
# %%capture --no-display
# # !wget https://public-pathway-releases.s3.eu-central-1.amazonaws.com/data/time-series-raw-trace.plt -O raw-trace.plt

# %% [markdown]
# To split the trace into two different traces, we simply read the file line by line and write each line into new CSV files:

# %%
input_trace = "./raw-trace.plt"
trace_taxi = "./trace_taxi.csv"
trace_passenger = "./trace_passenger.csv"
header_row = "lat,lng,const,alt,elapsed-time,date,time\n"

with open(input_trace, "r") as file:
    line = file.readline()
    for _ in range(5):
        line = file.readline()

    trace1 = open(trace_taxi, "w+")
    trace2 = open(trace_passenger, "w+")
    trace1.write(header_row)
    trace2.write(header_row)

    dataset = 1

    while line:
        line = file.readline()
        if dataset:
            trace1.write(line)
            dataset = 0
        else:
            trace2.write(line)
            dataset = 1
    trace1.close()
    trace2.close()

# %% [markdown]
# ## Loading the data sources in Pathway
#
# To connect Pathway to the two data sources, you have to use Pathway's input connectors.
# You do not need all the provided data; you can keep only the interesting ones, namely the latitude, longitude, altitude, date, and time:

# %%
import pathway as pw

columns = ["lat", "lng", "alt", "date", "time"]


class InputSchema(pw.Schema):
    lat: float
    lng: float
    alt: float
    date: str
    time: str


t1 = pw.io.csv.read(trace_taxi, schema=InputSchema, mode="static")
t2 = pw.io.csv.read(trace_passenger, schema=InputSchema, mode="static")

# %% [markdown]
# Note that you use the static mode in this example, but in practice, you should use the streaming mode.
# You can also [generate a datastream from a static file](/developers/user-guide/connect/connectors/custom-python-connectors).

# %% [markdown]
# ### Formatting the data
# After loading the raw data, it's important to format it properly to ensure it can be processed efficiently.
#
# First, the time is currently represented by two strings, one for the date and one for the time.
# Having a single timestamp can simplify the data processing task.
# You can use `dateutil` and `datetime` to parse them into a timestamp.

# %%
from datetime import datetime, time

from dateutil import parser, tz

default_date = datetime.combine(
    datetime.now(), time(0, tzinfo=tz.gettz("America/New_York"))
)


def convert_to_timestamp(date, time) -> int:
    datestring = date + " " + time
    yourdate = parser.parse(datestring, default=default_date)
    return int(datetime.timestamp(yourdate))


def format_table(t):
    return t.select(
        *pw.this.without(pw.this.date, pw.this.time),
        timestamp=pw.apply(convert_to_timestamp, pw.this.date, pw.this.time),
    )


t1_timestamp = format_table(t1)
t2_timestamp = format_table(t2)


# %% [markdown]
# Now your time series are ready to be combined and processed!

# %% [markdown]
# ### Obtaining a preview of the data
#
# Pathway is a framework capable of handling both static and streaming data.
# However, it is primarily designed to handle streaming data and enable real-time data processing.
#
# To ensure that the data is being processed correctly, you can define a function `preview_table` to preview the data.
# ⚠️ It's important to note that **this function should not be used in a production environment** since it relies on `pw.debug.compute_and_print` and static data.
# Its primary purpose is to check that the data is being processed correctly and to help with the development and testing phase of the data processing pipeline.
# You should use [Pathway's output connectors](/developers/user-guide/connect/pathway-connectors/) to access the data in streaming mode.
#
# In this case, you can filter all the entries with a timestamp higher than a given value to only display a small portion of the dataset.


# %%
def preview_table(table, max_timestamp=1224744825):
    table = table.filter(pw.this.timestamp < max_timestamp)
    pw.debug.compute_and_print(table)


preview_table(t1_timestamp)
preview_table(t2_timestamp)

# %% [markdown]
# The value 1224744825 is hand-picked to fit this dataset; you can replace it by any desired value.

# %% [markdown]
# ## Combining the time series and computing the average speed
# To compute the average speed of the traces, you will proceed as follows:
# 1. Concatenate the time series to obtain a table with the timestamps of both traces.
# 2. Add the existing positions: each timestamp should have a position and a missing position.
# 3. Do an interpolation to estimate the missing values.
# 4. Compute the average speed
#
# ### 1. Concatenation
#
# The first step to combine the two time series is to concatenate them: you want a table with all the timestamps.
#
# ![Concatenating the timestamps of two time series](/assets/content/tutorials/time_series/concatenating_timestamps.svg)
#
# You can do it easily in Pathway with `pw.Table.concat_reindex`:

# %%
merged_timestamps = pw.Table.concat_reindex(
    t1_timestamp[["timestamp"]], t2_timestamp[["timestamp"]]
)

# %%
preview_table(merged_timestamps)


# %% [markdown]
# ### 2. Adding existing positions
# Now that you have a table with the timestamps, you must add the positions.
# You can add the positions by doing a left join on the timestamp table.
#
# ![Time series with missing values](/assets/content/tutorials/time_series/time_series_missing_values.svg)
#
#
# You need to rename the columns `lat`, `lng`, and `alt` to `lat_1`, `lng_1`, `alt_1`, `lat_2`, `lng_2`, and `alt_2` to make the distinction between the two data sources.

# %%
joined_table = (
    merged_timestamps.join_left(t1_timestamp, pw.left.timestamp == pw.right.timestamp)
    .select(
        *pw.left,
        **pw.right[["lat", "lng", "alt"]].with_suffix("_1"),
    )
    .join_left(t2_timestamp, pw.left.timestamp == pw.right.timestamp)
    .select(
        *pw.left,
        **pw.right[["lat", "lng", "alt"]].with_suffix("_2"),
    )
)
preview_table(joined_table)

# %% [markdown]
# Your table now has all the existing positions, but the columns with the positions are half-empty.
# To fill the missing positions, you must compute an interpolation between the previous and last positions.

# %% [markdown]
# ### 3. Interpolating the positions
# Now, you will fill the missing values by interpolating with the previous and next values you found.
#
# ![Filling the missing values](/assets/content/tutorials/time_series/filling_missing_values.svg)
#

# %% [markdown]
# You need to do a linear interpolation on each column, using the column timestamp as index.
# This can be done using Pathway's `interpolate` function:

# %%
interpolated_table = joined_table.interpolate(
    pw.this.timestamp,
    pw.this.lat_1,
    pw.this.lng_1,
    pw.this.alt_1,
    pw.this.lat_2,
    pw.this.lng_2,
    pw.this.alt_2,
)

# %%
preview_table(interpolated_table)

# %% [markdown]
# And voila! You have successfully combined two time series! 🎉
#
# All you need to do it to compute the average speed now.

# %% [markdown]
# ### 4. Computing the average speed
#
# To compute the average speed, you will first calculate the speed by dividing the traveled distance by the time spent between the next and previous time.
#
# To simplify the computation of the speed, you can group the different values into two columns `position_1` and `positions_2`:

# %%
interpolated_table = interpolated_table.select(
    pw.this.timestamp,
    interpolated_position_1=pw.make_tuple(pw.this.lat_1, pw.this.lng_1, pw.this.alt_1),
    interpolated_position_2=pw.make_tuple(pw.this.lat_2, pw.this.lng_2, pw.this.alt_2),
)
preview_table(interpolated_table)

# %% [markdown]
# To compute the distance, you can use the `pyproj` package, which you can install by uncommenting and executing the following command:

# %%
# %%capture --no-display
# # !pip install pyproj

# %%
from pyproj import Geod

g = Geod(ellps="WGS84")


def compute_speed(t_prev, position_prev, t_next, position_next):
    try:
        _, _, distance_2d = g.inv(
            position_prev[1], position_prev[0], position_next[1], position_next[0]
        )
    except:
        return 0.0
    return float(distance_2d / (t_next - t_prev))


# %% [markdown]
# Note this is the simple 2-dimensional distance, but you can use your favorite distance metric.

# %% [markdown]
# You need to order the table to obtain the previous and next values.
# This can be done with the `sort` function which provides `prev` and `next` columns, containing pointers to the previous and next rows:

# %%
interpolated_table += interpolated_table.sort(key=pw.this.timestamp)
preview_table(interpolated_table)


# %% [markdown]
# Then, you can use `ix` method to fetch values from corresponding `prev` and `next` rows, and use them to compute the instant speed on all the points for both sources:

# %%
interpolated_table_prev = interpolated_table.ix(pw.this.prev, optional=True)
interpolated_table_next = interpolated_table.ix(pw.this.next, optional=True)

table_speed = interpolated_table.select(
    pw.this.timestamp,
    speed_1=compute_speed(
        pw.coalesce(interpolated_table_prev.timestamp, pw.this.timestamp),
        pw.coalesce(
            interpolated_table_prev.interpolated_position_1,
            pw.this.interpolated_position_1,
        ),
        pw.coalesce(interpolated_table_next.timestamp, pw.this.timestamp),
        pw.coalesce(
            interpolated_table_next.interpolated_position_1,
            pw.this.interpolated_position_1,
        ),
    ),
    speed_2=compute_speed(
        pw.coalesce(interpolated_table_prev.timestamp, pw.this.timestamp),
        pw.coalesce(
            interpolated_table_prev.interpolated_position_2,
            pw.this.interpolated_position_2,
        ),
        pw.coalesce(interpolated_table_next.timestamp, pw.this.timestamp),
        pw.coalesce(
            interpolated_table_next.interpolated_position_2,
            pw.this.interpolated_position_2,
        ),
    ),
)

preview_table(table_speed)

# %% [markdown]
# Finally, you can compute the average of the two speeds at each timestamp:

# %%
average_speed_table = table_speed.select(
    pw.this.timestamp,
    speed=(pw.this.speed_1 + pw.this.speed_2) / 2.0,
)
preview_table(average_speed_table)

# %% [markdown]
# You have now the speed for all the timestamps.
#
# Although initially designed for static GPS traces, this configuration can easily be adapted for a [streaming setup](/developers/user-guide/introduction/streaming-and-static-modes/) by modifying the [connectors](/developers/user-guide/connect/pathway-connectors/). In streaming mode, all calculations will be automatically refreshed whenever a new GPS position is received.

# %% [markdown]
# ## Conclusions
# Congratulations, you now have the skills to combine time series and unlock new insights in your data!
#
# This article focused on combining two GPS traces to compute the average speed.
# However, the principles we discussed can be applied to a wide range of time series data, from stock prices to weather data to sensor readings.
#
# By combining and analyzing time series data, you can uncover patterns, trends, and relationships that may not be immediately apparent from individual series.
# This can be especially useful in complex modeling scenarios that involve multiple data sources.
#
# For instance, imagine you want to predict weather patterns using a combination of humidity, temperature, and other metrics.
# By combining these time series, you could create a more comprehensive picture of the conditions affecting your area's weather patterns.
#
# The possibilities for time series analysis are endless, and if you're interested in learning more, be sure to check out [our article on log monitoring](/developers/templates/etl/realtime-log-monitoring).
# In it, we explore how to use time series analysis to detect anomalies in your web server's logs.
