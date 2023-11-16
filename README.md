<div align="center">
  <img src="https://pathway.com/logo-light.svg" /><br /><br />
</div>
<p align="center">
        <img src="https://img.shields.io/badge/OS-Linux-green" alt="Linux"/>
        <img src="https://img.shields.io/badge/OS-macOS-green" alt="macOS"/>
    <a href="https://github.com/pathwaycom/pathway/blob/main/LICENSE.txt">
        <img src="https://img.shields.io/badge/license-BSL-green" alt="License: BSL"/></a>
                     <a href="https://github.com/pathwaycom/pathway/graphs/contributors">
      <br>
    <a href="https://discord.gg/pathway">
        <img src="https://img.shields.io/discord/1042405378304004156?logo=discord"
            alt="chat on Discord"></a>
    <a href="https://twitter.com/intent/follow?screen_name=pathway_com">
        <img src="https://img.shields.io/twitter/follow/pathway_com?style=social&logo=twitter"
            alt="follow on Twitter"></a>
  <a href="https://linkedin.com/company/pathway">
        <img src="https://img.shields.io/badge/pathway-0077B5?style=social&logo=linkedin" alt="follow on LinkedIn"></a>
      <br>
    <a href="#getting-started">Getting Started</a> |
    <a href="#example">Example</a> |
    <a href="#performance">Performance</a> |
    <a href="#deployment">Deployment</a> |
    <a href="#resources">Resources</a> |
    <a href="https://pathway.com/developers/">Documentation</a> |
    <a href="https://pathway.com/blog/">Blog</a> |
    <a href="#get-help">Get Help</a>

  
</p>



# Pathway<a id="pathway"></a>

[Pathway](https://pathway.com) is an open framework for high-throughput and low-latency real-time data processing. It is used to create Python code which seamlessly combines batch processing, streaming, and real-time API's for LLM apps. Pathway's distributed runtime (🦀-🐍) provides fresh results of your data pipelines whenever new inputs and requests are received.

![Screencast animation of converting batch code to streaming by changing one keyword argument in the script.](https://github.com/pathwaycom/pathway/assets/68642378/79f4250d-0641-4b97-87f8-0820d9399c6b)

Pathway provides a high-level programming interface in Python for defining data transformations, aggregations, and other operations on data streams.
With Pathway, you can effortlessly design and deploy sophisticated data workflows that efficiently handle high volumes of data in real time.

Pathway is interoperable with various data sources and sinks such as Kafka, CSV files, SQL/noSQL databases, and REST API's, allowing you to connect and process data from different storage systems.

Typical use-cases of Pathway include realtime data processing, ETL (Extract, Transform, Load) pipelines, data analytics, monitoring, anomaly detection, and recommendation. Pathway can also independently provide the backbone of a light LLMops stack for [real-time LLM applications](https://github.com/pathwaycom/llm-app).

In Pathway, data is represented in the form of Tables. Live data streams are also treated as Tables. The library provides a rich set of operations like filtering, joining, grouping, and windowing.

For any questions, you will find the community and team behind the project [on Discord](https://discord.com/invite/pathway).

## Getting started<a id="getting-started"></a>


### Installation<a id="installation"></a>

Pathway requires Python 3.10 or above.

You can install the current release of Pathway using `pip`:

```
$ pip install -U pathway
```

⚠️ Pathway is available on MacOS and Linux. Users of other systems should run Pathway on a Virtual Machine.

### Running Pathway locally<a id="running-pathway-locally"></a>

To use Pathway, you only need to import it:

```python
import pathway as pw
```

Now, you can easily create your processing pipeline, and let Pathway handle the updates. Once your pipeline is created, you can launch the computation on streaming data with a one-line command:

```python
pw.run()
```

You can then run your Pathway project (say, `main.py`) just like a normal Python script: `$ python main.py`. Alternatively, use the pathway'ish version:

```
$ pathway spawn python main.py
```

Pathway natively supports multithreading.
To launch your application with 3 threads, you can do as follows:
```
$ pathway spawn --threads 3 python main.py
```

To jumpstart a Pathway project, you can use our [cookiecutter template](https://github.com/pathwaycom/cookiecutter-pathway).


### Example<a id="example"></a>

```python
import pathway as pw

# Using the `demo` module to create a data stream
table = pw.demo.range_stream(nb_rows=50)
# Storing the stream into a CSV file
pw.io.csv.write(table, "output_table.csv")

# Summing all the values in a new table
sum_table = table.reduce(sum=pw.reducers.sum(pw.this.value))
# Storing the sum (which is a stream) in another CSV file
pw.io.csv.write(sum_table, "sum_table.csv")

# Now that the pipeline is built, the computation is started
pw.run()
```

Run this example [in Google Colab](https://colab.research.google.com/drive/1kLx5-vKKg0IeQ88ydS-ehtrxSujEZrXK?usp=sharing)!

## Deployment<a id="deployment"></a>

Do you feel limited by a local run?
If you want to scale your Pathway application, you may be interested in our Pathway for Enterprise.
Pathway for Enterprise is specially tailored towards end-to-end data processing and real time intelligent analytics.
It scales using distributed computing on the cloud and supports Kubernetes deployment.

You can learn more about the features of Pathway for Enterprise on our [website](https://pathway.com/features).

If you are interested, don't hesitate to [contact us](mailto:contact@pathway.com) to learn more.

## Monitoring Pathway<a id="monitoring-pathway"></a>

Pathway comes with a monitoring dashboard that allows you to keep track of the number of messages sent by each connector and the latency of the system. The dashboard also includes log messages. 

This dashboard is enabled by default; you can disable it by passing `monitoring_level = pathway.MonitoringLevel.NONE` to `pathway.run()`.

<img src="https://d14l3brkh44201.cloudfront.net/pathway-dashboard.png" width="1326" alt="Pathway dashboard"/>

In addition to Pathway's built-in dashboard, you can [use Prometheus](https://pathway.com/developers/tutorials/prometheus-monitoring) to monitor your Pathway application.

## Resources<a id="resources"></a>

See also: **📖 [Pathway Documentation](https://pathway.com/developers/)** webpage (including API Docs).

### Videos about Pathway<a id="videos-about-pathway"></a>
[▶️ Building an LLM Application without a vector database](https://www.youtube.com/watch?v=kcrJSk00duw) - by [Jan Chorowski](https://scholar.google.com/citations?user=Yc94070AAAAJ) (7min 56s)

[▶️ Linear regression on a Kafka Stream](https://vimeo.com/805069039) - by [Richard Pelgrim](https://twitter.com/richardpelgrim) (7min 53s)

[▶️ Introduction to reactive data processing](https://pathway.com/developers/user-guide/introduction/welcome) - by [Adrian Kosowski](https://scholar.google.com/citations?user=om8De_0AAAAJ) (27min 54s)

### Guides<a id="guides"></a>
- [Core concepts of Pathway](https://pathway.com/developers/user-guide/introduction/key-concepts/)
- [Basic operations](https://pathway.com/developers/user-guide/introduction/survival-guide/)
- [Joins](https://pathway.com/developers/user-guide/table-operations/join-manual/)
- [Groupby](https://pathway.com/developers/user-guide/table-operations/groupby-reduce-manual/)
- [Windowby](https://pathway.com/developers/user-guide/table-operations/windowby-reduce-manual/)
- [Transformer classes](https://pathway.com/developers/user-guide/transformer-classes/transformer-intro/)
- [Input and output connectors](https://pathway.com/developers/user-guide/input-and-output-streams/connectors/)
- [Coming from pandas](https://pathway.com/developers/user-guide/migrate-from-pandas/)
- [API docs](https://pathway.com/developers/api-docs/pathway)
- [Troubleshooting](https://pathway.com/developers/user-guide/introduction/troubleshooting/)

### Tutorials<a id="tutorials"></a>
- [Linear regression on a Kafka Stream](https://pathway.com/developers/tutorials/linear_regression_with_kafka/) ([video](https://vimeo.com/805069039)) 
- Joins:
  - [Interval joins](https://pathway.com/developers/tutorials/fleet_eta_interval_join/)
  - [Window joins](https://pathway.com/developers/tutorials/clickstream-window-join/)
  - [ASOF joins](https://pathway.com/developers/tutorials/finance_ts_asof_join/)
- Connectors:
  - [CSV connectors](https://pathway.com/developers/tutorials/connectors/csv_connectors/)
  - [Database connectors](https://pathway.com/developers/tutorials/connectors/database-connectors/)
  - [Kafka connectors](https://pathway.com/developers/tutorials/connectors/kafka_connectors/)
  - [Custom Python connector](https://pathway.com/developers/tutorials/connectors/custom-python-connectors/)
  - [Switching from Kafka to Redpanda](https://pathway.com/developers/tutorials/connectors/switching-to-redpanda/)
- [Monitoring Pathway with Prometheus](https://pathway.com/developers/tutorials/prometheus-monitoring/)
- [Time between events in a multi-topic event stream](https://pathway.com/developers/tutorials/event_stream_processing_time_between_occurrences/)

### Showcases<a id="showcases"></a>
- [Realtime Twitter Analysis App](https://pathway.com/developers/showcases/twitter/)
- [Realtime classification with Nearest Neighbors](https://pathway.com/developers/showcases/lsh/lsh_chapter1/)
- [Realtime Fuzzy joins](https://pathway.com/developers/showcases/fuzzy_join/fuzzy_join_chapter1/)

### External and community content<a id="external-and-community-content"></a>
- [Real-time linear regression (Data Engineering Weekly)](https://pathway.com/developers/tutorials/unlocking-data-stream-processing-1/)
- [Realtime server logs monitoring (Data Engineering Weekly)](https://pathway.com/developers/tutorials/unlocking-data-stream-processing-2/)
- [Data enrichment with fuzzy joins (Data Engineering Weekly)](https://pathway.com/developers/tutorials/unlocking-data-stream-processing-3/)
- [▶️ How to do Realtime Twitter Sentiment Analysis in Python (video)](https://www.youtube.com/watch?v=V7T3xHfjE4o)

If you would like to share with us some Pathway-related content, please give an admin a shout on [Discord](https://discord.gg/pathway).

### Manul conventions<a id="manul-conventions"></a>

Manuls (aka Pallas's Cats) [are creatures with fascinating habits](https://www.youtube.com/watch?v=rlSTBvViflc). As a tribute to them, we usually read `pw`, one of the most frequent tokens in Pathway code, as: `"paw"`. 

<img src="https://d14l3brkh44201.cloudfront.net/PathwayManul.svg" alt="manul" width="50px"></img>

## Performance<a id="performance"></a>

Pathway is made to outperform state-of-the-art technologies designed for streaming and batch data processing tasks, including: Flink, Spark, and Kafka Streaming. It also makes it possible to implement a lot of algorithms/UDF's in streaming mode which are not readily supported by other streaming frameworks (especially: temporal joins, iterative graph algorithms, machine learning routines).

If you are curious, here are [some benchmarks to play with](https://github.com/pathwaycom/pathway-benchmarks). 

<img src="https://github.com/pathwaycom/pathway-benchmarks/raw/main/images/bm-wordcount-lineplot.png" width="1326" alt="WordCount Graph"/>

If you try your own benchmarks, please don't hesitate to let us know. We investigate situations in which Pathway is underperforming on par with bugs (i.e., to our knowledge, they shouldn't happen...).

## Coming soon<a id="coming-soon"></a>

Here are some features we plan to incorporate in the near future:

- Enhanced monitoring, observability, and data drift detection (integrates with Grafana visualization and other dashboarding tools).
- New connectors: interoperability with Delta Lake and Snowflake data sources.
- Easier connection setup for MongoDB.
- More performant garbage collection.



## Dependencies<a id="dependencies"></a>

Pathway is made to run in a "clean" Linux/MacOS + Python environment. When installing the pathway package with `pip` (from a wheel), you are likely to encounter a small number of Python package dependencies, such as sqlglot (used in the SQL API) and python-sat (useful for resolving dependencies during compilation). All necessary Rust crates are pre-built; the Rust compiler is not required to install Pathway, unless building from sources. A modified version of Timely/Differential Dataflow (which provides a dataflow assembly layer) is part of this repo. 

## License<a id="license"></a>

Pathway is distributed on a [BSL 1.1 License](https://github.com/pathwaycom/pathway/blob/main/LICENSE.txt) which allows for unlimited non-commercial use, as well as use of the Pathway package [for most commercial purposes](https://pathway.com/license/), free of charge. Code in this repository automatically converts to Open Source (Apache 2.0 License) after 4 years. Some [public repos](https://github.com/pathwaycom) which are complementary to this one (examples, libraries, connectors, etc.) are licensed as Open Source, under the MIT license.


## Contribution guidelines<a id="contribution-guidelines"></a>

If you develop a library or connector which you would like to integrate with this repo, we suggest releasing it first as a separate repo on a MIT/Apache 2.0 license. 

For all concerns regarding core Pathway functionalities, Issues are encouraged. For further information, don't hesitate to engage with Pathway's [Discord community](https://discord.gg/pathway).

## Get Help<a id="get-help"></a>

If you have any questions, issues, or just want to chat about Pathway, we're here to help! Feel free to:
- Check out the [documentation](https://pathway.com/developers/) for detailed information.
- [Open an issue on GitHub](https://github.com/pathwaycom/pathway/issues) if you encounter any bugs or have feature requests.
- Join us on [Discord](https://discord.com/invite/pathway) to connect with other users and get support.
- Reach out to us via email at [contact@pathway.com](mailto:contact@pathway.com).

 Our team is always happy to help you and ensure that you get the most out of Pathway.
If you would like to better understand how best to use Pathway in your project, please don't hesitate to reach out to us.