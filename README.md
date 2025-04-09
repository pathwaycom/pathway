<div align="center">
  <a href="https://pathway.com/">
    <img src="https://pathway.com/logo-light.svg"/>
  </a>
  <br /><br />
  <a href="https://trendshift.io/repositories/10388" target="_blank"><img src="https://trendshift.io/api/badge/repositories/10388" alt="pathwaycom%2Fpathway | Trendshift" style="width: 250px; height: 55px;" width="250" height="55"/></a>
  <br /><br />
</div>
<p align="center">
        <a href="https://github.com/pathwaycom/pathway/actions/workflows/ubuntu_test.yml">
        <img src="https://github.com/pathwaycom/pathway/actions/workflows/ubuntu_test.yml/badge.svg" alt="ubuntu"/>
        <br>
        <a href="https://github.com/pathwaycom/pathway/actions/workflows/release.yml">
        <img src="https://github.com/pathwaycom/pathway/actions/workflows/release.yml/badge.svg" alt="Last release"/></a>
        <a href="https://badge.fury.io/py/pathway"><img src="https://badge.fury.io/py/pathway.svg" alt="PyPI version" height="18"></a>
        <a href="https://badge.fury.io/py/pathway"><img src="https://static.pepy.tech/badge/pathway" alt="PyPI downloads" height="18"></a>
        <a href="https://github.com/pathwaycom/pathway/blob/main/LICENSE.txt">
        <img src="https://img.shields.io/badge/license-BSL-green" alt="License: BSL"/></a>
      <br>
        <a href="https://discord.gg/pathway">
        <img src="https://img.shields.io/discord/1042405378304004156?logo=discord"
            alt="chat on Discord"></a>
        <a href="https://twitter.com/intent/follow?screen_name=pathway_com">
        <img src="https://img.shields.io/twitter/follow/pathwaycom"
            alt="follow on Twitter"></a>
        <a href="https://linkedin.com/company/pathway">
        <img src="https://img.shields.io/badge/pathway-0077B5?style=social&logo=linkedin" alt="follow on LinkedIn"></a>
      <a href="https://github.com/dylanhogg/awesome-python/blob/main/README.md">
      <img src="https://awesome.re/badge.svg" alt="Awesome Python"></a>
      <a href="https://gurubase.io/g/pathway">
      <img src="https://img.shields.io/badge/Gurubase-Ask%20Pathway%20Guru-006BFF" alt="Pathway Guru"></a>
    <br>
    <a href="#getting-started">Getting Started</a> |
    <a href="#deployment">Deployment</a> |
    <a href="#resources">Documentation and Support</a> |
    <a href="https://pathway.com/blog/">Blog</a> |
    <a href="#license">License</a>

  
</p>

# Pathway<a id="pathway"> Live Data Framework</a>

[Pathway](https://pathway.com) is a Python ETL framework for stream processing, real-time analytics, LLM pipelines, and RAG.

Pathway comes with an **easy-to-use Python API**, allowing you to seamlessly integrate your favorite Python ML libraries.
Pathway code is versatile and robust: **you can use it in both development and production environments, handling both batch and streaming data effectively**.
The same code can be used for local development, CI/CD tests, running batch jobs, handling stream replays, and processing data streams.

Pathway is powered by a **scalable Rust engine** based on Differential Dataflow and performs incremental computation.
Your Pathway code, despite being written in Python, is run by the Rust engine, enabling multithreading, multiprocessing, and distributed computations.
All the pipeline is kept in memory and can be easily deployed with **Docker and Kubernetes**.

You can install Pathway with pip:
```
pip install -U pathway
```

For any questions, you will find the community and team behind the project [on Discord](https://discord.com/invite/pathway).

## Use-cases and templates

Ready to see what Pathway can do?

[Try one of our easy-to-run examples](https://pathway.com/developers/templates)!

Available in both notebook and docker formats, these ready-to-launch examples can be launched in just a few clicks. Pick one and start your hands-on experience with Pathway today!

### Event processing and real-time analytics pipelines
With its unified engine for batch and streaming and its full Python compatibility, Pathway makes data processing as easy as possible. It's the ideal solution for a wide range of data processing pipelines, including:

- [Showcase: Real-time ETL.](https://pathway.com/developers/templates/kafka-etl)
- [Showcase: Event-driven pipelines with alerting.](https://pathway.com/developers/templates/realtime-log-monitoring)
- [Showcase: Realtime analytics.](https://pathway.com/developers/templates/linear_regression_with_kafka/)
- [Docs: Switch from batch to streaming.](https://pathway.com/developers/user-guide/connecting-to-data/switch-from-batch-to-streaming)



### AI Pipelines

Pathway provides dedicated LLM tooling to build live LLM and RAG pipelines. Wrappers for most common LLM services and utilities are included, making working with LLMs and RAGs pipelines incredibly easy. Check out our [LLM xpack documentation](https://pathway.com/developers/user-guide/llm-xpack/overview).

Don't hesitate to try one of our runnable examples featuring LLM tooling.
You can find such examples [here](https://pathway.com/developers/user-guide/llm-xpack/llm-examples).

  - [Template: Unstructured data to SQL on-the-fly.](https://pathway.com/developers/templates/unstructured-to-structured/)
  - [Template: Private RAG with Ollama and Mistral AI](https://pathway.com/developers/templates/private-rag-ollama-mistral)
  - [Template: Adaptive RAG](https://pathway.com/developers/templates/adaptive-rag)
  - [Template: Multimodal RAG with gpt-4o](https://pathway.com/developers/templates/multimodal-rag)

## Features

- **A wide range of connectors**: Pathway comes with connectors that connect to external data sources such as Kafka, GDrive, PostgreSQL, or SharePoint. Its Airbyte connector allows you to connect to more than 300 different data sources. If the connector you want is not available, you can build your own custom connector using Pathway Python connector.
- **Stateless and stateful transformations**: Pathway supports stateful transformations such as joins, windowing, and sorting. It provides many transformations directly implemented in Rust. In addition to the provided transformation, you can use any Python function. You can implement your own or you can use any Python library to process your data.
- **Persistence**: Pathway provides persistence to save the state of the computation. This allows you to restart your pipeline after an update or a crash. Your pipelines are in good hands with Pathway!
- **Consistency**: Pathway handles the time for you, making sure all your computations are consistent. In particular, Pathway manages late and out-of-order points by updating its results whenever new (or late, in this case) data points come into the system. The free version of Pathway gives the "at least once" consistency while the enterprise version provides the "exactly once" consistency.
- **Scalable Rust engine**: with Pathway Rust engine, you are free from the usual limits imposed by Python. You can easily do multithreading, multiprocessing, and distributed computations.
- **LLM helpers**: Pathway provides an LLM extension with all the utilities to integrate LLMs with your data pipelines (LLM wrappers, parsers, embedders, splitters), including an in-memory real-time Vector Index, and integrations with LLamaIndex and LangChain. You can quickly build and deploy RAG applications with your live documents.


## Getting started<a id="getting-started"></a>

### Installation<a id="installation"></a>

Pathway requires Python 3.10 or above.

You can install the current release of Pathway using `pip`:

```
$ pip install -U pathway
```

⚠️ Pathway is available on MacOS and Linux. Users of other systems should run Pathway on a Virtual Machine.


### Example: computing the sum of positive values in real time.<a id="example"></a>

```python
import pathway as pw

# Define the schema of your data (Optional)
class InputSchema(pw.Schema):
  value: int

# Connect to your data using connectors
input_table = pw.io.csv.read(
  "./input/",
  schema=InputSchema
)

#Define your operations on the data
filtered_table = input_table.filter(input_table.value>=0)
result_table = filtered_table.reduce(
  sum_value = pw.reducers.sum(filtered_table.value)
)

# Load your results to external systems
pw.io.jsonlines.write(result_table, "output.jsonl")

# Run the computation
pw.run()
```

Run Pathway [in Google Colab](https://colab.research.google.com/drive/1aBIJ2HCng-YEUOMrr0qtj0NeZMEyRz55?usp=sharing).

You can find more examples [here](https://github.com/pathwaycom/pathway/tree/main/examples).


## Deployment<a id="deployment"></a>

### Locally<a id="running-pathway-locally"></a>

To use Pathway, you only need to import it:

```python
import pathway as pw
```

Now, you can easily create your processing pipeline, and let Pathway handle the updates. Once your pipeline is created, you can launch the computation on streaming data with a one-line command:

```python
pw.run()
```

You can then run your Pathway project (say, `main.py`) just like a normal Python script: `$ python main.py`.
Pathway comes with a monitoring dashboard that allows you to keep track of the number of messages sent by each connector and the latency of the system. The dashboard also includes log messages. 

<img src="https://d14l3brkh44201.cloudfront.net/pathway-dashboard.png" width="1326" alt="Pathway dashboard"/>

Alternatively, you can use the pathway'ish version:

```
$ pathway spawn python main.py
```

Pathway natively supports multithreading.
To launch your application with 3 threads, you can do as follows:
```
$ pathway spawn --threads 3 python main.py
```

To jumpstart a Pathway project, you can use our [cookiecutter template](https://github.com/pathwaycom/cookiecutter-pathway).


### Docker<a id="docker"></a>

You can easily run Pathway using docker.

#### Pathway image

You can use the [Pathway docker image](https://hub.docker.com/r/pathwaycom/pathway), using a Dockerfile:

```dockerfile
FROM pathwaycom/pathway:latest

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "./your-script.py" ]
```

You can then build and run the Docker image:

```console
docker build -t my-pathway-app .
docker run -it --rm --name my-pathway-app my-pathway-app
```

#### Run a single Python script

When dealing with single-file projects, creating a full-fledged `Dockerfile`
might seem unnecessary. In such scenarios, you can execute a
Python script directly using the Pathway Docker image. For example:

```console
docker run -it --rm --name my-pathway-app -v "$PWD":/app pathwaycom/pathway:latest python my-pathway-app.py
```

#### Python docker image

You can also use a standard Python image and install Pathway using pip with a Dockerfile:

```dockerfile
FROM --platform=linux/x86_64 python:3.10

RUN pip install -U pathway
COPY ./pathway-script.py pathway-script.py

CMD ["python", "-u", "pathway-script.py"]
```

### Kubernetes and cloud<a id="k8s"></a>

Docker containers are ideally suited for deployment on the cloud with Kubernetes.
If you want to scale your Pathway application, you may be interested in our Pathway for Enterprise.
Pathway for Enterprise is specially tailored towards end-to-end data processing and real time intelligent analytics.
It scales using distributed computing on the cloud and supports distributed Kubernetes deployment, with external persistence setup.

You can easily deploy Pathway using services like Render: see [how to deploy Pathway in a few clicks](https://pathway.com/developers/user-guide/deployment/render-deploy/).

If you are interested, don't hesitate to [contact us](mailto:contact@pathway.com) to learn more.

## Performance<a id="performance"></a>

Pathway is made to outperform state-of-the-art technologies designed for streaming and batch data processing tasks, including: Flink, Spark, and Kafka Streaming. It also makes it possible to implement a lot of algorithms/UDF's in streaming mode which are not readily supported by other streaming frameworks (especially: temporal joins, iterative graph algorithms, machine learning routines).

If you are curious, here are [some benchmarks to play with](https://github.com/pathwaycom/pathway-benchmarks).

<img src="https://github.com/pathwaycom/pathway-benchmarks/raw/main/images/bm-wordcount-lineplot.png" width="1326" alt="WordCount Graph"/>

## Documentation and Support<a id="resources"></a>

The entire documentation of Pathway is available at [pathway.com/developers/](https://pathway.com/developers/user-guide/introduction/welcome), including the [API Docs](https://pathway.com/developers/api-docs/pathway).

If you have any question, don't hesitate to [open an issue on GitHub](https://github.com/pathwaycom/pathway/issues), join us on [Discord](https://discord.com/invite/pathway), or send us an email at [contact@pathway.com](mailto:contact@pathway.com).

## License<a id="license"></a>

Pathway is distributed on a [BSL 1.1 License](https://github.com/pathwaycom/pathway/blob/main/LICENSE.txt) which allows for unlimited non-commercial use, as well as use of the Pathway package [for most commercial purposes](https://pathway.com/license/), free of charge. Code in this repository automatically converts to Open Source (Apache 2.0 License) after 4 years. Some [public repos](https://github.com/pathwaycom) which are complementary to this one (examples, libraries, connectors, etc.) are licensed as Open Source, under the MIT license.


## Contribution guidelines<a id="contribution-guidelines"></a>

If you develop a library or connector which you would like to integrate with this repo, we suggest releasing it first as a separate repo on a MIT/Apache 2.0 license. 

For all concerns regarding core Pathway functionalities, Issues are encouraged. For further information, don't hesitate to engage with Pathway's [Discord community](https://discord.gg/pathway).
