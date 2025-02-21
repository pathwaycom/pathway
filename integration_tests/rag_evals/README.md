# Pathway RAG evals

## Introduction
This folder contains the automated continuous integration tests for RAG evaluations.

Tests consist of two main categories, "retrieval metrics" and "RAGAS". App config, responses, and metrics are logged to the internal MLFlow server.

## Dataset

Currently, [CUAD](https://www.atticusprojectai.org/cuad) and synthetic data is used in the evals. See our blogpost to see how we created the synthetic data.

To view the triplets of file-question-response in CUAD dataset, visit [this sheet](https://docs.google.com/spreadsheets/d/1ZlpEyc61dV4UeRYrejwcCISBoh_zRUlQ9BEYZcdwTyU/edit?usp=sharing) and switch to tab `Eval Data`.

To use the dataset, download the sheet:
> File -> Download -> Tab Separated Values (.tsv)

Place it under the `dataset/` directory.

## Running locally

### Installation
Make sure that Pathway is installed, you can install with `pip install "pathway[all]"`.

Install the other requirements by running `pip install -r requirements.txt`

### App config
`app.yaml` is the config for the Pathway RAG app that will be running. 
You will want to modify `$sources` section and replace it with the connector(s) that has access to your files. 

Our connector config points to the Google Drive folder that has the selected CUAD dataset files.

The current config settings are generally sensible defaults. You may want to change the config and find the best settings that suits your RAG case.

### Environment

Create the `.env` file and put the OpenAI API key.
Set the `RUN_MODE=LOCAL` in the environment.
See the `example.env` file for reference.

### MLFlow

You need an MLFlow instance to be able to log the evaluation params, predictions and the metrics. You need to set up an MLFlow instance.
See: 
- https://mlflow.org/docs/latest/getting-started/intro-quickstart/index.html
- https://mlflow.org/docs/latest/getting-started/logging-first-model/step1-tracking-server.html

Then, set the `MLFLOW_URI` in the environment.


## Troubleshooting

> `App keeps giving the answer: "I don't know"`: This indicates that there is an issue with one of the ingesting modules. You should check the logs to determine the issue. If you don't see any file being parsed, check your connector configurations. Otherwise, the issue may be with the parser, splitter or the embedder. You may run the app as a standalone by `python debug_main.py`.

> `MLFlow: Request Entity Too Large for url`: Increase the "proxy-body-size" in the nginx settings. See [here](https://github.com/mlflow/mlflow/issues/10332).
