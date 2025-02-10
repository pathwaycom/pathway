import logging
import os
import shutil
from dataclasses import asdict, dataclass
from datetime import datetime

import mlflow
import pandas as pd
from ragas import EvaluationDataset

from .connector import RagConnector
from .dataset import DatasetUtils
from .eval_questions import eval_questions, question_mapper
from .evaluator import RAGEvaluator, compare_sim_with_date
from .logging_utils import get_run_params
from .ragas_utils import (
    create_ragas_dataset,
    ragas_dataset_to_eval,
    run_ragas_evaluations,
)
from .utils import save_pivot_table_as_confusion

MLFLOW_URI: str | None = os.environ.get("MLFLOW_URI")
assert MLFLOW_URI is not None, "`MLFLOW_URI` is not set in the environ."

mlflow.set_tracking_uri(MLFLOW_URI)  # setting to None doesn't raise exception

EXPERIMENT_NAME = "CI RAG Evals"
RUN_RAGAS_EVALS: bool = True

try:
    BRANCH_NAME = os.environ["BRANCH"]
except KeyError:
    try:
        import git

        BRANCH_NAME = git.Repo("../../../..").active_branch.name
    except Exception:
        BRANCH_NAME = ""


@dataclass
class NamedDataset:
    name: str
    dataset: EvaluationDataset
    file: str


def load_synthetic_tests(dataset_folder_path: str) -> list[NamedDataset]:
    dataset_paths = [
        os.path.join(dataset_folder_path, f)
        for f in os.listdir(dataset_folder_path)
        if f.endswith(".jsonl")
    ]

    dataset_ls = []
    for data_path in dataset_paths:
        logging.info(f"Loaded synthetic dataset: {data_path}")
        dataset = EvaluationDataset.from_jsonl(data_path)
        full_fname = data_path.split(os.path.sep)[-1]
        named_ds = NamedDataset(
            name=full_fname, dataset=dataset, file=full_fname.split("==")[0]
        )
        dataset_ls.append(named_ds)

    return dataset_ls


def run_ragas_evals(
    ragas_dataset: EvaluationDataset, subfolder_name: str, dataset_name: str
):
    CORRECTNESS_CUTOFF: float = 0.65  # TODO: adjust
    # ragas_dataset = create_ragas_dataset(evaluator.predicted_dataset)
    cleaned_dataset_name = dataset_name.split(".")[0]

    ragas_evals_dataset = run_ragas_evaluations(ragas_dataset)

    ragas_scores_df = pd.DataFrame(ragas_evals_dataset.scores)
    ragas_scores_df["answer_correctness_withcutoff"] = (
        ragas_scores_df["answer_correctness"] > CORRECTNESS_CUTOFF
    ).astype(float)

    ragas_scores: dict = ragas_scores_df.mean().to_dict()

    for metric_name, value in ragas_scores.items():
        mlflow.log_metric(f"Ragas-{cleaned_dataset_name}-{metric_name}", value=value)

    ragas_evaluation_df = ragas_evals_dataset.to_pandas()

    mlflow.log_table(
        ragas_evaluation_df,
        subfolder_name + f"/ragas_{dataset_name}_dataset.json",
    )
    # If this gets error: 'Request Entity Too Large for url'
    # increase nginx body size


def run_eval_experiment(
    experiment: str | None = None,
    base_url: str = "http://0.0.0.0:8000",
    dataset_path: str = "dataset/labeled.tsv",
    synthetic_dataset_path: str = "dataset/synthetic_tests/",
    config_file_path: str = "app.yaml",
    cleanup_dir_on_exit: bool = False,
) -> float:
    """Run & log eval results, return total accuracy."""

    if not experiment:
        experiment = datetime.now().strftime("%d-%m-%Y %H:%M")

    mlflow.set_experiment(experiment_name=EXPERIMENT_NAME)
    mlflow.start_run(
        run_name=experiment, description=os.environ.get("MLFLOW_DESCRIPTION", "")
    )
    mlflow.set_tag("Branch name", BRANCH_NAME)

    current_dir = os.path.dirname(__file__)

    subfolder_name = experiment.replace(":", "_")

    dataset_name = current_dir + "/" + subfolder_name  # : is forbidden in artifact name

    conn = RagConnector(base_url=base_url)

    df = pd.read_csv(dataset_path, sep="\t")

    question_mapping_dict = question_mapper  # new_questions_long
    os.makedirs(dataset_name, exist_ok=True)  # folder for results & logs

    for param_dict in get_run_params():
        mlflow.log_params(param_dict)

    mlflow.log_artifact(config_file_path)

    new_dataset: list[dict] = []

    # prep df
    for question in eval_questions:
        for index, row in df.iterrows():
            data_elem = {
                "file": row["file"],
                "question": question,
                "label": row[question],
            }

            new_dataset.append(data_elem)

    for d in new_dataset:
        d["reworded_question"] = question_mapping_dict[
            d["question"]
        ]  # map dataset questions to natural language format for gpt

    evaluator = RAGEvaluator(new_dataset, compare_sim_with_date, conn)

    evaluator.apredict_dataset()

    evaluator.save_predicted_dataset(dataset_name + "/predicted_dataset.json")

    retrieval_metrics = evaluator.calculate_retrieval_metrics()
    mlflow.log_metrics(retrieval_metrics)
    DatasetUtils.save_dataset(
        retrieval_metrics, dataset_name + "/retrieval_metrics.json"
    )

    df = pd.DataFrame(evaluator.predicted_dataset_as_dict_list)
    df = df.fillna("")  # Cyber holding effective start date is Nan, + some others

    df["sim"] = df.apply(
        lambda row: compare_sim_with_date(row["pred"], row["label"]),
        axis=1,
    )

    accuracy = df["sim"].mean()
    logging.info(f"Total accuracy: {accuracy}")
    mlflow.log_metric("Total RAG Accuracy", value=accuracy)

    file_eval_path = f"{dataset_name}/file_based_eval_scores.json"
    (df.groupby("file")["sim"].mean()).to_json(file_eval_path, indent=4)

    question_eval_path = f"{dataset_name}/question_based_eval_scores.json"
    (df.groupby("question")["sim"].mean()).to_json(question_eval_path, indent=4)

    mlflow.log_artifact(file_eval_path)
    mlflow.log_artifact(question_eval_path)

    pivot_table = df.pivot_table(
        index="file", columns="question", values="sim", aggfunc="max", fill_value=0
    )

    confusion_matrix_path = dataset_name + "/confusion_matrix.png"
    save_pivot_table_as_confusion(pivot_table, confusion_matrix_path)
    mlflow.log_artifact(confusion_matrix_path)

    mlflow.log_table(
        df[["label", "question", "pred", "file", "sim", "reworded_question"]],
        subfolder_name + "/predicted_dataset_artifact.json",
    )

    experiment_name: str = experiment.replace(":", "_")

    if RUN_RAGAS_EVALS:
        ragas_dataset = create_ragas_dataset(evaluator.predicted_dataset)

        run_ragas_evals(ragas_dataset, experiment_name, dataset_name="main_dataset")

    synthetic_datasets = load_synthetic_tests(synthetic_dataset_path)
    logging.info(
        f"Loaded synthetic datasets. Number of datasets: {len(synthetic_datasets)}"
    )

    for named_ds in synthetic_datasets:
        eval_dataset = ragas_dataset_to_eval(named_ds.dataset, named_ds.file)

        eval_dict_dataset = [asdict(d) for d in eval_dataset]
        logging.info(f"eval_dataset sample-{str(eval_dict_dataset[: 4])}")
        eval_dataset_name = named_ds.name

        logging.info(f"Running predictions for: {eval_dataset_name}")

        evaluator = RAGEvaluator(eval_dict_dataset, compare_sim_with_date, conn)
        logging.info(f"eval_dataset sample-{str(evaluator.dataset[: 4])}")
        evaluator.apredict_dataset()
        logging.info(
            f"predicted_dataset sample-{str(evaluator.predicted_dataset[: 4])}"
        )
        logging.info(f"Creating RAGAS dataset for: {eval_dataset_name}")
        ragas_dataset = create_ragas_dataset(evaluator.predicted_dataset)
        logging.info(f"Calculating RAGAS metrics for: {eval_dataset_name}")
        run_ragas_evals(ragas_dataset, experiment_name, dataset_name=eval_dataset_name)

    mlflow.end_run()  # this is also called if exception is thrown above (atexit)

    if cleanup_dir_on_exit:
        shutil.rmtree(dataset_name)

    return accuracy
