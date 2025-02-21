import logging
import os
import shutil
from dataclasses import asdict, dataclass
from datetime import datetime

import mlflow
import pandas as pd
from connector import RagConnector
from constants import CuadMissingData
from dataset import DatasetUtils
from evaluator import LLMAnswerEvaluator, RAGEvaluator  # , StringSimEvaluator
from logging_utils import get_run_params
from ragas import EvaluationDataset
from ragas_utils import (
    create_ragas_dataset,
    create_ragas_dataset_from_cuad,
    ragas_dataset_to_eval,
    run_ragas_evaluations,
)
from utils import CuadDataset, CuadDatasetUtils, save_pivot_table_as_confusion

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
    """Run & log eval results, return total accuracy for CUAD dataset."""
    MLFLOW_URI: str | None = os.environ.get("MLFLOW_URI")
    if not MLFLOW_URI:
        raise RuntimeError("`MLFLOW_URI` is not set in the environ.")

    mlflow.set_tracking_uri(MLFLOW_URI)  # setting to None doesn't raise exception

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

    ds = CuadDataset.from_tsv(dataset_path)
    cuad_dataset = ds.prepare_dataset()

    os.makedirs(dataset_name, exist_ok=True)  # folder for results & logs

    for param_dict in get_run_params():
        mlflow.log_params(param_dict)

    mlflow.log_artifact(config_file_path)

    evaluator = RAGEvaluator(cuad_dataset, conn)

    evaluator.apredict_dataset()

    evaluator.save_predicted_dataset(dataset_name + "/predicted_dataset.json")

    logging.info(f"Total dataset size: {len(evaluator.predicted_dataset_as_dict_list)}")

    retrieval_dataset, answer_dataset = (
        CuadDatasetUtils.split_retrieval_answer_datasets(
            evaluator.predicted_dataset_as_dict_list
        )
    )

    retrieval_dataset = CuadDatasetUtils.prepare_retrieval_dataset(retrieval_dataset)

    retrieval_metrics = evaluator.calculate_retrieval_metrics(retrieval_dataset)
    mlflow.log_metrics(retrieval_metrics)
    DatasetUtils.save_dataset(
        retrieval_metrics, dataset_name + "/retrieval_metrics.json"
    )

    # full_dataset_df = pd.DataFrame(evaluator.predicted_dataset_as_dict_list).fillna("")
    # Cyber holding effective start date is Nan, + some others also missing

    answer_dataset = CuadDatasetUtils.prepare_answer_dataset(answer_dataset)

    answer_df = pd.DataFrame(answer_dataset).fillna(CuadMissingData.ANSWER_LABEL)

    logging.info(
        f"Calculating accuracy metrics for dataset of length: {len(answer_df)}."
    )

    llm_answer_evaluator = LLMAnswerEvaluator()
    answer_df["sim"] = answer_df.apply(
        lambda row: llm_answer_evaluator.evaluate(str(row["pred"]), str(row["label"])),
        axis=1,
    )

    accuracy = answer_df["sim"].mean()
    logging.info(f"Total accuracy: {accuracy}")
    mlflow.log_metric("Total RAG Accuracy", value=accuracy)

    file_eval_path = f"{dataset_name}/file_based_eval_scores.json"
    (answer_df.groupby("file")["sim"].mean()).to_json(file_eval_path, indent=4)

    question_eval_path = f"{dataset_name}/question_based_eval_scores.json"
    (answer_df.groupby("question")["sim"].mean()).to_json(question_eval_path, indent=4)

    mlflow.log_artifact(file_eval_path)
    mlflow.log_artifact(question_eval_path)

    pivot_table = answer_df.pivot_table(
        index="file", columns="question", values="sim", aggfunc="max", fill_value=0
    )

    confusion_matrix_path = dataset_name + "/confusion_matrix.png"
    save_pivot_table_as_confusion(pivot_table, confusion_matrix_path)
    mlflow.log_artifact(confusion_matrix_path)

    # full_dataset_df
    mlflow.log_table(
        answer_df[["label", "question", "pred", "file", "sim", "reworded_question"]],
        subfolder_name + "/predicted_dataset_artifact.json",
    )

    experiment_name: str = experiment.replace(":", "_")

    if RUN_RAGAS_EVALS:
        cuad_preds_ds = [
            i
            for i in evaluator.predicted_dataset
            if isinstance(i.pred, str)
            and isinstance(i.label, str)
            and "-Answer" in i.question_category
        ]
        logging.info(f"Length of pred_ds: {len(cuad_preds_ds)}")
        ragas_dataset = create_ragas_dataset_from_cuad(cuad_preds_ds)

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

        evaluator = RAGEvaluator(eval_dict_dataset, conn)
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
