import logging
import os
import shutil
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

import mlflow
import mlflow.data.dataset
import pandas as pd
from connector import RagConnector
from constants import CuadMissingData
from dataset import DatasetUtils
from evaluator import LLMAnswerEvaluator, RAGEvaluator
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


# mapping from dataset's test answers to original filename and name of the dataset
FILE_TO_DATASET_META = {
    "20230203_alphabet_10K.jsonl": {
        "name": "AlphabetQA",
        "original_file": "20230203_alphabet_10K.pdf",
        "version": "v1",
    },
    "20230203_alphabet_10K_tables.jsonl": {
        "name": "AlphabetTables",
        "original_file": "20230203_alphabet_10K.pdf",
        "version": "v1",
    },
}


@dataclass
class NamedDataset:
    name: str
    version: str
    dataset: EvaluationDataset
    file: str


def load_synthetic_tests(dataset_folder_path: str) -> list[NamedDataset]:
    dataset_paths = Path(dataset_folder_path).glob("*.jsonl")

    dataset_ls = []
    for data_path in dataset_paths:
        logging.info(f"Loaded synthetic dataset: {data_path}")
        dataset = EvaluationDataset.from_jsonl(data_path)

        if data_path.name not in FILE_TO_DATASET_META:
            raise ValueError(f"Unknown dataset name for file: {data_path.name}")
        version = FILE_TO_DATASET_META[data_path.name]["version"]
        dataset_name = FILE_TO_DATASET_META[data_path.name]["name"]
        original_file_name = FILE_TO_DATASET_META[data_path.name]["original_file"]

        named_ds = NamedDataset(
            name=dataset_name, dataset=dataset, file=original_file_name, version=version
        )
        dataset_ls.append(named_ds)

    return dataset_ls


def run_ragas_evals(
    ragas_dataset: EvaluationDataset, subfolder_name: str, dataset_name: str
):
    ragas_evals_dataset = run_ragas_evaluations(ragas_dataset)
    ragas_scores_df = pd.DataFrame(ragas_evals_dataset.scores)
    ragas_scores: dict = ragas_scores_df.mean().to_dict()

    for metric_name, value in ragas_scores.items():
        mlflow.log_metric(f"{dataset_name}-{metric_name}-Ragas", value=value)

    ragas_evaluation_df = ragas_evals_dataset.to_pandas()
    mlflow.log_table(
        ragas_evaluation_df,
        subfolder_name.split("/")[-1] + f"/ragas_{dataset_name}_dataset.json",
    )
    # If this gets error: 'Request Entity Too Large for url'
    # increase nginx body size


def run_cuad_eval(dataset_path: str, run_subfolder: str, conn: RagConnector):

    ds = CuadDataset.from_tsv(dataset_path)
    cuad_dataset = ds.prepare_dataset()

    mlflow_dataset = mlflow.data.from_pandas(pd.DataFrame(cuad_dataset), name="cuad_v1")
    mlflow.log_input(mlflow_dataset)

    os.makedirs(run_subfolder, exist_ok=True)  # folder for results & logs

    evaluator = RAGEvaluator(cuad_dataset, conn)

    evaluator.apredict_dataset()

    evaluator.save_predicted_dataset(run_subfolder + "/predicted_dataset.json")

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
        retrieval_metrics, run_subfolder + "/retrieval_metrics.json"
    )

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

    file_eval_path = f"{run_subfolder}/file_based_eval_scores.json"
    (answer_df.groupby("file")["sim"].mean()).to_json(file_eval_path, indent=4)

    question_eval_path = f"{run_subfolder}/question_based_eval_scores.json"
    (answer_df.groupby("question")["sim"].mean()).to_json(question_eval_path, indent=4)

    mlflow.log_artifact(file_eval_path)
    mlflow.log_artifact(question_eval_path)

    pivot_table = answer_df.pivot_table(
        index="file", columns="question", values="sim", aggfunc="max", fill_value=0
    )

    confusion_matrix_path = run_subfolder + "/confusion_matrix.png"
    save_pivot_table_as_confusion(pivot_table, confusion_matrix_path)
    mlflow.log_artifact(confusion_matrix_path)

    # full_dataset_df
    mlflow.log_table(
        answer_df[["label", "question", "pred", "file", "sim", "reworded_question"]],
        run_subfolder.split("/")[-1] + "/predicted_dataset_artifact.json",
    )

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

        run_ragas_evals(ragas_dataset, run_subfolder, dataset_name="cuad_v1")

    return accuracy


def run_alphabet_eval(dataset_path: str, run_subfolder: str, conn: RagConnector):
    synthetic_datasets = load_synthetic_tests(dataset_path)
    logging.info(
        f"Loaded synthetic datasets. Number of datasets: {len(synthetic_datasets)}"
    )

    for named_ds in synthetic_datasets:
        eval_dataset = ragas_dataset_to_eval(named_ds.dataset, named_ds.file)

        mlflow_dataset = mlflow.data.from_pandas(
            pd.DataFrame(named_ds.dataset.to_list()),
            name=f"{named_ds.name}_{named_ds.version}",
        )
        mlflow.log_input(mlflow_dataset)

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
        run_ragas_evals(ragas_dataset, run_subfolder, dataset_name=eval_dataset_name)


def run_eval_experiment(
    run_name: str | None = None,
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
    mlflow.set_tracking_uri(MLFLOW_URI)

    if not run_name:
        run_name = datetime.now().strftime("%d-%m-%Y %H:%M")
    # : is forbidden in artifact name
    run_name = run_name.replace(":", "_").replace(" ", "_")

    mlflow.set_experiment(experiment_name=EXPERIMENT_NAME)
    mlflow.start_run(
        run_name=run_name, description=os.environ.get("MLFLOW_DESCRIPTION", "")
    )
    mlflow.set_tag("Branch name", BRANCH_NAME)
    mlflow.log_artifact(config_file_path)

    current_dir = os.path.dirname(__file__)
    run_subfolder = current_dir + "/" + run_name

    conn = RagConnector(base_url=base_url)

    accuracy = run_cuad_eval(dataset_path, run_subfolder, conn)

    run_alphabet_eval(synthetic_dataset_path, run_subfolder, conn)

    mlflow.end_run()  # this is also called if exception is thrown above (atexit)

    if cleanup_dir_on_exit:
        shutil.rmtree(run_subfolder, ignore_errors=True)

    return accuracy
