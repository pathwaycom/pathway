import logging
import os
import shutil
from datetime import datetime

import mlflow
import pandas as pd

from .connector import RagConnector
from .dataset import DatasetUtils
from .eval_questions import eval_questions, question_mapper
from .evaluator import RAGEvaluator, compare_sim_with_date
from .logging_utils import get_run_params
from .ragas_utils import create_ragas_dataset, run_ragas_evaluations
from .utils import save_pivot_table_as_confusion

mlflow.set_tracking_uri("https://mlflow.internal.pathway.com")

EXPERIMENT_NAME = "CI RAG Evals"
RUN_RAGAS_EVALS: bool = True


def run_eval_experiment(
    experiment: str | None = None,
    base_url: str = "http://0.0.0.0:8000",
    dataset_path: str = "dataset/labeled.tsv",
    config_file_path: str = "app.yaml",
    cleanup_dir_on_exit: bool = False,
) -> float:
    """Run & log eval results, return total accuracy."""

    if not experiment:
        experiment = datetime.now().strftime("%d-%m-%Y %H:%M")

    mlflow.set_experiment(experiment_name=EXPERIMENT_NAME)
    mlflow.start_run(run_name=experiment)

    current_dir = os.path.dirname(__file__)

    subfolder_name = experiment.replace(":", "_")

    dataset_name = current_dir + "/" + subfolder_name  # : is forbidden in artifact name

    conn = RagConnector(base_url=base_url)
    # dataset = DatasetUtils.read_dataset("labels.jsonl")

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
    # evaluator.load_predicted_dataset(dataset_name + "/predicted_dataset")

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

    # tot_accuracy = evaluator.calculate_tot_accuracy()
    # mlflow.log_metric("Total RAG Accuracy", value=tot_accuracy)
    # logging.info("Total accuracy:", tot_accuracy)

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

    if RUN_RAGAS_EVALS:
        CORRECTNESS_CUTOFF: float = 0.65  # TODO: adjust
        ragas_dataset = create_ragas_dataset(evaluator.predicted_dataset)

        ragas_evals_dataset = run_ragas_evaluations(ragas_dataset)

        ragas_scores_df = pd.DataFrame(ragas_evals_dataset.scores)
        ragas_scores_df["answer_correctness_withcutoff"] = (
            ragas_scores_df["answer_correctness"] > CORRECTNESS_CUTOFF
        ).astype(float)

        ragas_scores: dict = ragas_scores_df.mean().to_dict()

        for metric_name, value in ragas_scores.items():
            mlflow.log_metric(f"Ragas-{metric_name}", value=value)

        ragas_evaluation_df = ragas_evals_dataset.to_pandas()

        mlflow.log_table(
            ragas_evaluation_df,
            subfolder_name + "/ragas_dataset.json",
        )
        # Gets error: 'Request Entity Too Large for url'

    mlflow.end_run()

    if cleanup_dir_on_exit:
        shutil.rmtree(dataset_name)

    return accuracy


# if __name__ == "__main__":
#     main()
