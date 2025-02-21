import logging
import re
from copy import deepcopy
from dataclasses import dataclass, field

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from constants import CuadMissingData
from eval_questions import EVAL_QUESTIONS, question_mapper


def create_file_filter(file: str) -> str:
    """Create globmatch filter for a single file."""
    return "globmatch(`" + file + "`, path)"


def save_pivot_table_as_confusion(table, path: str) -> None:
    colors = ["red", "green"]

    sns.set(font_scale=1.2)
    plt.figure(figsize=(12, 10))
    plt.subplots_adjust(bottom=0.15, left=0.15)
    heatmap = sns.heatmap(table, cmap=colors, annot=True, fmt="d", cbar=False)
    plt.yticks(rotation=0)
    plt.xticks(rotation=90)

    heatmap.figure.savefig(path, bbox_inches="tight")


@dataclass
class CuadDataset:
    raw_dataset: pd.DataFrame
    prepared_dataset: list[dict] = field(default_factory=lambda: [])

    @classmethod
    def from_tsv(cls, tsv_path: str):
        df = pd.read_csv(tsv_path, sep="\t")
        return cls(raw_dataset=df)

    def prepare_dataset(self) -> list[dict]:
        dataset: list[dict] = []

        # filter dataset based on select questions
        for question in EVAL_QUESTIONS:
            for index, row in self.raw_dataset.iterrows():
                data_elem = {
                    "file": row["file"],
                    "question": question,
                    "label": row[question],
                    "question_category": question,
                    "reworded_question": question_mapper[
                        question
                    ],  # map dataset questions to natural language format
                }

                dataset.append(data_elem)

        self.prepared_dataset = dataset
        return self.prepared_dataset


class CuadDatasetUtils:
    @staticmethod
    def split_retrieval_answer_datasets(
        dataset: list[dict],
    ) -> tuple[list[dict], list[dict]]:
        retrieval_dataset = [
            i for i in dataset if ("-Answer" not in i["question_category"])
        ]
        answer_dataset = [i for i in dataset if "-Answer" in i["question_category"]]

        logging.info(
            f"Retrieval data size: {len(retrieval_dataset)},\n \
                     Answer data size: {len(answer_dataset)}."
        )

        return (retrieval_dataset, answer_dataset)

    @staticmethod
    def prepare_retrieval_dataset(dataset: list[dict]) -> list[dict]:
        # filter out rows with empty expected retrieved documents
        dataset = [
            i for i in dataset if i["label"] and (i["label"] not in ("[]", "nan"))
        ]

        def eval_stringified_list_to_list(text: str) -> list[str]:
            """Parse the label string into list of strings.
            Expected string format: "['a', 'b', 'c']"."""
            # return re.findall(r"'(.*?)'", text)
            matches = re.findall(r"'(.*?)'|\"(.*?)\"", text, re.DOTALL)

            result = [m[0] if m[0] else m[1] for m in matches]

            return [text.replace("\\'", "'") for text in result]

        # parse labels into list of str
        for elem in dataset:
            try:
                label_str = str(elem["label"])
                labels_ls: list[str] = eval_stringified_list_to_list(label_str)
            except ValueError as err:
                logging.error(f"Invalid value in labels: {label_str}. \n{str(err)}")
                continue

            elem["label"] = labels_ls + [" ".join(labels_ls)]
            # add concat string that may enable topk hit
            # if label: [a, b, c] -> "a b c"
            # chunk: "a   b c", this will be evaluated as TP
        return dataset

    @staticmethod
    def prepare_answer_dataset(dataset: list[dict]) -> list[dict]:
        data = deepcopy(dataset)

        for elem in data:
            if (
                str(elem["label"]).lower() in ("", " ", "[]", "()", "nan")
                or elem["label"] is None
            ):
                elem["label"] = CuadMissingData.ANSWER_LABEL

        return data
