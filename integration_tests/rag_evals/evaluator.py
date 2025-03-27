import asyncio
import logging
import re
from abc import abstractmethod
from dataclasses import asdict, dataclass, field
from datetime import datetime
from difflib import SequenceMatcher
from itertools import product

import numpy as np
import pandas as pd
from connector import RagConnector
from constants import CuadMissingData
from dataset import DatasetUtils
from pydantic import BaseModel
from structured_llm import OpenAIStructuredChat
from tqdm import tqdm
from utils import create_file_filter

import pathway as pw
from pathway.udfs import DiskCache, ExponentialBackoffRetryStrategy
from pathway.xpacks.llm._utils import _coerce_sync

STRDIFF_MIN_SIMILARITY: float = 0.65
SEQUENCE_MATCH_MIN_THRESHOLD: float = 0.4  # for answer correctness via str similarity
BERT_SIMILARITY_CUTOFF: float = 0.6


@dataclass
class Data:
    question_category: str
    question: str
    label: str
    file: str
    reworded_question: str
    reference_contexts: str | None = ""


@dataclass
class PredictedData(Data):
    pred: str = ""
    docs: list[dict] = field(default_factory=lambda: [])


class BaseAnswerEvaluator:
    @abstractmethod
    def evaluate(self, pred: str, label: str) -> bool: ...


class AnswerCorrectness(BaseModel):
    is_correct: bool


@dataclass
class LLMAnswerEvaluator(BaseAnswerEvaluator):
    model_name: str = "gpt-4o-mini"
    temperature: float = 0.0
    verbose: bool = False

    def evaluate(self, pred: str, label: str) -> bool:
        return _coerce_sync(self.aevaluate)(pred, label)

    async def aevaluate(self, pred: str, label: str) -> bool:

        llm = OpenAIStructuredChat(
            model=self.model_name,
            retry_strategy=ExponentialBackoffRetryStrategy(max_retries=3),
            cache_strategy=DiskCache(),
            temperature=self.temperature,
            verbose=self.verbose,
        )

        SYS = (
            "Your task is to determine the answer correctness in a RAG system. "
            "You will be given a prediction and a label. "
            "You will return a bool denoting if answer is True or False. "
            "Differences in wording, date format or the verbosity between the prediction "
            "and the label are fine. You can consider a case as `True` as long as answer "
            " is correct."
            "For instance, if the reference is `01/01/2021` and the answer is "
            "`According to this and that, date is 1st of January 2021.`, "
            "return `True`."
            "If the label is one of the following: '', 'nan' or 'no information', "
            "this means that the answer cannot be deduced. So, if the system's response is "
            "along the lines with 'I don't know.' or 'No information.', also return `True`."
        )

        messages = [
            {"role": "system", "content": SYS},
            {
                "role": "user",
                "content": f"Prediction: {pred}\nLabel: {label}",
            },
        ]

        response: dict = (
            await llm.func(
                messages,
                response_format=pw.PyObjectWrapper(AnswerCorrectness),
            )
        ).value
        return AnswerCorrectness(**response).is_correct


@dataclass
class StringSimEvaluator(BaseAnswerEvaluator):
    min_sequence_match: float = SEQUENCE_MATCH_MIN_THRESHOLD

    def evaluate(
        self,
        pred: str,
        label: str,
    ) -> bool:
        pred, label = pred.lower(), label.lower()

        if self._check_if_not_found_true_pred(pred, label):
            return True

        if self.is_date(label):
            return self.compare_dates(pred, label)

        a = "".join(e for e in pred if e.isalnum())
        b = "".join(e for e in label if e.isalnum())

        return SequenceMatcher(None, a, b).ratio() > self.min_sequence_match

    def is_date(self, date_str: str) -> bool:
        pattern = r"\b(0?[1-9]|1[0-2])/(0?[1-9]|[12]\d|3[01])/\d{2}\b"
        return bool(re.match(pattern, date_str))

    def parse_date(self, date_str) -> datetime | None:
        formats = ["%d %B %Y", "%B %d, %Y", "%m %d, %Y"]
        for fmt in formats:
            try:
                date = datetime.strptime(date_str, fmt)
                return date
            except ValueError:
                continue
        return None

    def compare_dates(self, pred: str, label: str) -> bool:
        pred_date = self.parse_date(pred)
        if pred_date:
            formatted_date: str = pred_date.strftime("%-m/%-d/%y")

            return formatted_date == label

        return False

    def _check_if_not_found_true_pred(self, pred: str, label: str) -> bool:
        # maybe we should just switch to LLMs
        if "no information" in pred or "don't know" in pred or "do not know" in pred:
            return label in (
                CuadMissingData.ANSWER_LABEL,
                CuadMissingData.CONTEXT_LABEL,
            )
        return False


@dataclass
class BertScoreEvaluator(BaseAnswerEvaluator):
    sim_cutoff: float = BERT_SIMILARITY_CUTOFF

    def evaluate(self, pred, label) -> bool:
        from evaluate import load

        bertscore = load("bertscore")
        results = bertscore.compute(predictions=[pred], references=[label], lang="en")
        return results["f1"] > self.sim_cutoff


def compare_ls_bert_score(preds: list[str], labels: list[str]) -> list[float]:
    from evaluate import load

    bertscore = load("bertscore")

    results = bertscore.compute(predictions=preds, references=labels, lang="en")
    return results["f1"]


def compare_ls_sentence_cos(preds: list[str], labels: list[str]):
    import numpy as np
    from sentence_transformers import SentenceTransformer, util

    model = SentenceTransformer("all-MiniLM-L6-v2")

    pred_embeds = model.encode(preds, convert_to_tensor=True)
    label_embeds = model.encode(labels, convert_to_tensor=True)

    cosine_scores = util.cos_sim(pred_embeds, label_embeds)
    return np.diag(cosine_scores)


class RAGEvaluator:
    def __init__(
        self,
        dataset: list[dict],
        connector: RagConnector,
    ) -> None:
        self.predicted_dataset: list[PredictedData] = []  # file, question, label

        self.dataset: list[Data] = [Data(**dc) for dc in dataset]
        self.connector = connector

        self.result_metrics: dict = {}

    @property
    def predicted_dataset_as_dict_list(self) -> list[dict]:
        return [asdict(i) for i in self.predicted_dataset]

    def _predict_single(self, question: str, file: str) -> dict:
        filter = create_file_filter(file)
        answer = self.connector.answer_question(
            question,
            filter,
        )
        return answer

    async def _apredict_single(self, question: str, file: str) -> dict:
        filter = create_file_filter(file)
        answer = await asyncio.to_thread(  # TODO: convert to await with async client
            self.connector.answer_question,
            question,
            filter,
        )
        return answer

    async def _apredict_dataset(self) -> list[dict]:
        tasks = []
        for dc in tqdm(self.dataset):
            question = dc.reworded_question
            file = dc.file
            task = self._apredict_single(question, file)
            tasks.append(task)

        print("Async predict dataset with number of tasks:", len(tasks))
        logging.info(f"Async predict dataset with number of tasks: {len(tasks)}")
        results = await asyncio.gather(*tasks)
        logging.info("Async predicted the dataset.")
        return results

    def apredict_dataset(self) -> None:
        """Populate `predicted_dataset`."""

        logging.info("Running `apredict_dataset`.")

        results = asyncio.run(self._apredict_dataset())

        for idx, dc in tqdm(enumerate(self.dataset)):
            question = dc.reworded_question
            file = dc.file
            api_response: dict = results[idx]

            pred = PredictedData(
                question_category=dc.question_category,
                question=question,
                label=dc.label,
                file=file,
                reworded_question=dc.reworded_question,
                pred=api_response["response"],
                docs=api_response["context_docs"],
                reference_contexts=dc.reference_contexts,
            )
            self.predicted_dataset.append(pred)

            logging.info(f"Constructing predicted ds for file: {file}")

        logging.info("Finished running `apredict_dataset`.")

    def calculate_retrieval_metrics(self, dataset: list[dict] | None = None):
        if dataset is None:
            dataset = self.predicted_dataset_as_dict_list
        return self._calculate_dataset_retrieval_metrics(dataset=dataset)

    def _calculate_dataset_retrieval_metrics(self, dataset: list[dict]) -> dict:

        def get_hit_index(
            returned_docs: list[str], labels: list[str] | None
        ) -> int | None:
            """
            Returns hit index for a label for returned docs.
            Assumes single ground truth text/phrase.
            Uses string intersection with percentage to decide.
            Returns `None` if not found.
            """
            if labels is None:
                return None

            def compare_intersect(pred: str, label: str) -> float:
                intersect_len = len(set(label.split(" ")).intersection(pred.split(" ")))
                return intersect_len / len(set(label.split(" ")))

            for idx, t in enumerate(returned_docs):
                if t and str(labels) != "nan" and labels:
                    try:
                        cartesian = list(product([t], labels))
                        sim_pass = list(
                            map(lambda x: compare_intersect(x[0], x[1]), cartesian)
                        )

                        if max(sim_pass) >= STRDIFF_MIN_SIMILARITY:
                            return idx
                    except Exception:
                        print("label empty:", labels)

            return None

        hit_list: list[int | None] = []

        logging.info(
            f"Calculating retrieval metrics for dataset of length: {len(dataset)}."
        )

        for i in dataset:
            text_ls = list(map(lambda d: d["text"], i["docs"]))
            labels = i["label"]

            hit_k = get_hit_index(text_ls, labels)

            hit_list.append(hit_k)

        mrr_score = get_mrr(hit_list)
        hit_3 = get_hit_k(hit_list, k=3)
        hit_6 = get_hit_k(hit_list, k=6)

        filtered_hits: list[int] = list(filter(None, hit_list))
        mean_hit = sum(filtered_hits) / len(hit_list)
        median_hit = np.median(filtered_hits)

        results = {
            "mrr": mrr_score,
            "hit_at_3": hit_3,
            "hit_at_6": hit_6,
            "mean_hit": mean_hit,
            "median_hit": median_hit,
        }

        return results

    def load_predicted_dataset(self, name="predicted_dataset.jsonl") -> None:
        self.predicted_dataset = DatasetUtils.read_dataset(name)

    def save_predicted_dataset(self, name="predicted_dataset.jsonl") -> None:
        DatasetUtils.save_dataset(self.predicted_dataset_as_dict_list, name)

    def get_dataset_as_df(self) -> pd.DataFrame:
        return pd.DataFrame(self.predicted_dataset_as_dict_list)


# document retrieval metrics


def get_mrr(hit_list: list[int | None]) -> float:
    reciprocal_ranks = []

    for rank in hit_list:
        if rank is not None:
            reciprocal_ranks.append(1 / (rank + 1))

    if not reciprocal_ranks:
        return 0.0

    return sum(reciprocal_ranks) / len(
        hit_list
    )  # divide to len(hit_list) to punish None elements


def get_hit_k(hit_list: list[int | None], k: int = 3) -> float:
    """Calculate the average hit@k (rank of the True Positive)."""
    k -= 1

    def is_hit(elem, k) -> bool:
        if elem is not None:
            return elem <= k
        return False

    bool_hits = map(lambda elem: is_hit(elem, k), hit_list)
    return sum(bool_hits) / len(hit_list)
