from evaluator import Data, PredictedData
from langchain_openai import ChatOpenAI
from ragas import EvaluationDataset, SingleTurnSample, evaluate
from ragas.llms import LangchainLLMWrapper
from ragas.metrics import AnswerCorrectness, Faithfulness

# set_llm_cache(SQLiteCache(database_path="./Cache/langchain_ragas_cache.db"))


def create_ragas_dataset_from_cuad(dataset: list[PredictedData]) -> EvaluationDataset:

    single_samples = [
        SingleTurnSample(
            user_input=elem.question,
            retrieved_contexts=[str(doc) for doc in elem.docs],
            response=elem.pred,
            reference=elem.label,
        )
        for elem in dataset
        if "-Answer" in elem.question_category
    ]

    return EvaluationDataset(samples=single_samples)


def create_ragas_dataset(dataset: list[PredictedData]) -> EvaluationDataset:

    single_samples = [
        SingleTurnSample(
            user_input=elem.question,
            retrieved_contexts=[str(doc) for doc in elem.docs],
            response=elem.pred,
            reference=elem.label,
            reference_contexts=(
                [str(doc) for doc in elem.reference_contexts]
                if elem.reference_contexts and isinstance(elem.reference_contexts, list)
                else None
            ),
        )
        for elem in dataset
    ]

    return EvaluationDataset(samples=single_samples)


def ragas_dataset_to_eval(dataset: EvaluationDataset, file: str) -> list[Data]:
    ls = []
    for sample in dataset:
        elem = Data(
            question=sample.user_input,
            reworded_question=sample.user_input,
            label=sample.reference,
            file=file,
            reference_contexts=sample.reference_contexts,
            question_category="fill",
        )
        ls.append(elem)

    return ls


def run_ragas_evaluations(dataset: EvaluationDataset):

    evaluator_llm = LangchainLLMWrapper(
        ChatOpenAI(model="gpt-4o-mini", temperature=0.0)
    )

    answer_correctness_metric = AnswerCorrectness(
        llm=evaluator_llm,
        weights=[1.0, 0.0],
        max_retries=3,
        beta=1.5,  # favor the recall a bit more
    )

    correctness_prompt = answer_correctness_metric.get_prompts()["correctness_prompt"]

    correctness_prompt.instruction += """ Answer may be less or more verbose than the ground truth, that is fine.
    If the ground truth is 'Yes' and answer is 'Yes, [... some details]', consider it true.

    In the case of dates, consider prediction as true of response and the reference has different formats or verbosity.
    For instance, if the reference is `01/01/2021` and the answer is `According to this and that, date is 1st of the January 2021.`,
    consider this as a True Positive case, and give full score."""  # noqa: E501
    answer_correctness_metric.set_prompts(**{"correctness_prompt": correctness_prompt})

    metrics: list = [
        answer_correctness_metric,
        Faithfulness(llm=evaluator_llm),
    ]
    results = evaluate(dataset=dataset, metrics=metrics)
    return results
