# Copyright © 2024 Pathway

import pathway.internals as pw
from pathway.internals import ColumnReference, Table
from pathway.xpacks.llm.llms import prompt_chat_single_qa
from pathway.xpacks.llm.prompts import prompt_qa


@pw.udf
def _limit_documents(documents: list[str], k: int) -> list[str]:
    return documents[:k]


_answer_not_known = "No information found."


def _query_chat(chat: pw.UDF, t: Table) -> pw.Table:
    t += t.select(prompt=prompt_qa(t.query, t.documents, _answer_not_known))
    answer = t.select(answer=chat(prompt_chat_single_qa(t.prompt)))
    answer = answer.select(
        answer=pw.if_else(pw.this.answer == _answer_not_known, None, pw.this.answer)
    )
    return answer


def _query_chat_with_k_documents(chat: pw.UDF, k: int, t: pw.Table) -> pw.Table:
    limited_documents = t.select(
        pw.this.query, documents=_limit_documents(t.documents, k)
    )
    result = _query_chat(chat, limited_documents)
    return result


def answer_with_geometric_rag_strategy(
    questions: ColumnReference,
    documents: ColumnReference,
    llm_chat_model: pw.UDF,
    n_starting_documents: int,
    factor: int,
    max_iterations: int,
) -> ColumnReference:
    """
    Function for querying LLM chat while providing increasing number of documents until an answer
    is found. Documents are taken from `documents` argument. Initially first `n_starting_documents` documents
    are embedded in the query. If the LLM chat fails to find an answer, the number of documents
    is multiplied by `factor` and the question is asked again.

    Args:
        questions (ColumnReference[str]): Column with questions to be asked to the LLM chat.
        documents (ColumnReference[list[str]]): Column with documents to be provided along
             with a question to the LLM chat.
        llm_chat_model: Chat model which will be queried for answers
        n_starting_documents: Number of documents embedded in the first query.
        factor: Factor by which a number of documents increases in each next query, if
            an answer is not found.
        max_iterations: Number of times to ask a question, with the increasing number of documents.

    Returns:
        A column with answers to the question. If answer is not found, then None is returned.

    Example:

    >>> import pandas as pd
    >>> import pathway as pw
    >>> from pathway.xpacks.llm.llms import OpenAIChat
    >>> from pathway.xpacks.llm.question_answering import answer_with_geometric_rag_strategy
    >>> chat = OpenAIChat()
    >>> df = pd.DataFrame(
    ...     {
    ...         "question": ["How do you connect to Kafka from Pathway?"],
    ...         "documents": [
    ...             [
    ...                 "`pw.io.csv.read reads a table from one or several files with delimiter-separated values.",
    ...                 "`pw.io.kafka.read` is a seneralized method to read the data from the given topic in Kafka.",
    ...             ]
    ...         ],
    ...     }
    ... )
    >>> t = pw.debug.table_from_pandas(df)
    >>> answers = answer_with_geometric_rag_strategy(t.question, t.documents, chat, 1, 2, 2)
    """
    n_documents = n_starting_documents
    t = Table.from_columns(query=questions, documents=documents)
    t = t.with_columns(answer=None)
    for _ in range(max_iterations):
        rows_without_answer = t.filter(pw.this.answer.is_none())
        results = _query_chat_with_k_documents(
            llm_chat_model, n_documents, rows_without_answer
        )
        new_answers = rows_without_answer.with_columns(answer=results.answer)
        t = t.update_rows(new_answers)
        n_documents *= factor
    return t.answer
