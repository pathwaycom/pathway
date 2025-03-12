import logging
from typing import Callable

import pathway as pw
import pathway.xpacks.llm.prompts as prompts
from pathway.internals import udfs
from pathway.internals.udfs.executors import FullyAsyncExecutor
from pathway.optional_import import optional_imports
from pathway.xpacks.llm import Doc, llms
from pathway.xpacks.llm._utils import _coerce_fully_async, _extract_value
from pathway.xpacks.llm.llms import prompt_chat_single_qa

logger = logging.getLogger(__name__)


@pw.udf
def rerank_topk_filter(
    docs: list[Doc], scores: list[float], k: int = 5
) -> tuple[list[Doc], list[float]]:
    """Apply top-k filtering to docs using the relevance scores.

    Args:
        docs: A column with lists of  documents or chunks to rank. Each row in this column
            is filtered separately.
        scores: A column with lists of re-ranking scores for chunks.
        k: The number of documents to keep after filtering.

    >>> import pathway as pw
    >>> from pathway.xpacks.llm import rerankers
    >>> import pandas as pd
    >>> retrieved_docs = [
    ...     {"text": "Something"},
    ...     {"text": "Something else"},
    ...     {"text": "Pathway"},
    ... ]
    >>> df = pd.DataFrame({"docs": retrieved_docs, "reranker_scores": [1.0, 3.0, 2.0]})
    >>> table = pw.debug.table_from_pandas(df)
    >>> docs_table = table.reduce(
    ...     doc_list=pw.reducers.tuple(pw.this.docs),
    ...     score_list=pw.reducers.tuple(pw.this.reranker_scores),
    ... )
    >>> docs_table = docs_table.select(
    ...     docs_scores_tuple=rerankers.rerank_topk_filter(
    ...         pw.this.doc_list, pw.this.score_list, 2
    ...     )
    ... )
    >>> docs_table = docs_table.select(
    ...     doc_list=pw.this.docs_scores_tuple[0],
    ...     score_list=pw.this.docs_scores_tuple[1],
    ... )
    >>> pw.debug.compute_and_print(docs_table, include_id=False)
    doc_list                                                            | score_list
    (pw.Json({'text': 'Something else'}), pw.Json({'text': 'Pathway'})) | (3.0, 2.0)
    """  # noqa: E501
    docs, scores = zip(*sorted(zip(docs, scores), key=lambda tup: tup[1], reverse=True))
    logging.info(f"Number of docs after rerank: {len(docs[:k])}\nScores: {scores[:k]}")
    return (docs[:k], scores[:k])


class LLMReranker:
    """Pointwise LLM reranking module.

    Asks LLM to evaluate a given doc against a query between 1 and 5.

    Args:
        llm: Chat instance to be called during reranking.
        prompt_template: str or Callable[[str, str], str] or pw.UDF. Template to be used for
            generating prompt for the LLM. Defaults to `prompts.prompt_rerank`.
            The prompt template should accept two arguments, `doc` and `query`. The prompt
            should ask the LLM to return jsonl with an attribute 'score'.
        response_parser: pw.UDF or Callable[[str], float]. Function to parse the response from the LLM.
            Must take a string as input and return a float. Defaults to `prompts.parse_score_json`.
    Example:

    >>> import pathway as pw
    >>> import pandas as pd
    >>> from pathway.xpacks.llm import rerankers, llms
    >>> chat = llms.OpenAIChat(model="gpt-4o-mini")
    >>> reranker = rerankers.LLMReranker(chat)
    >>> docs = [{"text": "Something"}, {"text": "Something else"}, {"text": "Pathway"}]
    >>> df = pd.DataFrame({"docs": docs, "prompt": "query text"})
    >>> table = pw.debug.table_from_pandas(df)
    >>> table += table.select(
    ...     reranker_scores=reranker(pw.this.docs["text"], pw.this.prompt)
    ... )
    >>> table
    <pathway.Table schema={'docs': <class 'pathway.internals.json.Json'>, 'prompt': <class 'str'>, 'reranker_scores': <class 'float'>}>
    """  # noqa: E501

    def __init__(
        self,
        llm: llms.BaseChat,
        *,
        prompt_template: (
            str | Callable[[str, str], str] | pw.UDF
        ) = prompts.prompt_rerank,
        response_parser: pw.UDF | Callable[[str], float] = prompts.parse_score_json,
    ) -> None:
        self.llm = llm

        self.prompt_udf = self._get_prompt_udf(prompt_template)
        self.parse_response_udf = self._get_parse_response_udf(response_parser)

    def __call__(
        self, doc: pw.ColumnExpression, query: pw.ColumnExpression
    ) -> pw.ColumnExpression:
        """Evaluates the doc against the query.

        Args:
            doc (pw.ColumnExpression[str]): Document or document chunk to be scored.
            query (pw.ColumnExpression[str]): User query or prompt that will be used
                to evaluate relevance of the doc.

        Returns:
            pw.ColumnExpression[float]: A column with the scores for each document.
        """
        doc, query = pw.udf(_extract_value)(doc), pw.udf(_extract_value)(query)

        call_kwargs = dict(
            temperature=0,
        )

        prompt = self.prompt_udf(doc, query)

        response = self.llm(
            prompt_chat_single_qa(prompt),
            **call_kwargs,
        )

        if isinstance(self.llm.executor, FullyAsyncExecutor):
            return _coerce_fully_async(self.parse_response_udf)(response)
        else:
            return self.parse_response_udf(response)

    def _get_prompt_udf(self, prompt_template):
        if isinstance(prompt_template, pw.UDF) or callable(prompt_template):
            verified_template: prompts.BasePromptTemplate = (
                prompts.RAGFunctionPromptTemplate(function_template=prompt_template)
            )
        elif isinstance(prompt_template, str):
            verified_template = prompts.RAGPromptTemplate(template=prompt_template)
        else:
            raise ValueError(
                f"Template is not of expected type. Got: {type(prompt_template)}."
            )

        return verified_template.as_udf()

    def _get_parse_response_udf(self, response_parser: pw.UDF | Callable) -> pw.UDF:
        response_parser_udf = None
        if isinstance(response_parser, pw.UDF):
            response_parser_udf = response_parser
        elif callable(response_parser):
            response_parser_udf = pw.udf(response_parser)
        else:
            raise ValueError(
                f"Response parser is not of expected type. Got: {type(response_parser)}."
            )

        return response_parser_udf


class CrossEncoderReranker(pw.UDF):
    """Pointwise Cross encoder reranker module.

    Uses the CrossEncoder from the sentence_transformers library.
    For reference, check out `Cross encoders documentation <https://www.sbert.net/docs/pretrained_cross-encoders.html>`_

    Args:
        model_name: Embedding model to be used.
        cache_strategy: Defines the caching mechanism. To enable caching,
            a valid `CacheStrategy` should be provided.
            See `Cache strategy <https://pathway.com/developers/api-docs/udfs#pathway.udfs.CacheStrategy>`_
            for more information. Defaults to None.

    Suggested model: `cross-encoder/ms-marco-TinyBERT-L-2-v2`

    Example:

    >>> import pathway as pw  # doctest: +SKIP
    >>> import pandas as pd  # doctest: +SKIP
    >>> from pathway.xpacks.llm import rerankers  # doctest: +SKIP
    >>> reranker = rerankers.CrossEncoderReranker(model_name="cross-encoder/ms-marco-TinyBERT-L-2-v2")  # doctest: +SKIP
    >>> docs = [{"text": "Something"}, {"text": "Something else"}, {"text": "Pathway"}]  # doctest: +SKIP
    >>> df = pd.DataFrame({"docs": docs, "prompt": "query text"})  # doctest: +SKIP
    >>> table = pw.debug.table_from_pandas(df)  # doctest: +SKIP
    >>> table += table.select(
    ...     reranker_scores=reranker(pw.this.docs["text"], pw.this.prompt)
    ... )  # doctest: +SKIP
    >>> table  # doctest: +SKIP
    <pathway.Table schema={'docs': <class 'pathway.internals.json.Json'>, 'prompt': <class 'str'>, 'reranker_scores': <class 'float'>}>
    """  # noqa: E501

    def __init__(
        self,
        model_name: str,
        *,
        cache_strategy: udfs.CacheStrategy | None = None,
        **init_kwargs,
    ) -> None:
        super().__init__(cache_strategy=cache_strategy)

        with optional_imports("xpack-llm-local"):
            from sentence_transformers import CrossEncoder

        self.model = CrossEncoder(model_name, **init_kwargs)

    def __wrapped__(self, doc: str, query: str, **kwargs) -> float:
        doc, query = _extract_value(doc), _extract_value(query)

        scores = self.model.predict([[query, doc]], **kwargs)
        return scores[0]

    def __call__(
        self, doc: pw.ColumnExpression, query: pw.ColumnExpression, **kwargs
    ) -> pw.ColumnExpression:
        """Evaluates the doc against the query.

        Args:
            doc (pw.ColumnExpression[str]): Document or document chunk to be scored.
            query (pw.ColumnExpression[str]): User query or prompt that will be used
                to evaluate relevance of the doc.
            **kwargs: override for defaults set in the constructor.
        """
        return super().__call__(doc, query, **kwargs)


class EncoderReranker(pw.UDF):
    """Pointwise encoder reranker module.

    Uses the encoders from the sentence_transformers library.
    For reference, check out `Pretrained models documentation <https://www.sbert.net/docs/pretrained_models.html>`_

    Args:
        model_name: Embedding model to be used.
        cache_strategy: Defines the caching mechanism. To enable caching,
            a valid `CacheStrategy` should be provided.
            See `Cache strategy <https://pathway.com/developers/api-docs/udfs#pathway.udfs.CacheStrategy>`_
            for more information. Defaults to None.

    Suggested model: `BAAI/bge-large-zh-v1.5`

    Example:

    >>> import pathway as pw  # doctest: +SKIP
    >>> import pandas as pd  # doctest: +SKIP
    >>> from pathway.xpacks.llm import rerankers  # doctest: +SKIP
    >>> reranker = rerankers.EncoderReranker(model_name="BAAI/bge-large-zh-v1.5")  # doctest: +SKIP
    >>> docs = [{"text": "Something"}, {"text": "Something else"}, {"text": "Pathway"}]  # doctest: +SKIP
    >>> df = pd.DataFrame({"docs": docs, "prompt": "query text"})  # doctest: +SKIP
    >>> table = pw.debug.table_from_pandas(df)  # doctest: +SKIP
    >>> table += table.select(
    ...     reranker_scores=reranker(pw.this.docs["text"], pw.this.prompt)
    ... )  # doctest: +SKIP
    >>> table  # doctest: +SKIP
    <pathway.Table schema={'docs': <class 'pathway.internals.json.Json'>, 'prompt': <class 'str'>, 'reranker_scores': <class 'float'>}>
    """  # noqa: E501

    def __init__(
        self,
        model_name: str,
        *,
        cache_strategy: udfs.CacheStrategy | None = None,
        **init_kwargs,
    ) -> None:
        super().__init__(cache_strategy=cache_strategy)

        with optional_imports("xpack-llm-local"):
            from sentence_transformers import SentenceTransformer

        self.model = SentenceTransformer(model_name, **init_kwargs)

    def __wrapped__(self, doc: str, query: str, **kwargs) -> float:
        doc, query = _extract_value(doc), _extract_value(query)

        embeddings = self.model.encode(
            [query, doc], normalize_embeddings=True, **kwargs
        )

        return embeddings[0] @ embeddings[1].T

    def __call__(
        self, doc: pw.ColumnExpression, query: pw.ColumnExpression, **kwargs
    ) -> pw.ColumnExpression:
        """Evaluates the doc against the query.

        Args:
            doc (pw.ColumnExpression[str]): Document or document chunk to be scored.
            query (pw.ColumnExpression[str]): User query or prompt that will be used
                to evaluate relevance of the doc.
            **kwargs: override for defaults set in the constructor.
        """
        return super().__call__(doc, query, **kwargs)


class FlashRankReranker(pw.UDF):
    """https://github.com/PrithivirajDamodaran/FlashRank"""

    def __init__(
        self,
        model_name: str = "ms-marco-TinyBERT-L-2-v2",
        *,
        cache_strategy: udfs.CacheStrategy | None = None,
        model_cache_dir: str = "flashrank_cache",
        max_length: int = 512,
    ) -> None:
        from flashrank import Ranker

        super().__init__(cache_strategy=cache_strategy)

        self.model = Ranker(
            model_name=model_name, cache_dir=model_cache_dir, max_length=max_length
        )

    def __wrapped__(self, doc: str, query: str):
        doc, query = _extract_value(doc), _extract_value(query)

        from flashrank import RerankRequest

        rerankrequest = RerankRequest(query=query, passages=[{"text": doc}])
        results = self.model.rerank(rerankrequest)
        return results[0]["score"]
