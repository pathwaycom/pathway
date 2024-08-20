import logging
import re

import pathway as pw
from pathway.internals import udfs
from pathway.optional_import import optional_imports
from pathway.xpacks.llm import Doc, llms
from pathway.xpacks.llm._utils import _coerce_sync, _extract_value
from pathway.xpacks.llm.llms import prompt_chat_single_qa

logger = logging.getLogger(__name__)


@pw.udf
def rerank_topk_filter(
    docs: list[Doc], scores: list[float], k: int = 5
) -> tuple[list[Doc], list[float]]:
    """Apply top-k filtering to docs using the relevance scores.

    Args:
        - docs: A column with lists of  documents or chunks to rank. Each row in this column
            is filtered separately.
        - scores: A column with lists of re-ranking scores for chunks.
        - k: Number of documents to keep after filtering.

    >>> import pathway as pw
    >>> from pathway.xpacks.llm import rerankers
    >>> import pandas as pd
    >>> retrieved_docs = [{"text": "Something"}, {"text": "Something else"}, {"text": "Pathway"}]
    >>> df = pd.DataFrame({"docs": retrieved_docs, "reranker_scores": [1.0, 3.0, 2.0]})
    >>> table = pw.debug.table_from_pandas(df)
    >>> docs_table = table.reduce(
    ... doc_list=pw.reducers.tuple(pw.this.docs),
    ... score_list=pw.reducers.tuple(pw.this.reranker_scores),
    ... )
    >>> docs_table = docs_table.select(
    ... docs_scores_tuple=rerankers.rerank_topk_filter(
    ... pw.this.doc_list, pw.this.score_list, 2
    ... )
    ... )
    >>> docs_table = docs_table.select(
    ... doc_list=pw.this.docs_scores_tuple[0],
    ... score_list=pw.this.docs_scores_tuple[1],
    ... )
    >>> pw.debug.compute_and_print(docs_table, include_id=False)
    doc_list                                                            | score_list
    (pw.Json({'text': 'Something else'}), pw.Json({'text': 'Pathway'})) | (3.0, 2.0)
    """  # noqa: E501
    docs, scores = zip(*sorted(zip(docs, scores), key=lambda tup: tup[1], reverse=True))
    logging.info(f"Number of docs after rerank: {len(docs[:k])}\nScores: {scores[:k]}")
    return (docs[:k], scores[:k])


class LLMReranker(pw.UDF):
    """Pointwise LLM reranking module.

    Asks LLM to evaluate a given doc against a query between 1 and 5.

    Args:
        - llm: Chat instance to be called during reranking.
        - retry_strategy: Strategy for handling retries in case of failures.
            Defaults to None, meaning no retries.
        - cache_strategy: Defines the caching mechanism. To enable caching,
            a valid `CacheStrategy` should be provided.
            See `Cache strategy <https://pathway.com/developers/api-docs/udfs#pathway.udfs.CacheStrategy>`_
            for more information. Defaults to None.
        - use_logit_bias: bool or None. Setting it as `None` checks if the LLM provider supports
            `logit_bias` argument, it can be overridden by setting it as `True` or `False`.
            Defaults to `None`.

    Example:

    >>> import pathway as pw
    >>> import pandas as pd
    >>> from pathway.xpacks.llm import rerankers, llms
    >>> chat = llms.OpenAIChat(model="gpt-3.5-turbo")
    >>> reranker = rerankers.LLMReranker(chat)
    >>> docs = [{"text": "Something"}, {"text": "Something else"}, {"text": "Pathway"}]
    >>> df = pd.DataFrame({"docs": docs, "prompt": "query text"})
    >>> table = pw.debug.table_from_pandas(df)
    >>> table += table.select(
    ... reranker_scores=reranker(pw.this.docs["text"], pw.this.prompt)
    ... )
    >>> table
    <pathway.Table schema={'docs': <class 'pathway.internals.json.Json'>, 'prompt': <class 'str'>, 'reranker_scores': <class 'float'>}>
    """  # noqa: E501

    number_biases = {"1": 1, "2": 1, "3": 1, "4": 1, "5": 1}

    def __init__(
        self,
        llm: llms.BaseChat,
        *,
        retry_strategy: (
            udfs.AsyncRetryStrategy | None
        ) = udfs.ExponentialBackoffRetryStrategy(max_retries=6),
        cache_strategy: udfs.CacheStrategy | None = None,
        use_logit_bias: bool | None = None,
    ) -> None:
        executor = udfs.async_executor(
            retry_strategy=retry_strategy,
        )
        super().__init__(
            executor=executor,
            cache_strategy=cache_strategy,
        )
        self.llm = llm

        if use_logit_bias is None:
            use_logit_bias = self.llm._accepts_call_arg("logit_bias")
            logger.info(
                f"""`use_logit_bias` not specified, setting to model default: {use_logit_bias}"""
            )

        self.use_logit_bias = use_logit_bias

    def __wrapped__(self, doc: str, query: str, **kwargs) -> float:
        doc, query = _extract_value(doc), _extract_value(query)

        prompt = (
            "You are a helper agent for RAG applications. "
            "Given a question, and a context document, "
            "rate the documents relevance for the question between 1 and 5 as number.\n"
            "5 means the question can be answered based on the given document, and document is very helpful for the question. "  # noqa: E501
            "1 means document is totally unrelated to the question. "
            "Now, it is your turn.\n"
            f"Context document: `{doc}`\n"
            f"Question: `{query}`\nScore:"
        )

        call_kwargs: dict = dict(
            max_tokens=1,
            temperature=0,
        )

        if self.use_logit_bias:
            call_kwargs["logit_bias"] = self._map_dict_keys_token(self.number_biases)

        call_kwargs.update(kwargs)

        response = _coerce_sync(self.llm.__wrapped__)(
            prompt_chat_single_qa.__wrapped__(prompt),
            **call_kwargs,
        )

        return float(self.get_first_number(response))

    @staticmethod
    def _map_dict_keys_token(dc: dict) -> dict:
        import tiktoken

        tokenizer = tiktoken.encoding_for_model("gpt-4")
        return {tokenizer.encode(k)[0]: v for k, v in dc.items()}

    def get_first_number(self, text: str) -> int:
        match = re.search(r"\b(\d+)\b", text)

        if match:
            number = int(match.group(1))
            if number < 1 or number > 5:
                raise ValueError("Ranking should be between 1 and 5")
            return number
        else:
            raise ValueError(
                f"Expected a number in the text, no number found in `{text}`."
            )

    def __call__(
        self, doc: pw.ColumnExpression, query: pw.ColumnExpression, **kwargs
    ) -> pw.ColumnExpression:
        """Evaluates the doc against the query.

        Args:
            - doc (pw.ColumnExpression[str]): Document or document chunk to be scored.
            - query (pw.ColumnExpression[str]): User query or prompt that will be used
                to evaluate relevance of the doc.
            - **kwargs: override for defaults set in the constructor
        """
        return super().__call__(doc, query, **kwargs)


class CrossEncoderReranker(pw.UDF):
    """Pointwise Cross encoder reranker module.

    Uses the CrossEncoder from the sentence_transformers library.
    For reference, check out `Cross encoders documentation <https://www.sbert.net/docs/pretrained_cross-encoders.html>`_

    Args:
        - model_name: Embedding model to be used.
        - cache_strategy: Defines the caching mechanism. To enable caching,
            a valid `CacheStrategy` should be provided.
            See `Cache strategy <https://pathway.com/developers/api-docs/udfs#pathway.udfs.CacheStrategy>`_
            for more information. Defaults to None.

    Suggested model: `cross-encoder/ms-marco-TinyBERT-L-2-v2`

    Example:

    >>> import pathway as pw
    >>> import pandas as pd
    >>> from pathway.xpacks.llm import rerankers
    >>> reranker = rerankers.CrossEncoderReranker(model_name="cross-encoder/ms-marco-TinyBERT-L-2-v2")
    >>> docs = [{"text": "Something"}, {"text": "Something else"}, {"text": "Pathway"}]
    >>> df = pd.DataFrame({"docs": docs, "prompt": "query text"})
    >>> table = pw.debug.table_from_pandas(df)
    >>> table += table.select(
    ... reranker_scores=reranker(pw.this.docs["text"], pw.this.prompt)
    ... )
    >>> table
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
            - doc (pw.ColumnExpression[str]): Document or document chunk to be scored.
            - query (pw.ColumnExpression[str]): User query or prompt that will be used
                to evaluate relevance of the doc.
            - **kwargs: override for defaults set in the constructor.
        """
        return super().__call__(doc, query, **kwargs)


class EncoderReranker(pw.UDF):
    """Pointwise encoder reranker module.

    Uses the encoders from the sentence_transformers library.
    For reference, check out `Pretrained models documentation <https://www.sbert.net/docs/pretrained_models.html>`_

    Args:
        - model_name: Embedding model to be used.
        - cache_strategy: Defines the caching mechanism. To enable caching,
            a valid `CacheStrategy` should be provided.
            See `Cache strategy <https://pathway.com/developers/api-docs/udfs#pathway.udfs.CacheStrategy>`_
            for more information. Defaults to None.

    Suggested model: `BAAI/bge-large-zh-v1.5`

    Example:

    >>> import pathway as pw
    >>> import pandas as pd
    >>> from pathway.xpacks.llm import rerankers
    >>> reranker = rerankers.EncoderReranker(model_name="BAAI/bge-large-zh-v1.5")
    >>> docs = [{"text": "Something"}, {"text": "Something else"}, {"text": "Pathway"}]
    >>> df = pd.DataFrame({"docs": docs, "prompt": "query text"})
    >>> table = pw.debug.table_from_pandas(df)
    >>> table += table.select(
    ... reranker_scores=reranker(pw.this.docs["text"], pw.this.prompt)
    ... )
    >>> table
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
            - doc (pw.ColumnExpression[str]): Document or document chunk to be scored.
            - query (pw.ColumnExpression[str]): User query or prompt that will be used
                to evaluate relevance of the doc.
            - **kwargs: override for defaults set in the constructor.
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
