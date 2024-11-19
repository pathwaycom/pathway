import multiprocessing
import resource
import time

import numpy as np

import pathway as pw
from pathway.stdlib.indexing.bm25 import TantivyBM25Factory
from pathway.stdlib.indexing.hybrid_index import HybridIndexFactory
from pathway.stdlib.indexing.nearest_neighbors import UsearchKnnFactory
from pathway.stdlib.indexing.retrievers import InnerIndexFactory


@pw.udf(deterministic=True)
def fake_embedder(x: str) -> np.ndarray:
    i = ord(x[0]) - 96
    return np.array([1, i // 5, i % 5]).astype(np.float64)


class QuerySchema(pw.Schema):
    query: str


class QuerySubject(pw.io.python.ConnectorSubject):
    def __init__(self, n: int) -> None:
        super().__init__()
        self.n = n

    def run(self):
        for i in range(self.n):
            time.sleep(0.001)
            self.next(query=chr(i % 26 + ord("a")))


def run(n: int, index_factory: InnerIndexFactory) -> None:
    query = pw.io.python.read(QuerySubject(n), schema=QuerySchema)
    docs = pw.debug.table_from_rows(
        pw.schema_from_types(doc=str), [("a",), ("b",), ("c",), ("d",), ("x",), ("z",)]
    )

    index = index_factory.build_index(docs.doc, docs)
    res = query + index.query_as_of_now(
        query.query, collapse_rows=True, number_of_matches=3
    ).select(pw.right.doc, pw.right._pw_index_reply_score)

    pw.io.null.write(res)
    pw.run(monitoring_level=pw.MonitoringLevel.NONE)
    assert resource.getrusage(resource.RUSAGE_SELF).ru_maxrss < 1_500_000


def test_index_queries_work_in_constant_memory():
    n = 400_000
    index_factory = HybridIndexFactory(
        [
            UsearchKnnFactory(
                embedder=fake_embedder,
            ),
            TantivyBM25Factory(),
        ]
    )
    p = multiprocessing.Process(
        target=run,
        args=(
            n,
            index_factory,
        ),
    )
    p.start()
    try:
        p.join(timeout=800)
        assert p.exitcode == 0
    finally:
        p.terminate()
        p.join()
