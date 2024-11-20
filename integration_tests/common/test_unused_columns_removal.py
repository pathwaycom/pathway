import multiprocessing
import resource
import time

import numpy as np

import pathway as pw


@pw.udf(deterministic=True)
def embedder(x: str) -> np.ndarray:
    return np.arange(100_000) + ord(x[0])


@pw.udf(deterministic=True)
def anti_embedder(x: np.ndarray) -> str:
    return chr(x[0])


class QuerySchema(pw.Schema):
    query: str


class QuerySubject(pw.io.python.ConnectorSubject):
    def __init__(self, n: int) -> None:
        super().__init__()
        self.n = n

    def run(self):
        for i in range(self.n):
            time.sleep(0.001)
            self.next(query=f"{chr(i%26 + 97)}")


class DocSchema(pw.Schema):
    doc: str


class DocsSubject(pw.io.python.ConnectorSubject):
    def __init__(self, n: int) -> None:
        super().__init__()
        self.n = n

    def run(self):
        for doc in ["a", "b", "c", "d", "x", "z"]:
            self.next(doc=doc)
        time.sleep(0.001 * self.n + 10)


def run(n: int) -> None:
    query = pw.io.python.read(
        QuerySubject(n), schema=QuerySchema, autocommit_duration_ms=100
    )
    max_depth_2 = query.with_columns(vec=embedder(pw.this.query))
    max_depth_3 = max_depth_2.with_columns(c=anti_embedder(pw.this.vec))
    docs = pw.io.python.read(DocsSubject(n), schema=DocSchema)
    res = max_depth_3.join(docs, pw.left.query == pw.right.doc).select(
        pw.left.query, pw.left.c, pw.right.doc
    )

    pw.io.null.write(res)
    pw.run(monitoring_level=pw.MonitoringLevel.NONE)
    assert resource.getrusage(resource.RUSAGE_SELF).ru_maxrss < 1_500_000


def test_big_columns_are_not_stored_if_not_needed():
    n = 100_000
    p = multiprocessing.Process(
        target=run,
        args=(n,),
    )
    p.start()
    try:
        p.join(timeout=400)
        assert p.exitcode == 0
    finally:
        p.terminate()
        p.join()
