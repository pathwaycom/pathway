import multiprocessing
import resource
import time

import numpy as np

import pathway as pw

# 80 KB per row: kept in an arrangement, the 100k rows of the run would take
# 8 GB and blow the bound below many times over, while the rows in flight at
# any moment (see `max_backlog_size` below) stay far from it even on a node
# where every allocation is retained longer than at home.
VECTOR_SIZE = 10_000


@pw.udf(deterministic=True)
def embedder(x: str) -> np.ndarray:
    return np.arange(VECTOR_SIZE) + ord(x[0])


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
    # The source produces a query every millisecond no matter how fast the
    # engine consumes them, so without backpressure the rows that pile up
    # while the engine is busy land in one mini-batch, and every row of that
    # batch materializes its `vec` at once: on a loaded CI node the peak RSS
    # of this pipeline exceeded the bound below with only a few thousand rows
    # in flight (measured with 800 KB vectors: 7.7 GB at 20k rows with a 2 ms
    # slowdown per row, 0.5 GB when keeping up). Bounding the rows in flight
    # bounds that transient; what the test is about - `vec` not being kept in
    # the arrangements of the downstream operators - is independent of it:
    # were it stored, the RSS would grow with `n` regardless of the bound.
    query = pw.io.python.read(
        QuerySubject(n),
        schema=QuerySchema,
        autocommit_duration_ms=100,
        max_backlog_size=500,
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
