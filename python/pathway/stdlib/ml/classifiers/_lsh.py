# Copyright © 2024 Pathway

"""Implementation of LSH bucketers
They map input data through bucketers, which group similar items together.
Multiple (L = number of ORs) bucketings are returned (usually needed for good quality).

See e.g. http://madscience.ucsd.edu/notes/lec9.pdf for detailed description.


  Typical usage example:

    bucketer = generate_euclidean_lsh_bucketer(d, M, L, A)
    table_bucketed = lsh(table, bucketer)

  The above will generate a flat representation, with L * len(table) rows.
  Instead, you can also directly apply a bucketer to keep the original length of a table:

    data += data.select(buckets=apply(bucketer, data.data))
"""

from __future__ import annotations

import numpy as np

# TODO change to `import pathway as pw` when it is not imported as part of stdlib, OR move the whole file to stdlib
import pathway.internals as pw
from pathway.internals.fingerprints import fingerprint
from pathway.stdlib.utils.col import unpack_col


def generate_euclidean_lsh_bucketer(d: int, M: int, L: int, A=1.0, seed=0):
    """Locality-sensitive hashing in the Euclidean space.
    See e.g. http://madscience.ucsd.edu/notes/lec9.pdf

    d - number of dimensions in the data
    M - number of ANDS
    L - number of ORS
    A - bucket length (after projecting on a line)
    """
    gen = np.random.default_rng(seed=seed)
    total_lines = M * L

    # generate random unit vectors
    random_lines = gen.standard_normal((d, total_lines))
    random_lines = random_lines / np.linalg.norm(random_lines, axis=0)
    shift = gen.random(size=total_lines) * A

    def bucketify(x: np.ndarray) -> np.ndarray:
        buckets = np.floor_divide(x @ random_lines + shift, A).astype(
            int
        )  # project on lines
        split = np.split(buckets, L)
        return np.hstack([fingerprint(X, format="i32") for X in split])

    return bucketify


def generate_cosine_lsh_bucketer(d: int, M: int, L: int, seed=0):
    """Locality-sensitive hashing for the cosine similarity.
    See e.g. http://madscience.ucsd.edu/notes/lec9.pdf

    M - number of ANDS
    L - number of ORS
    """

    gen = np.random.default_rng(seed=seed)
    total_hyperplanes = M * L

    # generate random unit vectors
    random_hyperplanes = gen.standard_normal((d, total_hyperplanes))

    def bucketify(x: np.ndarray) -> np.ndarray:
        signs = (x @ random_hyperplanes >= 0).astype(int)
        # compute single-number bucket identifiers (i.e. single bucket int representing ANDed buckets)
        split = np.split(signs, L)
        powers = 2 ** np.arange(M).reshape(-1, 1)  # powers of two
        return np.hstack([x @ powers for x in split])

    return bucketify


def lsh(data: pw.Table, bucketer, origin_id="origin_id", include_data=True) -> pw.Table:
    """Apply LSH bucketer for each row and flatten the table."""
    flat_data = data.select(
        buckets=pw.apply(lambda x: list(enumerate(bucketer(x))), data.data)
    )
    flat_data = flat_data.flatten(pw.this.buckets, origin_id=origin_id)
    flat_data = flat_data.select(flat_data[origin_id]) + unpack_col(
        flat_data.buckets,
        pw.this.bucketing,
        pw.this.band,
    )
    if include_data:
        flat_data += flat_data.select(
            data.ix(flat_data[origin_id]).data,
        )
    return flat_data
