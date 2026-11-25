# Copyright © 2026 Pathway

import threading
import warnings
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable

import pytest

from pathway.internals.runtime_type_check import check_arg_types


@pytest.mark.timeout(30)
def test_checked_functions_preserve_concurrent_calls_and_warnings():
    barrier = threading.Barrier(4, timeout=5)

    @check_arg_types
    def emit(values: Iterable[int]) -> int:
        value = sum(values)
        barrier.wait()
        warnings.warn(f"parallel-value-{value}")
        return value

    def run(worker):
        return [emit([worker * 20 + index]) for index in range(20)]

    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        with ThreadPoolExecutor(max_workers=4) as executor:
            rows = list(executor.map(run, range(4)))

    assert sorted(value for row in rows for value in row) == list(range(80))
    assert sorted(
        str(warning.message)
        for warning in recorded
        if str(warning.message).startswith("parallel-value-")
    ) == sorted(f"parallel-value-{value}" for value in range(80))

    with pytest.raises(TypeError):
        emit(["wrong"])
