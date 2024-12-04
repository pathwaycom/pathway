# Copyright © 2024 Pathway

import multiprocessing
import os
import time

import pytest

import pathway as pw
from pathway.tests.utils import (
    CsvPathwayChecker,
    needs_multiprocessing_fork,
    wait_result_with_checker,
    write_csv,
)


@pytest.mark.parametrize(
    "persistence_mode",
    [pw.PersistenceMode.PERSISTING, pw.PersistenceMode.OPERATOR_PERSISTING],
)
@needs_multiprocessing_fork
def test_groupby_count(persistence_mode, tmp_path):
    input_path = tmp_path / "data"
    os.makedirs(input_path)
    output_path = tmp_path / "output"
    os.makedirs(output_path)
    pstorage_path = tmp_path / "PStorage"

    def run(output_path):
        class InputSchema(pw.Schema):
            w: str

        t = pw.io.csv.read(input_path, schema=InputSchema)
        res = t.groupby(pw.this.w).reduce(pw.this.w, c=pw.reducers.count())
        pw.io.csv.write(res, output_path)

        pw.run(
            persistence_config=pw.persistence.Config(
                pw.persistence.Backend.filesystem(pstorage_path),
                snapshot_interval_ms=1000,
                persistence_mode=persistence_mode,
            ),
            monitoring_level=pw.MonitoringLevel.NONE,
        )

    file1 = """
    w
    abc
    def
    foo
    """
    write_csv(input_path / "1.csv", file1)
    p = multiprocessing.Process(target=run, daemon=True, args=(output_path / "1.csv",))
    p.start()
    time.sleep(2)  # sleep to write file2 after some time to simulate streaming behavior
    file2 = """
    w
    foo
    xyz
    """
    write_csv(input_path / "2.csv", file2)
    expected = """
       w | c
     abc | 1
     def | 1
     foo | 2
     xyz | 1
    """
    wait_result_with_checker(
        CsvPathwayChecker(expected, output_path, id_from=["w"]), 10, target=None, step=1
    )
    time.sleep(2)  # sleep needed to save persistence state (see snapshot_interval_ms)
    p.terminate()
    p.join()

    file3 = """
    w
    abc
    xxx
    """
    write_csv(input_path / "3.csv", file3)
    p = multiprocessing.Process(target=run, daemon=True, args=(output_path / "2.csv",))
    p.start()
    time.sleep(2)
    file4 = """
    w
    foo
    """
    write_csv(input_path / "4.csv", file4)
    expected = """
       w | c
     abc | 2
     def | 1
     foo | 3
     xyz | 1
     xxx | 1
    """
    wait_result_with_checker(
        CsvPathwayChecker(expected, output_path, id_from=["w"]), 10, target=None, step=1
    )
    time.sleep(2)
    p.terminate()
    p.join()

    file5 = """
    w
    abc
    def
    """
    write_csv(input_path / "5.csv", file5)
    p = multiprocessing.Process(target=run, daemon=True, args=(output_path / "3.csv",))
    p.start()
    time.sleep(2)
    file6 = """
    w
    xyz
    """
    write_csv(input_path / "6.csv", file6)
    expected = """
       w | c
     abc | 3
     def | 2
     foo | 3
     xyz | 2
     xxx | 1
    """
    wait_result_with_checker(
        CsvPathwayChecker(expected, output_path, id_from=["w"]), 10, target=None
    )
    time.sleep(2)
    p.terminate()
    p.join()
