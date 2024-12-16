# Copyright © 2024 Pathway

import json
import multiprocessing
import os
import time

import pytest

import pathway as pw
from pathway.internals.parse_graph import G
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


# Each run is denoted by a scenario.
# Each scenario consists of several sequences of commands.
# A scenario is executed as follows:
# - The command sequences are processed one by one.
# - When processing a sequence, the test first applies the commands that are denoted
#   by a sequence and then runs Pathway identity program in static mode.
# - After Pathway program finishes, the output is checked with the exepected output.
#
# There are two possible commands that can appear within scenario:
# - Upsert(X): create or modify a file named X.
# - Delete(X): delete a file named X.
#
# For simplicity, the files are named with natural integers.
# To be reproducible, this test creates all modifications deterministically.
@pytest.mark.parametrize(
    "scenario",
    [
        [["Upsert(1)", "Upsert(2)"], ["Delete(1)", "Delete(2)"]],
        [["Upsert(1)"], ["Upsert(1)"], ["Upsert(1)"]],
        [["Upsert(1)"], ["Upsert(1)"], ["Delete(1)"]],
        [["Upsert(1)"], ["Delete(1)"], ["Upsert(1)"]],
        [["Upsert(1)"], ["Delete(1)"], ["Upsert(2)"]],
        [
            ["Upsert(1)", "Upsert(2)", "Upsert(3)"],
            ["Delete(3)"],
            ["Upsert(4)"],
            ["Upsert(3)"],
        ],
        [
            ["Upsert(1)", "Upsert(2)", "Upsert(3)"],
            ["Delete(2)"],
            ["Delete(3)"],
            ["Delete(1)"],
        ],
        [
            ["Upsert(1)", "Upsert(2)", "Upsert(3)", "Upsert(4)"],
            ["Upsert(2)", "Upsert(3)"],
        ],
        [
            ["Upsert(1)", "Upsert(2)", "Upsert(3)", "Upsert(4)"],
            ["Delete(2)"],
            ["Upsert(3)"],
        ],
        [
            ["Upsert(1)", "Upsert(2)", "Upsert(3)", "Upsert(4)"],
            ["Delete(1)"],
        ],
        [
            ["Upsert(1)", "Upsert(2)", "Upsert(3)", "Upsert(4)"],
            ["Upsert(4)", "Upsert(3)", "Upsert(2)", "Upsert(1)"],
        ],
        [
            ["Upsert(1)", "Upsert(2)", "Upsert(3)", "Upsert(4)"],
            ["Delete(3)", "Delete(2)", "Upsert(1)"],
            ["Delete(1)"],
            ["Upsert(5)", "Upsert(1)", "Upsert(3)"],
        ],
        [
            ["Upsert(1)", "Upsert(2)"],
            ["Delete(2)", "Upsert(1)", "Upsert(3)"],
        ],
    ],
)
def test_persistence_modifications(tmp_path, scenario):
    inputs_path = tmp_path / "inputs"
    output_path = tmp_path / "output.txt"
    pstorage_path = tmp_path / "PStorage"
    os.mkdir(inputs_path)

    def pw_identity_program():
        G.clear()
        persistence_backend = pw.persistence.Backend.filesystem(pstorage_path)
        persistence_config = pw.persistence.Config(persistence_backend)
        table = pw.io.plaintext.read(inputs_path, mode="static")
        pw.io.jsonlines.write(table, output_path)
        pw.run(persistence_config=persistence_config)

    file_contents = {}
    next_file_contents = 0
    for sequence in scenario:
        expected_diffs = []
        for command in sequence:
            used_file_ids = set()
            if command.startswith("Upsert(") and command.endswith(")"):
                file_id = command[len("Upsert(") : -1]
                assert (
                    file_id not in used_file_ids
                ), "Incorrect scenario! File changed more than once in a single sequence"
                used_file_ids.add(file_id)

                # Record old state removal
                old_contents = file_contents.get(file_id)
                if old_contents is not None:
                    expected_diffs.append([old_contents, -1])

                # Handle new state change
                next_file_contents += 1
                new_contents = (
                    "a" * next_file_contents
                )  # This way, the metadata always changes: at least the file size
                file_contents[file_id] = new_contents
                expected_diffs.append([new_contents, 1])
                with open(inputs_path / file_id, "w") as f:
                    f.write(new_contents)
            elif command.startswith("Delete(") and command.endswith(")"):
                file_id = command[len("Delete(") : -1]
                assert (
                    file_id not in used_file_ids
                ), "Incorrect scenario! File changed more than once in a single sequence"
                used_file_ids.add(file_id)

                old_contents = file_contents.pop(file_id, None)
                assert (
                    old_contents is not None
                ), f"Incorrect scenario! Deletion of a nonexistent object {scenario}"
                expected_diffs.append([old_contents, -1])
                os.remove(inputs_path / file_id)
            else:
                raise ValueError(f"Unknown command: {command}")

        pw_identity_program()
        actual_diffs = []
        with open(output_path, "r") as f:
            for row in f:
                row = json.loads(row)
                actual_diffs.append([row["data"], row["diff"]])
        actual_diffs.sort()
        expected_diffs.sort()
        assert actual_diffs == expected_diffs
