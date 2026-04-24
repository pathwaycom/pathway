# Copyright © 2026 Pathway

import multiprocessing
import os
import pathlib
import time

import pandas as pd
import pytest

import pathway as pw
from pathway.internals import api
from pathway.internals.parse_graph import G
from pathway.tests.utils import (
    CsvPathwayChecker,
    assert_sets_equality_from_path,
    combine_columns,
    consolidate,
    needs_multiprocessing_fork,
    only_with_license_key,
    run,
    wait_result_with_checker,
    write_csv,
    write_lines,
)

PERSISTENCE_MODES = [
    pw.PersistenceMode.PERSISTING,
    pw.PersistenceMode.OPERATOR_PERSISTING,
]


# --- Common schemas ---


class LabelValSchema(pw.Schema):
    label: str
    val: int


class EventSchema(pw.Schema):
    event_time: int
    flag: bool
    data: str


# --- Sort + iterate chunk propagation pipeline builder ---


def _build_chunk_propagation_pipeline(t):
    """Sort → iterate: propagate chunk_start from flagged rows via prev pointers."""
    t_sorted = t + t.sort(key=t.event_time)
    t_init = t_sorted.with_columns(
        chunk_start=pw.if_else(pw.this.flag, pw.this.event_time, None)
    )

    def step(tab):
        tab_prev = tab.ix(tab.prev, optional=True)
        return tab.with_columns(
            chunk_start=pw.if_else(tab.flag, tab.event_time, tab_prev.chunk_start)
        )

    return pw.iterate(step, tab=t_init)


# --- Generic diff formatting ---


def _format_diffs(old_rows: set, new_rows: set) -> set:
    """Compute expected CSV diffs from old→new state transition.

    Rows are tuples of values.  Returns a set of strings like "10,a,10,1"
    with the diff (+1/-1) appended.
    """
    removed = old_rows - new_rows
    added = new_rows - old_rows
    diffs = set()
    for row in removed:
        diffs.add(",".join(str(v) for v in row) + ",-1")
    for row in added:
        diffs.add(",".join(str(v) for v in row) + ",1")
    return diffs


# --- Common per-file-event runner ---


def _make_event_runner(tmp_path, mode, pipeline_fn):
    """Helper for tests that write one CSV file per event and check diffs.

    ``pipeline_fn(t: pw.Table[EventSchema]) -> pw.Table`` builds the
    pipeline from the input table to the output table.
    """
    input_path = tmp_path / "input"
    os.makedirs(input_path)
    output_path = tmp_path / "out.csv"
    pstorage_path = tmp_path / "PStorage"
    events: dict = {}

    def do_run(changes, expected_diffs):
        for eid, edata in changes.items():
            events[eid] = edata
            et, flag, data = edata
            write_lines(
                input_path / f"{eid}.csv",
                ["event_time,flag,data", f"{et},{flag},{data}"],
            )

        G.clear()
        t = pw.io.csv.read(input_path, schema=EventSchema, mode="static")
        res = pipeline_fn(t)
        pw.io.csv.write(res, output_path)
        run(
            persistence_config=pw.persistence.Config(
                pw.persistence.Backend.filesystem(pstorage_path),
                persistence_mode=mode,
            )
        )
        _assert_diffs_are_unit(output_path)
        _assert_diffs_match(output_path, expected_diffs)

    return events, do_run


# --- Assertion helpers ---


def _assert_diffs_are_unit(output_path: pathlib.Path) -> None:
    """Assert every diff value in the CSV output is exactly +1 or -1."""
    try:
        df = pd.read_csv(output_path)
    except pd.errors.EmptyDataError:
        return  # empty output is fine
    bad = df[~df["diff"].isin([1, -1])]
    assert (
        bad.empty
    ), f"Found non-unit diffs (expected only +1 or -1):\n{bad.to_string()}"


def _make_runner(
    tmp_path: pathlib.Path,
    mode: api.PersistenceMode,
    logic,
    schema: type[pw.Schema],
):
    """Helper: runs iterate pipeline in static mode with persistence across multiple runs.

    Each call writes new input files, rebuilds the graph, and runs with the same
    persistence storage. The expected set uses combine_columns format: all non-time
    columns joined with commas. For a table with columns (label, val), the format
    is "label,val,diff".
    """
    input_path = tmp_path / "input"
    os.makedirs(input_path, exist_ok=True)
    output_path = tmp_path / "out.csv"
    pstorage_path = tmp_path / "PStorage"
    count = 0

    def run_computation(inputs, expected):
        nonlocal count
        count += 1
        G.clear()
        path = input_path / str(count)
        write_lines(path, inputs)
        t = pw.io.csv.read(input_path, schema=schema, mode="static")
        res = logic(t)
        pw.io.csv.write(res, output_path)
        run(
            persistence_config=pw.persistence.Config(
                pw.persistence.Backend.filesystem(pstorage_path),
                persistence_mode=mode,
            )
        )
        _assert_diffs_are_unit(output_path)
        assert_sets_equality_from_path(output_path, expected)

    return run_computation


# --- Static mode multi-run tests ---
#
# IMPORTANT: Every test runs the pipeline at least twice with the same persistence
# storage. A single run would be identical to running without persistence, so it
# proves nothing about persistence correctness.


@pytest.mark.parametrize("persistence_mode", PERSISTENCE_MODES)
@only_with_license_key("persistence_mode", [api.PersistenceMode.OPERATOR_PERSISTING])
@needs_multiprocessing_fork
def test_iterate_persistence_basic(persistence_mode, tmp_path):
    """Basic iterate convergence with persistence across two runs.

    Run 1: compute fixed point for initial data.
    Run 2: add new data, only new results appear (old results already persisted).
    """

    def logic(t):
        def step(iterated):
            return iterated.select(pw.this.label, val=iterated.val + 1)

        return pw.iterate(step, iteration_limit=3, iterated=t)

    runner = _make_runner(tmp_path, persistence_mode, logic, LabelValSchema)

    # Run 1: val + 3 (iteration_limit=3)
    runner(
        ["label,val", "a,10", "b,20", "c,5"],
        {"a,13,1", "b,23,1", "c,8,1"},
    )

    # Run 2: add label d — only d appears (old results already persisted)
    runner(
        ["label,val", "d,100"],
        {"d,103,1"},
    )


@pytest.mark.parametrize("persistence_mode", PERSISTENCE_MODES)
@only_with_license_key("persistence_mode", [api.PersistenceMode.OPERATOR_PERSISTING])
@needs_multiprocessing_fork
def test_iterate_persistence_with_limit(persistence_mode, tmp_path):
    """Iterate with iteration_limit and persistence across two runs."""

    def logic(t):
        def step(iterated):
            return iterated.select(pw.this.label, val=iterated.val + 1)

        return pw.iterate(step, iteration_limit=3, iterated=t)

    runner = _make_runner(tmp_path, persistence_mode, logic, LabelValSchema)

    # Run 1: val=0 -> 3, val=10 -> 13
    runner(
        ["label,val", "x,0", "y,10"],
        {"x,3,1", "y,13,1"},
    )

    # Run 2: add val=100 -> 103
    runner(
        ["label,val", "z,100"],
        {"z,103,1"},
    )


@pytest.mark.parametrize("persistence_mode", PERSISTENCE_MODES)
@only_with_license_key("persistence_mode", [api.PersistenceMode.OPERATOR_PERSISTING])
@needs_multiprocessing_fork
def test_iterate_persistence_multiple_columns(persistence_mode, tmp_path):
    """Iterate with multiple columns converging independently, across two runs."""

    class Schema(pw.Schema):
        label: str
        a: int
        b: int

    def logic(t):
        def step(iterated):
            @pw.udf(deterministic=True)
            def toward_zero(x: int) -> int:
                if x > 0:
                    return x - 1
                if x < 0:
                    return x + 1
                return 0

            return iterated.select(
                pw.this.label,
                a=toward_zero(iterated.a),
                b=toward_zero(iterated.b),
            )

        return pw.iterate(step, iterated=t)

    runner = _make_runner(tmp_path, persistence_mode, logic, Schema)

    # Run 1: two rows with different convergence speeds
    runner(
        ["label,a,b", "p,3,5", "q,-2,1"],
        {"p,0,0,1", "q,0,0,1"},
    )

    # Run 2: add new row
    runner(
        ["label,a,b", "r,10,-7"],
        {"r,0,0,1"},
    )


# ---------------------------------------------------------------------------
# Iterate + sort chunk-splitting pattern (parametrized)
# ---------------------------------------------------------------------------
#
# Pattern: given events with timestamps and a boolean flag, we assign each event
# to the "chunk" started by the most recent flagged event. The iterate body
# propagates chunk_start from flagged rows to subsequent non-flagged rows via
# prev pointers obtained from sort.
#
# Example:
#     event_time | flag  | data | chunk_start (computed)
#     10         | True  | a    | 10           ← flag → starts chunk
#     20         | False | b    | 10           ← inherited from prev (a)
#     30         | True  | c    | 30           ← flag → starts new chunk
#     40         | False | d    | 30           ← inherited from prev (c)
#
# Rows are interdependent: changing one flag cascades through all subsequent rows.
# The iteration converges in N steps for a chain of N non-flagged rows.
#
# Each scenario is a list of "runs". Each run is a dict mapping event_id (string)
# to (event_time, flag, data). The dict is merged into the current event state:
#   - New event_id  → creates a new file (addition)
#   - Existing event_id with different content → overwrites file (modification)
#   - Existing event_id mapped to None → deletes file (removal)
#
# After each run, we verify that the CSV output contains exactly the DIFFS from
# the previous state: deletions (-1) for changed/removed output rows, and
# insertions (+1) for new/changed output rows.
# ---------------------------------------------------------------------------


def _assert_diffs_match(output_path: pathlib.Path, expected: set) -> None:
    """Assert the CSV output matches expected diffs exactly.

    The CSV has columns like (event_time, data, chunk_start, time, diff).
    We consolidate, drop time, and combine remaining columns into strings.
    Diffs must be exactly +1 or -1.
    """
    _assert_diffs_are_unit(output_path)
    try:
        df = consolidate(pd.read_csv(output_path))
    except pd.errors.EmptyDataError:
        assert expected == set(), f"Expected {expected} but output was empty"
        return

    actual = set(combine_columns(df))
    assert (
        actual == expected
    ), f"Diffs mismatch:\n  actual:   {sorted(actual)}\n  expected: {sorted(expected)}"


def _compute_chunk_assignments(events: dict) -> dict:
    """Compute chunk_start for each event given the full event state.

    Args:
        events: dict of event_id → (event_time, flag, data)

    Returns:
        dict of event_time → chunk_start (int or None)
    """
    sorted_events = sorted(events.values(), key=lambda e: e[0])
    assignments = {}
    current_chunk = None
    for event_time, flag, _data in sorted_events:
        if flag:
            current_chunk = event_time
        assignments[event_time] = current_chunk
    return assignments


def _output_rows(events: dict, assignments: dict) -> set:
    """Build the set of (event_time, data, chunk_start) representing full output state."""
    rows = set()
    for _eid, (event_time, _flag, data) in events.items():
        cs = assignments[event_time]
        rows.add((event_time, data, cs))
    return rows


def _compute_expected_diffs(old_rows: set, new_rows: set) -> set:
    """Compute expected CSV output from old→new state transition.

    Returns set of strings like "10,a,10,-1" or "10,a,10,1".
    Each row exists at most once, so diffs are exactly +1 or -1.
    """
    removed = old_rows - new_rows
    added = new_rows - old_rows
    expected = set()
    for event_time, data, cs in removed:
        expected.add(f"{event_time},{data},{cs},-1")
    for event_time, data, cs in added:
        expected.add(f"{event_time},{data},{cs},1")
    return expected


# fmt: off
CHUNK_SCENARIOS = {
    # -----------------------------------------------------------------------
    # Scenario: append new events AFTER all existing ones.
    #
    # Run 1:  10(F,a)  20(b)  30(c)  40(F,d)  50(e)  60(f)
    #         chunk=10 ──────────────  chunk=40 ──────────────
    #
    # Run 2:  + 70(F,g)  80(h)  90(i)
    #           chunk=70 ─────────────
    #
    # Old chunks untouched; only the new chunk appears in output.
    # -----------------------------------------------------------------------
    "append_after": [
        {"a": (10, True, "a"), "b": (20, False, "b"), "c": (30, False, "c"),
         "d": (40, True, "d"), "e": (50, False, "e"), "f": (60, False, "f")},
        {"g": (70, True, "g"), "h": (80, False, "h"), "i": (90, False, "i")},
    ],

    # -----------------------------------------------------------------------
    # Scenario: insert a new FLAGGED event in the middle, splitting a chunk.
    #
    # Run 1:  10(F,a)  20(b)  30(c)  40(d)
    #         chunk=10 ────────────────────
    #
    # Run 2:  + 25(F,x)  ← inserted between 20 and 30
    #         10(F,a)  20(b)  25(F,x)  30(c)  40(d)
    #         chunk=10 ──────  chunk=25 ─────────────
    #
    # Events 30, 40 switch from chunk 10 → chunk 25.
    # Events 10, 20 stay in chunk 10.
    # -----------------------------------------------------------------------
    "insert_middle_with_flag": [
        {"a": (10, True, "a"), "b": (20, False, "b"), "c": (30, False, "c"),
         "d": (40, False, "d")},
        {"x": (25, True, "x")},
    ],

    # -----------------------------------------------------------------------
    # Scenario: insert a new NON-FLAGGED event in the middle.
    #
    # Run 1:  10(F,a)  20(b)  40(c)
    #         chunk=10 ─────────────
    #
    # Run 2:  + 30(x)  ← no flag, joins existing chunk
    #         10(F,a)  20(b)  30(x)  40(c)
    #         chunk=10 ──────────────────
    #
    # Only the new event appears; existing assignments unchanged.
    # -----------------------------------------------------------------------
    "insert_middle_no_flag": [
        {"a": (10, True, "a"), "b": (20, False, "b"), "c": (40, False, "c")},
        {"x": (30, False, "x")},
    ],

    # -----------------------------------------------------------------------
    # Scenario: insert a flagged event BEFORE all existing events.
    #
    # Run 1:  10(F,a)  20(b)  30(c)
    #         chunk=10 ─────────────
    #
    # Run 2:  + 5(F,x)
    #         5(F,x)  10(F,a)  20(b)  30(c)
    #         chunk=5  chunk=10 ─────────────
    #
    # Event 10 has its own flag, so events 10–30 stay in chunk 10.
    # Only the new event 5 appears.
    # -----------------------------------------------------------------------
    "insert_before": [
        {"a": (10, True, "a"), "b": (20, False, "b"), "c": (30, False, "c")},
        {"x": (5, True, "x")},
    ],

    # -----------------------------------------------------------------------
    # Scenario: flip a flag True→False, MERGING two chunks.
    #
    # Run 1:  10(F,a)  20(b)  30(F,c)  40(d)
    #         chunk=10 ──────  chunk=30 ──────
    #
    # Run 2:  event c loses its flag
    #         10(F,a)  20(b)  30(c)  40(d)
    #         chunk=10 ────────────────────
    #
    # Events 30, 40 switch from chunk 30 → chunk 10.
    # -----------------------------------------------------------------------
    "flip_true_to_false": [
        {"a": (10, True, "a"), "b": (20, False, "b"), "c": (30, True, "c"),
         "d": (40, False, "d")},
        {"c": (30, False, "c")},   # overwrite c: flag True→False
    ],

    # -----------------------------------------------------------------------
    # Scenario: flip a flag False→True, SPLITTING a chunk.
    #
    # Run 1:  10(F,a)  20(b)  30(c)  40(d)
    #         chunk=10 ────────────────────
    #
    # Run 2:  event c gains a flag
    #         10(F,a)  20(b)  30(F,c)  40(d)
    #         chunk=10 ──────  chunk=30 ──────
    #
    # Events 30, 40 switch from chunk 10 → chunk 30.
    # -----------------------------------------------------------------------
    "flip_false_to_true": [
        {"a": (10, True, "a"), "b": (20, False, "b"), "c": (30, False, "c"),
         "d": (40, False, "d")},
        {"c": (30, True, "c")},    # overwrite c: flag False→True
    ],

    # -----------------------------------------------------------------------
    # Multi-run: split then rejoin.
    #
    # Run 1:  10(F,a)  20(b)  30(c)  40(d)
    #         chunk=10 ────────────────────
    #
    # Run 2:  c gains flag → split
    #         chunk=10={a,b}  chunk=30={c,d}
    #
    # Run 3:  c loses flag → rejoin
    #         chunk=10={a,b,c,d}       (back to original)
    # -----------------------------------------------------------------------
    "split_then_rejoin": [
        {"a": (10, True, "a"), "b": (20, False, "b"), "c": (30, False, "c"),
         "d": (40, False, "d")},
        {"c": (30, True, "c")},    # split
        {"c": (30, False, "c")},   # rejoin
    ],

    # -----------------------------------------------------------------------
    # Multi-run: split twice.
    #
    # Run 1:  10(F,a)  20(b)  30(c)  40(d)  50(e)
    #         chunk=10 ────────────────────────────
    #
    # Run 2:  c gains flag → first split
    #         chunk=10={a,b}  chunk=30={c,d,e}
    #
    # Run 3:  + 45(F,x) → second split inside chunk=30
    #         chunk=10={a,b}  chunk=30={c,d}  chunk=45={x,e}
    # -----------------------------------------------------------------------
    "split_twice": [
        {"a": (10, True, "a"), "b": (20, False, "b"), "c": (30, False, "c"),
         "d": (40, False, "d"), "e": (50, False, "e")},
        {"c": (30, True, "c")},
        {"x": (45, True, "x")},
    ],

    # -----------------------------------------------------------------------
    # Multi-run: split one pair, merge another.
    #
    # Run 1:  10(F,a)  20(b)  30(F,c)  40(d)  50(F,e)  60(f)
    #         chunk=10 ──────  chunk=30 ──────  chunk=50 ──────
    #
    # Run 2:  c loses flag → merge chunks 10 and 30
    #         chunk=10={a,b,c,d}  chunk=50={e,f}
    #
    # Run 3:  + 55(F,x) → split chunk=50
    #         chunk=10={a,b,c,d}  chunk=50={e}  chunk=55={x,f}
    # -----------------------------------------------------------------------
    "merge_one_split_another": [
        {"a": (10, True, "a"), "b": (20, False, "b"), "c": (30, True, "c"),
         "d": (40, False, "d"), "e": (50, True, "e"), "f": (60, False, "f")},
        {"c": (30, False, "c")},   # merge
        {"x": (55, True, "x")},    # split
    ],

    # -----------------------------------------------------------------------
    # Multi-run: repeated appends (3 runs of adding events).
    #
    # Run 1:  10(F,a)  20(b)
    #         chunk=10 ──────
    #
    # Run 2:  + 30(c)  40(F,d)
    #         chunk=10={a,b,c}  chunk=40={d}
    #
    # Run 3:  + 50(e)  60(f)
    #         chunk=10={a,b,c}  chunk=40={d,e,f}
    #
    # Run 4:  + 70(F,g)
    #         chunk=10={a,b,c}  chunk=40={d,e,f}  chunk=70={g}
    # -----------------------------------------------------------------------
    "repeated_appends": [
        {"a": (10, True, "a"), "b": (20, False, "b")},
        {"c": (30, False, "c"), "d": (40, True, "d")},
        {"e": (50, False, "e"), "f": (60, False, "f")},
        {"g": (70, True, "g")},
    ],

    # -----------------------------------------------------------------------
    # Multi-run: split, append, then merge back.
    #
    # Run 1:  10(F,a)  20(b)  30(c)
    #         chunk=10 ──────────────
    #
    # Run 2:  b gains flag → split
    #         chunk=10={a}  chunk=20={b,c}
    #
    # Run 3:  + 40(d) → extends chunk=20
    #         chunk=10={a}  chunk=20={b,c,d}
    #
    # Run 4:  b loses flag → merge back
    #         chunk=10={a,b,c,d}
    # -----------------------------------------------------------------------
    "split_append_merge": [
        {"a": (10, True, "a"), "b": (20, False, "b"), "c": (30, False, "c")},
        {"b": (20, True, "b")},    # split
        {"d": (40, False, "d")},   # append
        {"b": (20, False, "b")},   # merge back
    ],
}
# fmt: on


@pytest.mark.parametrize("persistence_mode", PERSISTENCE_MODES)
@only_with_license_key("persistence_mode", [api.PersistenceMode.OPERATOR_PERSISTING])
@pytest.mark.parametrize("scenario_name", list(CHUNK_SCENARIOS.keys()))
@needs_multiprocessing_fork
def test_iterate_persistence_sort_propagation(
    scenario_name, persistence_mode, tmp_path
):
    """Iterate + sort chunk-splitting with persistence across multiple runs.

    See CHUNK_SCENARIOS for scenario documentation. Each run:
    1. Applies file additions/modifications to the input directory.
    2. Runs the pipeline (iterate + sort) with the same persistence storage.
    3. Verifies the CSV output contains exactly the expected diffs.
    """
    input_path = tmp_path / "input"
    os.makedirs(input_path)
    output_path = tmp_path / "out.csv"
    pstorage_path = tmp_path / "PStorage"

    runs = CHUNK_SCENARIOS[scenario_name]

    # Accumulated event state across runs
    events: dict = {}
    prev_output_rows: set = set()
    accumulated_state: dict = {}  # (event_time, data, chunk_start) → multiplicity

    for run_idx, run_changes in enumerate(runs):
        # --- Apply changes to event state and write files ---
        for event_id, event_data in run_changes.items():
            if event_data is None:
                events.pop(event_id, None)
                filepath = input_path / f"{event_id}.csv"
                if filepath.exists():
                    os.remove(filepath)
            else:
                events[event_id] = event_data
                event_time, flag, data = event_data
                filepath = input_path / f"{event_id}.csv"
                write_lines(
                    filepath,
                    [
                        "event_time,flag,data",
                        f"{event_time},{flag},{data}",
                    ],
                )

        # --- Compute expected output ---
        assignments = _compute_chunk_assignments(events)
        curr_output_rows = _output_rows(events, assignments)
        expected_diffs = _compute_expected_diffs(prev_output_rows, curr_output_rows)
        assert expected_diffs, (
            f"Run {run_idx + 1} of '{scenario_name}' produces no diffs — "
            "this means it doesn't test persistence (nothing changed)"
        )
        prev_output_rows = curr_output_rows

        # --- Build and run pipeline ---
        G.clear()
        t = pw.io.csv.read(input_path, schema=EventSchema, mode="static")
        result = _build_chunk_propagation_pipeline(t)
        pw.io.csv.write(
            result.select(pw.this.event_time, pw.this.data, pw.this.chunk_start),
            output_path,
        )
        run(
            persistence_config=pw.persistence.Config(
                pw.persistence.Backend.filesystem(pstorage_path),
                persistence_mode=persistence_mode,
            )
        )

        # --- Verify diffs ---
        _assert_diffs_match(output_path, expected_diffs)

        # --- Verify accumulated state ---
        # Apply this run's diffs to the running state and check that every row
        # has multiplicity exactly 1. With diff amplification (the concat bug),
        # applying e.g. diff=-4 to a row with multiplicity +1 would produce -3,
        # which is an impossible state for a table where each row exists once.
        try:
            df = pd.read_csv(output_path)
            for _, row in df.iterrows():
                key = (int(row["event_time"]), row["data"], int(row["chunk_start"]))
                accumulated_state[key] = accumulated_state.get(key, 0) + int(
                    row["diff"]
                )
        except pd.errors.EmptyDataError:
            pass

        for key, mult in accumulated_state.items():
            assert mult in (0, 1), (
                f"Run {run_idx + 1} of '{scenario_name}': row {key} has "
                f"multiplicity {mult} (expected 0 or 1)"
            )

        # The set of rows with multiplicity 1 must equal the expected full state
        active_rows = {k for k, v in accumulated_state.items() if v == 1}
        assert active_rows == curr_output_rows, (
            f"Run {run_idx + 1} of '{scenario_name}': accumulated state "
            f"doesn't match expected.\n"
            f"  missing: {curr_output_rows - active_rows}\n"
            f"  extra:   {active_rows - curr_output_rows}"
        )


# --- Streaming persistence tests ---


@pytest.mark.parametrize("persistence_mode", PERSISTENCE_MODES)
@only_with_license_key("persistence_mode", [api.PersistenceMode.OPERATOR_PERSISTING])
@needs_multiprocessing_fork
def test_iterate_persistence_streaming_restart(persistence_mode, tmp_path):
    """Streaming iterate: run, stop, restart with new data."""
    input_path = tmp_path / "data"
    os.makedirs(input_path)
    output_path_1 = tmp_path / "output1"
    output_path_2 = tmp_path / "output2"
    os.makedirs(output_path_1)
    os.makedirs(output_path_2)
    pstorage_path = tmp_path / "PStorage"

    def run_pipeline(output_path):
        def step(iterated):
            return iterated.select(pw.this.label, val=iterated.val + 1)

        t = pw.io.csv.read(input_path, schema=LabelValSchema)
        res = pw.iterate(step, iteration_limit=3, iterated=t)
        pw.io.csv.write(res, output_path)
        pw.run(
            persistence_config=pw.persistence.Config(
                pw.persistence.Backend.filesystem(pstorage_path),
                snapshot_interval_ms=1000,
                persistence_mode=persistence_mode,
            ),
            monitoring_level=pw.MonitoringLevel.NONE,
        )

    # Run 1: val + 3 (iteration_limit=4)
    write_csv(
        input_path / "1.csv",
        """
        label | val
        a     | 10
        b     | 20
        c     | 5
    """,
    )
    p = multiprocessing.Process(
        target=run_pipeline, daemon=True, args=(output_path_1 / "out.csv",)
    )
    p.start()
    expected = """
       label | val
       a     | 13
       b     | 23
       c     | 8
    """
    wait_result_with_checker(
        CsvPathwayChecker(expected, output_path_1, id_from=["label"]),
        15,
        target=None,
    )
    _assert_diffs_are_unit(output_path_1 / "out.csv")
    time.sleep(2)  # allow persistence snapshot
    p.terminate()
    p.join()

    # Run 2: restart with additional data — only new data appears in output
    write_csv(
        input_path / "2.csv",
        """
        label | val
        d     | 100
    """,
    )
    p = multiprocessing.Process(
        target=run_pipeline, daemon=True, args=(output_path_2 / "out.csv",)
    )
    p.start()
    expected = """
       label | val
       d     | 103
    """
    wait_result_with_checker(
        CsvPathwayChecker(expected, output_path_2, id_from=["label"]),
        15,
        target=None,
    )
    _assert_diffs_are_unit(output_path_2 / "out.csv")
    time.sleep(2)
    p.terminate()
    p.join()


def _snapshot_size(pstorage_path: pathlib.Path, kind: str) -> int:
    """Total bytes of snapshot files under PStorage/streams/.

    Args:
        kind: ``"operator"`` for operator snapshot chunks (filenames with dashes,
              e.g. ``0-1234567890-100``) or ``"input"`` for input snapshot chunks
              (digit-only filenames, e.g. ``1``, ``2``).
    """
    if kind not in ("operator", "input"):
        raise ValueError(f"kind must be 'operator' or 'input', got {kind!r}")
    streams_dir = pstorage_path / "streams"
    if not streams_dir.exists():
        return 0
    total = 0
    for f in streams_dir.rglob("*"):
        is_operator = "-" in f.name
        if f.is_file() and is_operator == (kind == "operator"):
            total += f.stat().st_size
    return total


@pytest.mark.parametrize("persistence_mode", PERSISTENCE_MODES)
@only_with_license_key("persistence_mode", [api.PersistenceMode.OPERATOR_PERSISTING])
@needs_multiprocessing_fork
def test_iterate_persistence_after_aggregation(persistence_mode, tmp_path):
    """Iterate after groupby/reduce with verifiable output values.

    Pipeline: words → groupby/count → iterate (count + 10, iteration_limit=2)
    The iterate adds 10 to each count, so the output is count + 10 for each
    word.  This lets us verify correctness: output = original_count + 10.

    Run 1: 100 words from a vocabulary of 10 → groupby produces 10 rows.
    Run 2: add more words including a new one → verify new and changed counts.
    """
    import random

    input_path = tmp_path / "input"
    os.makedirs(input_path, exist_ok=True)
    output_path = tmp_path / "out.csv"
    pstorage_path = tmp_path / "PStorage"

    vocab_size = 10
    add_amount = 10  # step adds 1, iteration_limit=add_amount applies it 10 times

    class WordSchema(pw.Schema):
        word: str

    rng = random.Random(42)
    words = [f"word{i}" for i in range(vocab_size)]

    from collections import Counter

    all_words: list = []
    prev_output: set = set()
    run_count = 0

    def expected_output():
        counts = Counter(all_words)
        return {(word, count + add_amount) for word, count in counts.items()}

    def do_run(new_words):
        nonlocal run_count
        all_words.extend(new_words)
        run_count += 1
        with open(input_path / f"{run_count}.csv", "w") as f:
            f.write("word\n" + "\n".join(new_words) + "\n")

        G.clear()
        t = pw.io.csv.read(input_path, schema=WordSchema, mode="static")
        counts = t.groupby(pw.this.word).reduce(pw.this.word, count=pw.reducers.count())

        def step(iterated):
            return iterated.select(pw.this.word, count=iterated.count + 1)

        result = pw.iterate(step, iteration_limit=add_amount, iterated=counts)
        pw.io.csv.write(result, output_path)
        run(
            persistence_config=pw.persistence.Config(
                pw.persistence.Backend.filesystem(pstorage_path),
                persistence_mode=persistence_mode,
            )
        )
        _assert_diffs_are_unit(output_path)

    # Run 1: 100 words from vocabulary of 10
    # groupby produces ~10 rows with various counts.
    # iterate adds 10 to each count → output is count + 10.
    run1_words = [rng.choice(words) for _ in range(100)]
    do_run(run1_words)
    rows1 = expected_output()
    _assert_diffs_match(output_path, _format_diffs(prev_output, rows1))
    prev_output = rows1

    # Run 2: 50 more words + a brand new word.
    # Existing words' counts increase, new word appears with count=1+10=11.
    run2_words = [rng.choice(words) for _ in range(50)] + ["brandnew"]
    do_run(run2_words)
    rows2 = expected_output()
    _assert_diffs_match(output_path, _format_diffs(prev_output, rows2))
    prev_output = rows2


@pytest.mark.parametrize("persistence_mode", PERSISTENCE_MODES)
@only_with_license_key("persistence_mode", [api.PersistenceMode.OPERATOR_PERSISTING])
@pytest.mark.parametrize("scenario_name", list(CHUNK_SCENARIOS.keys()))
@needs_multiprocessing_fork
def test_iterate_filter_reduce_pipeline(scenario_name, persistence_mode, tmp_path):
    """Sort + iterate (chunk propagation) → filter (chunk_start > 10) → reduce.

    Uses the same CHUNK_SCENARIOS as test_iterate_persistence_sort_propagation
    but adds a filter and a count-per-chunk reduce after the iterate.
    """

    def compute_expected(evts):
        assignments = _compute_chunk_assignments(evts)
        chunk_counts: dict = {}
        for event_time, cs in assignments.items():
            if cs is not None and cs > 10:
                chunk_counts[cs] = chunk_counts.get(cs, 0) + 1
        return {(cs, cnt) for cs, cnt in chunk_counts.items()}

    def pipeline(t):
        iterated = _build_chunk_propagation_pipeline(t)
        filtered = iterated.filter(pw.coalesce(iterated.chunk_start, 0) > 10)
        return filtered.groupby(filtered.chunk_start).reduce(
            chunk_start=filtered.chunk_start,
            count=pw.reducers.count(),
        )

    events, do_run = _make_event_runner(tmp_path, persistence_mode, pipeline)
    prev_output: set = set()

    for run_changes in CHUNK_SCENARIOS[scenario_name]:
        rows = compute_expected({**events, **run_changes})
        do_run(run_changes, _format_diffs(prev_output, rows))
        prev_output = rows


@pytest.mark.parametrize("persistence_mode", PERSISTENCE_MODES)
@only_with_license_key("persistence_mode", [api.PersistenceMode.OPERATOR_PERSISTING])
@needs_multiprocessing_fork
def test_reduce_iterate_reduce_pipeline(persistence_mode, tmp_path):
    """Reduce → iterate → reduce: iterate between two reductions.

    Pipeline:
        input (sales) → reduce (sum per product)
              → iterate (halve total until ≤ 50)
              → reduce (grand total of all discounted products)

    Products interact because the grand total depends on each product's
    converged value. Adding sales to one product changes the grand total.

    Run 1: 3 products with various totals.
    Run 2: add sales to existing product + new product.
    Run 3: no-op restart.
    """

    class Schema(pw.Schema):
        product: str
        amount: int

    input_path = tmp_path / "input"
    os.makedirs(input_path)
    output_path = tmp_path / "out.csv"
    pstorage_path = tmp_path / "PStorage"

    all_sales: dict = {}
    prev_output: set = set()

    def compute_expected(sales):
        product_totals: dict = {}
        for _sid, (product, amount) in sales.items():
            product_totals[product] = product_totals.get(product, 0) + amount
        discounted = {}
        for product, total in product_totals.items():
            v = total
            while v > 50:
                v = v // 2
            discounted[product] = v
        grand = sum(discounted.values())
        return {(grand,)}

    def do_run(changes, expected_diffs):
        for sid, data in changes.items():
            all_sales[sid] = data
            product, amount = data
            write_lines(
                input_path / f"{sid}.csv",
                ["product,amount", f"{product},{amount}"],
            )

        G.clear()
        t = pw.io.csv.read(input_path, schema=Schema, mode="static")

        product_sums = t.groupby(t.product).reduce(
            product=t.product, total=pw.reducers.sum(t.amount)
        )

        def step(iterated):
            @pw.udf(deterministic=True)
            def halve_if_over(x: int) -> int:
                return x // 2 if x > 50 else x

            return iterated.select(pw.this.product, total=halve_if_over(iterated.total))

        discounted = pw.iterate(step, iterated=product_sums)

        grand = discounted.reduce(
            grand=pw.reducers.sum(discounted.total),
        )
        pw.io.csv.write(grand, output_path)
        run(
            persistence_config=pw.persistence.Config(
                pw.persistence.Backend.filesystem(pstorage_path),
                persistence_mode=persistence_mode,
            )
        )
        _assert_diffs_are_unit(output_path)
        _assert_diffs_match(output_path, expected_diffs)

    # Run 1: widgets=80 (→40), gadgets=30 (→30), gizmos=120 (→60→30)
    # Grand = 40+30+30 = 100
    run1 = {
        "s1": ("widgets", 50),
        "s2": ("widgets", 30),
        "s3": ("gadgets", 30),
        "s4": ("gizmos", 120),
    }
    rows1 = compute_expected({**all_sales, **run1})
    do_run(run1, _format_diffs(prev_output, rows1))
    prev_output = rows1

    # Run 2: add bolts=200 (→100→50), more widgets (+20 → total 100→50)
    # Grand = 50+30+30+50 = 160
    run2 = {
        "s5": ("bolts", 200),
        "s6": ("widgets", 20),
    }
    rows2 = compute_expected({**all_sales, **run2})
    do_run(run2, _format_diffs(prev_output, rows2))
    prev_output = rows2

    # Run 3: no-op restart — no new data, output should be empty
    do_run({}, set())


@pytest.mark.parametrize("persistence_mode", PERSISTENCE_MODES)
@only_with_license_key("persistence_mode", [api.PersistenceMode.OPERATOR_PERSISTING])
@pytest.mark.parametrize("scenario_name", list(CHUNK_SCENARIOS.keys()))
@needs_multiprocessing_fork
def test_iterate_then_reduce_persistence(scenario_name, persistence_mode, tmp_path):
    """Sort + iterate (chunk propagation) → reduce (count per chunk).

    Uses the same CHUNK_SCENARIOS as test_iterate_persistence_sort_propagation
    but adds a count-per-chunk reduce after the iterate.  Verifies that
    iterate's T=0 output doesn't cause double-counting in the reduce.
    """

    def compute_reduce_rows(evts):
        assignments = _compute_chunk_assignments(evts)
        chunk_counts: dict = {}
        for cs in assignments.values():
            if cs is not None:
                chunk_counts[cs] = chunk_counts.get(cs, 0) + 1
        return {(cs, cnt) for cs, cnt in chunk_counts.items()}

    def pipeline(t):
        iterated = _build_chunk_propagation_pipeline(t)
        return iterated.groupby(iterated.chunk_start).reduce(
            chunk_start=iterated.chunk_start,
            count=pw.reducers.count(),
        )

    events, do_run = _make_event_runner(tmp_path, persistence_mode, pipeline)
    prev_output: set = set()

    for run_changes in CHUNK_SCENARIOS[scenario_name]:
        rows = compute_reduce_rows({**events, **run_changes})
        do_run(run_changes, _format_diffs(prev_output, rows))
        prev_output = rows


# ---------------------------------------------------------------------------
# Chained iterates: iterate → iterate with no persistence-aware operator in
# between.  Tests that filter_upstream_t0_for_iterate correctly prevents
# double T=0 entries in the second iterate's Variable.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("persistence_mode", PERSISTENCE_MODES)
@only_with_license_key("persistence_mode", [api.PersistenceMode.OPERATOR_PERSISTING])
@needs_multiprocessing_fork
def test_two_iterates_chained(persistence_mode, tmp_path):
    """Sort → iter1 (propagate chunk_start) → iter2 (propagate tag).

    iter2 uses the same prev pointers (from sort) as iter1 to propagate
    a different value.  Between iter1 and iter2 there is no persistence-
    aware operator, so in OPERATOR_PERSISTING iter2 receives iter1's T=0
    reconverged output alongside its own loaded snapshot — doubling the
    T=0 data.

    Run 1: 3 events in a chain.  Both chunk_start and tag propagate.
    Run 2: add 2 new events at the end — they should inherit the tag.
    """

    def compute_expected(evts):
        sorted_events = sorted(evts.values(), key=lambda e: e[0])
        current_chunk = None
        current_tag = None
        result = set()
        for event_time, flag, data in sorted_events:
            if flag:
                current_chunk = event_time
                current_tag = event_time * 10
            result.add((event_time, data, current_chunk, current_tag))
        return result

    def pipeline(t):
        t_sorted = t + t.sort(key=t.event_time)
        t_init = t_sorted.with_columns(
            chunk_start=pw.if_else(pw.this.flag, pw.this.event_time, None),
            tag=pw.if_else(pw.this.flag, pw.this.event_time * 10, None),
        )

        # iter1: propagate chunk_start
        def step1(tab):
            tab_prev = tab.ix(tab.prev, optional=True)
            return tab.with_columns(
                chunk_start=pw.if_else(tab.flag, tab.event_time, tab_prev.chunk_start)
            )

        result1 = pw.iterate(step1, tab=t_init)

        # iter2: propagate tag through the same prev chain
        def step2(tab):
            tab_prev = tab.ix(tab.prev, optional=True)
            return tab.with_columns(
                tag=pw.if_else(tab.flag, tab.event_time * 10, tab_prev.tag)
            )

        result2 = pw.iterate(step2, tab=result1)
        return result2.select(
            pw.this.event_time,
            pw.this.data,
            pw.this.chunk_start,
            pw.this.tag,
        )

    events, do_run = _make_event_runner(tmp_path, persistence_mode, pipeline)
    prev_output: set = set()

    # Run 1: 10(F,a) 20(b) 30(c)
    # chunk_start: all 10.  tag: all 100.
    run1 = {
        "a": (10, True, "a"),
        "b": (20, False, "b"),
        "c": (30, False, "c"),
    }
    rows1 = compute_expected({**events, **run1})
    do_run(run1, _format_diffs(prev_output, rows1))
    prev_output = rows1

    # Run 2: add 40(d) 50(e) — should inherit chunk_start=10, tag=100
    run2 = {
        "d": (40, False, "d"),
        "e": (50, False, "e"),
    }
    rows2 = compute_expected({**events, **run2})
    do_run(run2, _format_diffs(prev_output, rows2))
    prev_output = rows2
