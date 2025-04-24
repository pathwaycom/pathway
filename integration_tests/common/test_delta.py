import pytest
from deltalake import DeltaTable

import pathway as pw
from pathway.internals import api
from pathway.internals.parse_graph import G


@pytest.mark.parametrize(
    "backfilling_thresholds",
    [
        None,
        [api.BackfillingThreshold(field="value", comparison_op=">=", threshold=0)],
    ],
)
def test_deltalake_atomicity(tmp_path, backfilling_thresholds):
    # Stage 1: Generate an input stream that will be saved into a Delta table
    n_test_rows = 300000
    n_parts = 90
    n_seconds_to_generate = 300

    class Schema(pw.Schema, append_only=True):
        value: int
        part: int = pw.column_definition(
            default_value=-1
        )  # stage 2 fails without default value

    t = pw.demo.range_stream(
        nb_rows=n_test_rows,
        input_rate=n_test_rows / n_seconds_to_generate,
        autocommit_duration_ms=100,
    )
    part_size = n_test_rows / n_parts
    t = t.select(
        value=pw.cast(int, pw.this.value),
        part=pw.cast(int, pw.this.value / part_size),
    )

    pw.io.deltalake.write(
        t,
        tmp_path / "dl_order",
        min_commit_frequency=500,
        partition_columns=[t.part],
    )
    pw.run(monitoring_level=pw.MonitoringLevel.NONE)
    G.clear()

    # Stage 2: Move data from the created Delta table into another one, supposedly
    # preserving the order
    t = pw.io.deltalake.read(
        tmp_path / "dl_order",
        schema=Schema,
        mode="static",
        autocommit_duration_ms=10,
        _backfilling_thresholds=backfilling_thresholds,
    )
    pw.io.deltalake.write(
        t,
        tmp_path / "dl_order2",
        min_commit_frequency=100,
        partition_columns=[t.part],
    )
    pw.run(monitoring_level=pw.MonitoringLevel.NONE)
    G.clear()

    # Stage 3: Check that both tables are written in a proper order
    dt = DeltaTable(tmp_path / "dl_order")
    df = dt.to_pandas().sort_values(by=["time", "value"], ascending=True)
    assert df["value"].is_monotonic_increasing

    dt2 = DeltaTable(tmp_path / "dl_order2")
    df2 = dt2.to_pandas().sort_values(by=["time", "value"], ascending=True)
    assert df2["value"].is_monotonic_increasing
