# Copyright © 2024 Pathway

import pathlib
import sys

import pathway as pw
from pathway.tests.utils import CountDifferentTimestampsCallback


def run_graph(
    expected_count: int | None,
    rows_to_generate,
) -> int:
    class InputSchema(pw.Schema):
        number: int

    value_functions = {
        "number": lambda x: 2 * x + 1,
    }

    t = pw.demo.generate_custom_stream(
        value_functions,
        schema=InputSchema,
        nb_rows=rows_to_generate,
        input_rate=15,
        autocommit_duration_ms=50,
        name="1",
    )

    callback = CountDifferentTimestampsCallback(expected_count)

    pw.io.subscribe(t, callback, callback.on_end)

    pw.run()
    return len(callback.timestamps)


def main():
    expected_count: int | None = int(sys.argv[1])
    rows_to_generate = int(sys.argv[2])

    if len(sys.argv) > 3:
        timestamp_file = pathlib.Path(sys.argv[3])
    else:
        timestamp_file = None

    # When generating rows, we can't be sure that new rows will have distinct timestamps,
    # so we don't check their number
    if rows_to_generate > 0:
        expected_count = None
    n_timestamps = run_graph(expected_count, rows_to_generate)
    if timestamp_file is not None:
        timestamp_file.write_text(str(n_timestamps))


if __name__ == "__main__":
    main()
