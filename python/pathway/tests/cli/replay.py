import sys

import pathway as pw


class CountDifferentTimestampsCallback(pw.io.OnChangeCallback):
    times: set[int]

    def __init__(self, expected):
        self.times = set()
        self.expected = expected

    def __call__(self, key, row, time: int, is_addition):
        self.times.add(time)

    def on_end(self):
        assert len(self.times) == self.expected


def run_graph(
    expected_count,
    rows_to_generate,
):
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
        persistent_id="1",
    )

    callback = CountDifferentTimestampsCallback(expected_count)

    pw.io.subscribe(t, callback, callback.on_end)

    pw.run()


def main():
    expected_count = int(sys.argv[1])
    rows_to_generate = int(sys.argv[2])
    run_graph(expected_count, rows_to_generate)


if __name__ == "__main__":
    main()
