import json
import os

import pytest
from boto3.dynamodb.types import Binary, Decimal
from utils import EntryCountChecker

import pathway as pw
from pathway.internals.parse_graph import G
from pathway.tests.utils import (
    ExceptionAwareThread,
    wait_result_with_checker,
    write_lines,
)


@pytest.mark.parametrize("with_optionals", [False, True])
@pytest.mark.parametrize("with_sort_key", [False, True])
def test_dynamodb_static_mode_serialization(
    dynamodb, serialization_tester, with_optionals, with_sort_key
):
    (table, _) = serialization_tester.create_variety_table(with_optionals)

    table_name = dynamodb.generate_table_name()
    write_table_kwargs = {
        "table": table,
        "table_name": table_name,
        "partition_key": table.pkey,
        "init_mode": "create_if_not_exists",
    }
    if with_sort_key:
        write_table_kwargs["sort_key"] = table.skey
    pw.io.dynamodb.write(**write_table_kwargs)
    pw.run()

    table_contents = dynamodb.get_table_contents(table_name)
    table_contents.sort(key=lambda item: item["pkey"])
    assert len(table_contents) == 2 if with_optionals else 1
    row_contents = table_contents[0]
    expected_values = {
        "pkey": Decimal("1"),
        "skey": Decimal("10"),
        "floats": {
            "shape": [Decimal("2"), Decimal("2")],
            "elements": [
                Decimal("1.1"),
                Decimal("2.2"),
                Decimal("3.3"),
                Decimal("4.4"),
            ],
        },
        "string": "abcdef",
        "double": Decimal("-5.6"),
        "binary_data": Binary(b"fedcba"),
        "datetime_naive": "2025-01-17T00:00:00.000000000",
        "list_data": ["lorem", None, "ipsum"],
        "integer": Decimal("123"),
        "json_data": '{"a":15,"b":"hello"}',
        "ptr": "^Z5QKEQCDK9ZZ6TSYV0PM0G92JC",
        "duration": Decimal("432000000000000"),
        "floats_flat": {
            "shape": [Decimal("3")],
            "elements": [Decimal("1.1"), Decimal("2.2"), Decimal("3.3")],
        },
        "boolean": True,
        "tuple_data": [Binary(b"world"), True],
        "ints": {
            "shape": [Decimal("2"), Decimal("2"), Decimal("2")],
            "elements": [
                Decimal("1"),
                Decimal("2"),
                Decimal("3"),
                Decimal("4"),
                Decimal("5"),
                Decimal("6"),
                Decimal("7"),
                Decimal("8"),
            ],
        },
        "ints_flat": {
            "shape": [Decimal("3")],
            "elements": [Decimal("9"), Decimal("9"), Decimal("9")],
        },
        "datetime_utc_aware": "2025-01-17T00:00:00.000000000+0000",
    }
    assert row_contents == expected_values


def test_dynamodb_streaming(dynamodb, tmp_path):
    inputs_path = tmp_path / "inputs"
    table_name = dynamodb.generate_table_name()
    os.mkdir(inputs_path)
    inputs = [
        {
            "key": i,
            "value": "a" * i,
        }
        for i in range(10)
    ]

    class InputSchema(pw.Schema):
        key: int = pw.column_definition(primary_key=True)
        value: str

    def streaming_target(rows: list[dict]):
        for index, row in enumerate(rows):
            full_input_path = inputs_path / f"{index}"
            with open(full_input_path, "w") as f:
                json.dump(row, f)
            checker = EntryCountChecker(index + 1, dynamodb, table_name=table_name)
            wait_result_with_checker(checker, 15, target=None)

    table = pw.io.jsonlines.read(
        inputs_path, schema=InputSchema, autocommit_duration_ms=100
    )
    pw.io.dynamodb.write(table, table_name, table.key, init_mode="create_if_not_exists")

    t = ExceptionAwareThread(target=streaming_target, args=(inputs,))
    t.start()
    checker = EntryCountChecker(len(inputs), dynamodb, table_name=table_name)
    wait_result_with_checker(checker, 30)
    # Joining surfaces a failure of the streaming thread instead of silently
    # discarding it.
    t.join()


@pytest.mark.parametrize("append_init_mode", ["default", "create_if_not_exists"])
def test_append_init_mode(dynamodb, append_init_mode):
    # The write with "default" init mode fails, since the table isn't yet created
    table_name = dynamodb.generate_table_name()
    table = pw.debug.table_from_markdown(
        """
            key | value
            0   | zero
        """
    )
    pw.io.dynamodb.write(table, table_name, table.key)
    with pytest.raises(
        ValueError,
        match=(
            f"Failed to create DynamoDB writer: table {table_name} "
            "doesn't exist in the destination storage"
        ),
    ):
        pw.run()

    # The write succeeds and creates a table with a single row
    G.clear()
    table = pw.debug.table_from_markdown(
        """
            key | value
            1   | one
        """
    )
    pw.io.dynamodb.write(table, table_name, table.key, init_mode="create_if_not_exists")
    pw.run()
    table_contents = dynamodb.get_table_contents(table_name)
    assert table_contents == [{"key": 1, "value": "one"}]

    # The third write appends an entry to the table, regardless of the init_mode chosen
    # from {"default", "create_if_not_exists"}
    G.clear()
    table = pw.debug.table_from_markdown(
        """
            key | value
            2   | two
        """
    )
    pw.io.dynamodb.write(table, table_name, table.key, init_mode=append_init_mode)
    pw.run()
    table_contents = dynamodb.get_table_contents(table_name)
    table_contents.sort(key=lambda item: item["key"])
    assert table_contents == [{"key": 1, "value": "one"}, {"key": 2, "value": "two"}]


def test_recreate_init_mode(dynamodb):
    # A table with a single row is created
    table_name = dynamodb.generate_table_name()
    table = pw.debug.table_from_markdown(
        """
            key | value
            1   | one
        """
    )
    pw.io.dynamodb.write(table, table_name, table.key, init_mode="replace")
    pw.run()
    table_contents = dynamodb.get_table_contents(table_name)
    assert table_contents == [{"key": 1, "value": "one"}]

    # The table is overwritten because the init_mode is "replace"
    G.clear()
    table = pw.debug.table_from_markdown(
        """
            key | value
            2   | two
        """
    )
    pw.io.dynamodb.write(table, table_name, table.key, init_mode="replace")
    pw.run()
    table_contents = dynamodb.get_table_contents(table_name)
    table_contents.sort(key=lambda item: item["key"])
    assert table_contents == [{"key": 2, "value": "two"}]


def test_key_overwrite(dynamodb):
    table_name = dynamodb.generate_table_name()
    table = pw.debug.table_from_markdown(
        """
            key | value
            1   | one
        """
    )
    pw.io.dynamodb.write(table, table_name, table.key, init_mode="create_if_not_exists")
    pw.run()
    table_contents = dynamodb.get_table_contents(table_name)
    assert table_contents == [{"key": 1, "value": "one"}]

    G.clear()
    table = pw.debug.table_from_markdown(
        """
            key | value
            1   | two
        """
    )
    pw.io.dynamodb.write(table, table_name, table.key)
    pw.run()
    table_contents = dynamodb.get_table_contents(table_name)
    table_contents.sort(key=lambda item: item["key"])
    assert table_contents == [
        {"key": 1, "value": "two"},
    ]


def test_key_delete(dynamodb, tmp_path):
    table_name = dynamodb.generate_table_name()
    inputs_path = tmp_path / "inputs"
    os.mkdir(inputs_path)
    input_file_path = inputs_path / "input.jsonl"
    pstorage_path = tmp_path / "pstorage"

    class InputSchema(pw.Schema):
        key: int = pw.column_definition(primary_key=True)
        value: str

    def run_one_iteration(input_contents: list[dict]):
        prepared_lines = [json.dumps(x) for x in input_contents]
        write_lines(input_file_path, prepared_lines)
        G.clear()
        table = pw.io.jsonlines.read(inputs_path, mode="static", schema=InputSchema)
        pw.io.dynamodb.write(
            table, table_name, table.key, init_mode="create_if_not_exists"
        )
        pw.run(
            persistence_config=pw.persistence.Config(
                backend=pw.persistence.Backend.filesystem(pstorage_path)
            )
        )
        table_contents = dynamodb.get_table_contents(table_name)
        print(f"Table contents before comparison: {table_contents}")
        for row in table_contents:
            row["key"] = int(row["key"])
        table_contents.sort(key=lambda x: x["key"])
        assert input_contents == table_contents

    run_one_iteration([{"key": 1, "value": "one"}, {"key": 2, "value": "two"}])
    run_one_iteration([{"key": 2, "value": "two"}])


def test_dynamodb_bytes_partition_key(dynamodb):
    # A ``bytes`` column serializes to the DynamoDB ``Binary`` scalar type, which
    # is one of the three valid key attribute types (alongside ``String`` and
    # ``Number``). The connector must therefore accept a ``bytes`` column as a
    # partition key and round-trip its value unchanged.
    table_name = dynamodb.generate_table_name()

    class InputSchema(pw.Schema):
        key: bytes = pw.column_definition(primary_key=True)
        value: str

    table = pw.debug.table_from_rows(
        InputSchema,
        [(b"\x00\x01", "first"), (b"\x02\x03", "second")],
    )
    pw.io.dynamodb.write(table, table_name, table.key, init_mode="create_if_not_exists")
    pw.run()

    table_contents = dynamodb.get_table_contents(table_name)
    table_contents.sort(key=lambda item: bytes(item["key"]))
    assert table_contents == [
        {"key": Binary(b"\x00\x01"), "value": "first"},
        {"key": Binary(b"\x02\x03"), "value": "second"},
    ]


def test_dynamodb_json_partition_key(dynamodb):
    # A ``JSON`` column serializes to the DynamoDB ``String`` scalar type, so it
    # must be usable as a partition key. The stored key is the compact JSON text
    # produced by the serializer.
    table_name = dynamodb.generate_table_name()

    class InputSchema(pw.Schema):
        key: pw.Json = pw.column_definition(primary_key=True)
        value: str

    table = pw.debug.table_from_rows(
        InputSchema,
        [
            (pw.Json.parse('"alpha"'), "first"),
            (pw.Json.parse('{"n": 1}'), "second"),
        ],
    )
    pw.io.dynamodb.write(table, table_name, table.key, init_mode="create_if_not_exists")
    pw.run()

    table_contents = dynamodb.get_table_contents(table_name)
    stored = {item["key"]: item["value"] for item in table_contents}
    assert stored == {'"alpha"': "first", '{"n":1}': "second"}


def test_dynamodb_boolean_column_cannot_be_partition_key(dynamodb):
    # DynamoDB key attributes can only be of scalar type ``String``, ``Number``
    # or ``Binary`` -- there is no ``Boolean`` key type. A ``bool`` column
    # therefore cannot serve as a partition key, and the connector must reject it
    # up front with a clear error, rather than creating a table whose every
    # subsequent write fails with an opaque "Invalid attribute value type"
    # validation error from DynamoDB.
    table_name = dynamodb.generate_table_name()
    table = pw.debug.table_from_markdown(
        """
            flag  | value
            True  | yes
            False | no
        """
    )
    pw.io.dynamodb.write(
        table, table_name, table.flag, init_mode="create_if_not_exists"
    )
    with pytest.raises(
        ValueError,
        match=(
            "Failed to create DynamoDB writer: "
            "the type bool can't be used in the index"
        ),
    ):
        pw.run()


def test_dynamodb_update_within_single_minibatch(dynamodb):
    # When a row's value changes, snapshot semantics retract the old row and
    # insert the new one under the same primary key. Both events can land in the
    # same DynamoDB ``BatchWriteItem`` request, which rejects a batch that
    # contains both a put and a delete for the same key ("Provided list of item
    # keys contains duplicates"). The connector must reconcile the two within the
    # batch -- the insertion wins -- so the update is applied instead of failing.
    table_name = dynamodb.generate_table_name()

    class InputSchema(pw.Schema):
        key: int = pw.column_definition(primary_key=True)
        value: str

    # (key, value, __time__, __diff__): at time 4 the row keyed 1 is updated from
    # "a" to "b", emitting a retraction and an insertion in the same minibatch.
    rows = [
        (1, "a", 2, 1),
        (1, "a", 4, -1),
        (1, "b", 4, 1),
    ]
    table = pw.debug.table_from_rows(InputSchema, rows, is_stream=True)
    pw.io.dynamodb.write(table, table_name, table.key, init_mode="create_if_not_exists")
    pw.run()

    table_contents = dynamodb.get_table_contents(table_name)
    for row in table_contents:
        row["key"] = int(row["key"])
    assert table_contents == [{"key": 1, "value": "b"}]
