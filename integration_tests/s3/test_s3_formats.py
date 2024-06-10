import json

import pytest

import pathway as pw

from .base import create_table_for_storage, put_object_into_storage


@pytest.mark.parametrize("storage_type", ["s3", "minio"])
@pytest.mark.parametrize("format", ["binary", "plaintext", "plaintext_by_object"])
def test_formats_without_parsing(storage_type, format, tmp_path, s3_path):
    input_path = f"{s3_path}/input.txt"
    input_full_contents = "abc\n\ndef\nghi\njkl"
    output_path = tmp_path / "output.json"

    put_object_into_storage(storage_type, input_path, input_full_contents)
    table = create_table_for_storage(storage_type, input_path, format)
    pw.io.jsonlines.write(table, output_path)
    pw.run()

    if format in ("binary", "plaintext_by_object"):
        expected_output = (
            [ord(c) for c in input_full_contents]
            if format == "binary"
            else input_full_contents
        )
        with open(output_path) as f:
            result = json.load(f)
            assert result["data"] == expected_output
    else:
        lines = []
        with open(output_path, "r") as f:
            for row in f:
                lines.append(json.loads(row)["data"])
        lines.sort()
        target = input_full_contents.split("\n")
        target.sort()
        assert lines == target


@pytest.mark.parametrize("format", ["csv", "json"])
@pytest.mark.parametrize("storage_type", ["s3", "minio"])
def test_csv_json_formats(format, storage_type, tmp_path, s3_path):
    input_path = f"{s3_path}/input.csv"
    output_path = tmp_path / "output.csv"
    input_full_contents = {
        "csv": "key,value\n1,Hello\n2,World",
        "json": json.dumps({"key": 1, "value": "Hello"})
        + "\n"
        + json.dumps({"key": 2, "value": "World"}),
    }[format]

    class TestKVTableSchema(pw.Schema):
        key: int
        value: str

    put_object_into_storage(storage_type, input_path, input_full_contents)
    table = create_table_for_storage(
        storage_type,
        input_path,
        format,
        schema=TestKVTableSchema,
    )
    pw.io.jsonlines.write(table, output_path)
    pw.run()

    lines = []
    with open(output_path, "r") as f:
        for row in f:
            parsed_row = json.loads(row)
            key = parsed_row["key"]
            value = parsed_row["value"]
            lines.append((key, value))
    lines.sort()
    assert lines == [(1, "Hello"), (2, "World")]


@pytest.mark.parametrize("storage_type", ["s3", "minio"])
def test_empty_bytes_read(storage_type, s3_path: str):
    put_object_into_storage(storage_type, f"{s3_path}/input", "")
    put_object_into_storage(storage_type, f"{s3_path}/input2", "")

    table = create_table_for_storage(storage_type, s3_path, "binary")

    rows = []
    pw.io.subscribe(
        table, on_change=lambda key, row, time, is_addition: rows.append(row)
    )
    pw.run()

    assert (
        rows
        == [
            {
                "data": b"",
            }
        ]
        * 2
    )
