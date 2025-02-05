import base64
import json
import time

import pytest

import pathway as pw
from pathway.tests.utils import FileLinesNumberChecker, wait_result_with_checker

from .base import create_table_for_storage, put_object_into_storage


@pytest.mark.parametrize("storage_type", ["s3", "minio"])
@pytest.mark.parametrize("format", ["binary", "plaintext", "plaintext_by_object"])
@pytest.mark.parametrize("with_metadata", [True, False])
def test_formats_without_parsing(
    storage_type, format, with_metadata, tmp_path, s3_path
):
    input_path = f"{s3_path}/input.txt"
    input_full_contents = "abc\n\ndef\nghi\njkl"
    output_path = tmp_path / "output.json"
    uploaded_at = int(time.time())

    put_object_into_storage(storage_type, input_path, input_full_contents)
    table = create_table_for_storage(
        storage_type, input_path, format, with_metadata=with_metadata
    )
    pw.io.jsonlines.write(table, output_path)
    pw.run()

    def check_metadata(metadata):
        assert uploaded_at <= metadata["modified_at"] <= uploaded_at + 10
        assert metadata["path"] == input_path
        assert metadata["size"] == len(input_full_contents)

    if format in ("binary", "plaintext_by_object"):
        expected_output = (
            base64.b64encode(input_full_contents.encode("utf-8")).decode("utf-8")
            if format == "binary"
            else input_full_contents
        )
        with open(output_path) as f:
            result = json.load(f)
            assert result["data"] == expected_output
            if with_metadata:
                check_metadata(result["_metadata"])
    else:
        lines = []
        with open(output_path, "r") as f:
            for row in f:
                result = json.loads(row)
                lines.append(result["data"])
                if with_metadata:
                    check_metadata(result["_metadata"])
        lines.sort()
        target = input_full_contents.split("\n")
        target.sort()
        assert lines == target


@pytest.mark.parametrize("storage_type", ["s3", "minio"])
@pytest.mark.parametrize("format", ["plaintext", "plaintext_by_object"])
def test_streaming_mode(storage_type, format, tmp_path, s3_path):
    input_path_1 = f"{s3_path}/input_1.txt"
    input_path_2 = f"{s3_path}/input_2.txt"
    input_1_full_contents = "abc\n\ndef\nghi\njkl"
    input_2_full_contents = "mno\npqr"
    output_path = tmp_path / "output.json"

    put_object_into_storage(storage_type, input_path_1, input_1_full_contents)
    put_object_into_storage(storage_type, input_path_2, input_2_full_contents)

    table = create_table_for_storage(storage_type, s3_path, format, mode="streaming")
    pw.io.jsonlines.write(table, output_path)

    expected_lines_count = 7 if format == "plaintext" else 2
    wait_result_with_checker(
        FileLinesNumberChecker(output_path, expected_lines_count), 30, step=1.0
    )


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
