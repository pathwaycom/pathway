import json
import pathlib
from typing import Literal

from utils import MONGODB_BASE_NAME, MONGODB_CONNECTION_STRING, MongoDBContext

import pathway as pw
from pathway.internals.parse_graph import G


def write_items_with_connector(
    *,
    mongodb: MongoDBContext,
    test_items: list[dict],
    input_path: pathlib.Path,
    schema: type[pw.Schema],
    output_collection: str,
    output_table_type: Literal["stream_of_changes", "snapshot"],
    persistence_config: pw.persistence.Config | None = None,
) -> list[dict]:
    G.clear()
    with open(input_path, "w") as f:
        for test_item in test_items:
            f.write(json.dumps(test_item) + "\n")
    table = pw.io.jsonlines.read(input_path, schema=schema, mode="static")
    pw.io.mongodb.write(
        table,
        connection_string=MONGODB_CONNECTION_STRING,
        database=MONGODB_BASE_NAME,
        collection=output_collection,
        output_table_type=output_table_type,
    )
    pw.run(persistence_config=persistence_config)

    result = mongodb.get_collection(output_collection, schema.column_names())
    result.sort(key=lambda item: (item["name"], item["available"]))
    return result


def check_special_fields(
    mongodb: MongoDBContext, output_collection: str, *, are_expected: bool
):
    full_collection = mongodb.get_full_collection(output_collection)
    for document in full_collection:
        time_in_document = "time" in document
        diff_in_document = "diff" in document
        assert time_in_document == are_expected, document
        assert diff_in_document == are_expected, document


def test_mongodb_stream_of_changes(tmp_path, mongodb):
    class InputSchema(pw.Schema):
        name: str
        count: int
        price: float
        available: bool

    input_path = tmp_path / "input.txt"
    output_collection = mongodb.generate_collection_name()
    test_items = [
        {"name": "Milk", "count": 500, "price": 1.5, "available": False},
        {"name": "Water", "count": 600, "price": 0.5, "available": True},
    ]
    result = write_items_with_connector(
        mongodb=mongodb,
        test_items=test_items,
        input_path=input_path,
        schema=InputSchema,
        output_collection=output_collection,
        output_table_type="stream_of_changes",
    )
    assert result == test_items
    check_special_fields(mongodb, output_collection, are_expected=True)

    new_test_items = [{"name": "Milk", "count": 500, "price": 1.5, "available": True}]
    result = write_items_with_connector(
        mongodb=mongodb,
        test_items=new_test_items,
        input_path=input_path,
        schema=InputSchema,
        output_collection=output_collection,
        output_table_type="stream_of_changes",
    )
    expected_result = [
        {"name": "Milk", "count": 500, "price": 1.5, "available": False},
        {"name": "Milk", "count": 500, "price": 1.5, "available": True},
        {"name": "Water", "count": 600, "price": 0.5, "available": True},
    ]
    assert result == expected_result
    check_special_fields(mongodb, output_collection, are_expected=True)


def test_mongodb_snapshot(tmp_path, mongodb):
    class InputSchema(pw.Schema):
        name: str = pw.column_definition(primary_key=True)
        count: int
        price: float
        available: bool

    input_path = tmp_path / "input.txt"
    output_collection = mongodb.generate_collection_name()

    test_items = [
        {"name": "Milk", "count": 500, "price": 1.5, "available": False},
        {"name": "Water", "count": 600, "price": 0.5, "available": True},
    ]
    result = write_items_with_connector(
        mongodb=mongodb,
        test_items=test_items,
        input_path=input_path,
        schema=InputSchema,
        output_collection=output_collection,
        output_table_type="snapshot",
    )
    assert result == test_items
    check_special_fields(mongodb, output_collection, are_expected=False)

    new_test_items = [{"name": "Milk", "count": 500, "price": 1.5, "available": True}]
    result = write_items_with_connector(
        mongodb=mongodb,
        test_items=new_test_items,
        input_path=input_path,
        schema=InputSchema,
        output_collection=output_collection,
        output_table_type="snapshot",
    )
    expected_result = [
        {"name": "Milk", "count": 500, "price": 1.5, "available": True},
        {"name": "Water", "count": 600, "price": 0.5, "available": True},
    ]
    assert result == expected_result
    check_special_fields(mongodb, output_collection, are_expected=False)


def test_mongodb_snapshot_remove(tmp_path, mongodb):
    class InputSchema(pw.Schema):
        name: str = pw.column_definition(primary_key=True)
        count: int
        price: float
        available: bool

    input_path = tmp_path / "input.txt"
    pstorage_path = tmp_path / "PStorage"
    persistence_config = pw.persistence.Config(
        backend=pw.persistence.Backend.filesystem(pstorage_path)
    )

    output_collection = mongodb.generate_collection_name()
    test_items = [
        {"name": "Milk", "count": 500, "price": 1.5, "available": False},
        {"name": "Water", "count": 600, "price": 0.5, "available": True},
    ]
    result = write_items_with_connector(
        mongodb=mongodb,
        test_items=test_items,
        input_path=input_path,
        schema=InputSchema,
        output_collection=output_collection,
        output_table_type="snapshot",
        persistence_config=persistence_config,
    )
    assert result == test_items
    check_special_fields(mongodb, output_collection, are_expected=False)

    test_items = [
        {"name": "Water", "count": 600, "price": 0.5, "available": True},
    ]
    result = write_items_with_connector(
        mongodb=mongodb,
        test_items=test_items,
        input_path=input_path,
        schema=InputSchema,
        output_collection=output_collection,
        output_table_type="snapshot",
        persistence_config=persistence_config,
    )
    assert result == test_items
    check_special_fields(mongodb, output_collection, are_expected=False)
