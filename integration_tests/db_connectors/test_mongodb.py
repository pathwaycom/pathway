import json

from utils import MONGODB_BASE_NAME, MONGODB_CONNECTION_STRING

import pathway as pw
from pathway.internals.parse_graph import G


def test_mongodb(tmp_path, mongodb):
    class InputSchema(pw.Schema):
        name: str
        count: int
        price: float
        available: bool

    input_path = tmp_path / "input.txt"
    output_collection = mongodb.generate_collection_name()

    def run(test_items: list[dict]) -> None:
        G.clear()
        with open(input_path, "w") as f:
            for test_item in test_items:
                f.write(json.dumps(test_item) + "\n")
        table = pw.io.jsonlines.read(input_path, schema=InputSchema, mode="static")
        pw.io.mongodb.write(
            table,
            connection_string=MONGODB_CONNECTION_STRING,
            database=MONGODB_BASE_NAME,
            collection=output_collection,
        )
        pw.run()

    test_items = [
        {"name": "Milk", "count": 500, "price": 1.5, "available": False},
        {"name": "Water", "count": 600, "price": 0.5, "available": True},
    ]
    run(test_items)

    result = mongodb.get_collection(output_collection, InputSchema.column_names())
    result.sort(key=lambda item: (item["name"], item["available"]))
    assert result == test_items

    new_test_items = [{"name": "Milk", "count": 500, "price": 1.5, "available": True}]
    run(new_test_items)

    result = mongodb.get_collection(output_collection, InputSchema.column_names())
    result.sort(key=lambda item: (item["name"], item["available"]))
    expected_result = [
        {"name": "Milk", "count": 500, "price": 1.5, "available": False},
        {"name": "Milk", "count": 500, "price": 1.5, "available": True},
        {"name": "Water", "count": 600, "price": 0.5, "available": True},
    ]
    assert result == expected_result
