import pytest

import pathway as pw
from pathway.tests.utils import (
    ExceptionAwareThread,
    FileLinesNumberChecker,
    wait_result_with_checker,
)

from .base import (
    create_table_for_storage,
    delete_object_from_storage,
    put_object_into_storage,
)


@pytest.mark.parametrize("storage_type", ["s3", "minio"])
def test_object_modified(tmp_path, s3_path, storage_type):
    input_path = f"{s3_path}/input.txt"
    contents = ["one", "two", "three"]
    put_object_into_storage(storage_type, input_path, "\n".join(contents) + "\n")

    output_path = tmp_path / "output.json"

    def stream_data():
        wait_result_with_checker(
            FileLinesNumberChecker(output_path, 3), 30, target=None
        )

        contents.append("four")
        put_object_into_storage(storage_type, input_path, "\n".join(contents) + "\n")
        wait_result_with_checker(
            FileLinesNumberChecker(output_path, 4), 30, target=None
        )

        contents.append("five")
        contents.append("six")
        put_object_into_storage(storage_type, input_path, "\n".join(contents) + "\n")
        wait_result_with_checker(
            FileLinesNumberChecker(output_path, 6), 30, target=None
        )

    table = create_table_for_storage(
        storage_type, s3_path, "plaintext", mode="streaming"
    )
    pw.io.jsonlines.write(table, output_path)

    t = ExceptionAwareThread(target=stream_data)
    t.start()
    wait_result_with_checker(FileLinesNumberChecker(output_path, 6), 90)
    t.join()


@pytest.mark.parametrize("storage_type", ["s3", "minio"])
def test_object_deleted(tmp_path, s3_path, storage_type):
    input_path_1 = f"{s3_path}/input_1.txt"
    input_path_2 = f"{s3_path}/input_2.txt"
    input_path_3 = f"{s3_path}/input_3.txt"
    put_object_into_storage(storage_type, input_path_1, "one")

    output_path = tmp_path / "output.json"

    def stream_data():
        wait_result_with_checker(
            FileLinesNumberChecker(output_path, 1), 30, target=None
        )

        put_object_into_storage(storage_type, input_path_2, "two")
        wait_result_with_checker(
            FileLinesNumberChecker(output_path, 2), 30, target=None
        )

        delete_object_from_storage(storage_type, input_path_1)
        delete_object_from_storage(storage_type, input_path_2)
        wait_result_with_checker(
            FileLinesNumberChecker(output_path, 4), 30, target=None
        )

        put_object_into_storage(storage_type, input_path_1, "four")
        put_object_into_storage(storage_type, input_path_3, "three")
        wait_result_with_checker(
            FileLinesNumberChecker(output_path, 6), 30, target=None
        )

    table = create_table_for_storage(storage_type, s3_path, "binary", mode="streaming")
    pw.io.jsonlines.write(table, output_path)

    t = ExceptionAwareThread(target=stream_data)
    t.start()
    wait_result_with_checker(FileLinesNumberChecker(output_path, 6), 90)
    t.join()
