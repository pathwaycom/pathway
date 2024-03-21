import json

import pytest

import pathway as pw

TEST_FILE_SIZE = 1550383
FOLDER_WITH_ONE_FILE_ID = "1XisWrSjKMCx2jfUW8OSgt6L8veq8c4Mh"
FOLDER_WITH_SYMLINK_ID = "1wS3IdC1oNLxeTV5ZOqGEmz8z15kRmsBt"
TEST_GDRIVE_CREDS = "/".join(__file__.split("/")[:-1]) + "/credentials.json"


@pytest.mark.parametrize("object_size_limit", [None, 100, 2000000])
@pytest.mark.parametrize("with_metadata", [False, True])
def test_single_file_read_with_constraints(object_size_limit, with_metadata, tmp_path):
    files_table = pw.io.gdrive.read(
        FOLDER_WITH_ONE_FILE_ID,
        mode="static",
        service_user_credentials_file=TEST_GDRIVE_CREDS,
        object_size_limit=object_size_limit,
        with_metadata=with_metadata,
    )
    pw.io.jsonlines.write(files_table, tmp_path / "output.jsonl")
    pw.run()
    rows_count = 0
    with open(tmp_path / "output.jsonl", "r") as f:
        for raw_row in f:
            row = json.loads(raw_row)
            if object_size_limit is None or object_size_limit > TEST_FILE_SIZE:
                target_status = pw.io.gdrive.STATUS_DOWNLOADED
                assert len(row["data"]) == TEST_FILE_SIZE
            else:
                target_status = pw.io.gdrive.STATUS_SIZE_LIMIT_EXCEEDED
                assert len(row["data"]) == 0
            if with_metadata:
                metadata = row["_metadata"]
                assert metadata["status"] == target_status
            rows_count += 1
    assert rows_count == 1


@pytest.mark.parametrize("object_size_limit", [None, 100, 2000000])
@pytest.mark.parametrize("with_metadata", [True, False])
def test_gdrive_symlink(object_size_limit, with_metadata, tmp_path):
    files_table = pw.io.gdrive.read(
        FOLDER_WITH_SYMLINK_ID,
        mode="static",
        service_user_credentials_file=TEST_GDRIVE_CREDS,
        object_size_limit=object_size_limit,
        with_metadata=with_metadata,
    )
    pw.io.jsonlines.write(files_table, tmp_path / "output.jsonl")
    pw.run()
    rows_count = 0
    with open(tmp_path / "output.jsonl", "r") as f:
        for raw_row in f:
            row = json.loads(raw_row)
            assert len(row["data"]) == 0
            if with_metadata:
                metadata = row["_metadata"]
                assert metadata["status"] == pw.io.gdrive.STATUS_SYMLINKS_NOT_SUPPORTED
            rows_count += 1
    assert rows_count == 1
