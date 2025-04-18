import base64
import json

import pytest

import pathway as pw

TEST_FILE_SIZE = 1550383
FOLDER_WITH_ONE_FILE_ID = "1XisWrSjKMCx2jfUW8OSgt6L8veq8c4Mh"
FOLDER_WITH_SYMLINK_ID = "1wS3IdC1oNLxeTV5ZOqGEmz8z15kRmsBt"
FOLDER_WITH_TYPES = "19B2X3HxfCsCh2NQ8-uzrvkuKUV2gqzdj"
# https://drive.google.com/drive/u/0/folders/19B2X3HxfCsCh2NQ8-uzrvkuKUV2gqzdj


@pytest.mark.parametrize("object_size_limit", [None, 100, 2000000])
@pytest.mark.parametrize("with_metadata", [False, True])
def test_single_file_read_with_constraints(
    object_size_limit, with_metadata, tmp_path, credentials_dir
):
    files_table = pw.io.gdrive.read(
        FOLDER_WITH_ONE_FILE_ID,
        mode="static",
        service_user_credentials_file=str(credentials_dir / "credentials.json"),
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
                decoded_data = base64.b64decode(row["data"])
                assert len(decoded_data) == TEST_FILE_SIZE
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
def test_gdrive_symlink(object_size_limit, with_metadata, tmp_path, credentials_dir):
    files_table = pw.io.gdrive.read(
        FOLDER_WITH_SYMLINK_ID,
        mode="static",
        service_user_credentials_file=str(credentials_dir / "credentials.json"),
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


@pytest.mark.parametrize("with_metadata", [False, True])
@pytest.mark.parametrize(
    "name_pattern",
    [
        "*.txt",
        "*.csv",
        ["*.txt", "*.csv"],
        "*.md",
        "*.pdf",
        ["*.txt", "*.pdf", "*.xlsx", "non_existent.txt"],
        ["first.txt", "random.csv"],
        None,
    ],
)
def test_name_pattern_single_filter(
    with_metadata, name_pattern, tmp_path, credentials_dir
):
    object_size_limit = None

    NUM_TXT_FILES = 2
    NUM_CSV_FILES = 1
    NUM_MD_FILES = 1
    NUM_PDF_FILES = 0

    files_table = pw.io.gdrive.read(
        FOLDER_WITH_TYPES,
        mode="static",
        service_user_credentials_file=str(credentials_dir / "credentials.json"),
        object_size_limit=object_size_limit,
        with_metadata=with_metadata,
        file_name_pattern=name_pattern,
    )

    pw.io.jsonlines.write(files_table, tmp_path / "output.jsonl")
    pw.run()

    rows_count = 0
    with open(tmp_path / "output.jsonl", "r") as f:
        for raw_row in f:
            row = json.loads(raw_row)

            if with_metadata:
                metadata = row["_metadata"]
                assert metadata["status"] == pw.io.gdrive.STATUS_DOWNLOADED
            rows_count += 1

    if name_pattern == "*.txt":
        assert rows_count == NUM_TXT_FILES
    elif name_pattern == "*.csv":
        assert rows_count == NUM_CSV_FILES
    elif name_pattern == ["*.txt", "*.csv"]:
        assert rows_count == NUM_TXT_FILES + NUM_CSV_FILES
    elif name_pattern == "*.md":
        assert rows_count == NUM_MD_FILES
    elif name_pattern == "*.pdf":
        assert rows_count == NUM_PDF_FILES
    elif name_pattern == ["*.txt", "*.pdf", "*.xlsx", "non_existent.txt"]:
        assert rows_count == NUM_TXT_FILES + NUM_PDF_FILES
    elif name_pattern == ["first.txt", "random.csv"]:
        assert rows_count == 2
    elif name_pattern is None:
        assert (
            rows_count == NUM_TXT_FILES + NUM_CSV_FILES + NUM_MD_FILES + NUM_PDF_FILES
        )
