import json
import os

import pytest

import pathway as pw
from pathway.xpacks.connectors import sharepoint as sharepoint_connector

TEST_FILE_SIZE = 1550383
TEST_LINK_SIZE = 170
FOLDER_WITH_ONE_FILE_ID = "Shared Documents/IntegrationTests/simple"
FOLDER_WITH_SYMLINK_ID = "Shared Documents/IntegrationTests/symlink"
FOLDER_WITH_NESTED_FOLDERS_ID = "Shared Documents/IntegrationTests/recursive"
TEST_SHAREPOINT_CREDS = "/".join(__file__.split("/")[:-1]) + "/certificate.pem"


def connector_table(root_path, **kwargs):
    return sharepoint_connector.read(
        url="https://navalgo.sharepoint.com/sites/ConnectorSandbox",
        root_path=root_path,
        tenant=os.environ["SHAREPOINT_TENANT"],
        client_id=os.environ["SHAREPOINT_CLIENT_ID"],
        thumbprint=os.environ["SHAREPOINT_THUMBPRINT"],
        mode="static",
        cert_path=TEST_SHAREPOINT_CREDS,
        **kwargs,
    )


@pytest.mark.parametrize("object_size_limit", [None, 1000, 2000000])
@pytest.mark.parametrize("with_metadata", [False, True])
def test_single_file_read_with_constraints(object_size_limit, with_metadata, tmp_path):
    files_table = connector_table(
        FOLDER_WITH_ONE_FILE_ID,
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
                target_status = sharepoint_connector.STATUS_DOWNLOADED
                assert len(row["data"]) == TEST_FILE_SIZE
            else:
                target_status = sharepoint_connector.STATUS_SIZE_LIMIT_EXCEEDED
                assert len(row["data"]) == 0
            if with_metadata:
                metadata = row["_metadata"]
                assert metadata["status"] == target_status
            rows_count += 1
    assert rows_count == 1


@pytest.mark.parametrize("with_metadata", [True, False])
def test_sharepoint_link(with_metadata, tmp_path):
    files_table = connector_table(FOLDER_WITH_SYMLINK_ID, with_metadata=with_metadata)
    pw.io.jsonlines.write(files_table, tmp_path / "output.jsonl")
    pw.run()
    rows_count = 0
    with open(tmp_path / "output.jsonl", "r") as f:
        for raw_row in f:
            row = json.loads(raw_row)
            assert len(row["data"]) == TEST_LINK_SIZE
            if with_metadata:
                metadata = row["_metadata"]
                assert metadata["status"] == sharepoint_connector.STATUS_DOWNLOADED
            rows_count += 1
    assert rows_count == 1


@pytest.mark.parametrize("recursive", [True, False])
def test_recursive_flag(recursive, tmp_path):
    files_table = connector_table(FOLDER_WITH_NESTED_FOLDERS_ID, recursive=recursive)
    pw.io.jsonlines.write(files_table, tmp_path / "output.jsonl")
    pw.run()
    expected_rows_count = 1 if recursive else 0
    rows_count = 0
    with open(tmp_path / "output.jsonl", "r") as f:
        for _ in f:
            rows_count += 1
    assert rows_count == expected_rows_count
