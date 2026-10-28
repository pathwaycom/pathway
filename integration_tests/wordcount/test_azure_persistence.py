# Copyright © 2026 Pathway

import json
import logging
import os
import uuid

import pytest
from azure.storage.blob import BlobServiceClient

import pathway as pw
from pathway.internals.parse_graph import G


@pytest.mark.parametrize("trailing_slash", [False, True])
@pytest.mark.parametrize(
    "suffix", ["space + %/данные/%2F", "double//slash", "trailing//"]
)
def test_azure_recovers_snapshots_with_literal_prefix(
    tmp_path, caplog, trailing_slash, suffix
):
    account = os.environ["AZURE_BLOB_STORAGE_ACCOUNT"]
    password = os.environ["AZURE_BLOB_STORAGE_PASSWORD"]
    container = os.environ["AZURE_BLOB_STORAGE_CONTAINER"]
    namespace = f"integration-tests/azure-migration/{uuid.uuid4()}/"
    seed_root = namespace + "seed"
    prefix = namespace + suffix
    root = prefix + "/" if trailing_slash else prefix
    blob_prefix = root if root.endswith("/") else root + "/"
    client = BlobServiceClient(
        f"https://{account}.blob.core.windows.net", credential=password
    ).get_container_client(container)
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "output.jsonl"
    caplog.set_level(logging.DEBUG)

    def run_pipeline(storage_root):
        G.clear()
        table = pw.io.csv.read(
            input_path,
            schema=pw.schema_from_types(value=int),
            mode="static",
            name="azure-migration-input",
        )
        pw.io.jsonlines.write(table, output_path)
        pw.run(
            persistence_config=pw.persistence.Config(
                pw.persistence.Backend.azure(storage_root, account, password, container)
            )
        )

    try:
        input_path.write_text("value\n1\n2\n")
        run_pipeline(seed_root)
        assert sorted(
            json.loads(line)["value"] for line in output_path.read_text().splitlines()
        ) == [1, 2]
        blobs = list(client.list_blobs(name_starts_with=seed_root + "/"))
        assert blobs, "the first run must create snapshots"
        # Seed the exact blob names independently of the Rust client's path
        # handling. A new-client-only round trip could hide normalization bugs.
        for blob in blobs:
            relative = blob.name.removeprefix(seed_root + "/")
            client.upload_blob(
                blob_prefix + relative, client.download_blob(blob.name).readall()
            )

        input_path.write_text("value\n1\n2\n3\n")
        run_pipeline(root)
        assert [
            json.loads(line)["value"] for line in output_path.read_text().splitlines()
        ] == [3]
        input_path.write_text("value\n1\n2\n3\n4\n")
        run_pipeline(root)
        assert [
            json.loads(line)["value"] for line in output_path.read_text().splitlines()
        ] == [4]
        if password in caplog.text or f"SharedKey {account}:" in caplog.text:
            pytest.fail("Azure authentication material appeared in debug logs")
    finally:
        G.clear()
        for blob in client.list_blobs(name_starts_with=namespace):
            client.delete_blob(blob.name)
