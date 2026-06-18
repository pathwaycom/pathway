"""Merge-on-read (Iceberg v2) reader tests.

Pathway's own writer only ever produces *data* files (copy-on-write), so the
round-trip tests in ``test_iceberg.py`` never exercise a table that an external
engine wrote in *merge-on-read* mode, where a row deletion adds a separate
positional/equality *delete file* and leaves the data file untouched.

``pyiceberg`` cannot produce delete files either — its ``delete()`` warns
"Merge on read is not yet supported, falling back to copy-on-write". So these
tests hand-craft a spec-compliant positional delete file + a ``DELETES``
manifest + a new snapshot directly through pyiceberg's low-level primitives,
which is exactly the shape Spark/Flink/Trino emit for ``write.delete.mode=
merge-on-read``.

FRAGILITY NOTE: because pyiceberg exposes no public way to write a delete
file, ``add_position_delete_file`` below assembles the merge-on-read state by
reaching into pyiceberg internals (``ManifestWriterV2``, ``write_manifest_list``,
``Snapshot`` / ``AddSnapshotUpdate`` / ``SetSnapshotRefUpdate`` /
``AssertRefSnapshotId``, ``DataFile`` / ``ManifestEntry`` with the reserved
positional-delete field ids). These are not part of pyiceberg's stable API and
have already changed shape across releases — the constructors for ``DataFile`` /
``ManifestEntry`` and the ``avro_compression`` argument of the manifest writers
differ between the 0.9.x line the CI image pins (see the integration Dockerfile,
intentionally held back for ``pyiceberg[glue]`` compatibility) and newer
releases used in local dev, so the small ``_build_*`` shims below paper over that
gap. If a future pyiceberg version breaks this helper, the right move is to
re-point it at whatever the library then offers — a first-class merge-on-read
writer if one lands, otherwise the updated low-level primitives — or, if
generating a delete file becomes infeasible from Python, to drive an external
engine (e.g. Spark) instead. Treat this file as rewritable/removable scaffolding
around the connector behavior it pins, not as a contract with pyiceberg.

Two behaviors are pinned:

* static read — already correct: ``plan_files`` resolves the delete file and
  the reader applies it, so the deleted row never surfaces.
* streaming read — the reader must *retract* a row once a later snapshot adds a
  delete file that masks it. The incremental diff currently keys file-scan
  tasks on ``(path, start, length)`` only and ignores the attached delete
  files, so a snapshot that adds only a delete file looks like "no change" and
  the deleted row is never retracted.
"""

import inspect
import json
import os
import threading
import time
import uuid
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog.rest import RestCatalog as PyIcebergRestCatalog
from pyiceberg.exceptions import ServerError
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestWriterV2,
    write_manifest_list,
)
from pyiceberg.schema import Schema as PyIcebergSchema
from pyiceberg.table.refs import SnapshotRefType
from pyiceberg.table.snapshots import Operation, Snapshot, Summary
from pyiceberg.table.update import (
    AddSnapshotUpdate,
    AssertRefSnapshotId,
    SetSnapshotRefUpdate,
)
from pyiceberg.typedef import Record
from pyiceberg.types import IntegerType, NestedField, StringType

import pathway as pw
from pathway.internals.parse_graph import G
from pathway.tests.utils import run, wait_result_with_checker

CATALOG_URI = os.environ.get("ICEBERG_CATALOG_URI", "http://iceberg:8181")

# Reserved Iceberg field ids for the positional-delete file schema.
DELETE_FILE_PATH_FIELD_ID = 2147483546
DELETE_POS_FIELD_ID = 2147483545


def _build_data_file(**arguments) -> DataFile:
    """Construct a v2 ``DataFile`` across pyiceberg versions.

    pyiceberg >=0.10 exposes the ``DataFile.from_args`` classmethod, which binds
    every field of the struct (optional stats columns default to ``None``). The
    0.9.x line the CI image pins has no ``from_args``, and ``Record(**named)``
    binds *only* the named fields — so the optional columns (``column_sizes``,
    ``value_counts``, ...) are left unset and avro serialization of the manifest
    entry later raises ``AttributeError`` the moment it walks those positions.
    Pre-seed every struct field to ``None`` so the record is fully bound, then
    overlay the caller's values."""
    if hasattr(DataFile, "from_args"):
        return DataFile.from_args(_table_format_version=2, **arguments)
    from pyiceberg.manifest import DATA_FILE_TYPE

    fields = {field.name: None for field in DATA_FILE_TYPE[2].fields}
    fields.update(arguments)
    return DataFile(2, **fields)


def _build_manifest_entry(**arguments) -> ManifestEntry:
    """Construct a ``ManifestEntry`` across pyiceberg versions. >=0.10 exposes
    ``ManifestEntry.from_args``; 0.9.x builds it directly from keyword fields.
    As with :func:`_build_data_file`, pre-seed every struct field so the record
    is fully bound before avro serialization walks it."""
    if hasattr(ManifestEntry, "from_args"):
        return ManifestEntry.from_args(**arguments)
    from pyiceberg.manifest import DEFAULT_READ_VERSION, MANIFEST_ENTRY_SCHEMAS_STRUCT

    struct = MANIFEST_ENTRY_SCHEMAS_STRUCT[DEFAULT_READ_VERSION]
    fields = {field.name: None for field in struct.fields}
    fields.update(arguments)
    return ManifestEntry(**fields)


def _maybe_avro_compression(target) -> dict:
    """``avro_compression`` is only a parameter of the manifest writers in
    pyiceberg >=0.10; the pinned 0.9.x line rejects it. Pass it only when the
    target signature accepts it."""
    try:
        params = inspect.signature(target).parameters
    except (TypeError, ValueError):
        return {}
    if "avro_compression" in params:
        return {"avro_compression": "deflate"}
    return {}


def _pyiceberg_catalog() -> PyIcebergRestCatalog:
    return PyIcebergRestCatalog("default", uri=CATALOG_URI)


def _pw_catalog() -> pw.io.iceberg.RestCatalog:
    return pw.io.iceberg.RestCatalog(uri=CATALOG_URI)


def _retry_catalog_op(op, *, attempts: int = 5, base_delay: float = 0.2):
    """Mirror ``test_iceberg.py``: the ``tabulario/iceberg-rest`` backend
    intermittently answers ``500 ServerError`` under parallel workers; the same
    request succeeds on retry, so swallow only ``ServerError``."""
    for attempt in range(attempts):
        try:
            return op()
        except ServerError:
            if attempt == attempts - 1:
                raise
            time.sleep(base_delay * (2**attempt))


def _create_v2_table_with_rows(namespace: str, table_name: str):
    """Create a format-version-2 table with four rows in a single data file and
    return ``(catalog, qualified_name, data_file_path)``."""
    catalog = _pyiceberg_catalog()
    _retry_catalog_op(lambda: catalog.create_namespace(namespace))
    schema = PyIcebergSchema(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(2, "name", StringType(), required=False),
    )
    qualified_name = f"{namespace}.{table_name}"
    table = _retry_catalog_op(
        lambda: catalog.create_table(
            qualified_name, schema, properties={"format-version": "2"}
        )
    )
    table.append(
        pa.table(
            {
                "id": pa.array([1, 2, 3, 4], pa.int32()),
                "name": pa.array(["John", "Jane", "Alice", "Bob"]),
            },
            schema=table.schema().as_arrow(),
        )
    )
    table = catalog.load_table(qualified_name)
    data_file_path = next(iter(table.scan().plan_files())).file.file_path
    return catalog, qualified_name, data_file_path


class _DeletesManifestWriterV2(ManifestWriterV2):
    """``write_manifest`` always stamps the manifest as ``DATA`` content; a
    positional-delete file must live in a ``DELETES`` manifest so readers treat
    its entries as delete files rather than data files."""

    def content(self) -> ManifestContent:
        return ManifestContent.DELETES


def add_position_delete_file(catalog, qualified_name: str, deletes):
    """Commit a new snapshot that adds a single positional-delete file masking
    the given ``(data_file_path, position)`` pairs — without rewriting any data
    file. This is the merge-on-read shape external engines produce.

    ``deletes``: list of ``(data_file_path, row_position)`` tuples.
    """
    table = catalog.load_table(qualified_name)
    io = table.io
    current = table.metadata.current_snapshot()
    assert current is not None, "table must already have data committed"
    existing_manifests = current.manifests(io)
    new_snapshot_id = table.metadata.new_snapshot_id()
    sequence_number = table.metadata.next_sequence_number()

    # 1. Positional-delete parquet. file_path is spec type `string` (Utf8) —
    #    iceberg-rust downcasts it to a StringArray and rejects large_string.
    deletes = sorted(deletes)
    delete_schema = pa.schema(
        [
            pa.field(
                "file_path",
                pa.string(),
                nullable=False,
                metadata={b"PARQUET:field_id": str(DELETE_FILE_PATH_FIELD_ID).encode()},
            ),
            pa.field(
                "pos",
                pa.int64(),
                nullable=False,
                metadata={b"PARQUET:field_id": str(DELETE_POS_FIELD_ID).encode()},
            ),
        ]
    )
    delete_table = pa.table(
        {
            "file_path": pa.array([d[0] for d in deletes], pa.string()),
            "pos": pa.array([d[1] for d in deletes], pa.int64()),
        },
        schema=delete_schema,
    )
    buffer = pa.BufferOutputStream()
    pq.write_table(delete_table, buffer)
    payload = buffer.getvalue().to_pybytes()
    delete_file_path = f"{table.location()}/data/pos-delete-{uuid.uuid4().hex}.parquet"
    with io.new_output(delete_file_path).create(overwrite=True) as f:
        f.write(payload)

    # 2. DataFile describing the positional-delete file.
    delete_data_file = _build_data_file(
        content=DataFileContent.POSITION_DELETES,
        file_path=delete_file_path,
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=len(deletes),
        file_size_in_bytes=len(payload),
        spec_id=0,
    )

    # 3. DELETES manifest carrying that file as an ADDED entry.
    manifest_path = f"{table.location()}/metadata/{uuid.uuid4().hex}-deletes.avro"
    writer = _DeletesManifestWriterV2(
        spec=table.spec(),
        schema=table.schema(),
        output_file=io.new_output(manifest_path),
        snapshot_id=new_snapshot_id,
        **_maybe_avro_compression(_DeletesManifestWriterV2),
    )
    with writer:
        writer.add_entry(
            _build_manifest_entry(
                status=ManifestEntryStatus.ADDED,
                snapshot_id=new_snapshot_id,
                sequence_number=None,
                file_sequence_number=None,
                data_file=delete_data_file,
            )
        )
    delete_manifest = writer.to_manifest_file()

    # 4. New manifest list: keep the existing data manifests, add the deletes one.
    manifest_list_path = (
        f"{table.location()}/metadata/snap-{new_snapshot_id}-{uuid.uuid4().hex}.avro"
    )
    with write_manifest_list(
        format_version=2,
        output_file=io.new_output(manifest_list_path),
        snapshot_id=new_snapshot_id,
        parent_snapshot_id=current.snapshot_id,
        sequence_number=sequence_number,
        **_maybe_avro_compression(write_manifest_list),
    ) as manifest_list_writer:
        manifest_list_writer.add_manifests(existing_manifests + [delete_manifest])

    # 5. New snapshot + atomic commit on the `main` branch. These pyiceberg
    # models declare kebab-case field aliases with populate-by-name, and their
    # field types differ across the pinned 0.9.x line and newer releases (e.g.
    # the ref ``type`` is ``Literal["tag", "branch"]`` in 0.9.x vs the
    # ``SnapshotRefType`` enum later). Build the arguments as ``dict[str, Any]``
    # and splat them so the calls type-check against either version's stubs —
    # and against the mypy environment having no pyiceberg installed at all (CI
    # runs mypy without it, where per-call ``type: ignore``s would be reported
    # as unused).
    snapshot_kwargs: dict[str, Any] = {
        "snapshot_id": new_snapshot_id,
        "parent_snapshot_id": current.snapshot_id,
        "manifest_list": manifest_list_path,
        "sequence_number": sequence_number,
        "summary": Summary(operation=Operation.DELETE),
        "schema_id": table.metadata.current_schema_id,
        "timestamp_ms": int(time.time() * 1000),
    }
    snapshot = Snapshot(**snapshot_kwargs)
    assert_ref_kwargs: dict[str, Any] = {
        "snapshot_id": current.snapshot_id,
        "ref": "main",
    }
    set_ref_kwargs: dict[str, Any] = {
        "snapshot_id": new_snapshot_id,
        "parent_snapshot_id": current.snapshot_id,
        "ref_name": "main",
        "type": SnapshotRefType.BRANCH,
    }
    catalog.commit_table(
        table,
        (AssertRefSnapshotId(**assert_ref_kwargs),),
        (
            AddSnapshotUpdate(snapshot=snapshot),
            SetSnapshotRefUpdate(**set_ref_kwargs),
        ),
    )


class _InputSchema(pw.Schema):
    id: int = pw.column_definition(primary_key=True)
    name: str


def test_iceberg_static_read_applies_merge_on_read_position_delete(tmp_path):
    """A static read of a merge-on-read table must not surface a row that a
    positional delete file masks. This already works: the static plan resolves
    delete files against their data files. It also pins that the hand-crafted
    delete file is spec-valid, so a streaming failure is a connector bug, not a
    crafting artifact."""
    namespace = uuid.uuid4().hex
    table_name = uuid.uuid4().hex
    catalog, qualified_name, data_file_path = _create_v2_table_with_rows(
        namespace, table_name
    )
    # Delete the row at position 1 (id=2) via a positional delete file.
    add_position_delete_file(catalog, qualified_name, [(data_file_path, 1)])

    table = pw.io.iceberg.read(
        catalog=_pw_catalog(),
        namespace=[namespace],
        table_name=table_name,
        mode="static",
        schema=_InputSchema,
    )
    seen: list[int] = []
    pw.io.subscribe(
        table,
        on_change=lambda key, row, time, is_addition: (
            seen.append(row["id"]) if is_addition else None
        ),
    )
    run()
    assert sorted(seen) == [1, 3, 4]


def test_iceberg_streaming_read_retracts_row_after_merge_on_read_delete(tmp_path):
    """A streaming read must retract a row once a later snapshot adds a
    positional delete file that masks it — even though the underlying data file
    is unchanged. The reader picks up the four initial rows, then an external
    merge-on-read delete of id=2 is committed; the live set must become
    {1, 3, 4}."""
    namespace = uuid.uuid4().hex
    table_name = uuid.uuid4().hex
    output_path = tmp_path / "output.jsonl"
    catalog, qualified_name, data_file_path = _create_v2_table_with_rows(
        namespace, table_name
    )

    def net_ids() -> frozenset | None:
        if not output_path.exists():
            return None
        counts: dict[int, int] = {}
        with open(output_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                counts[record["id"]] = counts.get(record["id"], 0) + record.get(
                    "diff", 1
                )
        return frozenset(i for i, c in counts.items() if c > 0)

    def apply_delete_once_ingested():
        # Wait for the four initial rows, then commit the merge-on-read delete.
        for _ in range(600):
            if net_ids() == frozenset({1, 2, 3, 4}):
                break
            time.sleep(0.1)
        else:
            return
        add_position_delete_file(catalog, qualified_name, [(data_file_path, 1)])

    table = pw.io.iceberg.read(
        catalog=_pw_catalog(),
        namespace=[namespace],
        table_name=table_name,
        mode="streaming",
        schema=_InputSchema,
    )
    pw.io.jsonlines.write(table, output_path)

    class Checker:
        def __call__(self) -> bool:
            return net_ids() == frozenset({1, 3, 4})

        def provide_information_on_failure(self) -> str:
            return (
                f"live ids = {net_ids()}; expected {{1, 3, 4}} after a "
                "merge-on-read positional delete of id=2"
            )

    driver = threading.Thread(target=apply_delete_once_ingested)
    driver.start()
    try:
        wait_result_with_checker(Checker(), timeout_sec=60)
    finally:
        driver.join(timeout=5)
        G.clear()
