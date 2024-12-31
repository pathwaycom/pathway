from pathway.internals import api, datasink
from pathway.internals._io_helpers import _format_output_value_fields
from pathway.internals.config import _check_entitlements
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame


@check_arg_types
@trace_user_frame
def write(
    table: Table,
    catalog_uri: str,
    namespace: list[str],
    table_name: str,
    *,
    warehouse: str | None = None,
    min_commit_frequency: int | None = 60_000,
):
    _check_entitlements("iceberg")
    data_storage = api.DataStorage(
        storage_type="iceberg",
        path=catalog_uri,
        min_commit_frequency=min_commit_frequency,
        database=warehouse,
        table_name=table_name,
        namespace=namespace,
    )

    data_format = api.DataFormat(
        format_type="identity",
        key_field_names=None,
        value_fields=_format_output_value_fields(table),
    )

    table.to(
        datasink.GenericDataSink(
            data_storage,
            data_format,
            datasink_name="iceberg",
        )
    )
