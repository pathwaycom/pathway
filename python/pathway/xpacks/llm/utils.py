import pathway as pw
from pathway.xpacks.llm._utils import _is_text_with_meta, _to_dict


def combine_metadata(
    table: pw.Table,
    from_column: pw.ColumnReference | str = "text",
    to_column: pw.ColumnReference | str = "metadata",
    clean_from_column: bool = True,
) -> pw.Table:
    """
    Combines metadata from one column with another column.
    It is often the case that some UDFs return tuples with text and metadata
    e.g. ("This is manul", {"color": "grayish"}). This function moves metadata from
    one column to another (which possibly already contains some previous metadata),
    and optionally cleans the original column. If there is no ``to_column`` in the original
    table, it will be created as an empty dictionary.

    Args:
        table (pw.Table): Table to operate on.
        from_column (str): Column name with text and metadata.
        to_column (str): Column name where metadata should be moved.
        clean_from_column (bool): If True, the original column will be cleaned from metadata.
            Only text will remain.

    Returns:
        pw.Table: Table with combined metadata.
    """
    from_column_ref: pw.ColumnReference
    to_column_ref: pw.ColumnReference

    @pw.udf
    def move_metadata(text_with_meta: tuple[str, dict], metadata: dict) -> dict:
        if _is_text_with_meta(text_with_meta):
            return {**_to_dict(metadata), **_to_dict(text_with_meta[1])}
        else:
            return metadata

    @pw.udf
    def clean_metadata(text_with_meta: tuple[str, dict] | str) -> str:
        if _is_text_with_meta(text_with_meta):
            return text_with_meta[0]
        elif isinstance(text_with_meta, str):
            return text_with_meta
        else:
            raise ValueError(
                f"Expected string or tuple with string and dict, got {text_with_meta}"
            )

    if isinstance(from_column, str):
        from_column_ref = table[from_column]
    else:
        from_column_ref = from_column

    if isinstance(to_column, str):
        if not hasattr(table.schema, to_column):
            table += table.select(**{to_column: dict()})
        to_column_ref = table[to_column]
    else:
        to_column_ref = to_column

    table = table.with_columns(
        **{
            to_column_ref._name: move_metadata(from_column_ref, to_column_ref),
            from_column_ref._name: (
                clean_metadata(from_column_ref)
                if clean_from_column
                else from_column_ref
            ),
        }
    )

    return table
