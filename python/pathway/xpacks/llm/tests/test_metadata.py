import pandas as pd
import pytest

import pathway as pw
from pathway.tests.utils import assert_table_equality
from pathway.xpacks.llm.utils import combine_metadata


@pytest.mark.parametrize(
    "clean_from_column",
    [True, False],
)
def test_combine_metadata(clean_from_column):
    data = {"text": [("Text", {"tag": "test"})], "metadata": [{"meta": "data"}]}
    expected = {
        "text": ["Text"] if clean_from_column else [("Text", {"tag": "test"})],
        "metadata": [{"meta": "data", "tag": "test"}],
    }

    df = pd.DataFrame(data)
    table = pw.debug.table_from_pandas(df)

    df_expected = pd.DataFrame(expected)
    table_expected = pw.debug.table_from_pandas(df_expected)

    table = combine_metadata(
        table,
        from_column="text",
        to_column="metadata",
        clean_from_column=clean_from_column,
    )
    assert_table_equality(table, table_expected)


@pytest.mark.parametrize(
    "clean_from_column",
    [True, False],
)
def test_combine_metadata_no_to_column(clean_from_column):
    data = {"text": [("Text", {"tag": "test"})]}
    expected = {
        "text": ["Text"] if clean_from_column else [("Text", {"tag": "test"})],
        "metadata": [{"tag": "test"}],
    }

    df = pd.DataFrame(data)
    table = pw.debug.table_from_pandas(df)

    df_expected = pd.DataFrame(expected)
    table_expected = pw.debug.table_from_pandas(df_expected)

    table = combine_metadata(
        table,
        from_column="text",
        to_column="metadata",
        clean_from_column=clean_from_column,
    )
    assert_table_equality(table, table_expected)


@pytest.mark.parametrize(
    "clean_from_column",
    [True, False],
)
def test_combine_metadata_no_metadata(clean_from_column):

    data = {"text": ["Text"]}
    expected = {"text": ["Text"], "metadata": [{}]}

    df = pd.DataFrame(data)
    table = pw.debug.table_from_pandas(df)

    df_expected = pd.DataFrame(expected)
    table_expected = pw.debug.table_from_pandas(df_expected)

    table = combine_metadata(
        table,
        from_column="text",
        to_column="metadata",
        clean_from_column=clean_from_column,
    )
    assert_table_equality(table, table_expected)
