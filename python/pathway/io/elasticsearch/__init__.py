# Copyright © 2024 Pathway

from __future__ import annotations

from typing import Iterable

from pathway.internals import api, datasink
from pathway.internals._io_helpers import _format_output_value_fields
from pathway.internals.config import _check_entitlements
from pathway.internals.expression import ColumnReference
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame


class ElasticSearchAuth:
    def __init__(self, engine_es_auth: api.ElasticSearchAuth) -> None:
        self._engine_es_auth = engine_es_auth

    @classmethod
    def apikey(cls, apikey_id, apikey):
        return cls(
            api.ElasticSearchAuth(
                "apikey",
                apikey_id=apikey_id,
                apikey=apikey,
            )
        )

    @classmethod
    def basic(cls, username, password):
        return cls(
            api.ElasticSearchAuth(
                "basic",
                username=username,
                password=password,
            )
        )

    @classmethod
    def bearer(cls, bearer):
        return cls(
            api.ElasticSearchAuth(
                "bearer",
                bearer=bearer,
            )
        )

    @property
    def engine_es_auth(self) -> api.ElasticSearchAuth:
        return self._engine_es_auth


@check_arg_types
@trace_user_frame
def write(
    table: Table,
    host: str,
    auth: ElasticSearchAuth,
    index_name: str,
    *,
    name: str | None = None,
    sort_by: Iterable[ColumnReference] | None = None,
) -> None:
    """Write a table to a given index in ElasticSearch.

    Args:
        table: the table to output.
        host: the host and port, on which Elasticsearch server works.
        auth: credentials for Elasticsearch authorization.
        index_name: name of the index, which gets the docs.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards.
        sort_by: If specified, the output will be sorted in ascending order based on the
            values of the given columns within each minibatch. When multiple columns are provided,
            the corresponding value tuples will be compared lexicographically.

    Returns:
        None

    Example:

    Consider there is an instance of Elasticsearch, running locally on a port 9200.
    There we have an index "animals", containing an information about pets and their
    owners.

    For the sake of simplicity we will also consider that the cluster has a simple
    username-password authentication having both username and password equal to "admin".

    Now suppose we want to send a Pathway table pets to this local instance of
    Elasticsearch.

    >>> import pathway as pw
    >>> pets = pw.debug.table_from_markdown('''
    ... age | owner | pet
    ... 10  | Alice | dog
    ... 9   | Bob   | cat
    ... 8   | Alice | cat
    ... ''')

    It can be done as follows:

    >>> pw.io.elasticsearch.write(
    ...     table=pets,
    ...     host="http://localhost:9200",
    ...     auth=pw.io.elasticsearch.ElasticSearchAuth.basic("admin", "admin"),
    ...     index_name="animals",
    ... )

    All the updates of table "pets" will be indexed to "animals" as well.
    """

    _check_entitlements("elasticsearch")
    data_storage = api.DataStorage(
        storage_type="elasticsearch",
        elasticsearch_params=api.ElasticSearchParams(
            host=host,
            index_name=index_name,
            auth=auth.engine_es_auth,
        ),
    )

    data_format = api.DataFormat(
        format_type="jsonlines",
        key_field_names=[],
        value_fields=_format_output_value_fields(table),
    )

    table.to(
        datasink.GenericDataSink(
            data_storage,
            data_format,
            datasink_name="elasticsearch",
            unique_name=name,
            sort_by=sort_by,
        )
    )
