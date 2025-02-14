import logging
from typing import Any, Iterable

from google.cloud import pubsub_v1  # type: ignore

import pathway.internals.dtype as dt
from pathway.internals.api import Pointer
from pathway.internals.expression import ColumnReference
from pathway.io._subscribe import subscribe


class _OutputBuffer:
    MAX_BUFFER_SIZE = 1024

    def __init__(
        self, publisher: pubsub_v1.PublisherClient, project_id: str, topic_id: str
    ) -> None:
        self._publisher = publisher
        self._topic_path = publisher.topic_path(project_id, topic_id)
        self._publish_futures: list = []

    def on_change(
        self, key: Pointer, row: dict[str, Any], time: int, is_addition: bool
    ) -> None:
        if len(row) != 1:
            raise ValueError(f"Unexpected number of columns: {len(row)}")
        data = next(iter(row.values()))
        if not isinstance(data, bytes):
            raise ValueError(f"Unexpected value type. Expected bytes, got {type(data)}")

        diff = 1 if is_addition else -1
        publish_future = self._publisher.publish(
            self._topic_path, data, pathway_time=str(time), pathway_diff=str(diff)
        )
        self._publish_futures.append(publish_future)

    def on_time_end(self, time: int) -> None:
        if self._publish_futures:
            self._flush_publish_futures()

    def _flush_publish_futures(self) -> None:
        for future in self._publish_futures:
            try:
                future.result()
            except Exception:
                logging.exception("Failed to publish message")
        self._publish_futures = []


def write(
    table,
    publisher: pubsub_v1.PublisherClient,
    project_id: str,
    topic_id: str,
    *,
    name: str | None = None,
    sort_by: Iterable[ColumnReference] | None = None,
) -> None:
    """Publish the ``table``'s stream of changes into the specified PubSub topic. Please note
    that ``table`` must consist of a single column of the binary type. In addition, the connector
    adds two attributes: ``pathway_time`` containing the logical time of the change and
    ``pathway_diff`` corresponding to the change type: either addition (``pathway_diff = 1``)
    or deletion (``pathway_diff = -1``).

    Args:
        table: The table to publish.
        publisher: The configured ``pubsub_v1.PublisherClient`` object. You can refer to \
the Google Cloud `documentation \
<https://cloud.google.com/pubsub/docs/samples/pubsub-quickstart-publisher?hl=en>`_ for \
the example of a simple publisher configuration. You can also see the examples for \
`batching settings configuration \
<https://cloud.google.com/pubsub/docs/samples/pubsub-publisher-batch-settings?hl=en>`_ or \
`flow control configuration \
<https://cloud.google.com/pubsub/docs/samples/pubsub-publisher-flow-control?hl=en>`_.
        project_id: The ID of the project where the changes are published.
        topic_id: The topic ID where the changes are published.
        name: A unique name for the connector. If provided, this name will be used in
            logs and monitoring dashboards.
        sort_by: If specified, the output will be sorted in ascending order based on the
            values of the given columns within each minibatch. When multiple columns are provided,
            the corresponding value tuples will be compared lexicographically.

    Returns:
        None

    Example:

    Consider that you have a table ``blobs``, consisting of a single column that has a binary
    type. You would like to publish changes that happen to this table into a topic called
    ``blobs`` in your Google Cloud pub/sub project.

    For simplicity, let's consider that the project ID is stored in the ``project_id``
    variable. The ``topic_id`` variable then would denote the name of the target topic
    which is ``blobs`` in our case:

    >>> project_id = "YOUR_PROJECT_ID"
    >>> topic_id = "blobs"

    Now, you need to create the publisher object of ``pubsub_v1.PublisherClient``
    type. If you have the `service account
    <https://cloud.google.com/iam/docs/service-account-overview>`_ credentials stored
    in a file ``./credentials.json``, it can be done with the following code:

    >>> from google.cloud import pubsub_v1
    >>> publisher = pubsub_v1.PublisherClient.from_service_account_file(  # doctest: +SKIP
    ...     "./credentials.json"
    ... )

    If you don't have the topic created yet, you may want to create it first:

    >>> topic_path = publisher.topic_path(project_id, topic_id)  # doctest: +SKIP
    >>> topic = publisher.create_topic(request={"name": topic_path})  # doctest: +SKIP

    After that you can configure the table output with the following code:

    >>> import pathway as pw
    >>> pw.io.pubsub.write(table, publisher, project_id, topic_id)  # doctest: +SKIP

    At last, don't forget to add ``pw.run()`` to run your pipeline.
    """

    columns = list(table._columns.values())
    if len(columns) != 1:
        raise ValueError(
            f"Unexpected number of columns in table: {len(table._columns)}"
        )

    allowed_column_types = (dt.BYTES, dt.ANY)
    if columns[0].dtype not in allowed_column_types:
        raise ValueError("The column should be of the type 'bytes'")

    output_buffer = _OutputBuffer(publisher, project_id, topic_id)
    subscribe(
        table,
        on_change=output_buffer.on_change,
        on_time_end=output_buffer.on_time_end,
        name=name,
        sort_by=sort_by,
    )
