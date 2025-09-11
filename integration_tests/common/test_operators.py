import datetime

import pathway as pw
from pathway.tests.utils import run_all


def test_physical_compaction_in_multiworkers(monkeypatch):
    monkeypatch.setenv("PATHWAY_THREADS", 4)

    class InputSchema(pw.Schema):
        device_id: str
        datetime_utc: pw.DateTimeUtc
        geofence_visit: bool

    class StreamingSubject(pw.io.python.ConnectorSubject):
        def run(self) -> None:
            datetime_start = pw.DateTimeUtc(
                datetime.datetime(2010, 1, 1, 0, 0, 1, tzinfo=datetime.timezone.utc)
            )
            for i in range(120000):
                self.next(
                    device_id="AAA",
                    datetime_utc=datetime_start + pw.Duration(seconds=i * 300),
                    geofence_visit=not bool(i % 100),
                )
                self.commit()

        @property
        def _deletions_enabled(self) -> bool:
            return False

    window_size = pw.Duration(days=7)
    table = pw.io.python.read(
        StreamingSubject(), schema=InputSchema, max_backlog_size=10
    )
    table = table._forget(
        pw.this.datetime_utc + window_size,
        pw.this.datetime_utc,
        mark_forgetting_records=False,
    )
    table += table.sort(table.datetime_utc)
    result = table._remove_retractions()
    pw.io.null.write(result)
    run_all()
