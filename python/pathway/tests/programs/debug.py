import pathway as pw

table = pw.debug.table_from_markdown(
    """
     a | t | __time__
     1 | 2 |     2
     2 | 2 |     2
    10 | 5 |     6
     3 | 8 |     8
     5 | 7 |     8
"""
)


table.debug("foo")
pw.run(monitoring_level=pw.MonitoringLevel.NONE, debug=True)
