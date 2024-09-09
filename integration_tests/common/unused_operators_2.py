import pathway as pw

t = pw.debug.table_from_markdown(
    """
        a | b
        1 | 2
    """
)
pw.debug.compute_and_print(t)
t.with_columns(c=10)
