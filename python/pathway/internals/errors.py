import contextlib
from collections.abc import Generator

import pathway.internals.table as tables
from pathway.internals.parse_graph import ErrorLogSchema, G


def global_error_log() -> tables.Table[ErrorLogSchema]:
    return G.get_global_error_log()


@contextlib.contextmanager
def local_error_log() -> Generator[tables.Table[ErrorLogSchema], None, None]:
    try:
        error_log = G.add_error_log()
        yield error_log
    finally:
        G.remove_error_log(error_log)
