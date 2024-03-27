import contextlib
from collections.abc import Generator

import pathway.internals.table as tables
from pathway.internals.parse_graph import G, create_error_log


def global_error_log() -> tables.Table:
    return G.error_log_stack[0]


@contextlib.contextmanager
def local_error_log() -> Generator[tables.Table, None, None]:
    try:
        error_log = create_error_log(G)
        G.error_log_stack.append(error_log)
        yield error_log
    finally:
        assert G.error_log_stack[-1] == error_log
        G.error_log_stack.pop()
