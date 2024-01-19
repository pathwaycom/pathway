import os
import threading

import pytest


class UniquePortDispenser:
    """
    Tests are run simultaneously by several workers.
    Since they involve running a web server, they shouldn't interfere, so they
    should occupy distinct ports.
    This class automates unique port assignments for different tests.
    """

    def __init__(self, range_start: int = 12345, worker_range_size: int = 1000):
        # the main worker is sometimes 'master', sometimes just not defined
        pytest_worker_id = os.environ.get("PYTEST_XDIST_WORKER", "master")
        if pytest_worker_id == "master":
            worker_id = 0
        elif pytest_worker_id.startswith("gw"):
            worker_id = int(pytest_worker_id[2:])
        else:
            raise ValueError(f"Unknown xdist worker id: {pytest_worker_id}")

        self._next_available_port = range_start + worker_id * worker_range_size
        self._lock = threading.Lock()

    def get_unique_port(self) -> int:
        with self._lock:
            result = self._next_available_port
            self._next_available_port += 1
        return result


PORT_DISPENSER = UniquePortDispenser()


@pytest.fixture
def port():
    yield PORT_DISPENSER.get_unique_port()
