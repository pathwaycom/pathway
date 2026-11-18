import shutil

import pytest

from pathway.tests.utils import make_port_fixture

# A wordcount case is minutes long, so it can't be told from a stuck one by
# looking at the clock. This is the last-resort bound: the case fails with a
# traceback (and its pathway processes are killed by the `finally` blocks it
# unwinds) instead of holding a CI worker until the whole job times out.
MAX_TEST_DURATION_SEC = 5400

# A case here runs up to four pathway processes, each needing a port of its own
port = make_port_fixture(block_size=5)


def pytest_configure(config):
    # Registered here as well so that the mark is silent in an environment
    # without pytest-timeout; there the bound simply doesn't apply.
    config.addinivalue_line(
        "markers", "timeout(seconds): fail the test if it runs longer than that"
    )


def pytest_collection_modifyitems(config, items):
    for item in items:
        item.add_marker(pytest.mark.timeout(MAX_TEST_DURATION_SEC))


# Each wordcount case dumps hundreds of MB into tmp_path (input files and fs pstorage).
# pytest's tmp_path is session-scoped for cleanup, so without this the whole suite's
# ~100 cases pile up on disk and sporadically fill the Jenkins scratch partition.
@pytest.fixture(autouse=True)
def _cleanup_tmp_path(tmp_path):
    yield
    shutil.rmtree(tmp_path, ignore_errors=True)
