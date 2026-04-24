import shutil

import pytest

from pathway.tests.utils import UniquePortDispenser

# The configuration is different because there are many workers
# and each one must provide unique range of 5 consecutive ports,
# not just one as in other tests
PORT_DISPENSER = UniquePortDispenser(
    range_start=1000,
    worker_range_size=600,
    step_size=5,
)


@pytest.fixture
def port(testrun_uid):
    yield PORT_DISPENSER.get_unique_port(testrun_uid)


# Each wordcount case dumps hundreds of MB into tmp_path (input files and fs pstorage).
# pytest's tmp_path is session-scoped for cleanup, so without this the whole suite's
# ~100 cases pile up on disk and sporadically fill the Jenkins scratch partition.
@pytest.fixture(autouse=True)
def _cleanup_tmp_path(tmp_path):
    yield
    shutil.rmtree(tmp_path, ignore_errors=True)
