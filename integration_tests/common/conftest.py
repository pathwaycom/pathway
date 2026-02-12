import pytest

from pathway.tests.utils import UniquePortDispenser

# The configuration is different because there are many workers
# and each one must provide unique range of 8 consecutive ports,
# (as the max number of workers) not just one as in other tests
PORT_DISPENSER = UniquePortDispenser(
    range_start=1000,
    worker_range_size=600,
    step_size=8,
)


@pytest.fixture
def port(testrun_uid):
    yield PORT_DISPENSER.get_unique_port(testrun_uid)
