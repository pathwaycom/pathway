from pathway.tests.utils import make_port_fixture

# A case here runs up to eight pathway processes, each needing a port of its own
port = make_port_fixture(block_size=8)
