import nltk

from pathway.tests.utils import make_port_fixture

port = make_port_fixture()


def pytest_configure(config):
    nltk.download("punkt")
