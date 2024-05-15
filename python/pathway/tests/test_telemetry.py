import logging
import re

import pytest

import pathway as pw
from pathway.internals import api
from pathway.tests.utils import run_all


@pytest.mark.parametrize("monitoring_endpoint", [None, "https://example.com"])
@pytest.mark.skip("move to integration")
def test_telemetry(caplog, monitoring_endpoint):
    caplog.set_level(level=logging.DEBUG)
    pw.set_license_key("EVALUATION-WITH-TELEMETRY")
    pw.set_monitoring_config(server_endpoint=monitoring_endpoint)

    run_all()

    assert "Telemetry enabled" in caplog.text


def test_telemetry_disabled(caplog):
    caplog.set_level(level=logging.DEBUG)

    pw.set_license_key(None)
    run_all()

    assert "Telemetry disabled" in caplog.text


def test_monitoring_insufficient_license():
    pw.set_license_key(None)
    pw.set_monitoring_config(server_endpoint="https://example.com")
    with pytest.raises(
        api.EngineError,
        match=re.escape(
            'one of the features you used ["monitoring"] requires upgrading your Pathway license'
        ),
    ):
        run_all()
