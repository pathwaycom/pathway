import logging

import pytest

import pathway as pw
from pathway.internals import api
from pathway.tests.utils import run_all


def test_license_invalid():
    with pytest.raises(api.EngineError, match="invalid license key"):
        run_all(license_key="invalid")


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

    run_all(license_key="")

    assert "Telemetry disabled" in caplog.text


def test_monitoring_insufficient_license():
    pw.set_license_key("")
    pw.set_monitoring_config(server_endpoint="https://example.com")
    with pytest.raises(
        api.EngineError,
        match="insufficient license: monitoring cannot be enabled",
    ):
        run_all()
