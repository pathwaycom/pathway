import logging

import pytest

import pathway as pw
from pathway.internals import api


def test_license_invalid():
    with pytest.raises(api.EngineError, match="invalid license key"):
        pw.run_all(license_key="invalid")


def test_telemetry_disabled(caplog):
    caplog.set_level(level=logging.DEBUG)

    pw.run_all(license_key="", monitoring_level=pw.MonitoringLevel.NONE)

    assert "Telemetry disabled" in caplog.text


def test_telemetry_disabled_no_feature_flag(caplog):
    caplog.set_level(level=logging.DEBUG)

    pw.run_all(
        license_key="EVALUATION-WITH-TELEMETRY",
        monitoring_level=pw.MonitoringLevel.NONE,
    )

    assert "Telemetry disabled" in caplog.text
