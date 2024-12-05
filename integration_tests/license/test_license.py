# import pathway.internals as pw
import json
import logging
import os
import re

import pytest

import pathway as pw
from pathway.internals.config import _check_entitlements
from pathway.tests.utils import run_all

PATHWAY_LICENSES = json.loads(os.environ.get("PATHWAY_LICENSES", "{}"))


def test_license_malformed():
    pw.set_license_key(PATHWAY_LICENSES["malformed"])
    with pytest.raises(
        RuntimeError,
        match=r"unable to validate license",
    ):
        pw.run_all()


def test_license_wrong_signature():
    pw.set_license_key(PATHWAY_LICENSES["wrong-signature"])
    with pytest.raises(
        RuntimeError,
        match=r"unable to validate license: license file is invalid",
    ):
        pw.run_all()


@pytest.mark.parametrize("license", ["default", "default-key"])
def test_license_default_key(license, caplog):
    caplog.set_level(level=logging.INFO)
    pw.set_license_key(PATHWAY_LICENSES[license])
    run_all()
    assert "Telemetry enabled" in caplog.text
    assert "Monitoring server:" in caplog.text


@pytest.mark.parametrize("license", ["default", "default-key"])
def test_license_default_key_insufficient_entitlements(license, caplog):
    pw.set_license_key(PATHWAY_LICENSES[license])
    with pytest.raises(
        RuntimeError,
        match=re.escape(
            'one of the features you used ["XPACK-SPATIAL"] requires upgrading your Pathway license.'
        ),
    ):
        _check_entitlements("xpack-spatial")


def test_license_enterprise(caplog):
    caplog.set_level(level=logging.INFO)
    pw.set_license_key(PATHWAY_LICENSES["enterprise-no-expire"])

    _check_entitlements("xpack-spatial")
    run_all()

    assert "Telemetry enabled" not in caplog.text
    assert "Monitoring server:" in caplog.text


def test_license_enterprise_expired(caplog):
    caplog.set_level(level=logging.INFO)
    pw.set_license_key(PATHWAY_LICENSES["enterprise-expired"])

    _check_entitlements("xpack-spatial")
    run_all()

    assert (
        "License has expired. Please renew to continue using the service" in caplog.text
    )
    assert "Telemetry enabled" not in caplog.text
    assert "Monitoring server:" in caplog.text
