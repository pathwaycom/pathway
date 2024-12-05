# import pathway.internals as pw
import json
import logging
import os
import re

import pytest

import pathway as pw
from pathway.internals import api
from pathway.internals.config import _check_entitlements
from pathway.tests.utils import run_all

PATHWAY_LICENSES = json.loads(os.environ.get("PATHWAY_LICENSES", "{}"))
PATHWAY_LICENSES["none"] = None

ENTERPRISE_BUILD = pw.__version__.endswith("+enterprise")
only_standard_build = pytest.mark.xfail(
    ENTERPRISE_BUILD,
    reason="only works on standard build",
    strict=True,
)
only_enterprise_build = pytest.mark.xfail(
    not ENTERPRISE_BUILD,
    reason="only works on enterprise build",
    strict=True,
)


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


@only_standard_build
def test_no_license(caplog):
    caplog.set_level(level=logging.DEBUG)

    pw.set_monitoring_config(server_endpoint=None)
    pw.set_license_key(None)
    run_all()

    assert "Telemetry enabled" not in caplog.text


@only_standard_build
def test_monitoring_insufficient_license():
    pw.set_license_key(None)
    pw.set_monitoring_config(server_endpoint="https://example.com")
    with pytest.raises(
        api.EngineError,
        match=re.escape(
            'one of the features you used ["MONITORING"] requires upgrading your Pathway license'
        ),
    ):
        run_all()


@only_standard_build
def test_license_default_policy(caplog):
    caplog.set_level(level=logging.INFO)
    pw.set_license_key(PATHWAY_LICENSES["default-key"])
    run_all()
    assert "Telemetry enabled" in caplog.text
    assert "Monitoring server:" in caplog.text


@only_standard_build
def test_license_default_policy_insufficient_entitlements():
    pw.set_license_key(PATHWAY_LICENSES["default-key"])
    with pytest.raises(
        RuntimeError,
        match=re.escape(
            'one of the features you used ["XPACK-SPATIAL"] requires upgrading your Pathway license.'
        ),
    ):
        _check_entitlements("xpack-spatial")


@only_standard_build
@pytest.mark.parametrize("license", ["default", "enterprise"])
def test_license_default_policy_offline_license(license):
    pw.set_license_key(PATHWAY_LICENSES[license])
    with pytest.raises(
        RuntimeError,
        match=re.escape("offline license not allowed"),
    ):
        _check_entitlements("xpack-sharepoint")


@only_enterprise_build
def test_license_enterprise(caplog):
    caplog.set_level(level=logging.INFO)
    pw.set_license_key(PATHWAY_LICENSES["enterprise"])

    _check_entitlements("xpack-spatial")
    run_all()

    assert "Telemetry enabled" not in caplog.text
    assert "Monitoring server:" in caplog.text


@only_enterprise_build
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


@only_enterprise_build
@pytest.mark.parametrize("license", ["default", "default-key", "none"])
def test_license_enterprise_default_policy_insufficient(license):
    pw.set_license_key(PATHWAY_LICENSES[license])
    with pytest.raises(
        RuntimeError,
        match=r"insufficient license",
    ):
        run_all()
