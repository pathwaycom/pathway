import os
import pathlib
import re
import subprocess
import time

import pytest
import requests

import pathway as pw
from pathway.internals import api
from pathway.tests.utils import run_all


def test_detailed_monitoring_insufficient_license(tmp_path: pathlib.Path):
    pw.set_license_key(None)
    pw.set_monitoring_config(
        detailed_metrics_dir=tmp_path / "metrics",
    )
    with pytest.raises(
        api.EngineError,
        match=re.escape(
            'one of the features you used ["MONITORING"] requires upgrading your Pathway license'
        ),
    ):
        run_all()


def test_monitoring_license_detailed_metrics_created(
    tmp_path: pathlib.Path, monkeypatch
):
    pw.set_license_key(os.environ.get("PATHWAY_LICENSE_KEY"))
    monkeypatch.setenv("PATHWAY_RUN_ID", "test_run")

    metrics_path = tmp_path / "metrics"
    db_file_path = metrics_path / "metrics_test_run.db"

    pw.set_monitoring_config(detailed_metrics_dir=metrics_path)
    run_all()

    assert metrics_path.exists()
    assert metrics_path.is_dir()
    assert db_file_path.exists()
    assert db_file_path.is_file()


@pytest.mark.flaky(reruns=2)
def test_web_dashboard_spawn(tmp_path, tcp_port):
    metrics_dir = tmp_path / "metrics"
    metrics_dir.mkdir()
    # Create an empty metrics db file so dashboard can start
    db_file = metrics_dir / "metrics_empty.db"
    db_file.write_bytes(b"\x00")
    port = tcp_port
    env = os.environ.copy()
    env["PATHWAY_DETAILED_METRICS_DIR"] = str(metrics_dir)
    proc = subprocess.Popen(
        [
            "uvicorn",
            "pathway.web_dashboard.dashboard:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        env=env,
    )
    try:
        # Wait for server to start
        for _ in range(30):
            try:
                r = requests.get(f"http://127.0.0.1:{port}/")
                if r.status_code == 200:
                    break
            except Exception:
                time.sleep(0.5)
        else:
            pytest.fail("Dashboard did not start in time")
        # Check main page loads
        assert "<html" in r.text.lower()
        # Check API endpoint returns something
        r_api = requests.get(f"http://127.0.0.1:{port}/metrics/available_range")
        assert r_api.status_code == 200
        assert "min" in r_api.json()
        assert "max" in r_api.json()
    finally:
        proc.terminate()
        proc.wait()
