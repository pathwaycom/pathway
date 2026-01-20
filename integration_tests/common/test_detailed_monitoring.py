# Copyright © 2026 Pathway
import pathlib
import re

import pytest

from pathway.internals import api
from pathway.internals.config import local_pathway_config
from pathway.tests.utils import run_all


def test_detailed_monitoring_insufficient_license(tmp_path: pathlib.Path):
    with local_pathway_config() as config:
        config.license_key = None
        config.detailed_metrics_dir = str(tmp_path / "metrics")
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
    monkeypatch.setenv("PATHWAY_RUN_ID", "test_run")

    metrics_path = tmp_path / "metrics"
    db_file_path = metrics_path / "metrics_test_run.db"

    with local_pathway_config() as config:
        config.detailed_metrics_dir = str(metrics_path)
        run_all()

    assert metrics_path.exists()
    assert metrics_path.is_dir()
    assert db_file_path.exists()
    assert db_file_path.is_file()
