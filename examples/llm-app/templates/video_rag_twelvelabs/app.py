#!/usr/bin/env python

# Copyright © 2026 Pathway

import logging
import os
from pathlib import Path
from warnings import warn

from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, InstanceOf

import pathway as pw
from pathway.xpacks.llm.question_answering import BaseRAGQuestionAnswerer
from pathway.xpacks.llm.servers import QASummaryRestServer

# The TwelveLabsVideoParser used by this template is a Pathway Live Data Framework Scale feature.
# Get your free license key from https://pathway.com/features and set it in the
# PATHWAY_LICENSE_KEY environment variable (see .env.example) or paste it below.
pw.set_license_key(
    os.environ.get("PATHWAY_LICENSE_KEY", "demo-license-key-with-telemetry")
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class App(BaseModel):
    question_answerer: InstanceOf[BaseRAGQuestionAnswerer]
    host: str = "0.0.0.0"
    port: int = 8000

    with_cache: bool | None = None  # deprecated
    persistence_backend: pw.persistence.Backend | None = None
    persistence_mode: pw.PersistenceMode | None = pw.PersistenceMode.UDF_CACHING
    terminate_on_error: bool = False

    def run(self) -> None:
        server = QASummaryRestServer(  # noqa: F841
            self.host, self.port, self.question_answerer
        )

        if self.persistence_mode is None:
            if self.with_cache is True:
                warn(
                    "`with_cache` is deprecated. Please use `persistence_mode` instead.",
                    DeprecationWarning,
                )
                persistence_mode = pw.PersistenceMode.UDF_CACHING
            else:
                persistence_mode = None
        else:
            persistence_mode = self.persistence_mode

        if persistence_mode is not None:
            if self.persistence_backend is None:
                persistence_backend = pw.persistence.Backend.filesystem("./Cache")
            else:
                persistence_backend = self.persistence_backend
            persistence_config = pw.persistence.Config(
                persistence_backend,
                persistence_mode=persistence_mode,
            )
        else:
            persistence_config = None

        pw.run(
            persistence_config=persistence_config,
            terminate_on_error=self.terminate_on_error,
            monitoring_level=pw.MonitoringLevel.NONE,
        )

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)


if __name__ == "__main__":
    base_dir = Path(__file__).resolve().parent

    load_dotenv(base_dir / ".env")

    with open(base_dir / "app.yaml") as f:
        config = pw.load_yaml(f)

    app = App(**config)
    app.run()
