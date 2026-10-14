import logging

from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict

import pathway as pw
from pathway.xpacks.llm.mcp_server import PathwayMcp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

load_dotenv()


class App(BaseModel):
    mcp_http: PathwayMcp
    host: str = "0.0.0.0"
    port: int = 8000

    terminate_on_error: bool = False
    persistence_backend: pw.persistence.Backend | None = None
    persistence_mode: pw.PersistenceMode | None = pw.PersistenceMode.UDF_CACHING

    def run(self) -> None:
        if self.persistence_mode is not None:
            if self.persistence_backend is None:
                persistence_backend = pw.persistence.Backend.filesystem("./Cache")
            else:
                persistence_backend = self.persistence_backend
            persistence_config = pw.persistence.Config(
                persistence_backend,
                persistence_mode=self.persistence_mode,
            )
        else:
            persistence_config = None
        pw.run(
            terminate_on_error=self.terminate_on_error,
            persistence_config=persistence_config,
            monitoring_level=pw.MonitoringLevel.NONE,
        )

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)


if __name__ == "__main__":
    with open("app.yaml") as f:
        config = pw.load_yaml(f)
    print(config)
    app = App(**config)
    app.run()
