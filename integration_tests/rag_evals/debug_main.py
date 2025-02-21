import logging
import os

from dotenv import load_dotenv
from experiment import run_eval_experiment
from pydantic import BaseModel, ConfigDict, InstanceOf, computed_field

import pathway as pw
from pathway.tests.utils import wait_result_with_checker
from pathway.xpacks.llm.question_answering import SummaryQuestionAnswerer
from pathway.xpacks.llm.servers import QASummaryRestServer

EVAL_GDRIVE_ID = "1ErwN5WajWsEdIRMBIjyBfncNUmkxRfRy"

PATHWAY_HOST = "127.0.0.1"

TEST_TIMEOUT: float = 480.0


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


load_dotenv()


class App(BaseModel):
    question_answerer: InstanceOf[SummaryQuestionAnswerer]
    host: str = PATHWAY_HOST
    port: int

    with_cache: bool = True
    terminate_on_error: bool = False
    # threaded: bool = True

    def run(self):
        server = QASummaryRestServer(self.host, self.port, self.question_answerer)
        server.run(
            with_cache=self.with_cache,
            terminate_on_error=self.terminate_on_error,
            cache_backend=pw.persistence.Backend.filesystem("Cache"),
            # threaded=self.threaded,
        )

    @computed_field(return_type=str)
    def app_url(self):
        return f"http://{PATHWAY_HOST}:{self.port}"

    model_config = ConfigDict(extra="forbid")


def create_app(port: int, config_file: str):
    with open(config_file) as f:
        config: dict = pw.load_yaml(f)

    print("Config:", config)
    config.update(port=port)
    app = App(**config)
    return app


def test_rag_app_accuracy(port: int = 8092):
    current_dir = os.path.dirname(__file__)
    config_file_path: str = f"{current_dir}/app.yaml"
    dataset_file = f"{current_dir}/dataset/labeled.tsv"

    app = create_app(port, config_file=config_file_path)
    app_url = app.app_url
    # app_url: str = f"http://{PATHWAY_HOST}:{port}"

    def checker() -> bool:
        MIN_ACCURACY: float = 0.6

        eval_accuracy: float = run_eval_experiment(
            base_url=app_url, dataset_path=dataset_file
        )
        assert eval_accuracy >= MIN_ACCURACY  # TODO: update

        return eval_accuracy >= MIN_ACCURACY

    wait_result_with_checker(checker, TEST_TIMEOUT, target=app.run)


if __name__ == "__main__":
    port = 8092
    current_dir = os.path.dirname(__file__)
    config_file_path: str = f"{current_dir}/app.yaml"
    dataset_file = f"{current_dir}/dataset/labeled.tsv"

    app = create_app(port, config_file=config_file_path)
    app.run()
