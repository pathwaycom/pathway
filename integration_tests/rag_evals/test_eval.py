import logging
import os
import time

from connector import RagConnector
from dotenv import load_dotenv
from experiment import run_eval_experiment
from logging_utils import setup_logging
from pydantic import BaseModel, ConfigDict, InstanceOf, computed_field

import pathway as pw
from pathway.tests.utils import wait_result_with_checker
from pathway.xpacks.llm.question_answering import SummaryQuestionAnswerer
from pathway.xpacks.llm.servers import QASummaryRestServer

load_dotenv()

os.environ["PATHWAY_PERSISTENT_STORAGE"] = "./Cache"

PATHWAY_HOST = "127.0.0.1"

TEST_TIMEOUT: float = 60.0 * 20  # 20 minutes TODO: I think this doesn't work currently

LOCAL_RUN: bool = os.environ.get("RUN_MODE", "CI") == "LOCAL"


setup_logging(LOCAL_RUN)


class App(BaseModel):
    question_answerer: InstanceOf[SummaryQuestionAnswerer]
    host: str = PATHWAY_HOST
    port: int

    with_cache: bool = True
    terminate_on_error: bool = True

    def run(self):
        server = QASummaryRestServer(self.host, self.port, self.question_answerer)
        server.run(
            with_cache=self.with_cache,
            terminate_on_error=self.terminate_on_error,
            cache_backend=pw.persistence.Backend.filesystem("Cache"),
        )

    @computed_field(return_type=str)
    def base_url(self):
        return f"http://{PATHWAY_HOST}:{self.port}"

    model_config = ConfigDict(extra="forbid")


def create_app(port: int, config_file: str):
    with open(config_file) as f:
        config: dict = pw.load_yaml(f)

    print("Config:", config)
    config.update(port=port)
    app = App(**config)
    return app


def test_rag_app_accuracy(port: int):
    current_dir = os.path.dirname(__file__)
    config_file_path: str = f"{current_dir}/app.yaml"
    dataset_file = f"{current_dir}/dataset/labeled.tsv"
    synthetic_dataset = f"{current_dir}/dataset/synthetic_tests/"

    logging.info(f"Creating pathway app on port: {port}.")
    app = create_app(port, config_file=config_file_path)

    app_url = app.base_url

    conn = RagConnector(app_url)

    def wait_for_start(retries: int = 10, interval: int | float = 45.0) -> bool:
        logging.info("Running wait_for_start")
        EXPECTED_DOCS_COUNT: int = 23 + 1  # +1 for synthetic data
        docs: list[dict] = []

        for iter in range(retries):
            logging.info(
                f"wait_for_start iteration: {iter}. \
            Indexed docs count: {len(docs)}, expected: {EXPECTED_DOCS_COUNT}"
            )
            logging.info(f"Indexed documents: {docs}")
            try:
                docs = conn.list_documents()
                if docs and len(docs) >= EXPECTED_DOCS_COUNT:
                    logging.info(
                        f"Fetched docs: ({len(docs)}) List: {docs}, \
                        expected: {EXPECTED_DOCS_COUNT}"
                    )
                    return True
            except ConnectionError as conn_err:
                logging.error(
                    f"""Connection error on iteration: {iter}.
                Server not ready. {str(conn_err)}"""
                )
            except (Exception, TimeoutError) as e:
                logging.error(f"Unreachable error on iteration: {iter}. \n{str(e)}")

            logging.info(f"Sleeping for {interval}")
            time.sleep(interval)
        return False

    def checker() -> bool:
        MIN_ACCURACY: float = 0.5

        logging.info("starting checker")

        server_started = wait_for_start()

        if not server_started:
            logging.error("Server was not started properly.")
            return False

        docs = conn.list_documents()

        logging.info(f"Indexed test documents: {docs}")

        eval_accuracy: float = run_eval_experiment(
            base_url=app_url,
            dataset_path=dataset_file,
            synthetic_dataset_path=synthetic_dataset,
            config_file_path=config_file_path,
            cleanup_dir_on_exit=True,
        )
        return eval_accuracy >= MIN_ACCURACY  # TODO: update

    wait_result_with_checker(checker, TEST_TIMEOUT, target=app.run)
