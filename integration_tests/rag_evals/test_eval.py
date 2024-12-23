import logging
import os
import sys
import time

from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, InstanceOf, computed_field

import pathway as pw
from pathway.tests.utils import wait_result_with_checker
from pathway.xpacks.llm.question_answering import SummaryQuestionAnswerer
from pathway.xpacks.llm.servers import QASummaryRestServer

from .connector import RagConnector
from .experiment import run_eval_experiment

PATHWAY_HOST = "127.0.0.1"

TEST_TIMEOUT: float = 600.0 * 2


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

console_handler = logging.StreamHandler(sys.stderr)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(
    logging.Formatter(
        "%(asctime)s %(name)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
)


file_handler = logging.FileHandler(
    "/integration_tests/rag_integration_test_cache/rag_eval_logs.txt"
)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(
    logging.Formatter(
        "%(asctime)s %(name)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
)

logging.getLogger().addHandler(console_handler)
logging.getLogger().addHandler(file_handler)

load_dotenv()


class App(BaseModel):
    question_answerer: InstanceOf[SummaryQuestionAnswerer]
    host: str = PATHWAY_HOST
    port: int

    with_cache: bool = True
    terminate_on_error: bool = False

    def run(self):
        server = QASummaryRestServer(self.host, self.port, self.question_answerer)
        server.run(
            with_cache=self.with_cache,
            terminate_on_error=self.terminate_on_error,
            cache_backend=pw.persistence.Backend.filesystem("Cache"),
            # threaded=self.threaded,
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
    # thread = app.run()
    # return thread


def test_rag_app_accuracy(port: int):
    current_dir = os.path.dirname(__file__)
    config_file_path: str = f"{current_dir}/app.yaml"
    dataset_file = f"{current_dir}/dataset/labeled.tsv"

    logging.error(f"Creating pathwap app on port: {port}.")
    app = create_app(port, config_file=config_file_path)

    app_url = app.base_url

    conn = RagConnector(app_url)

    def wait_for_start(retries: int = 10, interval: int | float = 45.0) -> bool:
        logging.error("Running wait_for_start")
        EXPECTED_DOCS_COUNT: int = 23  # 2
        docs: list[dict] = []

        for iter in range(retries):
            logging.error(
                f"wait_for_start iteration: {iter}. \
            Indexed docs count: {len(docs)}, expected: {EXPECTED_DOCS_COUNT}"
            )
            logging.error(f"Indexed documents: {docs}")
            try:
                docs = conn.pw_list_documents()
                if docs and len(docs) >= EXPECTED_DOCS_COUNT:
                    logging.error(
                        f"Fetched docs: {docs} ({len(docs)}), \
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

            logging.error(f"Sleeping for {interval}")
            time.sleep(interval)
        return False

    def checker() -> bool:
        MIN_ACCURACY: float = 0.6

        logging.error("starting checker")

        server_started = wait_for_start()

        if not server_started:
            logging.error("Server was not started properly.")
            return False

        docs = conn.pw_list_documents()

        logging.error(f"Indexed test documents: {docs}")

        eval_accuracy: float = run_eval_experiment(
            base_url=app_url,
            dataset_path=dataset_file,
            config_file_path=config_file_path,
            cleanup_dir_on_exit=True,
        )
        assert eval_accuracy >= MIN_ACCURACY  # TODO: update

        return eval_accuracy >= MIN_ACCURACY

    wait_result_with_checker(checker, TEST_TIMEOUT, target=app.run)
