import logging
import sys


def remove_keys(dc: dict, remove_keys: list | set) -> dict:
    return {k: v for k, v in dc.items() if k not in remove_keys}


def add_prefix(dc: dict, prefix: str) -> dict:
    """Add prefix string to the keys in a dict."""
    return {prefix + k: v for k, v in dc.items()}


def setup_logging(local_run: bool):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(name)s %(levelname)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )

    log_file = (
        "/integration_tests/rag_integration_test_cache/rag_eval_logs.txt"
        if not local_run
        else "rag_eval_logs.txt"
    )
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(name)s %(levelname)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )

    logging.getLogger().addHandler(console_handler)
    logging.getLogger().addHandler(file_handler)
