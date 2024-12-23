def remove_keys(dc: dict, remove_keys: list | set) -> dict:
    return {k: v for k, v in dc.items() if k not in remove_keys}


def add_prefix(dc: dict, prefix: str) -> dict:
    return {prefix + k: v for k, v in dc.items()}


def get_run_params() -> list[dict]:
    # from importlib.metadata import version
    # return {"pathway_version": version("pathway")}
    return []
