# Copyright © 2024 Pathway

from contextlib import contextmanager


@contextmanager
def optional_imports(extra: str):
    try:
        yield
    except ImportError as e:
        raise ImportError(f"{e}. Consider installing 'pathway[{extra}]'")
