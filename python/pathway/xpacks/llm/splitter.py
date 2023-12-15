"""
A library of text spliiters - routines which slit a long text into smaller chunks.
"""

from typing import Dict, List, Tuple


def null_splitter(txt: str) -> List[Tuple[str, Dict]]:
    """A splitter which returns its argument as one long text ith null metadata.

    Args:
        txt: text to be split

    Returns:
        list of pairs: chunk text and metadata.

    The null splitter always return a list of length one containing the full text and empty metadata.
    """
    return [(txt, {})]
