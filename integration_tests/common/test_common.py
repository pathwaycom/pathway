import os
import subprocess as sp

import pytest


@pytest.mark.parametrize("filename", ["unused_operators.py", "unused_operators_2.py"])
def test_some_operators_unused(filename: str):
    test_dir = os.path.dirname(os.path.abspath(__file__))
    p = sp.run(["python3", f"{test_dir}/{filename}"], capture_output=True, check=True)
    assert (
        "There are operators in the computation graph that haven't been used."
        + " Use pathway.run() (or similar) to run the computation involving these nodes."
    ) in p.stderr.decode()


def test_all_operators_used():
    test_dir = os.path.dirname(os.path.abspath(__file__))
    p = sp.run(
        ["python3", f"{test_dir}/used_operators.py"], capture_output=True, check=True
    )
    assert (
        "There are operators in the computation graph that haven't been used."
        + " Use pathway.run() (or similar) to run the computation involving these nodes."
    ) not in p.stderr.decode()
