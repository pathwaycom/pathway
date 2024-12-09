# Copyright © 2024 Pathway

import copy
import pickle
from dataclasses import dataclass
from pathlib import Path

import cloudpickle
import dill
import pandas as pd
import pytest

import pathway as pw
import pathway.internals.dtype as dt
from pathway.internals import api
from pathway.internals.parse_graph import G
from pathway.tests.utils import (
    assert_table_equality,
    assert_table_equality_wo_index,
    needs_multiprocessing_fork,
    run,
    wait_result_with_checker,
)


# note: to be picklable, classes can't be defined locally in the test function
@dataclass
class Simple:
    a: int

    def add(self, x: int) -> int:
        return self.a + x


def test_py_object_simple():

    @pw.udf
    def create_py_object(a: int) -> pw.PyObjectWrapper[Simple]:
        return pw.PyObjectWrapper(Simple(a))

    @pw.udf
    def use_py_object(a: int, b: pw.PyObjectWrapper[Simple]) -> int:
        return b.value.add(a)

    t = pw.debug.table_from_markdown(
        """
        a
        1
        2
        3
    """
    ).with_columns(b=create_py_object(pw.this.a))

    res = t.select(res=use_py_object(pw.this.a, pw.this.b))

    expected = pw.debug.table_from_markdown(
        """
        res
        2
        4
        6
    """
    )
    assert_table_equality(res, expected)


@dataclass
class Inc:
    a: int
    df: pd.DataFrame

    def add(self, x: int) -> int:
        return self.df["y"].sum() - 2 * self.a + x


def test_py_object():
    @pw.udf
    def create_inc(a: int) -> pw.PyObjectWrapper:
        return pw.PyObjectWrapper(
            Inc(a, pd.DataFrame({"x": [1, 2, 3], "y": [a, a, a]}))
        )

    t = pw.debug.table_from_markdown(
        """
        a | instance
        1 |     0
        2 |     2
        3 |     0
        4 |     2
    """
    )

    z = t.filter(pw.this.a > 2)
    t = t.with_columns(inc=create_inc(pw.this.a))

    @pw.udf
    def use_python_object(a: pw.PyObjectWrapper, x: int) -> int:
        return a.value.add(x)

    res = t.join(
        z, left_instance=pw.left.instance, right_instance=pw.right.instance
    ).select(res=use_python_object(pw.left.inc, pw.right.a))
    expected = pw.debug.table_from_markdown(
        """
        res
        4
        6
        6
        8
    """
    )
    assert_table_equality_wo_index(res, expected)


def test_dtypes():
    py_object_int = pw.PyObjectWrapper(10)
    assert dt.wrap(pw.PyObjectWrapper[int]).is_value_compatible(py_object_int)
    assert dt.wrap(pw.PyObjectWrapper).is_value_compatible(py_object_int)
    assert not dt.wrap(pw.PyObjectWrapper[str]).is_value_compatible(py_object_int)

    @dataclass
    class Simple:
        b: bytes

    py_object_simple = pw.PyObjectWrapper(Simple("abc".encode()))
    assert dt.wrap(pw.PyObjectWrapper[Simple]).is_value_compatible(py_object_simple)
    assert dt.wrap(pw.PyObjectWrapper).is_value_compatible(py_object_simple)
    assert not dt.wrap(pw.PyObjectWrapper[bytes]).is_value_compatible(py_object_simple)
    assert not dt.wrap(pw.PyObjectWrapper[int]).is_value_compatible(py_object_simple)


def test_groupby():
    @pw.udf
    def create_simple(a: int) -> pw.PyObjectWrapper[Simple]:
        return pw.PyObjectWrapper(Simple(a))

    t = pw.debug.table_from_markdown(
        """
        a
        1
        2
        2
        3
        1
    """
    ).select(simple=create_simple(pw.this.a))

    res = t.groupby(pw.this.simple).reduce(cnt=pw.reducers.count())

    expected = pw.debug.table_from_markdown(
        """
        cnt
        2
        2
        1
    """
    )

    assert_table_equality_wo_index(res, expected)


def test_compute_and_print():
    @pw.udf
    def create_simple(a: int) -> pw.PyObjectWrapper[Simple]:
        return pw.PyObjectWrapper(Simple(a))

    t = pw.debug.table_from_markdown(
        """
        a
        1
        2
        2
        3
        1
    """
    ).with_columns(simple=create_simple(pw.this.a))

    pw.debug.compute_and_print(t)


@dataclass
class SimpleStr:
    a: str

    def concat(self, x: str) -> str:
        return self.a + x


class SimpleStrSerializer:

    @staticmethod
    def dumps(object: SimpleStr) -> bytes:
        return object.a.encode()

    @staticmethod
    def loads(b: bytes) -> SimpleStr:
        return SimpleStr(b.decode())


def get_serializer(serialization: str) -> api.PyObjectWrapperSerializerProtocol | None:
    match serialization:
        case "default":
            return None
        case "pickle":
            return pickle  # type: ignore[return-value]
        case "SimpleStrSerializer":
            return SimpleStrSerializer
        case "dill":
            return dill
        case "cloudpickle":
            return cloudpickle
        case _:
            raise ValueError(f"Incorrect serialization arg: {serialization}.")


@pytest.mark.flaky(reruns=2)
@pytest.mark.parametrize(
    "serialization", ["default", "pickle", "SimpleStrSerializer", "dill", "cloudpickle"]
)
@needs_multiprocessing_fork
def test_serialization(tmp_path: Path, serialization: str, port: int) -> None:
    input_path = tmp_path / "in.csv"
    output_path = tmp_path / "out.csv"

    with open(input_path, "w") as f:
        f.write(
            """instance,a
0,ab
2,def
2,x
"""
        )

    class InputSchema(pw.Schema):
        instance: int
        a: str

    class OutputSchema(pw.Schema):
        res: str

    def target():
        t = pw.io.csv.read(input_path, schema=InputSchema, mode="static")
        z = t.copy()

        serializer = get_serializer(serialization)

        @pw.udf
        def create_simple(a: str) -> pw.PyObjectWrapper[SimpleStr]:
            return pw.wrap_py_object(SimpleStr(a), serializer=serializer)

        @pw.udf
        def use_python_object(a: pw.PyObjectWrapper[SimpleStr], x: str) -> str:
            return a.value.concat(x)

        res = (
            t.with_columns(s=create_simple(pw.this.a))
            .join(z, left_instance=pw.left.instance, right_instance=pw.right.instance)
            .select(res=use_python_object(pw.left.s, pw.right.a))
        )
        pw.io.csv.write(res, output_path)
        run()

    @dataclass
    class Checker:
        error: AssertionError | None = None

        def __call__(self) -> bool:
            if not output_path.exists():
                return False
            try:
                G.clear()
                result = pw.io.csv.read(output_path, schema=OutputSchema, mode="static")
                expected = pw.debug.table_from_markdown(
                    """
                    res
                    abab
                    defx
                    defdef
                    xx
                    xdef
                """
                )
                assert_table_equality_wo_index(result, expected)
                return True
            except AssertionError as e:
                self.error = e
                return False

        def provide_information_on_failure(self) -> str:
            return str(self.error)

    wait_result_with_checker(
        Checker(),
        5,
        target=target,
        processes=2,
        first_port=port,
    )
    # TODO: when Pathway terminates correctly when used with multiprocessing,
    # replace that with a verions that doesn't wait and checks the result after termination


@pytest.mark.parametrize(
    "serialization", ["pickle", "SimpleStrSerializer", "dill", "cloudpickle"]
)
def test_serialization_simple(serialization: str) -> None:
    serializer = get_serializer(serialization)
    assert serializer is not None
    serializer_wrapped = api.wrap_serializer(serializer)
    b = serializer_wrapped.dumps(SimpleStr("def"))
    ser_bytes = pickle.dumps(serializer_wrapped)
    des = pickle.loads(ser_bytes)
    ob = des.loads(b)
    assert ob == SimpleStr("def")


def test_copy():
    s = [1, 2, 3]
    p = pw.PyObjectWrapper(s)
    q = copy.copy(p)
    q.value.append(4)
    assert s == [1, 2, 3, 4]
    assert q.value == [1, 2, 3, 4]


def test_deepcopy():
    s = [1, 2, 3]
    p = pw.PyObjectWrapper(s)
    q = copy.deepcopy(p)
    q.value.append(4)
    assert s == [1, 2, 3]
    assert q.value == [1, 2, 3, 4]


def test_cache(tmp_path):
    t = pw.debug.table_from_markdown(
        """
        a
        1
        2
        3
        2
    """
    )

    @pw.udf(cache_strategy=pw.udfs.DiskCache())
    def f(a: int) -> pw.PyObjectWrapper:
        return pw.PyObjectWrapper(a)

    @pw.udf
    def g(a: pw.PyObjectWrapper) -> int:
        return a.value

    res = t.select(a=g(f(pw.this.a)))
    persistence_config = pw.persistence.Config(
        pw.persistence.Backend.filesystem(tmp_path),
        persistence_mode=pw.PersistenceMode.UDF_CACHING,
    )
    assert_table_equality(t, res, persistence_config=persistence_config)
