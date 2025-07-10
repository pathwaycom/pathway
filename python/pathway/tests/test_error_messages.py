# Copyright © 2024 Pathway

import contextlib
import os
import re

import pandas as pd
import pytest

import pathway as pw
from pathway.internals import ClassArg, input_attribute, output_attribute, transformer
from pathway.internals.runtime_type_check import check_arg_types
from pathway.tests.utils import (
    T,
    assert_table_equality,
    remove_ansi_escape_codes,
    run_all,
)


def test_select_args():
    tab = T(
        """a
            1
            2"""
    )

    with _assert_error_trace(
        ValueError,
        match=re.escape(
            "Expected a ColumnReference, found a string. Did you mean this.a instead of 'a'?"
        ),
    ):
        tab.select("a")  # cause


def test_reduce_args():
    tab = T(
        """a
            1
            2"""
    )

    with _assert_error_trace(
        ValueError,
        match=re.escape(
            "Expected a ColumnReference, found a string. Did you mean this.a instead of 'a'?"
        ),
    ):
        tab.reduce("a")  # cause

    with _assert_error_trace(
        ValueError,
        match=re.escape(
            "In reduce() all positional arguments have to be a ColumnReference."
        ),
    ):
        tab.reduce(1)  # cause

    with _assert_error_trace(
        ValueError,
        match=re.escape(
            "Expected a ColumnReference, found a string. Did you mean this.a instead of 'a'?"
        ),
    ):
        tab.groupby().reduce("a")  # cause

    with _assert_error_trace(
        ValueError,
        match=re.escape(
            "In reduce() all positional arguments have to be a ColumnReference."
        ),
    ):
        tab.groupby().reduce(1)  # cause


def test_groupby_extrakwargs():
    tab = T(
        """a
            1
            2"""
    )

    with _assert_error_trace(
        ValueError,
        match=re.escape(
            "Table.groupby() received extra kwargs.\n"
            + "You probably want to use Table.groupby(...).reduce(**kwargs) to compute output columns."
        ),
    ):
        tab.groupby(pw.this.a, output=pw.this.a)  # cause


def test_windowby_extraargs():
    t = T(
        """
        instance | t  | tt |  v
        0        | 1  | 1  |  10
        0        | 2  | 2  |  1
        0        | 4  | 4  |  3
        0        | 8  | 8  |  2
        0        | 9  | 9  |  4
        0        | 10 | 10 |  8
        1        | 1  | 1  |  9
        1        | 2  | 2  |  16
    """
    )

    with _assert_error_trace(
        ValueError,
        match=re.escape(
            "Table.windowby() received extra args.\nIt handles grouping only by a single column."
        ),
    ):
        t.windowby(  # cause
            t.t,
            t.tt,
            window=pw.temporal.session(predicate=lambda a, b: abs(a - b) <= 1),
            instance=t.instance,
        )


def test_join_args():
    left = T(
        """a
            1
            2"""
    )
    right = left.copy()

    with _assert_error_trace(
        ValueError,
        match=re.escape(
            "Received `how` argument but was not expecting any.\n"
            + "Consider using a generic join method that handles `how` to decide on a type of a join to be used."
        ),
    ):
        left.join_left(right, how=pw.JoinMode.LEFT)  # cause

    with _assert_error_trace(
        ValueError,
        match=re.escape(
            "Received `how` argument of join that is a string.\n"
            + "You probably want to use one of JoinMode.INNER, JoinMode.LEFT,"
            + " JoinMode.RIGHT or JoinMode.OUTER values."
        ),
    ):
        left.join(right, how="left")  # cause

    with _assert_error_trace(
        ValueError,
        match=re.escape(
            "How argument of join should be one of JoinMode.INNER, JoinMode.LEFT,"
            + " JoinMode.RIGHT or JoinMode.OUTER values."
        ),
    ):
        left.join(right, how=1)  # cause

    with _assert_error_trace(
        ValueError,
        match=re.escape("The id argument of a join has to be a ColumnReference."),
    ):
        left.join(right, id=1)  # cause

    with _assert_error_trace(
        ValueError,
        match=re.escape(
            "Join received extra kwargs.\n"
            + "You probably want to use TableLike.join(...).select(**kwargs) to compute output columns."
        ),
    ):
        left.join(right, a=left.a)  # cause


def test_session_simple():
    t = T(
        """
        instance | t  |  v
        0        | 1  |  10
        0        | 2  |  1
        0        | 4  |  3
        0        | 8  |  2
        0        | 9  |  4
        0        | 10 |  8
        1        | 1  |  9
        1        | 2  |  16
    """
    )

    with _assert_error_trace(
        ValueError,
        match=re.escape(
            "Table.windowby() received extra kwargs.\n"
            + "You probably want to use Table.windowby(...).reduce(**kwargs) to compute output columns."
        ),
    ):
        t.windowby(  # cause
            t.t,
            window=pw.temporal.session(predicate=lambda a, b: abs(a - b) <= 1),
            instance=t.instance,
            min_t=pw.reducers.min(pw.this.t),
            max_v=pw.reducers.max(pw.this.v),
        )


def test_runtime_type_check_decorator():
    @check_arg_types
    def foo(x: int):
        pass

    with pytest.raises(TypeError) as e:
        foo("123")
    assert (
        "parameter x='123' violates type hint <class 'int'>,"
        + " as str '123' not instance of int."
        in remove_ansi_escape_codes(str(e.value))
    )


@contextlib.contextmanager
def _assert_error_trace(error_type: type, *, match: str | None = None):
    file_name = os.path.abspath(__file__)
    with pytest.raises(error_type, match=match) as e:
        yield e
    assert re.match(
        rf"Occurred here:\n    Line: .* # cause\n    File: {re.escape(file_name)}:\d+$",
        e.value._pathway_trace_note,
    )


def test_traceback_expression():
    input = T(
        """
        v
        1
        2
        3
        """
    )

    with _assert_error_trace(TypeError):
        input.select(ret=pw.this.v <= "foo")  # cause


def test_traceback_rust_expression():
    input = T(
        """
        foo | bar
        1   | 2
        2   | 3
        3   | 0
        """
    )

    input.select(r=pw.this.foo / pw.this.bar)  # cause

    with _assert_error_trace(ZeroDivisionError):
        run_all(runtime_typechecking=False)


@pytest.mark.xfail
def test_traceback_async_apply():
    input = T(
        """
            | foo
        1   | 1
        2   | 2
        3   | 3
        """
    )

    async def inc(_):
        raise ValueError()

    input.select(ret=pw.apply_async(inc, pw.this.foo))  # cause

    with _assert_error_trace(ValueError):
        run_all()


def test_traceback_iterate():
    def iterate(iterated):
        result = iterated.select(val=pw.declare_type(str, pw.this.val))  # cause

        return result

    input = T(
        """
        val
        1
        2
        """
    )

    with _assert_error_trace(TypeError):
        pw.iterate(iterate, iterated=input)


def test_traceback_transformers_1():
    t = T(
        """
        time
        1
        """
    )

    @transformer
    class syntax_error_transformer:
        class my_table(ClassArg):
            time = input_attribute()

            @output_attribute
            def output_col(self):
                return self.transfomer.my_table[self.id].time  # cause

    t = syntax_error_transformer(my_table=t).my_table

    with _assert_error_trace(AttributeError):
        run_all()


def test_traceback_transformers_2():
    t = T(
        """
        time
        1
        """
    )

    @transformer
    class syntax_error_transformer:
        class my_table(ClassArg):
            time = input_attribute()

            @output_attribute
            def output_col(self):
                return self.transformer.my_tablee[self.id].time  # cause

    t = syntax_error_transformer(my_table=t).my_table

    with _assert_error_trace(AttributeError):
        run_all()


def test_traceback_transformers_3():
    t = T(
        """
        time
        1
        """
    )

    @transformer
    class syntax_error_transformer:
        class my_table(ClassArg):
            time = input_attribute()

            @output_attribute
            def output_col(self):
                return self.transformer.my_table[self.id].foo  # cause

    t = syntax_error_transformer(my_table=t).my_table

    with _assert_error_trace(AttributeError):
        run_all()


def test_traceback_transformers_4():
    t = T(
        """
        time
        1
        """
    )

    @transformer
    class syntax_error_transformer:
        class my_table(ClassArg):
            time = input_attribute()

            @output_attribute
            def output_col(self):
                return self.transformer.my_table["asdf"].time  # cause

    t = syntax_error_transformer(my_table=t).my_table

    with _assert_error_trace(TypeError):
        run_all()


def test_traceback_connectors_1():
    df = pd.DataFrame({"data": [1, 2, 3]})
    with _assert_error_trace(KeyError):
        pw.debug.table_from_pandas(df, id_from=["non-existing-column"])  # cause


def test_traceback_connectors_2(tmp_path):
    pw.io.csv.write(  # cause
        pw.Table.empty(), tmp_path / "non_existing_directory" / "output.csv"
    )
    with _assert_error_trace(OSError):
        run_all()


def test_traceback_static():
    table1 = T(
        """
            | foo
        1   | 1
        """
    )
    table2 = T(
        """
            | bar
        2   | 2
        """
    )
    with _assert_error_trace(ValueError):
        table1.concat(table2)  # cause
    with _assert_error_trace(ValueError):
        table1 + table2  # cause
    with _assert_error_trace(AttributeError):
        table1.non_existing  # cause
    with _assert_error_trace(KeyError):
        table1.select(pw.this.non_existing)  # cause


@pytest.mark.parametrize(
    "func",
    [
        pw.io.csv.read,
        pw.io.fs.read,
        pw.io.http.read,
        pw.io.jsonlines.read,
        pw.io.kafka.read,
        pw.io.minio.read,
        pw.io.plaintext.read,
        pw.io.python.read,
        pw.io.redpanda.read,
        pw.io.csv.write,
        pw.io.fs.write,
        pw.io.http.write,
        pw.io.jsonlines.write,
        pw.io.kafka.write,
        pw.io.logstash.write,
        pw.io.null.write,
        pw.io.postgres.write,
        pw.io.redpanda.write,
        pw.io.elasticsearch.write,
    ],
)
def test_traceback_early_connector_errors(func):
    with _assert_error_trace(TypeError):
        func()  # cause


def test_groupby_reduce_bad_column():
    with _assert_error_trace(
        ValueError,
        match=re.escape(
            "You cannot use <table1>.email in this reduce statement.\n"
            + "Make sure that <table1>.email is used in a groupby or wrap it with a reducer, "
            + "e.g. pw.reducers.count(<table1>.email)"
        ),
    ):
        purchases = T(
            """
        purchase_id | user_id |  email            | amount
        1           | 1       | user1@example.com | 15
        2           | 2       | user2@example.com | 18
        """
        )

        purchases.groupby(purchases.user_id).reduce(  # cause
            user_id=pw.this.user_id,
            email=pw.this.email,
            total_amount=pw.reducers.sum(pw.this.amount),
        )


def test_filter_bad_expression():
    with _assert_error_trace(
        ValueError,
        match=re.escape(
            "You cannot use <table1>.last_timestamp in this context."
            + " Its universe is different than the universe of the table the method"
            + " was called on. You can use <table1>.with_universe_of(<table2>)"
            + " to assign universe of <table2> to <table1> if you're sure their sets of"
            + " keys are equal."
        ),
    ):
        t_input = T(
            """
         request_uri | timestamp
         /home       | 1633024800
         /about      | 1633024860
            """
        )

        last_timestamp = t_input.reduce(
            last_timestamp=pw.reducers.max(t_input.timestamp)
        )
        t_input.filter(  # cause
            t_input.timestamp >= last_timestamp.last_timestamp - 3600
        )


def test_method_in_pathway_this():
    t1 = pw.debug.table_from_markdown(
        """
    join
      2
     12
    """
    )
    with _assert_error_trace(
        ValueError,
        match=re.escape(
            "join is a method name. It is discouraged to use it as a column name. "
            + "If you really want to use it, use pw.this['join']."
        ),
    ):
        t1.select(pw.this.join)  # cause


def test_table_getitem():
    tab = T(
        """
            a
            1
            2
        """
    )

    with _assert_error_trace(
        ValueError,
        match=re.escape(
            "Table.__getitem__ argument has to be a ColumnReference to the same table or pw.this, or a string "
            + "(or a list of those)."
        ),
    ):
        tab[tab.copy().a]  # cause


def test_from_columns():
    with _assert_error_trace(
        ValueError,
        match=re.escape("Table.from_columns() cannot have empty arguments list"),
    ):
        pw.Table.from_columns()  # cause


def test_groupby():
    left = T(
        """
    pet  |  owner  | age
     1   | Alice   | 10
     1   | Bob     | 9
     2   | Alice   | 8
     1   | Bob     | 7
    """
    ).with_columns(pet=pw.this.pointer_from(pw.this.pet))

    res = left.groupby(left.pet, id=left.pet).reduce(
        left.pet,
        agesum=pw.reducers.sum(left.age),
    )

    expected = T(
        """
          | pet | agesum
        1 | 1   | 26
        2 | 2   | 8
        """
    ).with_columns(pet=left.pointer_from(pw.this.pet))

    assert_table_equality(res, expected)

    with _assert_error_trace(
        ValueError,
        match=re.escape(
            "Table.groupby() received id argument and is grouped by a single column,"
            + " but the arguments are not equal.\n"
            + "Consider using <table>.groupby(id=...), skipping the positional argument."
        ),
    ):
        res = left.groupby(left.age, id=left.pet).reduce(  # cause
            left.pet,
        )

    with _assert_error_trace(
        ValueError,
        match=re.escape(
            "Table.groupby() cannot have id argument when grouping by multiple columns."
        ),
    ):
        res = left.groupby(left.age, left.pet, id=left.pet).reduce(  # cause
            left.pet,
        )


def test_update_cells():
    left = T(
        """
      | pet  |  owner
    1 |  1   | Alice
    2 |  1   | Bob
    3 |  2   | Alice
    4 |  1   | Bob
    """
    )

    right = T(
        """
      | pet  |  owner  | age
    1 |  1   | Alice   | 10
    2 |  1   | Bob     | 9
    """
    )

    with _assert_error_trace(
        ValueError,
        match=re.escape(
            "Columns of the argument in Table.update_cells() not present in the updated table: ['age']."
        ),
    ):
        left.update_cells(right)  # cause


def test_update_types():
    with _assert_error_trace(
        ValueError,
        match=re.escape(
            "Table.update_types() argument name has to be an existing table column name."
        ),
    ):
        t = T(
            """
            foor
            22
            24
            """
        )
        t.update_types(bar=int)  # cause


def test_flatten():
    with _assert_error_trace(
        TypeError,
        match=re.escape(
            "Table.flatten() missing 1 required positional argument: 'to_flatten'"
        ),
    ):
        T(
            """
            foor
            22
            24
            """
        ).flatten()  # cause


def test_slices_1():
    tab = T(
        """
            col | on
            a   | 11
            b   | 12
            c   | 13
        """
    )

    with _assert_error_trace(
        ValueError,
        match=re.escape(
            "TableSlice method arguments should refer to table of which the slice was created."
        ),
    ):
        tab.slice[tab.copy().col]  # cause

    with _assert_error_trace(
        ValueError,
        match=re.escape(
            "TableSlice expects 'col' or this.col argument as column reference."
        ),
    ):
        tab.slice[pw.left.col]  # cause

    with _assert_error_trace(
        KeyError,
        match=re.escape(
            "Column name 'foo' not found in a TableSlice({'col': <table1>.col, 'on': <table1>.on})."
        ),
    ):
        tab.slice.without("foo")  # cause

    with _assert_error_trace(
        KeyError,
        match=re.escape(
            "Column name 'foo' not found in a TableSlice({'col': <table1>.col, 'on': <table1>.on})."
        ),
    ):
        tab.slice.rename({"foo": "bar"})  # cause

    with _assert_error_trace(
        ValueError,
        match=re.escape(
            "'select' is a method name. It is discouraged to use it as a column name."
            + " If you really want to use it, use ['select']."
        ),
    ):
        tab.slice.select  # cause


def test_this():
    with _assert_error_trace(
        TypeError,
        match=re.escape("You cannot instantiate `this` class."),
    ):
        pw.this()  # cause


def test_restrict():
    t1 = pw.debug.table_from_markdown(
        """
          | a | b
        1 | 6 | 2
       """
    )
    t2 = pw.debug.table_from_markdown(
        """
            | c
          2 | 2
        """
    )

    with _assert_error_trace(
        ValueError,
        match=re.escape("other universe has to be a subset of self universe."),
    ):
        t1.restrict(t2)  # cause

    with _assert_error_trace(
        KeyError,
        match=re.escape("key missing in output table"),
    ):
        pw.universes.promise_is_subset_of(t2, t1)
        t1.restrict(t2)  # cause
        run_all()
