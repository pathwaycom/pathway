# Copyright © 2024 Pathway

import pandas as pd

import pathway as pw
from pathway.internals.expression_printer import ExpressionFormatter
from pathway.tests.utils import T, deprecated_call_here


def test_column_reference():
    t = T(
        """
    pet  |  owner  | age
     1   | Alice   | 10
        """
    )
    assert repr(t.pet) == "<table1>.pet"


def test_column_binary_op():
    t = T(
        """
    pet  |  owner  | age
     1   | Alice   | 10
        """
    )
    assert repr(t.pet + t.age) == "(<table1>.pet + <table1>.age)"
    assert repr(t.pet - t.age) == "(<table1>.pet - <table1>.age)"
    assert repr(t.pet * t.age) == "(<table1>.pet * <table1>.age)"
    assert repr(t.pet / t.age) == "(<table1>.pet / <table1>.age)"
    assert repr(t.pet // t.age) == "(<table1>.pet // <table1>.age)"
    assert repr(t.pet**t.age) == "(<table1>.pet ** <table1>.age)"
    assert repr(t.pet % t.age) == "(<table1>.pet % <table1>.age)"
    assert repr(t.pet == t.age) == "(<table1>.pet == <table1>.age)"
    assert repr(t.pet != t.age) == "(<table1>.pet != <table1>.age)"
    assert repr(t.pet < t.age) == "(<table1>.pet < <table1>.age)"
    assert repr(t.pet <= t.age) == "(<table1>.pet <= <table1>.age)"
    assert repr(t.pet > t.age) == "(<table1>.pet > <table1>.age)"
    assert repr(t.pet >= t.age) == "(<table1>.pet >= <table1>.age)"


def test_2_args():
    t = T(
        """
    pet  |  owner  | age
     1   | Alice   | 10
        """
    )
    tt = t.copy()
    assert repr(t.pet + tt.age) == "(<table1>.pet + <table2>.age)"


def test_3_args():
    t = T(
        """
    pet  |  owner  | age
     1   | Alice   | 10
        """
    )
    tt = t.copy()
    assert (
        repr(pw.if_else(t.pet == 1, tt.pet, t.age))
        == "pathway.if_else((<table1>.pet == 1), <table2>.pet, <table1>.age)"
    )


def test_column_unary_op():
    t = T(
        """
    pet  |  owner  | age
     1   | Alice   | 10
        """
    )
    assert repr(-t.pet) == "(-<table1>.pet)"
    assert repr(~t.pet) == "(~<table1>.pet)"


def test_reducer():
    t = T(
        """
    pet  |  owner  | age
     1   | Alice   | 10
        """
    )
    assert repr(pw.reducers.min(t.pet)) == "pathway.reducers.min(<table1>.pet)"
    assert repr(pw.reducers.max(t.pet)) == "pathway.reducers.max(<table1>.pet)"
    assert repr(pw.reducers.sum(t.pet)) == "pathway.reducers.sum(<table1>.pet)"
    assert (
        repr(pw.reducers.sorted_tuple(t.pet))
        == "pathway.reducers.sorted_tuple(<table1>.pet, skip_nones=False)"
    )
    assert (
        repr(pw.reducers.tuple(t.pet, skip_nones=True))
        == "pathway.reducers.tuple(<table1>.pet, skip_nones=True)"
    )
    assert repr(pw.reducers.count()) == "pathway.reducers.count()"
    assert repr(pw.reducers.argmin(t.pet)) == "pathway.reducers.argmin(<table1>.pet)"
    assert repr(pw.reducers.argmax(t.pet)) == "pathway.reducers.argmax(<table1>.pet)"


def test_apply():
    t = T(
        """
    pet  |  owner  | age
     1   | Alice   | 10
        """
    )
    assert (
        repr(pw.apply(lambda x, y: x + y, t.pet, t.age))
        == "pathway.apply(<lambda>, <table1>.pet, <table1>.age)"
    )


def test_apply_udf():
    @pw.udf
    def foo(a: int) -> int:
        return a

    t = T(
        """
    pet  |  owner  | age
     1   | Alice   | 10
        """
    )
    assert repr(foo(t.age)) == "pathway.apply(foo, <table1>.age)"


def test_async_apply_udf():
    @pw.udf
    async def foo(a: int) -> int:
        return a

    t = T(
        """
    pet  |  owner  | age
     1   | Alice   | 10
        """
    )
    assert repr(foo(t.age)) == "pathway.apply_async(foo, <table1>.age)"


def test_fully_async_apply_udf():
    @pw.udf(executor=pw.udfs.fully_async_executor())
    async def foo(a: int) -> int:
        return a

    t = T(
        """
    pet  |  owner  | age
     1   | Alice   | 10
        """
    )
    assert repr(foo(t.age)) == "pathway.apply_fully_async(foo, <table1>.age)"


def test_cast():
    t = T(
        """
    pet  |  owner  | age
     1   | Alice   | 10
        """
    )
    assert repr(pw.cast(int, t.pet)) == "pathway.cast(INT, <table1>.pet)"
    assert repr(pw.cast(float, t.pet)) == "pathway.cast(FLOAT, <table1>.pet)"


def test_convert():
    t = T(
        """
            | pet
        1   | foo
        """
    )
    assert repr(t.pet.as_int()) == "pathway.as_int(<table1>.pet)"
    assert repr(t.pet.as_float()) == "pathway.as_float(<table1>.pet)"
    assert repr(t.pet.as_str()) == "pathway.as_str(<table1>.pet)"
    assert repr(t.pet.as_bool()) == "pathway.as_bool(<table1>.pet)"


def test_declare_type():
    t = T(
        """
    pet  |  owner  | age
     1   | Alice   | 10
        """
    )
    assert (
        repr(pw.declare_type(int, t.pet)) == "pathway.declare_type(INT, <table1>.pet)"
    )
    assert (
        repr(pw.declare_type(float, t.pet))
        == "pathway.declare_type(FLOAT, <table1>.pet)"
    )


def test_coalesce():
    t = T(
        """
    pet  |  owner  | age
     1   | Alice   | 10
        """
    )
    assert (
        repr(pw.coalesce(t.pet, t.age))
        == "pathway.coalesce(<table1>.pet, <table1>.age)"
    )


def test_require():
    t = T(
        """
    pet  |  owner  | age
     1   | Alice   | 10
        """
    )
    assert (
        repr(pw.require(t.pet, t.age)) == "pathway.require(<table1>.pet, <table1>.age)"
    )


def test_if_else():
    t = T(
        """
    pet  |  owner  | age
     1   | Alice   | 10
        """
    )
    assert (
        repr(pw.if_else(t.pet == 1, t.pet, t.age))
        == "pathway.if_else((<table1>.pet == 1), <table1>.pet, <table1>.age)"
    )


def test_pointer():
    t = T(
        """
    pet  |  owner  | age
     1   | Alice   | 10
        """
    )
    assert repr(t.pointer_from(4)) == "<table1>.pointer_from(4)"
    assert repr(t.pointer_from(t.pet)) == "<table1>.pointer_from(<table1>.pet)"


def test_method_call():
    date_str = "2023-03-25 12:05:00.000000001+01:00"
    fmt = "%Y-%m-%d %H:%M:%S.%f%z"
    df = pd.DataFrame(
        {
            "txt": [date_str],
            "ts": [pd.to_datetime(date_str, format=fmt)],
            "td": pd.Timedelta(days=1),
            "i": [42],
            "f": [42.3],
        }
    )
    t = pw.debug.table_from_pandas(df)
    assert repr(t.ts.dt.nanosecond()) == "(<table1>.ts).dt.nanosecond()"
    assert repr(t.ts.dt.microsecond()) == "(<table1>.ts).dt.microsecond()"
    assert repr(t.ts.dt.millisecond()) == "(<table1>.ts).dt.millisecond()"
    assert repr(t.ts.dt.second()) == "(<table1>.ts).dt.second()"
    assert repr(t.ts.dt.minute()) == "(<table1>.ts).dt.minute()"
    assert repr(t.ts.dt.hour()) == "(<table1>.ts).dt.hour()"
    assert repr(t.ts.dt.day()) == "(<table1>.ts).dt.day()"
    assert repr(t.ts.dt.month()) == "(<table1>.ts).dt.month()"
    assert repr(t.ts.dt.year()) == "(<table1>.ts).dt.year()"
    with deprecated_call_here(match="unit"):
        assert repr(t.ts.dt.timestamp()) == "(<table1>.ts).dt.timestamp()"
    assert repr(t.ts.dt.timestamp("s")) == "(<table1>.ts).dt.timestamp('s')"
    assert repr(t.ts.dt.strftime("%m")) == "(<table1>.ts).dt.strftime('%m')"
    assert repr(t.td.dt.nanoseconds()) == "(<table1>.td).dt.nanoseconds()"
    assert repr(t.td.dt.microseconds()) == "(<table1>.td).dt.microseconds()"
    assert repr(t.td.dt.milliseconds()) == "(<table1>.td).dt.milliseconds()"
    assert repr(t.td.dt.seconds()) == "(<table1>.td).dt.seconds()"
    assert repr(t.td.dt.hours()) == "(<table1>.td).dt.hours()"
    assert repr(t.td.dt.days()) == "(<table1>.td).dt.days()"
    assert repr(t.td.dt.weeks()) == "(<table1>.td).dt.weeks()"
    assert repr(t.txt.dt.strptime("%m")) == "(<table1>.txt).dt.strptime('%m')"
    assert repr(t.ts.dt.round("D")) == "(<table1>.ts).dt.round('D')"
    assert repr(t.ts.dt.round(t.td)) == "(<table1>.ts).dt.round(<table1>.td)"
    assert repr(t.ts.dt.floor("D")) == "(<table1>.ts).dt.floor('D')"
    assert repr(t.ts.dt.floor(t.td)) == "(<table1>.ts).dt.floor(<table1>.td)"
    assert repr(t.ts.dt.weekday()) == "(<table1>.ts).dt.weekday()"
    assert repr(t.i.dt.from_timestamp("s")) == "(<table1>.i).dt.from_timestamp('s')"
    assert repr(t.f.dt.from_timestamp("s")) == "(<table1>.f).dt.from_timestamp('s')"


def test_3_args_with_info():
    t = T(
        """
    pet  |  owner  | age
     1   | Alice   | 10
        """
    )
    tt = t.copy()
    printer = ExpressionFormatter()
    assert (
        printer.eval_expression(pw.if_else(t.pet == 1, tt.pet, t.age))
        == "pathway.if_else((<table1>.pet == 1), <table2>.pet, <table1>.age)"
    )
    print(printer.print_table_infos())


def test_make_tuple():
    t = T(
        """
    pet  |  owner  | age
     1   | Alice   | 10
        """
    )
    assert (
        repr(pw.make_tuple(t.pet, t.age * 2, t.owner))
        == "pathway.make_tuple(<table1>.pet, (<table1>.age * 2), <table1>.owner)"
    )


def test_sequence_get():
    t = T(
        """
    pet  |  owner  | age
     1   | Alice   | 10
        """
    )
    assert repr(t.owner.get(2, "x")) == "(<table1>.owner).get(2, 'x')"
    assert repr(t.owner.get(t.pet, "x")) == "(<table1>.owner).get(<table1>.pet, 'x')"
    assert repr(t.owner[2]) == "(<table1>.owner)[2]"
    assert repr(t.owner[t.pet]) == "(<table1>.owner)[<table1>.pet]"


def test_unwrap():
    t = T(
        """
    pet  |  owner  | age
     1   | Alice   | 10
        """
    )
    assert repr(pw.unwrap(t.pet)) == "pathway.unwrap(<table1>.pet)"


def test_fill_error():
    t = T(
        """
    pet  |  owner  | age
     1   | Alice   | 10
        """
    )
    assert (
        repr(pw.fill_error(t.pet, t.age))
        == "pathway.fill_error(<table1>.pet, <table1>.age)"
    )
