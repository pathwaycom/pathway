# Copyright © 2024 Pathway

from __future__ import annotations

import pytest

from pathway import Schema, Table, reducers, this
from pathway.internals import (
    ClassArg,
    attribute,
    input_attribute,
    input_method,
    method,
    output_attribute,
    transformer,
)
from pathway.tests.utils import (
    T,
    assert_table_equality,
    assert_table_equality_wo_index,
    assert_table_equality_wo_types,
    run_all,
)


def test_simple_transformer():
    class OutputSchema(Schema):
        ret: int

    @transformer
    class foo_transformer:
        class table(ClassArg, output=OutputSchema):
            arg = input_attribute()

            @output_attribute
            def ret(self) -> int:
                return self.arg + 1

    table = T(
        """
            | arg
        1   | 1
        2   | 2
        3   | 3
        """
    )
    ret = foo_transformer(table).table

    assert_table_equality(
        ret,
        T(
            """
                | ret
            1   | 2
            2   | 3
            3   | 4
            """
        ),
    )


def test_aux_objects():
    @transformer
    class foo_transformer:
        class table(ClassArg):
            arg = input_attribute()

            const = 10

            def fun(self, a) -> int:
                return a * self.arg + self.const

            @staticmethod
            def sfun(b) -> int:
                return b * 100

            @attribute
            def attr(self) -> int:
                return self.arg / 2

            @output_attribute
            def ret(self) -> int:
                return (
                    self.arg
                    + self.const
                    + self.fun(1)
                    + self.sfun(self.arg)
                    + self.attr
                )

    table = T(
        """
            | arg
        1   | 10
        2   | 20
        3   | 30
        """
    )

    ret = foo_transformer(table).table
    assert_table_equality(
        ret,
        T(
            """
                | ret
            1   | 1045
            2   | 2070
            3   | 3095
            """
        ),
    )


def test_skips():
    @transformer
    class list_traversal:
        class nodes(ClassArg):
            next = input_attribute()
            val = input_attribute()

        class requests(ClassArg):
            node = input_attribute()
            steps = input_attribute()

            @output_attribute
            def reached_node(self) -> int:
                node = self.transformer.nodes[self.node]
                for _ in range(self.steps):
                    node = self.transformer.nodes[node.next]
                return node.id

            @output_attribute
            def reached_value(self) -> int:
                node = self.transformer.nodes[self.reached_node]
                return node.val

    nodes = T(
        """
            | next | val
        1   | 2    | 11
        2   | 3    | 12
        3   |      | 13
        """
    ).with_columns(next=this.pointer_from(this.next))

    requests = T(
        """
            | node | steps
        10  | 1    | 1
        20  | 3    | 0
        """
    ).with_columns(node=nodes.pointer_from(this.node))

    replies = list_traversal(nodes, requests).requests
    assert_table_equality_wo_types(  # XXX: no Pointer in schema
        replies,
        T(
            """
                | reached_node  | reached_value
            10  | 2             | 12
            20  | 3             | 13
            """
        ).with_columns(reached_node=nodes.pointer_from(this.reached_node)),
    )


def test_output_attribute_rename():
    class OutputSchema(Schema):
        foo: int

    @transformer
    class foo_transformer:
        class table(ClassArg, output=OutputSchema):
            arg = input_attribute()

            @output_attribute(output_name="foo")
            def ret(self) -> int:
                return self.arg + 1

    table = T(
        """
            | arg
        1   | 1
        """
    )
    ret = foo_transformer(table).table

    assert_table_equality(
        ret,
        T(
            """
                | foo
            1   | 2
            """
        ),
    )


def test_output_attribute_rename_without_schema():
    @transformer
    class foo_transformer:
        class table(ClassArg):
            arg = input_attribute()

            @output_attribute(output_name="foo")
            def ret(self) -> int:
                return self.arg + 1

    table = T(
        """
            | arg
        1   | 1
        """
    )
    ret = foo_transformer(table).table

    assert_table_equality(
        ret,
        T(
            """
                | foo
            1   | 2
            """
        ),
    )


def test_output_schema_validation_error():
    with pytest.raises(Exception):

        class OutputSchema(Schema):
            foo: int

        @transformer
        class foo_transformer:
            class table(ClassArg, output=OutputSchema):
                arg = input_attribute()

                @output_attribute(output_name="bar")
                def foo(self) -> int:
                    return self.arg + 1

        foo_transformer(
            T(
                """
                    | arg
                1   | 1
                """
            )
        )


def test_call_self_method():
    @transformer
    class foo_transformer:
        class table(ClassArg):
            a: int = input_attribute()

            @output_attribute
            def b(self) -> int:
                return self.a + self.c(self.a)

            @method
            def c(self, arg) -> int:
                return self.a * arg

    table = T(
        """
            | a
        1   | 1
        """
    )

    method_table = foo_transformer(table).table
    result = method_table.select(ret=method_table.b)

    assert_table_equality(
        result,
        T(
            """
                | ret
            1   | 2
            """
        ),
    )


def test_simple_method():
    @transformer
    class foo_transformer:
        class table(ClassArg):
            a: int = input_attribute()

            @output_attribute
            def b(self) -> int:
                return self.a * 10

            @method
            def c(self, arg) -> int:
                return (self.a + self.b) * arg

    table = T(
        """
            | a
        1   | 1
        2   | 2
        3   | 3
        """
    )

    method_table = foo_transformer(table).table
    result = method_table.select(ret=method_table.c(10))

    assert_table_equality(
        result,
        T(
            """
                | ret
            1   | 110
            2   | 220
            3   | 330
            """
        ),
    )


def test_select_method_column():
    @transformer
    class foo_transformer:
        class table(ClassArg):
            a: int = input_attribute()

            @output_attribute
            def b(self) -> int:
                return self.a * 10

            @method
            def c(self, arg) -> int:
                return (self.a + self.b) * arg

    input = T(
        """
            | a
        1   | 1
        2   | 2
        3   | 3
        """
    )

    method_table = foo_transformer(input).table
    method_table = method_table.select(foo=method_table.c)
    result = method_table.select(ret=method_table.foo(10))

    assert_table_equality(
        result,
        T(
            """
                | ret
            1   | 110
            2   | 220
            3   | 330
            """
        ),
    )


def test_table_add_method_table():
    @transformer
    class foo_transformer:
        class table(ClassArg):
            a: int = input_attribute()

            @output_attribute
            def b(self) -> int:
                return self.a * 10

            @method
            def c(self, arg) -> int:
                return (self.a + self.b) * arg

    input = T(
        """
            | a
        1   | 1
        2   | 2
        3   | 3
        """
    )

    method_table = foo_transformer(input).table + input.select(d=-input.a)
    result = method_table.select(ret=method_table.c(10))

    assert_table_equality(
        result,
        T(
            """
                | ret
            1   | 110
            2   | 220
            3   | 330
            """
        ),
    )


def test_method_call_in_transformer_with_input_attribute():
    @transformer
    class foo_transformer:
        class table(ClassArg):
            a: int = input_attribute()

            @method
            def c(self, arg) -> int:
                return self.a * arg

    @transformer
    class transformer_using_method:
        class table(ClassArg):
            a = input_attribute()
            method: int = input_method(int)

            @output_attribute
            def result(self) -> int:
                return self.method(self.a)

    input1 = T(
        """
            | a
        1   | 1
        2   | 2
        3   | 3
        """
    )
    input2 = T(
        """
            | a
        1   | 4
        2   | 5
        3   | 6
        """
    )

    method_table: Table = foo_transformer(input1).table

    table = method_table.select(method=method_table.c, a=input2.a)
    result = transformer_using_method(table=table).table

    assert_table_equality(
        result,
        T(
            """
                | result
            1   | 4
            2   | 10
            3   | 18
            """
        ),
    )


def test_method_call_in_transformer_with_output_attribute():
    @transformer
    class method_transformer:
        class table(ClassArg):
            a: int = input_attribute()

            @output_attribute
            def b(self) -> int:
                return self.a * 10

            @method
            def c(self, arg) -> int:
                return (self.a + self.b) * arg

    @transformer
    class transformer_using_method:
        class table(ClassArg):
            c: int = input_method(int)
            arg: int = input_attribute()

            @output_attribute
            def result(self) -> int:
                return self.c(self.arg) + self.c(10)

    input = T(
        """
            | a
        1   | 1
        2   | 2
        3   | 3
        """
    )
    method_table: Table = method_transformer(input).table
    input = method_table.select(c=method_table.c, arg=method_table.b)

    result = transformer_using_method(table=input).table

    assert_table_equality(
        result,
        T(
            """
                | result
            1   | 220
            2   | 660
            3   | 1320
            """
        ),
    )


def test_method_call_in_transformer_with_attribute():
    @transformer
    class method_transformer:
        class table(ClassArg):
            a = input_attribute()

            @attribute
            def b(self) -> int:
                return self.a * 10

            @method
            def c(self, arg) -> int:
                return (self.a + self.b) * arg

    @transformer
    class transformer_using_method:
        class table(ClassArg):
            c: int = input_method(int)
            arg: int = input_attribute()

            @output_attribute
            def result(self) -> int:
                return self.c(self.arg) + self.c(10)

    input = T(
        """
            | a
        1   | 1
        2   | 2
        3   | 3
        """
    )
    method_table: Table = method_transformer(input).table
    input = method_table.select(c=method_table.c, arg=20)

    result = transformer_using_method(table=input).table

    assert_table_equality(
        result,
        T(
            """
                | result
            1   | 330
            2   | 660
            3   | 990
            """
        ),
    )


def test_method_call_in_transformer_with_method():
    @transformer
    class method_transformer:
        class table(ClassArg):
            a: int = input_attribute()

            @method
            def b(self, arg) -> int:
                return self.a * arg

            @method
            def c(self, arg) -> int:
                return self.b(arg) + arg

    @transformer
    class transformer_using_method:
        class table(ClassArg):
            c: int = input_method(int)
            arg: int = input_attribute()

            @output_attribute
            def result(self) -> int:
                return self.c(self.arg) + self.arg

    input = T(
        """
            | a
        1   | 1
        2   | 2
        3   | 3
        """
    )
    method_table: Table = method_transformer(input).table
    input = method_table.select(c=method_table.c, arg=input.a)

    result = transformer_using_method(table=input).table

    assert_table_equality(
        result,
        T(
            """
                | result
            1   | 3
            2   | 8
            3   | 15
            """
        ),
    )


def test_method_call_in_transformer_with_input_method():
    @transformer
    class foo:
        class table(ClassArg):
            a = input_attribute()

            @method
            def c(self, arg) -> int:
                return self.a * arg

    @transformer
    class bar:
        class table(ClassArg):
            a = input_attribute()
            b = input_method(int)

            @method
            def c(self, arg) -> int:
                return self.b(self.a + arg)

    @transformer
    class baz:
        class table(ClassArg):
            a = input_attribute()
            b = input_method(int)

            @output_attribute
            def result(self) -> int:
                return self.b(self.a)

    input = T(
        """
            | foo
        1   | 10
        2   | 20
        3   | 30
        """
    )

    foo_table: Table = foo(table=input.select(a=10)).table
    bar_table: Table = bar(table=foo_table.select(b=foo_table.c, a=5)).table

    result: Table = baz(table=input.select(a=input.foo, b=bar_table.c)).table

    assert_table_equality(
        result,
        T(
            """
                | result
            1   | 150
            2   | 250
            3   | 350
            """
        ),
    )


def test_method_call_in_transformer_neighbor_dependency():
    @transformer
    class method_transformer:
        class input_table(ClassArg):
            a: int = input_attribute()

        class method_table(ClassArg):
            @method
            def c(self, arg) -> int:
                a = self.transformer.input_table[self.id].a
                return a * arg

    @transformer
    class transformer_using_method:
        class table(ClassArg):
            c: int = input_method(int)
            arg: int = input_attribute()

            @output_attribute
            def result(self) -> int:
                return self.c(self.arg)

    table = T(
        """
            | bar
        1   | 10
        2   | 20
        3   | 30
        """
    )
    method_table: Table = method_transformer(
        input_table=table.select(a=table.bar),
        method_table=table.select(),
    ).method_table

    input = method_table.select(c=method_table.c, arg=table.bar)
    result = transformer_using_method(table=input).table

    assert_table_equality(
        result,
        T(
            """
                | result
            1   | 100
            2   | 400
            3   | 900
            """
        ),
    )


def test_forbid_ref_unrelated_class_arg():
    @transformer
    class foo_transformer:
        class param(ClassArg):
            a: int = input_attribute()

        class table(ClassArg):
            a_ref: int = input_attribute()

            @output_attribute
            def fetch_param(self) -> int:
                return self.transformer.param[self.a_ref].a

    param = T(
        """
            | a
        10 | 100
        """
    )

    table = T(
        """
            | a_ref
        1 | 10
        """
    )

    # Now check that we can't refer to classargs from other transformers
    @transformer
    class transformer_buggy:
        class param(ClassArg):
            a: int = input_attribute()

        class table(ClassArg):
            a_ref: int = input_attribute()

            @output_attribute
            def fetch_param(self) -> int:
                # bug: ref to wrong table
                return foo_transformer.param(self, self.a_ref).a

    transformer_buggy(param, table)
    with pytest.raises(ValueError):
        run_all()


def _fixture_twoclass_transformers():
    @transformer
    class foo_transformer:
        class hypers(ClassArg):
            a: int = input_attribute()

        class table(ClassArg):
            a: int = input_attribute()
            h: int = input_attribute()

            @output_attribute
            def b(self) -> int:
                return self.a * 10 + self.transformer.hypers[self.h].a

            @method
            def c(self, arg) -> int:
                return (self.a + self.b) * arg

    @transformer
    class transformer2:
        # reusing the names on purpose for more testing
        class hypers(ClassArg):
            a: int = input_attribute()

        class table(ClassArg):
            a: int = input_attribute()
            h: int = input_attribute()

            @output_attribute
            def b(self) -> int:
                return self.a * 100 + self.transformer.hypers[self.h].a

            @method
            def c(self, arg) -> int:
                return (self.a + self.b) * arg

    @transformer
    class transformer_common:
        # reusing the names on purpose for more testing
        class hypers(ClassArg):
            a: int = input_attribute()

        class table(ClassArg):
            a: int = input_attribute()
            h: int = input_attribute()
            c1 = input_method()
            c2 = input_method()

            @output_attribute
            def b(self) -> int:
                bb = self.a * 20 + self.transformer.hypers[self.h].a
                c1_ret = self.c1(1_000)
                c2_ret = self.c2(1_000_000)
                return bb + c1_ret + c2_ret

            @method
            def c(self, arg) -> int:
                c1_ret = self.c1(1_000)
                c2_ret = self.c2(1_000_000)
                return self.b * 1_000_000_000 + c1_ret + c2_ret + arg

    return foo_transformer, transformer2, transformer_common


def test_twoclass_method():
    (
        foo_transformer,
        transformer2,
        transformer_common,
    ) = _fixture_twoclass_transformers()

    hypers = T(
        """
            | a
        11 | 1
        12 | 2
        """
    )
    table = T(
        """
            | a |  h
        6   | 1 | 11
        7   | 2 | 11
        8   | 3 | 12
        """
    ).with_columns(h=hypers.pointer_from(this.h))

    method_table: Table = foo_transformer(hypers, table).table

    assert_table_equality(
        method_table.select(method_table.b),
        T(
            """
                | b
            6   | 11
            7   | 21
            8   | 32
            """
        ),
    )

    assert_table_equality(
        method_table.select(b=method_table.b, c=method_table.c(1000)),
        T(
            """
                |  b |     c
            6   | 11 | 12000
            7   | 21 | 23000
            8   | 32 | 35000
            """
        ),
    )

    hypers2 = T(
        """
            | a
        13 | 3
        """
    )
    table2 = T(
        """
            | a |  h
        6   | 1 | 13
        7   | 2 | 13
        8   | 3 | 13
        """
    ).with_columns(h=hypers2.pointer_from(this.h))

    method_table2: Table = transformer2(hypers=hypers2, table=table2).table
    assert_table_equality(
        method_table2.select(b=method_table2.b, c=method_table2.c(1000000)),
        T(
            """
                |   b |         c
            6   | 103 | 104000000
            7   | 203 | 205000000
            8   | 303 | 306000000
            """
        ),
    )

    hypersc = T(
        """
            | a
        14 | 4
        """
    )
    tablec_constants = T(
        """
            | a |  h
        6   | 1 | 14
        7   | 2 | 14
        8   | 3 | 14
        """
    ).with_columns(h=hypers.pointer_from(this.h))

    tablec = tablec_constants.select(
        a=tablec_constants.a,
        h=tablec_constants.h,
        c1=method_table.c,
        c2=method_table2.c,
    )

    method_tablec: Table = transformer_common(hypers=hypersc, table=tablec).table
    assert_table_equality(
        method_tablec.select(b=method_tablec.b, c=method_tablec.c(1)),
        T(
            """
                |         b |                   c
            6 | 104012024 | 104012024104012001
            7 | 205023044 | 205023044205023001
            8 | 306035064 | 306035064306035001
            """
        ),
    )


def test_twoclass_method2():
    # Same as test_twoclass_method but now the common table is shared
    (
        foo_transformer,
        transformer2,
        transformer_common,
    ) = _fixture_twoclass_transformers()

    hypers = T(
        """
            | a
        11 | 1
        12 | 2
        """
    )
    table = T(
        """
            | a1 | h1 | a2 | h2 | ac | hc
        6   |  1 | 11 |  1 | 13 |  1 | 14
        7   |  2 | 11 |  2 | 13 |  2 | 14
        8   |  3 | 12 |  3 | 13 |  3 | 14
        """
    ).with_columns(
        h1=hypers.pointer_from(this.h1),
        h2=hypers.pointer_from(this.h2),
        hc=hypers.pointer_from(this.hc),
    )

    method_table: Table = foo_transformer(
        hypers=hypers, table=table.select(a=table.a1, h=table.h1)
    ).table
    assert_table_equality(
        method_table.select(b=method_table.b, c=method_table.c(1000)),
        T(
            """
                |  b |     c
            6   | 11 | 12000
            7   | 21 | 23000
            8   | 32 | 35000
            """
        ),
    )

    hypers2 = T(
        """
            | a
        13 | 3
        """
    )

    method_table2: Table = transformer2(
        hypers=hypers2, table=table.select(a=table.a2, h=table.h2)
    ).table
    assert_table_equality(
        method_table2.select(b=method_table2.b, c=method_table2.c(1000000)),
        T(
            """
                |   b |         c
            6   | 103 | 104000000
            7   | 203 | 205000000
            8   | 303 | 306000000
            """
        ),
    )

    hypersc = T(
        """
            | a
        14 | 4
        """
    )
    tablec: Table = table.select(
        a=table.ac,
        h=table.hc,
        c1=method_table.c,
        c2=method_table2.c,
    )

    method_tablec = transformer_common(hypers=hypersc, table=tablec).table
    assert_table_equality(
        method_tablec.select(b=method_tablec.b, c=method_tablec.c(1)),
        T(
            """
                |         b |                   c
            6 | 104012024 | 104012024104012001
            7 | 205023044 | 205023044205023001
            8 | 306035064 | 306035064306035001
            """
        ),
    )


def test_method_in_ix():
    @transformer
    class foo_transformer:
        class table(ClassArg):
            a: int = input_attribute()

            @method
            def method(self, arg) -> int:
                return self.a * arg

    input = T(
        """
            | c | b
        7   | 1 | 10
        8   | 2 | 10
        9   | 3 | 20
        """
    )

    reduced = input.groupby(input.b).reduce(a=input.b)

    oracle = foo_transformer(reduced).table

    result = input.select(res=oracle.ix_ref(input.b).method(input.c))

    assert_table_equality(
        result,
        T(
            """
                | res
            7   | 10
            8   | 20
            9   | 60
            """
        ),
    )


def test_method_in_groupby():
    @transformer
    class foo_transformer:
        class table(ClassArg):
            a: int = input_attribute()

            @method
            def method(self, arg) -> int:
                return self.a * arg

    input = T(
        """
        a | grouper
        1 | 1
        2 | 1
        3 | 2
        4 | 2
        5 | 3
        6 | 3
        """,
    )

    oracle = foo_transformer(input).table

    combined = input + oracle

    result = combined.groupby(combined.grouper).reduce(
        res=reducers.sum(combined.method(combined.a))
    )
    assert_table_equality_wo_index(
        result,
        T(
            """
            res
            5
            25
            61
            """,
        ),
    )
