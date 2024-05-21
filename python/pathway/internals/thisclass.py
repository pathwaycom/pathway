# Copyright © 2024 Pathway

from __future__ import annotations

from typing import TYPE_CHECKING, Any, overload

from pathway.internals import expression as expr
from pathway.internals.trace import trace_user_frame

if TYPE_CHECKING:
    from pathway.internals.joins import Joinable

import itertools

from pathway.internals.column_namespace import ColumnNamespace

KEY_GUARD = "__pathway_kwargs_hack"
_key_guard_counter = itertools.count()


class ThisMetaclass(type):
    @trace_user_frame
    def __getattr__(self, name: str) -> expr.ColumnReference:
        if name.startswith("__"):
            raise AttributeError

        # below a workaround so the doctest is actually run by pytest --doctest-modules
        # pytest tries to be smart and captures metaclasses that overload all getattrs
        if name == "pytest_mock_example_attribute_that_shouldnt_exist":
            raise AttributeError

        from pathway.internals.table import Table

        # special treatment for 'id' column is caused by the fact that
        # Table class has id method
        if hasattr(Table, name) and name != "id":
            raise ValueError(
                f"{name} is a method name. It is discouraged to use it as a column"
                + f" name. If you really want to use it, use pw.this['{name}']."
            )
        return self._get_colref_by_name(name, AttributeError)

    def _get_colref_by_name(self, name: str, exception_type) -> expr.ColumnReference:
        return expr.ColumnReference(_table=self, _column=None, _name=name)  # type: ignore

    # TODO:
    # create an abstract base class for Table and ThisMetaclass (AbstractTable?)
    # have ThisMetaclass explicitly implement all the methods of AbstractTable like:

    def rename(self, *args, **kwargs):
        return self._create_mock("rename", args, kwargs)

    def without(self, *args, **kwargs):
        return self._create_mock("without", args, kwargs)

    def with_prefix(self, *args, **kwargs):
        return self._create_mock("with_prefix", args, kwargs)

    def with_suffix(self, *args, **kwargs):
        return self._create_mock("with_suffix", args, kwargs)

    def ix(self, *args, **kwargs):
        return self._create_mock("ix", args, kwargs)

    def ix_ref(self, *args, **kwargs):
        return self._create_mock("ix_ref", args, kwargs)

    @property
    def slice(self):
        return self

    @property
    def C(self) -> ColumnNamespace:
        return ColumnNamespace(self)  # type: ignore

    @property
    def _C(self):
        return self.C

    @overload
    def __getitem__(self, args: str | expr.ColumnReference) -> expr.ColumnReference: ...

    @overload
    def __getitem__(self, args: list[str | expr.ColumnReference]) -> ThisMetaclass: ...

    @trace_user_frame
    def __getitem__(
        self, arg: str | expr.ColumnReference | list[str | expr.ColumnReference]
    ) -> expr.ColumnReference | ThisMetaclass:
        if isinstance(arg, expr.ColumnReference):
            if isinstance(arg.table, ThisMetaclass):
                assert arg.table is self
            return arg.table._get_colref_by_name(arg.name, KeyError)
        elif isinstance(arg, str):
            if arg.startswith(KEY_GUARD):
                return self
            else:
                return self._get_colref_by_name(arg, KeyError)
        else:
            return self._create_mock("__getitem__", [arg], {})

    @trace_user_frame
    def __iter__(self):
        class subclass(self, iter_guard):  # type: ignore[valid-type,misc]
            @classmethod
            def __iter__(self):
                raise TypeError("You cannot iterate over mock class.")

        subclass.__qualname__ = self.__qualname__ + "." + "__iter__" + "(...)"
        subclass.__name__ = "__iter__"
        return iter([subclass])

    def keys(self):
        # _key_guard_counter is necessary, otherwise key-collisions happen
        return [f"{KEY_GUARD}_{next(_key_guard_counter)}"]

    @trace_user_frame
    def __call__(self):
        raise TypeError("You cannot instantiate `this` class.")

    def pointer_from(
        self, *args: Any, optional=False, instance: expr.ColumnReference | None = None
    ):
        return expr.PointerExpression(self, *args, optional=optional, instance=instance)  # type: ignore[arg-type]

    def _base_this(self) -> ThisMetaclass:
        raise NotImplementedError

    def _eval_table(self, table: Joinable) -> Joinable:
        raise NotImplementedError

    def _eval_substitution(
        self, substitution: dict[ThisMetaclass, Joinable]
    ) -> Joinable:
        base_this: ThisMetaclass = self._base_this()
        if base_this not in substitution:
            raise TypeError(f"Usage of {base_this} not supported here.")
        return self._eval_table(substitution[base_this])

    def _create_mock(self, name, args, kwargs) -> ThisMetaclass:
        raise NotImplementedError

    def _delayed_op(self, op, *, expression=None, name=None, qualname=None):
        raise NotImplementedError

    def _delay_depth(self):
        raise NotImplementedError

    def _expression(self):
        raise NotImplementedError

    def _with_new_expression(self, expression):
        raise NotImplementedError


class _those(metaclass=ThisMetaclass):
    @classmethod
    def _eval_table(self, table: Joinable) -> Joinable:
        return table

    @classmethod
    def _delay_depth(self):
        return 0

    @classmethod
    def _create_mock(self, name, args, kwargs):
        ret = self._delayed_op(
            lambda table, _: getattr(table.slice, name)(*args, **kwargs),
            qualname=f"{self.__qualname__}.{name}(...)",
            name=name,
        )

        return ret

    @classmethod
    def _delayed_op(self, op, *, expression=None, name=None, qualname=None):
        class subclass(self):  # type: ignore[valid-type,misc]
            @classmethod
            def _expression(cls):
                return expression

            @classmethod
            def _eval_table(cls, table):
                return op(super()._eval_table(table), cls._expression())

            @classmethod
            def _delay_depth(cls):
                return super()._delay_depth() + 1

            @classmethod
            def _with_new_expression(cls, expression):
                return self._delayed_op(
                    op, expression=expression, name=name, qualname=qualname
                )

            @classmethod
            def _create_mock(self, name, args, kwargs):
                return self._delayed_op(
                    lambda table, _: getattr(table.slice, name)(*args, **kwargs),
                    expression=expression,
                    qualname=f"{self.__qualname__}.{name}(...)",
                    name=name,
                )

        if name is not None:
            subclass.__name__ = name
        if qualname is not None:
            subclass.__qualname__ = qualname
        return subclass


class iter_guard(metaclass=ThisMetaclass):
    pass


class this(_those):
    """
    Object for generating column references without holding the actual table in hand.
    Needs to be evaluated in the proper context.
    For most of the Table methods, it refers to `self`.
    For JoinResult, it refers to the left input table.


    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown('''
    ... age | owner | pet
    ... 10  | Alice | 1
    ... 9   | Bob   | 1
    ... 8   | Alice | 2
    ... ''')
    >>> t2 = t1.select(pw.this.owner, pw.this.age)
    >>> pw.debug.compute_and_print(t2, include_id=False)
    owner | age
    Alice | 8
    Alice | 10
    Bob   | 9
    """

    @classmethod
    def _base_this(self):
        return this


class left(_those):
    """
    Object for generating column references without holding the actual table in hand.
    Needs to be evaluated in the proper context.
    For `Table.join()` and `JoinResult.select()`, refers to the left input table.
    For all other situations, you need `pw.this` object.

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown('''
    ... age  | owner  | pet
    ...  10  | Alice  | 1
    ...   9  | Bob    | 1
    ...   8  | Alice  | 2
    ... ''')
    >>> t2 = pw.debug.table_from_markdown('''
    ... age  | owner  | pet | size
    ...  10  | Alice  | 3   | M
    ...  9   | Bob    | 1   | L
    ...  8   | Tom    | 1   | XL
    ... ''')
    >>> t3 = t1.join(t2, pw.left.pet == pw.right.pet, pw.left.owner == pw.right.owner).select(
    ...          age=pw.left.age, owner_name=pw.right.owner, size=pw.this.size
    ...      )
    >>> pw.debug.compute_and_print(t3, include_id=False)
    age | owner_name | size
    9   | Bob        | L
    """

    @classmethod
    def _base_this(self):
        return left


class right(_those):
    """
    Object for generating column references without holding the actual table in hand.
    Needs to be evaluated in the proper context.
    For `Table.join()` and `JoinResult.select()`, refers to the right input table.
    For all other situations, you need `pw.this` object.

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown('''
    ... age  | owner  | pet
    ...  10  | Alice  | 1
    ...   9  | Bob    | 1
    ...   8  | Alice  | 2
    ... ''')
    >>> t2 = pw.debug.table_from_markdown('''
    ... age  | owner  | pet | size
    ...  10  | Alice  | 3   | M
    ...  9   | Bob    | 1   | L
    ...  8   | Tom    | 1   | XL
    ... ''')
    >>> t3 = t1.join(t2, pw.left.pet == pw.right.pet, pw.left.owner == pw.right.owner).select(
    ...          age=pw.left.age, owner_name=pw.right.owner, size=pw.this.size
    ...      )
    >>> pw.debug.compute_and_print(t3, include_id=False)
    age | owner_name | size
    9   | Bob        | L
    """

    @classmethod
    def _base_this(self):
        return right
