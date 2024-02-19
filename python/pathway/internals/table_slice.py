# Copyright © 2024 Pathway

from __future__ import annotations

from typing import TYPE_CHECKING, overload

from pathway.internals.expression import ColumnReference
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.thisclass import ThisMetaclass, this
from pathway.internals.trace import trace_user_frame

if TYPE_CHECKING:
    from pathway.internals.table import Table


class TableSlice:
    """Collection of references to Table columns.
    Created by Table.slice method, or automatically by using left/right/this constructs.
    Supports basic column manipulation methods.

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown('''
    ... age | owner | pet
    ... 10  | Alice | dog
    ... 9   | Bob   | dog
    ... 8   | Alice | cat
    ... 7   | Bob   | dog
    ... ''')
    >>> t1.slice.without("age").with_suffix("_col")
    TableSlice({'owner_col': <table1>.owner, 'pet_col': <table1>.pet})
    """

    _mapping: dict[str, ColumnReference]
    _table: Table

    def __init__(self, mapping, table):
        self._mapping = mapping
        self._table = table

    def __iter__(self):
        return iter(self._mapping.values())

    def __repr__(self):
        return f"TableSlice({self._mapping})"

    def keys(self):
        return self._mapping.keys()

    @overload
    def __getitem__(self, args: str | ColumnReference) -> ColumnReference: ...

    @overload
    def __getitem__(self, args: list[str | ColumnReference]) -> TableSlice: ...

    @trace_user_frame
    def __getitem__(
        self, arg: str | ColumnReference | list[str | ColumnReference]
    ) -> ColumnReference | TableSlice:
        if isinstance(arg, (ColumnReference, str)):
            return self._mapping[self._normalize(arg)]
        else:
            return TableSlice({self._normalize(k): self[k] for k in arg}, self._table)

    @trace_user_frame
    def __getattr__(self, name: str) -> ColumnReference:
        from pathway.internals import Table

        if hasattr(Table, name) and name != "id":
            raise ValueError(
                f"{name!r} is a method name. It is discouraged to use it as a column"
                + f" name. If you really want to use it, use [{name!r}]."
            )
        if name not in self._mapping:
            raise AttributeError(f"Column name {name!r} not found in {self!r}.")
        return self._mapping[name]

    @trace_user_frame
    @check_arg_types
    def without(self, *cols: str | ColumnReference) -> TableSlice:
        mapping = self._mapping.copy()
        for col in cols:
            colname = self._normalize(col)
            if colname not in mapping:
                raise KeyError(f"Column name {repr(colname)} not found in a {self}.")
            mapping.pop(colname)
        return TableSlice(mapping, self._table)

    @trace_user_frame
    @check_arg_types
    def rename(
        self,
        rename_dict: dict[str | ColumnReference, str | ColumnReference],
    ) -> TableSlice:
        rename_dict_normalized = {
            self._normalize(old): self._normalize(new)
            for old, new in rename_dict.items()
        }
        mapping = self._mapping.copy()
        for old in rename_dict_normalized.keys():
            if old not in mapping:
                raise KeyError(f"Column name {repr(old)} not found in a {self}.")
            mapping.pop(old)
        for old, new in rename_dict_normalized.items():
            mapping[new] = self._mapping[old]
        return TableSlice(mapping, self._table)

    @trace_user_frame
    @check_arg_types
    def with_prefix(self, prefix: str) -> TableSlice:
        return self.rename({name: prefix + name for name in self.keys()})

    @trace_user_frame
    @check_arg_types
    def with_suffix(self, suffix: str) -> TableSlice:
        return self.rename({name: name + suffix for name in self.keys()})

    @trace_user_frame
    def ix(self, expression, *, optional: bool = False, context=None) -> TableSlice:
        new_table = self._table.ix(expression, optional=optional, context=context)
        return TableSlice(
            {name: new_table[colref._name] for name, colref in self._mapping.items()},
            new_table,
        )

    @trace_user_frame
    def ix_ref(self, *args, optional: bool = False, context=None) -> TableSlice:
        new_table = self._table.ix_ref(*args, optional=optional, context=context)
        return TableSlice(
            {name: new_table[colref._name] for name, colref in self._mapping.items()},
            new_table,
        )

    @property
    def slice(self):
        return self

    def _normalize(self, arg: str | ColumnReference):
        if isinstance(arg, ColumnReference):
            if isinstance(arg.table, ThisMetaclass):
                if arg.table != this:
                    raise ValueError(
                        f"TableSlice expects {repr(arg.name)} or this.{arg.name} argument as column reference."
                    )
            else:
                if arg.table != self._table:
                    raise ValueError(
                        "TableSlice method arguments should refer to table of which the slice was created."
                    )
            return arg.name
        else:
            return arg
