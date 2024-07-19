# Copyright © 2024 Pathway

from collections.abc import Iterable

import pathway.internals.expression as expr
from pathway.internals import api, dtype as dt


class StringNamespace:
    """A module containing methods related to string.
    They can be called using a `str` attribute of an expression.

    Typical use:

    >>> import pathway as pw
    >>> table = pw.debug.table_from_markdown(
    ...     '''
    ...      | name
    ...    1 | ALICE
    ... '''
    ... )
    >>> table += table.select(name_lower=table.name.str.lower())
    >>> pw.debug.compute_and_print(table, include_id=False)
    name  | name_lower
    ALICE | alice
    """

    _expression: expr.ColumnExpression

    def __init__(self, expression: expr.ColumnExpression):
        self._expression = expression

    def lower(self) -> expr.ColumnExpression:
        """Returns a lowercase copy of a string.

        Returns:
            Lowercase string

        Example:

        >>> import pathway as pw
        >>> table = pw.debug.table_from_markdown(
        ...     '''
        ...      | name
        ...    1 | Alice
        ...    2 | Bob
        ...    3 | CAROLE
        ...    4 | david
        ... '''
        ... )
        >>> table += table.select(name_lower=table.name.str.lower())
        >>> pw.debug.compute_and_print(table, include_id=False)
        name   | name_lower
        Alice  | alice
        Bob    | bob
        CAROLE | carole
        david  | david
        """

        return expr.MethodCallExpression(
            (
                (
                    dt.STR,
                    dt.STR,
                    lambda x: api.Expression.apply(
                        str.lower, x, dtype=dt.STR.to_engine()
                    ),
                ),
            ),
            "str.lower",
            self._expression,
        )

    def upper(self) -> expr.ColumnExpression:
        """Returns a uppercase copy of a string.

        Returns:
            Uppercase string

        Example:

        >>> import pathway as pw
        >>> table = pw.debug.table_from_markdown(
        ...     '''
        ...      | name
        ...    1 | Alice
        ...    2 | Bob
        ...    3 | CAROLE
        ...    4 | david
        ... '''
        ... )
        >>> table += table.select(name_upper=table.name.str.upper())
        >>> pw.debug.compute_and_print(table, include_id=False)
        name   | name_upper
        Alice  | ALICE
        Bob    | BOB
        CAROLE | CAROLE
        david  | DAVID
        """

        return expr.MethodCallExpression(
            (
                (
                    dt.STR,
                    dt.STR,
                    lambda x: api.Expression.apply(
                        str.upper, x, dtype=dt.STR.to_engine()
                    ),
                ),
            ),
            "str.upper",
            self._expression,
        )

    def reversed(self) -> expr.ColumnExpression:
        """Returns a reverse copy of a string.

        Returns:
            Reverse string

        Example:

        >>> import pathway as pw
        >>> table = pw.debug.table_from_markdown(
        ...     '''
        ...      | name
        ...    1 | Alice
        ...    2 | Bob
        ...    3 | CAROLE
        ...    4 | david
        ... '''
        ... )
        >>> table += table.select(name_reverse=table.name.str.reversed())
        >>> pw.debug.compute_and_print(table, include_id=False)
        name   | name_reverse
        Alice  | ecilA
        Bob    | boB
        CAROLE | ELORAC
        david  | divad
        """

        return expr.MethodCallExpression(
            (
                (
                    dt.STR,
                    dt.STR,
                    lambda x: api.Expression.apply(
                        lambda y: y[::-1], x, dtype=dt.STR.to_engine()
                    ),
                ),
            ),
            "str.reverse",
            self._expression,
        )

    def len(self) -> expr.ColumnExpression:
        """Returns the length of a string.

        Returns:
            Length of the string

        Example:

        >>> import pathway as pw
        >>> table = pw.debug.table_from_markdown(
        ...     '''
        ...      | name
        ...    1 | Alice
        ...    2 | Bob
        ...    3 | CAROLE
        ...    4 | david
        ... '''
        ... )
        >>> table += table.select(length=table.name.str.len())
        >>> pw.debug.compute_and_print(table, include_id=False)
        name   | length
        Alice  | 5
        Bob    | 3
        CAROLE | 6
        david  | 5
        """

        return expr.MethodCallExpression(
            (
                (
                    dt.STR,
                    dt.INT,
                    lambda x: api.Expression.apply(len, x, dtype=dt.INT.to_engine()),
                ),
            ),
            "str.len",
            self._expression,
        )

    def replace(
        self,
        old_value: expr.ColumnExpression | str,
        new_value: expr.ColumnExpression | str,
        count: expr.ColumnExpression | int = -1,
        /,
    ) -> expr.ColumnExpression:
        """Returns the a string where the occurrences of the old_value substrings are
            replaced by the new_value substring.

        Args:
            count: Maximum number of occurrences to replace. When set to -1, replaces
                all occurrences. Defaults to -1.

        Returns:
            The new string where old_value is replaced by new_value

        Example:

        >>> import pathway as pw
        >>> table = pw.debug.table_from_markdown(
        ...     '''
        ...      | name
        ...    1 | Alice
        ...    2 | Bob
        ...    3 | CAROLE
        ...    4 | david
        ...    5 | Edward
        ... '''
        ... )
        >>> table += table.select(name_replace=table.name.str.replace("d","Z"))
        >>> pw.debug.compute_and_print(table, include_id=False)
        name   | name_replace
        Alice  | Alice
        Bob    | Bob
        CAROLE | CAROLE
        Edward | EZwarZ
        david  | ZaviZ
        >>> table = pw.debug.table_from_markdown(
        ...     '''
        ...      | value      | old | new | count
        ...    1 | Scaciscics | c   | t   | 3
        ...    2 | yelliwwiid | i   | o   | 2
        ... '''
        ... )
        >>> table = table.select(
        ...    pw.this.value,
        ...    value_replace=pw.this.value.str.replace(
        ...       pw.this.old, pw.this.new, pw.this.count
        ...    )
        ... )
        >>> pw.debug.compute_and_print(table, include_id=False)
        value      | value_replace
        Scaciscics | Statistics
        yelliwwiid | yellowwoid
        """

        return expr.MethodCallExpression(
            (
                (
                    (dt.STR, dt.STR, dt.STR, dt.INT),
                    dt.STR,
                    lambda x, y, z, c: api.Expression.apply(
                        lambda s1, s2, s3, cnt: s1.replace(s2, s3, cnt),
                        x,
                        y,
                        z,
                        c,
                        dtype=dt.STR.to_engine(),
                    ),
                ),
            ),
            "str.replace",
            self._expression,
            old_value,
            new_value,
            count,
        )

    def startswith(
        self,
        prefix: expr.ColumnExpression | str,
    ) -> expr.ColumnExpression:
        """Returns True if the string starts with prefix.

        Example:

        >>> import pathway as pw
        >>> table = pw.debug.table_from_markdown(
        ...     '''
        ...      | name
        ...    1 | Alice
        ...    2 | Bob
        ...    3 | CAROLE
        ...    4 | david
        ... '''
        ... )
        >>> table += table.select(starts_with_A=table.name.str.startswith("A"))
        >>> pw.debug.compute_and_print(table, include_id=False)
        name   | starts_with_A
        Alice  | True
        Bob    | False
        CAROLE | False
        david  | False
        """

        return expr.MethodCallExpression(
            (
                (
                    (dt.STR, dt.STR),
                    dt.BOOL,
                    lambda x, y: api.Expression.apply(
                        str.startswith, x, y, dtype=dt.BOOL.to_engine()
                    ),
                ),
            ),
            "str.starts_with",
            self._expression,
            prefix,
        )

    def endswith(
        self,
        suffix: expr.ColumnExpression | str,
    ) -> expr.ColumnExpression:
        """Returns True if the string ends with suffix.

        Example:

        >>> import pathway as pw
        >>> table = pw.debug.table_from_markdown(
        ...     '''
        ...      | name
        ...    1 | Alice
        ...    2 | Bob
        ...    3 | CAROLE
        ...    4 | david
        ... '''
        ... )
        >>> table += table.select(ends_with_e=table.name.str.endswith("e"))
        >>> pw.debug.compute_and_print(table, include_id=False)
        name   | ends_with_e
        Alice  | True
        Bob    | False
        CAROLE | False
        david  | False
        """

        return expr.MethodCallExpression(
            (
                (
                    (dt.STR, dt.STR),
                    dt.BOOL,
                    lambda x, y: api.Expression.apply(
                        str.endswith, x, y, dtype=dt.BOOL.to_engine()
                    ),
                ),
            ),
            "str.ends_with",
            self._expression,
            suffix,
        )

    def swapcase(self) -> expr.ColumnExpression:
        """Returns a copy of the string where the case is inverted.

        Example:

        >>> import pathway as pw
        >>> table = pw.debug.table_from_markdown(
        ...     '''
        ...      | name
        ...    1 | Alice
        ...    2 | Bob
        ...    3 | CAROLE
        ...    4 | david
        ... '''
        ... )
        >>> table += table.select(name_swap=table.name.str.swapcase())
        >>> pw.debug.compute_and_print(table, include_id=False)
        name   | name_swap
        Alice  | aLICE
        Bob    | bOB
        CAROLE | carole
        david  | DAVID
        """

        return expr.MethodCallExpression(
            (
                (
                    dt.STR,
                    dt.STR,
                    lambda x: api.Expression.apply(
                        str.swapcase, x, dtype=dt.STR.to_engine()
                    ),
                ),
            ),
            "str.swap_case",
            self._expression,
        )

    def strip(
        self, chars: expr.ColumnExpression | str | None = None
    ) -> expr.ColumnExpression:
        """Returns a copy of the string with specified leading and trailing characters
        removed. If no arguments are passed, remove the leading and trailing whitespaces.


        Example:

        >>> import pathway as pw
        >>> table = pw.debug.table_from_markdown(
        ...     '''
        ...      | name
        ...    1 | Alice
        ...    2 | Bob
        ...    3 | CAROLE
        ...    4 | david
        ... '''
        ... )
        >>> table += table.select(name_strip=table.name.str.strip("Aod"))
        >>> pw.debug.compute_and_print(table, include_id=False)
        name   | name_strip
        Alice  | lice
        Bob    | Bob
        CAROLE | CAROLE
        david  | avi
        """

        return expr.MethodCallExpression(
            (
                (
                    (dt.STR, dt.Optional(dt.STR)),
                    dt.STR,
                    lambda x, y: api.Expression.apply(
                        str.strip, x, y, dtype=dt.STR.to_engine()
                    ),
                ),
            ),
            "str.strip",
            self._expression,
            chars,
        )

    def title(self) -> expr.ColumnExpression:
        """Returns a copy of the string where where words start with an uppercase character
        and the remaining characters are lowercase.


        Example:

        >>> import pathway as pw
        >>> table = pw.debug.table_from_markdown(
        ...     '''
        ...      | col
        ...    1 | title
        ... '''
        ... )
        >>> table = table.select(col_title=table["col"].str.title())
        >>> pw.debug.compute_and_print(table, include_id=False)
        col_title
        Title
        """

        return expr.MethodCallExpression(
            (
                (
                    dt.STR,
                    dt.STR,
                    lambda x: api.Expression.apply(
                        str.title, x, dtype=dt.STR.to_engine()
                    ),
                ),
            ),
            "str.title",
            self._expression,
        )

    def count(
        self,
        sub: expr.ColumnExpression | str,
        start: expr.ColumnExpression | int | None = None,
        end: expr.ColumnExpression | int | None = None,
    ) -> expr.ColumnExpression:
        """Returns the number of non-overlapping occurrences of substring sub in the range [start, end).
        Optional arguments start and end are interpreted as in slice notation.


        Example:

        >>> import pathway as pw
        >>> table = pw.debug.table_from_markdown(
        ...     '''
        ...      | name
        ...    1 | Alice
        ...    2 | Hello
        ...    3 | World
        ...    4 | Zoo
        ... '''
        ... )
        >>> table += table.select(count=table.name.str.count("o"))
        >>> pw.debug.compute_and_print(table, include_id=False)
        name  | count
        Alice | 0
        Hello | 1
        World | 1
        Zoo   | 2
        """

        return expr.MethodCallExpression(
            (
                (
                    (
                        dt.STR,
                        dt.STR,
                        dt.Optional(dt.INT),
                        dt.Optional(dt.INT),
                    ),
                    dt.INT,
                    lambda *args: api.Expression.apply(
                        str.count, *args, dtype=dt.INT.to_engine()
                    ),
                ),
            ),
            "str.count",
            self._expression,
            sub,
            start,
            end,
        )

    def find(
        self,
        sub: expr.ColumnExpression | str,
        start: expr.ColumnExpression | int | None = None,
        end: expr.ColumnExpression | int | None = None,
    ) -> expr.ColumnExpression:
        """Return the lowest index in the string where substring sub is found within
        the slice s[start:end]. Optional arguments start and end are interpreted as in
        slice notation. Return -1 if sub is not found.


        Example:

        >>> import pathway as pw
        >>> table = pw.debug.table_from_markdown(
        ...     '''
        ...      | name
        ...    1 | Alice
        ...    2 | Hello
        ...    3 | World
        ...    4 | Zoo
        ... '''
        ... )
        >>> table += table.select(pos=table.name.str.find("o"))
        >>> pw.debug.compute_and_print(table, include_id=False)
        name  | pos
        Alice | -1
        Hello | 4
        World | 1
        Zoo   | 1
        """

        return expr.MethodCallExpression(
            (
                (
                    (
                        dt.STR,
                        dt.STR,
                        dt.Optional(dt.INT),
                        dt.Optional(dt.INT),
                    ),
                    dt.INT,
                    lambda *args: api.Expression.apply(
                        str.find, *args, dtype=dt.INT.to_engine()
                    ),
                ),
            ),
            "str.find",
            self._expression,
            sub,
            start,
            end,
        )

    def rfind(
        self,
        sub: expr.ColumnExpression | str,
        start: expr.ColumnExpression | int | None = None,
        end: expr.ColumnExpression | int | None = None,
    ) -> expr.ColumnExpression:
        """Return the highest index in the string where substring sub is found within
        the slice s[start:end]. Optional arguments start and end are interpreted as in
        slice notation. Return -1 if sub is not found.


        Example:

        >>> import pathway as pw
        >>> table = pw.debug.table_from_markdown(
        ...     '''
        ...      | name
        ...    1 | Alice
        ...    2 | Hello
        ...    3 | World
        ...    4 | Zoo
        ... '''
        ... )
        >>> table += table.select(pos=table.name.str.rfind("o"))
        >>> pw.debug.compute_and_print(table, include_id=False)
        name  | pos
        Alice | -1
        Hello | 4
        World | 1
        Zoo   | 2
        """

        return expr.MethodCallExpression(
            (
                (
                    (
                        dt.STR,
                        dt.STR,
                        dt.Optional(dt.INT),
                        dt.Optional(dt.INT),
                    ),
                    dt.INT,
                    lambda *args: api.Expression.apply(
                        str.rfind, *args, dtype=dt.INT.to_engine()
                    ),
                ),
            ),
            "str.rfind",
            self._expression,
            sub,
            start,
            end,
        )

    def removeprefix(
        self,
        prefix: expr.ColumnExpression | str,
        /,
    ) -> expr.ColumnExpression:
        """If the string starts with prefix, returns a copy of the string without the prefix.
        Otherwise returns the original string.

        Example:

        >>> import pathway as pw
        >>> table = pw.debug.table_from_markdown(
        ...     '''
        ...      | name
        ...    1 | Alice
        ...    2 | Bob
        ...    3 | CAROLE
        ...    4 | david
        ... '''
        ... )
        >>> table += table.select(without_da=table.name.str.removeprefix("da"))
        >>> pw.debug.compute_and_print(table, include_id=False)
        name   | without_da
        Alice  | Alice
        Bob    | Bob
        CAROLE | CAROLE
        david  | vid
        >>> table = pw.debug.table_from_markdown(
        ...     '''
        ...      | note | prefix
        ...    1 | AAA  | A
        ...    2 | BB   | B
        ... '''
        ... )
        >>> table = table.select(
        ...    pw.this.note,
        ...    new_note=pw.this.note.str.removeprefix(pw.this.prefix)
        ... )
        >>> pw.debug.compute_and_print(table, include_id=False)
        note | new_note
        AAA  | AA
        BB   | B
        """

        return expr.MethodCallExpression(
            (
                (
                    (dt.STR, dt.STR),
                    dt.STR,
                    lambda x, y: api.Expression.apply(
                        str.removeprefix, x, y, dtype=dt.STR.to_engine()
                    ),
                ),
            ),
            "str.remove_prefix",
            self._expression,
            prefix,
        )

    def removesuffix(
        self,
        suffix: expr.ColumnExpression | str,
        /,
    ) -> expr.ColumnExpression:
        """If the string ends with suffix, returns a copy of the string without the suffix.
        Otherwise returns the original string.

        Example:

        >>> import pathway as pw
        >>> table = pw.debug.table_from_markdown(
        ...     '''
        ...      | name
        ...    1 | Alice
        ...    2 | Bob
        ...    3 | CAROLE
        ...    4 | david
        ... '''
        ... )
        >>> table += table.select(without_LE=table.name.str.removesuffix("LE"))
        >>> pw.debug.compute_and_print(table, include_id=False)
        name   | without_LE
        Alice  | Alice
        Bob    | Bob
        CAROLE | CARO
        david  | david
        >>> table = pw.debug.table_from_markdown(
        ...     '''
        ...      | fruit  | suffix
        ...    1 | bamboo | o
        ...    2 | banana | na
        ... '''
        ... )
        >>> table = table.select(
        ...    pw.this.fruit,
        ...    fruit_cropped=pw.this.fruit.str.removesuffix(pw.this.suffix)
        ... )
        >>> pw.debug.compute_and_print(table, include_id=False)
        fruit  | fruit_cropped
        bamboo | bambo
        banana | bana
        """

        return expr.MethodCallExpression(
            (
                (
                    (dt.STR, dt.STR),
                    dt.STR,
                    lambda x, y: api.Expression.apply(
                        str.removesuffix, x, y, dtype=dt.STR.to_engine()
                    ),
                ),
            ),
            "str.remove_suffix",
            self._expression,
            suffix,
        )

    def slice(
        self,
        start: expr.ColumnExpression | int,
        end: expr.ColumnExpression | int,
        /,
    ) -> expr.ColumnExpression:
        """Return a slice of the string.

        Example:

        >>> import pathway as pw
        >>> table = pw.debug.table_from_markdown(
        ...     '''
        ...      | name
        ...    1 | Alice
        ...    2 | Bob
        ...    3 | CAROLE
        ...    4 | david
        ... '''
        ... )
        >>> table += table.select(slice=table.name.str.slice(1,4))
        >>> pw.debug.compute_and_print(table, include_id=False)
        name   | slice
        Alice  | lic
        Bob    | ob
        CAROLE | ARO
        david  | avi
        """

        return expr.MethodCallExpression(
            (
                (
                    (dt.STR, dt.INT, dt.INT),
                    dt.STR,
                    lambda x, y, z: api.Expression.apply(
                        lambda s, slice_start, slice_end: s[slice_start:slice_end],
                        x,
                        y,
                        z,
                        dtype=dt.STR.to_engine(),
                    ),
                ),
            ),
            "str.slice",
            self._expression,
            start,
            end,
        )

    def parse_int(self, optional: bool = False) -> expr.ColumnExpression:
        """Parses the string to int. If optional argument is set to True, then the
        return type is Optional[int] and if some string cannot be parsed, None is
        returned.

        Example:

        >>> import pathway as pw
        >>> import pandas as pd
        >>> df = pd.DataFrame({"a": ["-5", "0", "200"]}, dtype=str)
        >>> table = pw.debug.table_from_pandas(df)
        >>> table.typehints()
        mappingproxy({'a': <class 'str'>})
        >>> table = table.select(a=table.a.str.parse_int())
        >>> table.typehints()
        mappingproxy({'a': <class 'int'>})
        >>> pw.debug.compute_and_print(table, include_id=False)
        a
        -5
        0
        200
        """
        return expr.MethodCallExpression(
            (
                (
                    dt.STR,
                    dt.Optional(dt.INT) if optional else dt.INT,
                    lambda x: api.Expression.parse_int(x, optional),
                ),
            ),
            "str.parse_int",
            self._expression,
        )

    def parse_float(self, optional: bool = False) -> expr.ColumnExpression:
        """Parses the string to float. If optional argument is set to True, then the
        return type is Optional[float] and if some string cannot be parsed, None is
        returned.

        Example:

        >>> import pathway as pw
        >>> import pandas as pd
        >>> df = pd.DataFrame({"a": ["-5", "0.1", "200.999"]}, dtype=str)
        >>> table = pw.debug.table_from_pandas(df)
        >>> table.typehints()
        mappingproxy({'a': <class 'str'>})
        >>> table = table.select(a=table.a.str.parse_float())
        >>> table.typehints()
        mappingproxy({'a': <class 'float'>})
        >>> pw.debug.compute_and_print(table, include_id=False)
        a
        -5.0
        0.1
        200.999
        """
        return expr.MethodCallExpression(
            (
                (
                    dt.STR,
                    dt.Optional(dt.FLOAT) if optional else dt.FLOAT,
                    lambda x: api.Expression.parse_float(x, optional),
                ),
            ),
            "str.parse_float",
            self._expression,
        )

    default_true_values = ["on", "true", "yes", "1"]
    default_false_values = ["off", "false", "no", "0"]

    def parse_bool(
        self,
        true_values: Iterable[str] = default_true_values,
        false_values: Iterable[str] = default_false_values,
        optional: bool = False,
    ) -> expr.ColumnExpression:
        """Parses the string to bool, by checking if given string is either in
        true_values or false_values. The given string and all values in true_vales and
        false_values are made lowercase, so parsing is case insensitive.

        When true_values and false_values arguments are
        not provided, strings "True", "On", "1" and "Yes" are interpreted as True value,
        and "False", "Off", "0", and "No" are interpreted as False.

        If true_values or false_values is provided, then these values are mapped to
        respectively True and False, while all other either raise an exception or return
        None, depending on argument optional.

        If optional argument is set to True, then the
        return type is Optional[bool] and if some string cannot be parsed, None is
        returned.

        Example:

        >>> import pathway as pw
        >>> import pandas as pd
        >>> df = pd.DataFrame({"a": ["0", "TRUE", "on"]}, dtype=str)
        >>> table = pw.debug.table_from_pandas(df)
        >>> table.typehints()
        mappingproxy({'a': <class 'str'>})
        >>> pw.debug.compute_and_print(table, include_id=False)
        a
        0
        TRUE
        on
        >>> table = table.select(a=table.a.str.parse_bool())
        >>> table.typehints()
        mappingproxy({'a': <class 'bool'>})
        >>> pw.debug.compute_and_print(table, include_id=False)
        a
        False
        True
        True
        """
        lowercase_true_values = [s.lower() for s in true_values]
        lowercase_false_values = [s.lower() for s in false_values]

        return expr.MethodCallExpression(
            (
                (
                    dt.STR,
                    dt.Optional(dt.BOOL) if optional else dt.BOOL,
                    lambda x: api.Expression.parse_bool(
                        x, lowercase_true_values, lowercase_false_values, optional
                    ),
                ),
            ),
            "str.parse_bool",
            self._expression,
        )
