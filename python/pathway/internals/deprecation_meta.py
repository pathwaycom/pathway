# Copyright © 2024 Pathway


class DeprecationMetaclass(type):
    def __getattr__(cls, name):
        from pathway.internals.table import Joinable, Table, TableLike

        def _unsafe_promise_universe_is_subset_of(self, *others):
            for other in others:
                TableLike.promise_universe_is_subset_of(self, other)
            return self

        RENAMES = {
            "unsafe_promise_same_universe_as": (
                Table,
                "with_universe_of",
                Table.with_universe_of,
            ),
            "unsafe_promise_universes_are_pairwise_disjoint": (
                TableLike,
                "promise_are_pairwise_disjoint",
                TableLike.promise_universes_are_disjoint,
            ),
            "unsafe_promise_universe_is_subset_of": (
                TableLike,
                "promise_universe_is_subset_of",
                _unsafe_promise_universe_is_subset_of,
            ),
            "left_join": (Joinable, "join_left", Joinable.join_left),
            "right_join": (Joinable, "join_right", Joinable.join_right),
            "outer_join": (Joinable, "join_outer", Joinable.join_outer),
            "update_columns": (Table, "with_columns", Table.with_columns),
        }
        if name in RENAMES:
            base_cls, new_name, new_fun = RENAMES[name]
            if issubclass(cls, base_cls):
                from warnings import warn

                warn(
                    f"{name!r} is deprecated, use {new_name!r}",
                    DeprecationWarning,
                    stacklevel=2,
                )
                return new_fun

        return super().__getattr__(name)  # type: ignore


class DeprecationSuperclass(metaclass=DeprecationMetaclass):
    def _column_deprecation_rename(self, name):
        from pathway.internals.table import Table

        RENAMES = {"_pw_shard": (Table, "_pw_instance")}
        if name in RENAMES:
            base_cls, new_name = RENAMES[name]
            if isinstance(self, base_cls):
                from warnings import warn

                warn(
                    f"{name!r} is deprecated, use {new_name!r}",
                    DeprecationWarning,
                    stacklevel=3,
                )
                return new_name
        else:
            return name

    def __getattr__(self, name):
        from pathway.internals.table import Joinable, Table, TableLike

        def _unsafe_promise_same_universe_as(other):
            return self.with_universe_of(other)

        def _unsafe_promise_universes_are_pairwise_disjoint(*others):
            return self.promise_universes_are_disjoint(*others)

        def _unsafe_promise_universe_is_subset_of(*others):
            for other in others:
                self.promise_universe_is_subset_of(other)
            return self

        def _left_join(other, *args, **kwargs):
            return self.join_left(other, *args, **kwargs)

        def _right_join(other, *args, **kwargs):
            return self.join_right(other, *args, **kwargs)

        def _outer_join(other, *args, **kwargs):
            return self.join_outer(other, *args, **kwargs)

        def _update_columns(*args, **kwargs):
            return Table.with_columns(self, *args, **kwargs)

        RENAMES = {
            "unsafe_promise_same_universe_as": (
                Table,
                "with_universe_of",
                _unsafe_promise_same_universe_as,
            ),
            "unsafe_promise_universes_are_pairwise_disjoint": (
                TableLike,
                "promise_are_pairwise_disjoint",
                _unsafe_promise_universes_are_pairwise_disjoint,
            ),
            "unsafe_promise_universe_is_subset_of": (
                TableLike,
                "promise_universe_is_subset_of",
                _unsafe_promise_universe_is_subset_of,
            ),
            "left_join": (Joinable, "join_left", _left_join),
            "right_join": (Joinable, "join_right", _right_join),
            "outer_join": (Joinable, "join_outer", _outer_join),
            "update_columns": (Table, "with_columns", _update_columns),
        }
        if name in RENAMES:
            base_cls, new_name, new_fun_delayed = RENAMES[name]
            if isinstance(self, base_cls):
                from warnings import warn

                warn(
                    f"{name!r} is deprecated, use {new_name!r}",
                    DeprecationWarning,
                    stacklevel=2,
                )
                return new_fun_delayed
        raise AttributeError
