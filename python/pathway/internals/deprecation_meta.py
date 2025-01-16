# Copyright © 2024 Pathway


class DeprecationMetaclass(type):
    def __getattr__(cls, name):
        from pathway.internals.table import Table

        RENAMES = {
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
        RENAMES: dict[str, tuple[type, str]] = {}
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
        from pathway.internals.table import Table

        def _update_columns(*args, **kwargs):
            return Table.with_columns(self, *args, **kwargs)

        RENAMES = {
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
