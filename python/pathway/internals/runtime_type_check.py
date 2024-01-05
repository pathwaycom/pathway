# Copyright © 2024 Pathway

"""Runtime type-checking"""
import functools

import beartype


def check_arg_types(f):
    """Decorator allowing validating types in runtime."""

    @functools.wraps(f)
    def with_type_validation(*args, **kwargs):
        """Hides beartype dependency by reraising beartype exception as TypeError.

        Should not be needed after resolving https://github.com/beartype/beartype/issues/234
        """
        try:
            return beartype.beartype(f)(*args, **kwargs)
        except beartype.roar.BeartypeCallHintParamViolation as e:
            raise TypeError(e) from None

    return with_type_validation
