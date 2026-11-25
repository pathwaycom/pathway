# Copyright © 2026 Pathway

"""Runtime type-checking"""
import functools
import threading

import beartype

# Beartype captures and reissues warnings while building wrappers. On Python
# versions with process-global warning filters, concurrent decoration can lose
# warnings or reissue them into the very list being iterated indefinitely.
_decoration_lock = threading.RLock()


def check_arg_types(f):
    """Decorator allowing validating types in runtime."""

    @functools.wraps(f)
    def with_type_validation(*args, **kwargs):
        """Hides beartype dependency by reraising beartype exception as TypeError.

        Should not be needed after resolving https://github.com/beartype/beartype/issues/234
        """
        try:
            with _decoration_lock:
                checked = beartype.beartype(f)
            # The function itself may run concurrently or call other checked
            # functions from different threads; do not hold the lock here.
            return checked(*args, **kwargs)
        except beartype.roar.BeartypeCallHintParamViolation as e:
            raise TypeError(e) from None

    return with_type_validation
