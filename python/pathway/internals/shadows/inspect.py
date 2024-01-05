# Copyright © 2024 Pathway

from __future__ import annotations

import inspect
from inspect import *  # noqa
from typing import Any, no_type_check


@no_type_check  # type: ignore  # we replace the other signature
def signature(obj, *, follow_wrapped=True):
    """Get a signature object for the passed callable.

    Fixed for functions. Would probably break for other callables, use vanilla inspect.signature for those.

    Will be deprecated once python version is bumped to >=3.9
    """
    obj = inspect.unwrap(obj)

    sig = inspect.Signature.from_callable(obj, follow_wrapped=follow_wrapped)
    for name, param in sig.parameters.items():
        annot = param.annotation
        if isinstance(annot, str):
            sig.parameters[name]._annotation = eval(annot, obj.__globals__)
        if sig.parameters[name]._annotation is inspect._empty:
            sig.parameters[name]._annotation = Any
    if isinstance(sig._return_annotation, str):
        sig._return_annotation = eval(sig._return_annotation, obj.__globals__)
    if sig._return_annotation is inspect._empty:
        sig._return_annotation = Any
    return sig
