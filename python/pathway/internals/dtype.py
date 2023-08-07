# Copyright © 2023 Pathway

from __future__ import annotations

import sys
from typing import Any, Callable, NewType, Optional, Tuple, Union, get_args, get_origin

from pathway.internals import api

DType = NewType("DType", object)

NoneType = DType(type(None))

# TODO: require later that types need to be typing.Optional or EmptyType


def _is_optional(input_type):
    return (
        get_origin(input_type) is Union
        and len(get_args(input_type)) == 2
        and isinstance(None, get_args(input_type)[1])
    )


def _strip_optional(input_type):
    assert get_origin(input_type) is Union
    left, right = get_args(input_type)
    assert right is type(None)  # noqa: E721
    return left


def sanitize_type(input_type):
    if (
        input_type is api.BasePointer
        or input_type is api.Pointer
        or get_origin(input_type) is api.Pointer
    ):
        return api.Pointer
    elif sys.version_info >= (3, 10) and isinstance(input_type, NewType):
        # NewType is a class starting from Python 3.10
        return sanitize_type(input_type.__supertype__)
    elif isinstance(input_type, str):
        return Any  # TODO: input_type is annotation for class
    elif get_origin(input_type) is get_origin(
        Callable
    ):  # for some weird reason get_origin(Callable) == collections.abc.Callable
        return Callable
    elif _is_optional(input_type):
        return sanitize_type(_strip_optional(input_type))
    else:
        return input_type


def dtype_issubclass(left: DType, right: DType):
    if _is_optional(left) and not _is_optional(right):
        return False
    if left == NoneType:
        return _is_optional(right) or right == NoneType
    left_ = sanitize_type(left)
    right_ = sanitize_type(right)
    if right_ is Any:
        return True
    elif left_ is Any:
        return False
    elif left_ is int and right_ is float:
        return True
    elif isinstance(left_, type):
        return issubclass(left_, right_)
    else:
        return left_ == right_


def types_lca(left: DType, right: DType) -> DType:
    """LCA of two types."""
    if dtype_issubclass(left, right):
        return right
    elif dtype_issubclass(right, left):
        return left
    elif left == NoneType:
        return DType(Optional[right])
    elif right == NoneType:
        return DType(Optional[left])
    else:
        return DType(Any)


def unoptionalize(left_dtype: DType, right_dtype) -> Tuple[DType, DType]:
    """
    Unpacks type out of typing.Optional and matches
    a second type with it if it is an EmptyType.
    """
    if left_dtype == NoneType and _is_optional(right_dtype):
        left_dtype = right_dtype
    if right_dtype == NoneType and _is_optional(left_dtype):
        right_dtype = left_dtype
    left_dtype = get_args(left_dtype)[0] if _is_optional(left_dtype) else left_dtype
    right_dtype = get_args(right_dtype)[0] if _is_optional(right_dtype) else right_dtype

    return left_dtype, right_dtype
