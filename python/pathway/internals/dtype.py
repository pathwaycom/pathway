# Copyright © 2023 Pathway

from __future__ import annotations

import sys
from typing import Any, Callable, NewType, Optional, Tuple, Union, get_args, get_origin

from pathway.internals import api

DType = NewType("DType", object)

NoneType = DType(type(None))

# TODO: require later that types need to be typing.Optional or EmptyType


def is_optional(input_type: DType) -> bool:
    return (
        get_origin(input_type) is Union
        and len(get_args(input_type)) == 2
        and isinstance(None, get_args(input_type)[1])
    )


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
    elif is_optional(input_type):
        return sanitize_type(unoptionalize(input_type))
    else:
        return input_type


def is_pointer(input_type: DType) -> bool:
    return sanitize_type(input_type) == api.Pointer


def dtype_equivalence(left: DType, right: DType):
    return dtype_issubclass(left, right) and dtype_issubclass(right, left)


def dtype_tuple_equivalence(left: DType, right: DType):
    largs = get_args(left)
    rargs = get_args(right)
    if len(largs) != len(rargs):
        return False
    if largs[-1] == Ellipsis:
        largs = largs[:-1]
    if rargs[-1] == Ellipsis:
        rargs = rargs[:-1]
    if len(largs) != len(rargs):
        return False
    return all(dtype_equivalence(l_arg, r_arg) for l_arg, r_arg in zip(largs, rargs))


def dtype_issubclass(left: DType, right: DType):
    if right == Any:  # catch the case, when left=Optional[T] and right=Any
        return True
    elif is_optional(left) and not is_optional(right):
        return False
    elif left == NoneType:
        return is_optional(right) or right == NoneType
    elif is_tuple_like(left) and is_tuple_like(right):
        left = normalize_tuple_like(left)
        right = normalize_tuple_like(right)
        if left == Tuple or right == Tuple:
            return True
        else:
            return dtype_tuple_equivalence(left, right)
    elif is_tuple_like(left) or is_tuple_like(right):
        return False
    left_ = sanitize_type(left)
    right_ = sanitize_type(right)
    if right_ is Any:  # repeated to check after sanitization
        return True
    elif left_ is Any:
        return False
    elif left_ is int and right_ is float:
        return True
    elif left_ is bool and right_ is int:
        return False
    elif isinstance(left_, type):
        return issubclass(left_, right_)
    else:
        return left_ == right_


def is_tuple_like(dtype: DType):
    return dtype in [list, tuple] or get_origin(dtype) in [list, tuple]


def normalize_tuple_like(dtype: DType):
    if get_origin(dtype) == list:
        args = get_args(dtype)
        if len(args) == 0:
            return DType(Tuple)
        (arg,) = args
        return DType(Tuple[arg])
    assert dtype == tuple or get_origin(dtype) == tuple
    return dtype


def types_lca(left: DType, right: DType) -> DType:
    """LCA of two types."""
    if is_optional(left) or is_optional(right):
        dtype = types_lca(unoptionalize(left), unoptionalize(right))
        return DType(Optional[dtype]) if dtype is not Any else DType(Any)
    elif is_tuple_like(left) and is_tuple_like(right):
        left = normalize_tuple_like(left)
        right = normalize_tuple_like(right)
        if left == Tuple or right == Tuple:
            return DType(Tuple)
        elif dtype_tuple_equivalence(left, right):
            return left
        else:
            return DType(Tuple)
    elif is_tuple_like(left) or is_tuple_like(right):
        return DType(Any)
    elif dtype_issubclass(left, right):
        return right
    elif dtype_issubclass(right, left):
        return left
    elif left == NoneType:
        return DType(Optional[right])
    elif right == NoneType:
        return DType(Optional[left])
    else:
        return DType(Any)


def unoptionalize(dtype: DType) -> DType:
    return get_args(dtype)[0] if is_optional(dtype) else dtype


def unoptionalize_pair(left_dtype: DType, right_dtype) -> Tuple[DType, DType]:
    """
    Unpacks type out of typing.Optional and matches
    a second type with it if it is an EmptyType.
    """
    if left_dtype == NoneType and is_optional(right_dtype):
        left_dtype = right_dtype
    if right_dtype == NoneType and is_optional(left_dtype):
        right_dtype = left_dtype

    return unoptionalize(left_dtype), unoptionalize(right_dtype)
