# Copyright © 2024 Pathway

from __future__ import annotations

from pathway.internals.arg_tuple import wrap_arg_tuple


def test_arg_tuple_wrapper_scalar():
    result = wrap_arg_tuple(lambda: 1)()
    assert result == 1


def test_arg_tuple_wrapper_dict():
    result = wrap_arg_tuple(lambda: {"a": 1, "b": 2})()
    a, b = result
    assert a == 1
    assert b == 2
    assert result.a == 1
    assert result.b == 2
    assert result["a"] == 1
    assert result["b"] == 2


def test_arg_tuple_wrapper_dict_with_one_element():
    result = wrap_arg_tuple(lambda: {"a": 1})()
    assert result.a == 1
    assert result["a"] == 1


def test_arg_tuple_wrapper_iterable():
    result = wrap_arg_tuple(lambda: [1, 2])()
    a, b = result
    assert a == 1
    assert b == 2
    assert result["0"] == 1
    assert result["1"] == 2


def test_arg_tuple_wrapper_iterable_with_one_element():
    result = wrap_arg_tuple(lambda: (1,))()
    assert result == 1
