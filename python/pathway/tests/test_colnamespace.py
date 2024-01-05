# Copyright © 2024 Pathway

from __future__ import annotations

import pathway as pw


def test_namespace_1():
    tab = pw.Table.empty(select=int)
    assert isinstance(tab.C.select, pw.ColumnReference)


def test_namespace_2():
    tab = pw.Table.empty(select=int)
    assert isinstance(tab.C["select"], pw.ColumnReference)


def test_namespace_3():
    tab = pw.Table.empty(C=int)
    assert isinstance(tab.C.C, pw.ColumnReference)


def test_namespace_4():
    tab = pw.Table.empty(select=int)
    tab2 = tab.select(pw.this.C.select)
    assert tab.schema == tab2.schema


def test_namespace_5():
    tab = pw.Table.empty(C=int)
    tab2 = tab.select(pw.this.C.C)
    assert tab.schema == tab2.schema


def test_namespace_6():
    tab = pw.Table.empty(C=int)
    tab2 = tab.select(pw.this.C["C"])
    assert tab.schema == tab2.schema


def test_namespace_7():
    tab = pw.Table.empty(C=int)
    tab2 = tab.select(pw.this["C"])
    assert tab.schema == tab2.schema
