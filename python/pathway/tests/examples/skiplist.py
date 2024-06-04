# Copyright © 2024 Pathway

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pathway.internals as pw
from pathway.internals.fingerprints import fingerprint


def hash(val) -> int:
    return fingerprint(int(val), format="i64", seed=0)


class Hash(pw.Schema):
    hash: int


class Node(pw.Schema):
    val: Any
    next: pw.Pointer[Any]


class Shortcuts(pw.Schema):
    shortcut_val: Any
    shortcut_next: pw.Pointer[Any] | None


@pw.transformer
class shortcut_transformer:
    # TODO non-table arguments?
    class agg_fun(pw.ClassArg):
        # TODO input-output arguments
        agg_fun = pw.input_attribute()

    class shortcuts(pw.ClassArg, input=Shortcuts | Node | Hash, output=Shortcuts):
        shortcut_val = pw.input_attribute()
        shortcut_next = pw.input_attribute()
        val = pw.input_attribute()
        next = pw.input_attribute()
        hash = pw.input_attribute()

        @pw.attribute
        def _shortcut(self):
            if (
                self.shortcut_next is None
                or self.transformer.shortcuts[self.shortcut_next].hash > self.hash
            ):
                return (self.shortcut_next, self.shortcut_val)
            else:
                return (
                    self.transformer.shortcuts[self.shortcut_next].shortcut_next,
                    shortcut_transformer.agg_fun(self, self.pointer_from()).agg_fun(
                        self.shortcut_val,
                        self.transformer.shortcuts[self.shortcut_next].shortcut_val,
                    ),
                )

        @pw.output_attribute(output_name="shortcut_next")
        def new_shortcut_next(self) -> pw.Pointer[Any] | None:
            ret, _ = self._shortcut
            return ret

        @pw.output_attribute(output_name="shortcut_val")
        def new_shortcut_val(self):
            _, ret = self._shortcut
            return ret


def compute_shortcuts(
    nodes: pw.Table[Node], *, agg_fun: Callable[[Any, Any], Any]
) -> pw.Table[Shortcuts]:  # non-table parameter only as kwarg
    nodes = nodes + nodes.select(hash=pw.apply(hash, nodes.id))

    shortcuts_init = nodes.select(
        shortcut_next=pw.declare_type(Shortcuts["shortcut_next"], nodes.next),
        shortcut_val=pw.declare_type(Shortcuts["shortcut_val"], nodes.val),
    )  # can take unnamed args (columns), then no renaming applied

    # Iterate takes transformer and a dict of args
    # named args of transformer should match named args of iterate
    # and should match named tuple of returned args.
    return pw.iterate(
        lambda shortcuts, nodes, agg_fun:
        # TODO input-only tables needed (then we do not need to filter out agg_fun)
        shortcut_transformer(shortcuts=shortcuts + nodes, agg_fun=agg_fun).shortcuts,
        shortcuts=shortcuts_init,
        nodes=nodes,
        agg_fun=shortcuts_init.reduce(agg_fun=agg_fun),
    )
