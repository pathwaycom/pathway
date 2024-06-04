# Copyright © 2024 Pathway

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pathway.internals as pw


class Node(pw.Schema):
    next: pw.Pointer[Any] | None


class Output(pw.Schema):
    len: float
    forward: Callable[..., pw.Pointer[Any] | None]


@pw.transformer
class linked_list_transformer:
    class linked_list(pw.ClassArg, input=Node, output=Output):
        next = pw.input_attribute()

        @pw.output_attribute
        def len(self) -> float:
            if self.next is None:
                return 1
            else:
                return 1 + self.transformer.linked_list[self.next].len

        @pw.method
        def forward(self, steps) -> pw.Pointer[Any] | None:
            if steps == 0:
                return self.id
            elif self.next is not None:
                return self.transformer.linked_list[self.next].forward(steps - 1)
            else:
                return None


def reverse_linked_list(nodes: pw.Table) -> pw.Table:
    reversed = (
        (filtered := nodes.filter(nodes.next.is_not_none()))
        .select(next=filtered.id)
        .with_id(filtered.next)
    )
    return nodes.select(next=None).update_rows(reversed)
