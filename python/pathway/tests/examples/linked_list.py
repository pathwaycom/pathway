# Copyright © 2023 Pathway

from __future__ import annotations

from typing import Callable, Optional

import pathway as pw


class Node(pw.Schema):
    next: Optional[pw.Pointer[Node]]


class Output(pw.Schema):
    len: float
    forward: Callable[..., Optional[pw.Pointer[Node]]]


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
        def forward(self, steps) -> Optional[pw.Pointer[Node]]:
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
