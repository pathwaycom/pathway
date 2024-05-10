from typing import Callable, Iterable, TypeAlias, Union

import pathway as pw

Doc: TypeAlias = dict[str, str | dict]


DocTransformerCallable: TypeAlias = Union[
    Callable[[Iterable[Doc]], Iterable[Doc]],
    Callable[[Iterable[Doc], float], Iterable[Doc]],
]

DocTransformer: TypeAlias = Union[pw.UDF, DocTransformerCallable]
