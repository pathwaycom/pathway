# Copyright © 2024 Pathway

# mypy: disallow-untyped-defs, extra-checks, disallow-any-generics, warn-return-any

from __future__ import annotations

import builtins
import importlib
import io
import os
import re
import warnings
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any, Callable, cast, overload

import yaml
from typing_extensions import Self

VARIABLE_TAG = "tag:pathway.com,2024:variable"


def verify_dict_keys(d: dict[object, object]) -> dict[str | Variable, object]:
    for key in d.keys():
        if not isinstance(key, str) and not isinstance(key, Variable):
            raise ValueError(f"expected string key, got {type(key)}")
    return cast(dict[str | Variable, object], d)


@dataclass(frozen=True)
class Variable:
    name: str

    def __str__(self) -> str:
        return f"${self.name}"


@dataclass(eq=False)
class Value:
    constructor: Callable[..., object] | None = None
    kwargs: dict[str | Variable, object] = field(default_factory=lambda: {})
    constructed: bool = False
    value: object = None


def import_object(path: str) -> object:
    if path.startswith("pw.") or path.startswith("pw:"):
        path = "pathway" + path.removeprefix("pw")

    module_path, colon, attribute_path = path.partition(":")

    attributes = attribute_path.split(".") if attribute_path else []

    module = builtins
    if not colon:
        names = module_path.split(".") if module_path else []
        for index, name in enumerate(names):
            prefix = ".".join(names[: index + 1])
            try:
                module = importlib.import_module(prefix)
            except ModuleNotFoundError:
                attributes = names[index:]
                break
    elif module_path:
        module = importlib.import_module(module_path)

    res = module
    for attribute in attributes:
        res = getattr(res, attribute)

    return res


class PathwayYamlLoader(yaml.Loader):
    def construct_pathway_variable(self, node: yaml.Node) -> Variable:
        name = self.construct_yaml_str(node)
        if not name.startswith("$"):
            raise yaml.MarkedYAMLError(
                problem=f"variable name {name!r} does not start with '$'",
                problem_mark=node.start_mark,
            )
        name = name[1:]
        if not name.isidentifier():
            raise yaml.MarkedYAMLError(
                problem=f"variable name {name!r} is not a valid identifier",
                problem_mark=node.start_mark,
            )
        return Variable(name)

    def construct_pathway_value(self, tag: str, node: yaml.Node) -> Value:
        constructor = import_object(tag)

        match node:
            case yaml.ScalarNode(value="") if callable(constructor):
                warnings.warn(
                    (
                        "Using Callables in YAML configuration file without providing any mapping is now deprecated, "
                        "as it's behavior will change in the future. To use Callables provide "
                        "explicitly a matching, for example `{}` as an empty one."
                    ),
                    DeprecationWarning,
                )
                kwargs: dict[str | Variable, object] = {}
                return Value(constructor, kwargs)
            case yaml.ScalarNode(value="") if not callable(constructor):
                return Value(constructed=True, value=constructor)
            case yaml.MappingNode() if callable(constructor):
                kwargs = verify_dict_keys(self.construct_mapping(node))
                return Value(constructor, kwargs)
            case _:
                if not callable(constructor):
                    raise yaml.MarkedYAMLError(
                        problem=f"constructor {tag!r} is not callable",
                        problem_mark=node.start_mark,
                    )
                else:
                    raise yaml.MarkedYAMLError(
                        problem="expected a mapping or empty node",
                        problem_mark=node.start_mark,
                    )


PathwayYamlLoader.add_implicit_resolver(VARIABLE_TAG, re.compile(r"\$.*"), "$")
PathwayYamlLoader.add_constructor(
    VARIABLE_TAG, PathwayYamlLoader.construct_pathway_variable
)
PathwayYamlLoader.add_multi_constructor("!", PathwayYamlLoader.construct_pathway_value)


@dataclass(eq=False)
class Resolver:
    context: dict[Variable, object] = field(default_factory=dict)
    done: dict[int, object] = field(default_factory=dict)
    parent: Resolver | None = None

    def subresolver(self, context: dict[Variable, object]) -> Self:
        return type(self)(parent=self, context=context, done=self.done)

    def _resolve_variable(self, v: Variable) -> object:
        if v in self.context:
            return self.resolve(self.context[v])

        if self.parent is not None:
            return self.parent.resolve_variable(v)

        if all(c.upper() or c == "_" for c in v.name):
            s = os.environ.get(v.name)
            if s is not None:
                # using yaml.Loader instead of PathwayYamlLoader to prevent recursive
                # parsing of environment variables
                parsed_value = yaml.load(s, yaml.Loader)
                if (
                    isinstance(parsed_value, int)
                    or isinstance(parsed_value, float)
                    or isinstance(parsed_value, bool)
                ):
                    return parsed_value
                else:
                    return s

        raise KeyError(f"variable {v} is not defined")

    def resolve_variable(self, v: Variable) -> object:
        res = self._resolve_variable(v)
        self.context[v] = res
        self.done[id(res)] = res
        return res

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> None:
        if exc_value is not None:
            return
        self.check_unused_variables()

    def check_unused_variables(self) -> None:
        for name, value in self.context.items():
            if id(value) not in self.done:
                warnings.warn(f"unused YAML variable {name}", stacklevel=2)

    @overload
    def resolve(self, obj: dict[str | Variable, object]) -> dict[str, object]: ...

    @overload
    def resolve(self, obj: object) -> object: ...

    def resolve(self, obj: object) -> object:
        if id(obj) in self.done:
            return obj

        match obj:
            case Variable() as v:
                return self.resolve_variable(v)
            case Value(constructed=True, value=value):
                return value
            case Value(constructor=constructor, kwargs=kwargs) as v:
                resolved_kwargs = self.resolve(kwargs)
                assert constructor is not None
                obj = constructor(**resolved_kwargs)
                v.constructed = True
                v.value = obj
            case dict() as d:
                context = {}
                variables = list(v for v in d.keys() if isinstance(v, Variable))
                if variables:
                    for v in variables:
                        context[v] = d.pop(v)
                    with self.subresolver(context) as resolver:
                        return resolver.resolve(d)
                for key, value in d.items():
                    d[key] = self.resolve(value)
            case list() as ls:
                for index, value in enumerate(ls):
                    ls[index] = self.resolve(value)

        self.done[id(obj)] = obj
        return obj


def resolve(obj: object) -> object:
    with Resolver() as resolver:
        return resolver.resolve(obj)


def load_yaml(stream: str | bytes | io.IOBase) -> Any:
    return resolve(yaml.load(stream, PathwayYamlLoader))


__all__ = ["load_yaml"]
