# Copyright © 2024 Pathway

# mypy: disallow-untyped-defs, extra-checks, disallow-any-generics, warn-return-any

from __future__ import annotations

import builtins
import importlib
import io
import re
import warnings
from dataclasses import dataclass
from typing import Any, Callable, cast, overload

import yaml

VARIABLE_TAG = "tag:pathway.com,2024:variable"


def verify_string_keys(d: dict[object, object]) -> dict[str, object]:
    for key in d.keys():
        if not isinstance(key, str):
            raise ValueError("expected string key, got {type(key)}")
    return cast(dict[str, object], d)


@dataclass(frozen=True, slots=True)
class Variable:
    name: str

    def __str__(self) -> str:
        return f"${self.name}"


@dataclass(slots=True, eq=False)
class Value:
    constructor: Callable[..., object]
    kwargs: dict[str, object]
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
        if not callable(constructor):
            raise yaml.MarkedYAMLError(
                problem=f"constructor {tag!r} is not callable",
                problem_mark=node.start_mark,
            )

        match node:
            case yaml.ScalarNode(value=""):
                kwargs = {}
            case yaml.MappingNode():
                kwargs = verify_string_keys(self.construct_mapping(node))
            case _:
                raise yaml.MarkedYAMLError(
                    problem="expected a mapping or empty node",
                    problem_mark=node.start_mark,
                )

        return Value(constructor, kwargs)


PathwayYamlLoader.add_implicit_resolver(VARIABLE_TAG, re.compile(r"\$.*"), "$")
PathwayYamlLoader.add_constructor(
    VARIABLE_TAG, PathwayYamlLoader.construct_pathway_variable
)
PathwayYamlLoader.add_multi_constructor("!", PathwayYamlLoader.construct_pathway_value)


@overload
def resolve_node(
    obj: dict[str, object], context: dict[Variable, object], resolved: dict[int, object]
) -> dict[str, object]: ...


@overload
def resolve_node(
    obj: object, context: dict[Variable, object], resolved: dict[int, object]
) -> object: ...


def resolve_node(
    obj: object, context: dict[Variable, object], resolved: dict[int, object]
) -> object:
    if id(obj) in resolved:
        return obj

    match obj:
        case Variable() as v:
            context[v] = resolve_node(context[v], context, resolved)
            return context[v]
        case Value(constructed=True, value=value):
            return value
        case Value(constructor=constructor, kwargs=kwargs) as v:
            kwargs = resolve_node(kwargs, context, resolved)
            obj = constructor(**kwargs)
            v.constructed = True
            v.value = obj
        case dict() as d:
            for key, value in d.items():
                if not isinstance(key, str):
                    raise ValueError("expected string key, got {type(key)}")
                d[key] = resolve_node(value, context, resolved)
        case list() as ls:
            for index, value in enumerate(ls):
                ls[index] = resolve_node(value, context, resolved)

    resolved[id(obj)] = obj
    return obj


def resolve(obj: object) -> object:
    if not isinstance(obj, dict):
        return obj
    context = {}
    variables = list(v for v in obj.keys() if isinstance(v, Variable))
    for v in variables:
        context[v] = obj.pop(v)

    resolved: dict[int, object] = {}
    obj = resolve_node(obj, context, resolved)

    for name, value in context.items():
        if id(value) not in resolved:
            warnings.warn(f"unused YAML variable {name}", stacklevel=2)

    return obj


def load_yaml(stream: str | bytes | io.IOBase) -> Any:
    return resolve(yaml.load(stream, PathwayYamlLoader))
