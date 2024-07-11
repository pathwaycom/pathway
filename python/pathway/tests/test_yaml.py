# Copyright © 2024 Pathway
import pytest
from pydantic import BaseModel, InstanceOf

from pathway.internals.yaml_loader import load_yaml


class Foo:
    def __init__(self, a: int, b: int | None = None, c: str = "foo"):
        self.a = a
        self.b = b
        self.c = c

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class Bar:
    def __init__(self, d: Foo | str):
        self.d = d

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


def baz(a, b, c):
    return Foo(a, b, c)


def test_class_initialization():
    yaml_config = """
foo: !pw.tests.test_yaml.Foo
  a: 1
  b: 2
  c: bar
"""

    d = load_yaml(yaml_config)
    assert "foo" in d.keys()
    assert len(d.keys()) == 1
    assert d["foo"] == Foo(1, 2, "bar")


def test_function_call():
    yaml_config = """
foo: !pw.tests.test_yaml.baz
  a: 1
  b: 2
  c: bar
"""

    d = load_yaml(yaml_config)
    assert "foo" in d.keys()
    assert len(d.keys()) == 1
    assert d["foo"] == Foo(1, 2, "bar")


def test_variables():
    yaml_config = """
$foo: !pw.tests.test_yaml.Foo
  a: 1
  c: "bar"

bar: !pw.tests.test_yaml.Bar
  d: $foo
"""

    d = load_yaml(yaml_config)
    assert "bar" in d.keys()
    assert d["bar"] == Bar(Foo(a=1, c="bar"))

    yaml_config2 = """
foo: !pw.tests.test_yaml.Foo
  a: 1
  c: "bar"

bar: !pw.tests.test_yaml.Bar
  d: foo
"""

    d = load_yaml(yaml_config2)
    assert "bar" in d.keys()
    assert d["bar"] == Bar("foo")


def test_typo_in_key():
    yaml_config = """
foo: !pw.tests.test_yaml.Foo
  d: 1
"""

    with pytest.raises(TypeError):
        load_yaml(yaml_config)


def test_typo_in_variable():
    yaml_config = """
$foo: !pw.tests.test_yaml.Foo
  a: 1
  c: "bar"

bar: !pw.tests.test_yaml.Bar
  d: $fooo
"""

    with pytest.raises(KeyError):
        load_yaml(yaml_config)


def test_read_from_file(tmp_path):
    yaml_config = """
foo: !pw.tests.test_yaml.Foo
  a: 1
  b: 2
  c: bar
"""
    config_path = tmp_path / "config.yml"
    with open(config_path, "w") as f:
        f.write(yaml_config)

    with open(config_path) as f:
        d = load_yaml(f)
    assert "foo" in d.keys()
    assert len(d.keys()) == 1
    assert d["foo"] == Foo(1, 2, "bar")


def test_list():
    yaml_config = """
$foo: !pw.tests.test_yaml.Foo
  a: 2

foo_list:
  - !pw.tests.test_yaml.Foo
    a: 1
    b: 2
    c: bar
  - $foo
"""

    d = load_yaml(yaml_config)
    assert "foo_list" in d.keys()
    assert d["foo_list"] == [Foo(1, 2, "bar"), Foo(2)]


def test_pydantic_config_from_yaml():

    class Config(BaseModel):
        foo: InstanceOf[Foo]
        bar_list: list[InstanceOf[Bar]]

    yaml_config = """
$foo: !pw.tests.test_yaml.Foo
  a: 2

foo: $foo

bar_list:
  - !pw.tests.test_yaml.Bar
    d: !pw.tests.test_yaml.Foo
      a: 1
      b: 2
      c: bar
  - !pw.tests.test_yaml.Bar
    d: $foo
"""

    d = load_yaml(yaml_config)
    config = Config(**d)
    assert config.foo == Foo(2)
    assert config.bar_list == [Bar(Foo(1, 2, "bar")), Bar(Foo(2))]
