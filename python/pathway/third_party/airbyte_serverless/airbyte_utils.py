#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
# This file has been created from extracts of the following Airbyte files with minor modifications:
#  - https://github.com/airbytehq/airbyte/blob/master/octavia-cli/octavia_cli/generate/renderers.py
#  - https://github.com/airbytehq/airbyte/blob/master/octavia-cli/octavia_cli/generate/templates/source_or_destination.yaml.j2  # noqa
#

from typing import Any, Callable, List

import jinja2


TEMPLATE = jinja2.Template(
    """# PREGENERATED | object | PLEASE UPDATE this pre-generated config by following the documentation {{ documentation_url }}
{%- macro render_field(field, is_commented) %}
{%- if is_commented %}# {% endif %}{{ field.name }}:{% if field.default %} {% if field.airbyte_secret %}{{ field.default }}{% else %}{{ field.default | tojson() }}{% endif %}{% endif %} # {{ field.comment }}
{%- endmacro %}

{%- macro render_sub_fields(sub_fields, is_commented) %}
{%- for f in sub_fields %}
{%- if f.type == "object" and not f.oneOf %}
{{- render_object_field(f)|indent(2, False) }}
{%- elif f.oneOf %}}
{{- render_one_of(f) }}
{%- elif f.is_array_of_objects %}}
{{- render_array_of_objects(f) }}
{%- else %}
{{ render_field(f, is_commented) }}
{%- endif %}
{%- endfor %}
{%- endmacro %}

{%- macro render_array_sub_fields(sub_fields, is_commented) %}
{%- for f in sub_fields %}
{% if loop.first %}- {% else %}  {% endif %}{{ render_field(f, is_commented) }}
{%- endfor %}
{%- endmacro %}


{%- macro render_one_of(field) %}
{{ field.name }}:
{%- for one_of_value in field.one_of_values %}
  {%- if loop.first %}
  ## -------- Pick one valid structure among the examples below: --------
  {{- render_sub_fields(one_of_value, False)|indent(2, False) }}
  {%- else %}
  ## -------- Another valid structure for {{ field.name }}: --------
  {{- render_sub_fields(one_of_value, True)|indent(2, False) }}
  {%- endif %}
{%- endfor %}
{%- endmacro %}

{%- macro render_object_field(field) %}
{{ field.name }}:
  {{- render_sub_fields(field.object_properties, is_commented=False)|indent(2, False)}}
{%- endmacro %}

{%- macro render_array_of_objects(field) %}
{{ field.name }}:
  {{- render_array_sub_fields(field.array_items, is_commented=False)|indent(2, False)}}
{%- endmacro %}

{%- macro render_root(root, is_commented) %}
{%- for f in root %}
{%- if f.type == "object" and not f.oneOf %}
{{- render_object_field(f)|indent(0, False) }}
{%- elif f.oneOf %}
{{- render_one_of(f)|indent(0, False) }}
{%- elif f.is_array_of_objects %}
{{- render_array_of_objects(f)|indent(0, False) }}
{%- else %}
{{ render_field(f, is_commented=is_commented) }}
{%- endif %}
{%- endfor %}
{%- endmacro %}

{%- for root in configuration_fields %}
{%- if loop.first %}
{{- render_root(root, is_commented=False)}}
{%- else %}
{{- render_root(root, is_commented=True)}}
{%- endif %}
{% endfor %}
""",  # noqa
    autoescape=jinja2.select_autoescape(),
    trim_blocks=False,
    lstrip_blocks=True,
)


class FieldToRender:
    def __init__(self, name: str, required: bool, field_metadata: dict) -> None:
        """Initialize a FieldToRender instance
        Args:
            name (str): name of the field
            required (bool): whether it's a required field or not
            field_metadata (dict): metadata associated with the field
        """
        self.name = name
        self.required = required
        self.field_metadata = field_metadata
        self.one_of_values = self._get_one_of_values()
        self.object_properties = get_object_fields(field_metadata)
        self.array_items = self._get_array_items()
        self.comment = self._build_comment(
            [
                self._get_secret_comment,
                self._get_required_comment,
                self._get_type_comment,
                self._get_description_comment,
                self._get_example_comment,
            ]
        )
        self.default = self._get_default()

    def __getattr__(self, name: str) -> Any:
        """Map field_metadata keys to attributes of Field.
        Args:
            name (str): attribute name
        Returns:
            [Any]: attribute value
        """
        if name in self.field_metadata:
            return self.field_metadata.get(name)

    @property
    def is_array_of_objects(self) -> bool:
        if self.type == "array" and self.items:
            if self.items.get("type") == "object":
                return True
        return False

    def _get_one_of_values(self) -> List[List["FieldToRender"]]:
        """An object field can have multiple kind of values if it's a oneOf.
        This functions returns all the possible one of values the field can take.
        Returns:
            [list]: List of oneof values.
        """
        if not self.oneOf:
            return []
        one_of_values = []
        for one_of_value in self.oneOf:
            properties = get_object_fields(one_of_value)
            one_of_values.append(properties)
        return one_of_values

    def _get_array_items(self) -> List["FieldToRender"]:
        """If the field is an array of objects, retrieve fields of these objects.
        Returns:
            [list]: List of fields
        """
        if self.is_array_of_objects:
            required_fields = self.items.get("required", [])
            return parse_fields(required_fields, self.items["properties"])
        return []

    def _get_required_comment(self) -> str:
        return "REQUIRED" if self.required else "OPTIONAL"

    def _get_type_comment(self) -> str | None:
        if isinstance(self.type, list):
            return ", ".join(self.type)
        return self.type if self.type else None

    def _get_secret_comment(self) -> str | None:
        return (
            "SECRET (please store in environment variables)"
            if self.airbyte_secret
            else None
        )

    def _get_description_comment(self) -> str | None:
        return self.description if self.description else None

    def _get_example_comment(self) -> str | None:
        example_comment = None
        if self.examples:
            if isinstance(self.examples, list):
                if len(self.examples) > 1:
                    example_comment = f"Examples: {', '.join([str(example) for example in self.examples])}"
                else:
                    example_comment = f"Example: {self.examples[0]}"
            else:
                example_comment = f"Example: {self.examples}"
        return example_comment

    def _get_default(self) -> str:
        if self.const:
            return self.const
        if self.airbyte_secret:
            return f"${{{self.name.upper()}}}"
        return self.default

    @staticmethod
    def _build_comment(comment_functions: list[Callable]) -> str:
        return " | ".join(
            filter(None, [comment_fn() for comment_fn in comment_functions])
        ).replace("\n", "")


def parse_fields(required_fields: List[str], fields: dict) -> List["FieldToRender"]:
    return [
        FieldToRender(f_name, f_name in required_fields, f_metadata)
        for f_name, f_metadata in fields.items()
    ]


def get_object_fields(field_metadata: dict) -> List["FieldToRender"]:
    if field_metadata.get("properties"):
        required_fields = field_metadata.get("required", [])
        return parse_fields(required_fields, field_metadata["properties"])
    return []


def parse_connection_specification(schema: dict) -> List[List["FieldToRender"]]:
    """Create a renderable structure from the specification schema
    Returns:
        List[List["FieldToRender"]]: List of list of fields to render.
    """
    one_of = schema.get("oneOf")
    if one_of:
        roots = []
        for one_of_value in one_of:
            required_fields = one_of_value.get("required", [])
            roots.append(parse_fields(required_fields, one_of_value["properties"]))
        return roots
    else:
        required_fields = schema.get("required", [])
        return [parse_fields(required_fields, schema["properties"])]


def generate_connection_yaml_config_sample(source_spec):
    parsed_schema = parse_connection_specification(
        source_spec["connectionSpecification"]
    )
    return TEMPLATE.render(
        {
            "documentation_url": source_spec["documentationUrl"],
            "configuration_fields": parsed_schema,
        }
    )
