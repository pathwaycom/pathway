# Copyright © 2024 Pathway

import openapi_spec_validator

import pathway as pw


def test_one_endpoint_no_additional_props_all_fields_required():
    class InputSchema(pw.Schema):
        k: int
        v: int

    webserver = pw.io.http.PathwayWebserver(host="127.0.0.1", port=8080)
    pw.io.http.rest_connector(
        webserver=webserver,
        schema=InputSchema,
        delete_completed_queries=False,
    )

    description = webserver.openapi_description_json("127.0.0.1:8080")
    openapi_spec_validator.validate(description)

    assert not description["paths"]["/"]["post"]["requestBody"]["content"][
        "application/json"
    ]["schema"]["additionalProperties"]


def test_additional_props():
    class InputSchema(pw.Schema):
        k: int
        v: dict

    webserver = pw.io.http.PathwayWebserver(host="127.0.0.1", port=8080)
    pw.io.http.rest_connector(
        webserver=webserver,
        schema=InputSchema,
        delete_completed_queries=False,
    )

    description = webserver.openapi_description_json("127.0.0.1:8080")
    openapi_spec_validator.validate(description)

    assert description["paths"]["/"]["post"]["requestBody"]["content"][
        "application/json"
    ]["schema"]["additionalProperties"]


def test_optional_fields():
    class InputSchema(pw.Schema):
        k: int
        v: str = pw.column_definition(default_value="hello")

    webserver = pw.io.http.PathwayWebserver(host="127.0.0.1", port=8080)
    pw.io.http.rest_connector(
        webserver=webserver,
        schema=InputSchema,
        delete_completed_queries=False,
    )

    description = webserver.openapi_description_json("127.0.0.1:8080")
    openapi_spec_validator.validate(description)

    assert not description["paths"]["/"]["post"]["requestBody"]["content"][
        "application/json"
    ]["schema"]["additionalProperties"]

    assert description["paths"]["/"]["post"]["requestBody"]["content"][
        "application/json"
    ]["schema"]["required"] == ["k"]


def test_two_endpoints():
    class InputSchema(pw.Schema):
        k: int
        v: str

    webserver = pw.io.http.PathwayWebserver(host="127.0.0.1", port=8080)
    pw.io.http.rest_connector(
        webserver=webserver,
        schema=InputSchema,
        route="/one",
        delete_completed_queries=False,
    )
    pw.io.http.rest_connector(
        webserver=webserver,
        schema=InputSchema,
        route="/two",
        delete_completed_queries=False,
    )

    description = webserver.openapi_description_json("127.0.0.1:8080")
    openapi_spec_validator.validate(description)

    assert description["paths"].keys() == {"/one", "/two"}


def test_raw_input_format():
    webserver = pw.io.http.PathwayWebserver(host="127.0.0.1", port=8080)
    pw.io.http.rest_connector(
        webserver=webserver,
        delete_completed_queries=False,
    )

    description = webserver.openapi_description_json("127.0.0.1:8080")
    openapi_spec_validator.validate(description)


def test_no_required_fields():
    class InputSchema(pw.Schema):
        k: int = pw.column_definition(default_value=1)
        v: str = pw.column_definition(default_value="hello")

    webserver = pw.io.http.PathwayWebserver(host="127.0.0.1", port=8080)
    pw.io.http.rest_connector(
        webserver=webserver,
        schema=InputSchema,
        delete_completed_queries=False,
    )

    description = webserver.openapi_description_json("127.0.0.1:8080")
    openapi_spec_validator.validate(description)

    assert (
        "required"
        not in description["paths"]["/"]["post"]["requestBody"]["content"][
            "application/json"
        ]["schema"]
    )


def test_no_routes():
    webserver = pw.io.http.PathwayWebserver(host="127.0.0.1", port=8080)
    description = webserver.openapi_description_json("127.0.0.1:8080")
    openapi_spec_validator.validate(description)


def test_several_methods():
    class InputSchema(pw.Schema):
        k: int
        v: str = pw.column_definition(default_value="hello")

    webserver = pw.io.http.PathwayWebserver(host="127.0.0.1", port=8080)
    pw.io.http.rest_connector(
        webserver=webserver,
        methods=("GET", "POST"),
        schema=InputSchema,
        delete_completed_queries=False,
    )

    description = webserver.openapi_description_json("127.0.0.1:8080")
    openapi_spec_validator.validate(description)

    assert set(description["paths"]["/"].keys()) == set(["get", "post"])


def test_all_methods():
    class InputSchema(pw.Schema):
        k: int
        v: str = pw.column_definition(default_value="hello")

    webserver = pw.io.http.PathwayWebserver(host="127.0.0.1", port=8080)
    pw.io.http.rest_connector(
        webserver=webserver,
        methods=("GET", "POST", "PUT", "PATCH"),
        schema=InputSchema,
        delete_completed_queries=False,
    )

    description = webserver.openapi_description_json("127.0.0.1:8080")
    openapi_spec_validator.validate(description)

    assert set(description["paths"]["/"].keys()) == set(["get", "post", "put", "patch"])


def test_nullable_fields():
    class InputSchema(pw.Schema):
        k: int
        v: str | None

    webserver = pw.io.http.PathwayWebserver(host="127.0.0.1", port=8080)
    pw.io.http.rest_connector(
        webserver=webserver,
        methods=("GET", "POST", "PUT", "PATCH"),
        schema=InputSchema,
        delete_completed_queries=False,
    )

    description = webserver.openapi_description_json("127.0.0.1:8080")
    openapi_spec_validator.validate(description)

    assert set(description["paths"]["/"].keys()) == set(["get", "post", "put", "patch"])


def test_document_methods_partially():
    class InputSchema(pw.Schema):
        k: int
        v: str = pw.column_definition(default_value="hello")

    webserver = pw.io.http.PathwayWebserver(host="127.0.0.1", port=8080)
    pw.io.http.rest_connector(
        webserver=webserver,
        methods=("GET", "POST", "PUT", "PATCH"),
        schema=InputSchema,
        delete_completed_queries=False,
        documentation=pw.io.http.EndpointDocumentation(method_types=("GET", "POST")),
    )

    description = webserver.openapi_description_json("127.0.0.1:8080")
    openapi_spec_validator.validate(description)

    assert set(description["paths"]["/"].keys()) == set(["get", "post"])


def test_no_extra_fields_on_no_description():
    class InputSchema(pw.Schema):
        k: int
        v: str = pw.column_definition(default_value="hello")

    webserver = pw.io.http.PathwayWebserver(host="127.0.0.1", port=8080)
    pw.io.http.rest_connector(
        webserver=webserver,
        methods=("GET", "POST", "PUT", "PATCH"),
        schema=InputSchema,
        delete_completed_queries=False,
        documentation=pw.io.http.EndpointDocumentation(method_types=("GET", "POST")),
    )

    description = webserver.openapi_description_json("127.0.0.1:8080")
    openapi_spec_validator.validate(description)

    assert set(description["paths"]["/"].keys()) == set(["get", "post"])

    assert "tags" not in description["paths"]["/"]["post"]
    assert "description" not in description["paths"]["/"]["post"]
    assert "summary" not in description["paths"]["/"]["post"]

    assert "tags" not in description["paths"]["/"]["get"]
    assert "description" not in description["paths"]["/"]["get"]
    assert "summary" not in description["paths"]["/"]["get"]


def test_extended_description():
    test_tags = ["Testing", "Endpoint", "Hello world"]

    class InputSchema(pw.Schema):
        k: int
        v: str = pw.column_definition(default_value="hello")

    webserver = pw.io.http.PathwayWebserver(host="127.0.0.1", port=8080)
    pw.io.http.rest_connector(
        webserver=webserver,
        methods=("GET", "POST", "PUT", "PATCH"),
        schema=InputSchema,
        delete_completed_queries=False,
        documentation=pw.io.http.EndpointDocumentation(
            description="Endpoint description",
            summary="Endpoint summary",
            tags=test_tags,
            method_types=("GET", "POST"),
        ),
    )

    description = webserver.openapi_description_json("127.0.0.1:8080")
    openapi_spec_validator.validate(description)

    assert set(description["paths"]["/"].keys()) == set(["get", "post"])

    assert description["paths"]["/"]["post"]["tags"] == test_tags
    assert description["paths"]["/"]["post"]["description"] == "Endpoint description"
    assert description["paths"]["/"]["post"]["summary"] == "Endpoint summary"

    assert description["paths"]["/"]["get"]["tags"] == test_tags
    assert description["paths"]["/"]["get"]["description"] == "Endpoint description"
    assert description["paths"]["/"]["get"]["summary"] == "Endpoint summary"


def test_column_example():
    test_tags = ["Testing", "Endpoint", "Hello world"]

    class InputSchema(pw.Schema):
        k: int
        v: str = pw.column_definition(
            default_value="Hello", description="Some value", example="Example"
        )

    webserver = pw.io.http.PathwayWebserver(host="127.0.0.1", port=8080)
    pw.io.http.rest_connector(
        webserver=webserver,
        methods=("GET", "POST", "PUT", "PATCH"),
        schema=InputSchema,
        delete_completed_queries=False,
        documentation=pw.io.http.EndpointDocumentation(
            description="Endpoint description",
            summary="Endpoint summary",
            tags=test_tags,
            method_types=("GET", "POST"),
        ),
    )

    description = webserver.openapi_description_json("127.0.0.1:8080")
    openapi_spec_validator.validate(description)

    assert set(description["paths"]["/"].keys()) == set(["get", "post"])

    schema_for_post = description["paths"]["/"]["post"]["requestBody"]["content"][
        "application/json"
    ]["schema"]["properties"]

    assert schema_for_post["v"]["description"] == "Some value"
    assert schema_for_post["v"]["example"] == "Example"

    field_found = False
    schema_for_get_parameters = description["paths"]["/"]["get"]["parameters"]
    for parameter in schema_for_get_parameters:
        if parameter["name"] != "v":
            continue
        assert parameter["description"] == "Some value"
        assert parameter["example"] == "Example"
        field_found = True
        break

    assert field_found


def test_endpoint_examples_for_payload():
    test_tags = ["Testing", "Endpoint", "Hello world"]

    class InputSchema(pw.Schema):
        k: int
        v: str = pw.column_definition(default_value="hello")

    examples = pw.io.http.EndpointExamples()
    examples.add_example("default", "Default example", values={"k": 1, "v": "Hello"})
    examples.add_example("kv_empty", "Default example", values={"k": 0, "v": ""})

    webserver = pw.io.http.PathwayWebserver(host="127.0.0.1", port=8080)
    pw.io.http.rest_connector(
        webserver=webserver,
        methods=("GET", "POST", "PUT", "PATCH"),
        schema=InputSchema,
        delete_completed_queries=False,
        documentation=pw.io.http.EndpointDocumentation(
            description="Endpoint description",
            summary="Endpoint summary",
            tags=test_tags,
            method_types=("GET", "POST"),
            examples=examples,
        ),
    )

    description = webserver.openapi_description_json("127.0.0.1:8080")
    openapi_spec_validator.validate(description)

    assert set(description["paths"]["/"].keys()) == set(["get", "post"])

    assert description["paths"]["/"]["post"]["tags"] == test_tags
    assert description["paths"]["/"]["post"]["description"] == "Endpoint description"
    examples = description["paths"]["/"]["post"]["requestBody"]["content"][
        "application/json"
    ]["examples"]
    assert len(examples) == 2
