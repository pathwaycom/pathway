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

    description = webserver.openapi_description_json
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

    description = webserver.openapi_description_json
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

    description = webserver.openapi_description_json
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

    description = webserver.openapi_description_json
    openapi_spec_validator.validate(description)

    assert description["paths"].keys() == {"/one", "/two"}


def test_raw_input_format():
    webserver = pw.io.http.PathwayWebserver(host="127.0.0.1", port=8080)
    pw.io.http.rest_connector(
        webserver=webserver,
        delete_completed_queries=False,
    )

    description = webserver.openapi_description_json
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

    description = webserver.openapi_description_json
    openapi_spec_validator.validate(description)

    assert (
        "required"
        not in description["paths"]["/"]["post"]["requestBody"]["content"][
            "application/json"
        ]["schema"]
    )


def test_no_routes():
    webserver = pw.io.http.PathwayWebserver(host="127.0.0.1", port=8080)
    description = webserver.openapi_description_json
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

    description = webserver.openapi_description_json
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

    description = webserver.openapi_description_json
    openapi_spec_validator.validate(description)

    assert set(description["paths"]["/"].keys()) == set(["get", "post", "put", "patch"])
