# Copyright © 2026 Pathway

from __future__ import annotations

import dataclasses
import datetime
import json
import os

from pathway.internals import api, schema
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame

S3_PATH_PREFIXES = ["s3://", "s3a://"]
S3_REGION_DETECTION_TIMEOUT_S = 30.0


class TLSSettings:
    """Stores TLS connection settings for connectors that support encrypted
    communication (e.g. PostgreSQL, RabbitMQ).

    Args:
        mode: The SSL verification mode. Determines how strictly the server
            certificate is validated. Possible values: ``"disable"``, ``"allow"``,
            ``"prefer"`` (default), ``"require"``, ``"verify-ca"``, ``"verify-full"``.
        root_cert_path: Path to the root CA certificate file used to verify the
            server's certificate.
        client_cert_path: Path to the client certificate file for mutual TLS
            authentication.
        client_key_path: Path to the client private key file for mutual TLS
            authentication.
        trust_certificates: If True, trust server certificates without
            verification. Use only for development and testing.
    """

    @trace_user_frame
    def __init__(
        self,
        *,
        mode: str = "prefer",
        root_cert_path: str | None = None,
        client_cert_path: str | None = None,
        client_key_path: str | None = None,
        trust_certificates: bool = False,
    ):
        self._mode = _parse_ssl_mode(mode)
        self._root_cert_path = root_cert_path
        self._client_cert_path = client_cert_path
        self._client_key_path = client_key_path
        self._trust_certificates = trust_certificates

    @property
    def settings(self) -> api.TlsSettings:
        return api.TlsSettings(
            mode=self._mode,
            root_cert_path=self._root_cert_path,
            client_cert_path=self._client_cert_path,
            client_key_path=self._client_key_path,
            trust_certificates=self._trust_certificates,
        )


def _parse_ssl_mode(ssl_mode: str) -> api.SslMode:
    match ssl_mode.lower():
        case "disable":
            return api.SslMode.DISABLE
        case "allow":
            return api.SslMode.ALLOW
        case "prefer":
            return api.SslMode.PREFER
        case "require":
            return api.SslMode.REQUIRE
        case "verify-ca" | "verify_ca":
            return api.SslMode.VERIFY_CA
        case "verify-full" | "verify_full":
            return api.SslMode.VERIFY_FULL
        case _:
            raise ValueError(
                f"invalid ssl mode '{ssl_mode}', expected one of "
                "disable, allow, prefer, require, verify-ca, verify-full"
            )


class AwsS3Settings:
    """Stores Amazon S3 connection settings. You may also use this class to store
    configuration settings for any custom S3 installation, however you will need to
    specify the region and the endpoint.

    Args:
        bucket_name: Name of S3 bucket.
        access_key: Access key for the bucket.
        secret_access_key: Secret access key for the bucket.
        with_path_style: Whether to use path-style requests.
        region: Region of the bucket.
        endpoint: Custom endpoint in case of self-hosted storage.
        session_token: Session token, an alternative way to authenticate to S3.
    """

    @trace_user_frame
    def __init__(
        self,
        *,
        bucket_name=None,
        access_key=None,
        secret_access_key=None,
        with_path_style=False,
        region=None,
        endpoint=None,
        session_token=None,
    ):
        self._bucket_name = bucket_name
        self._access_key = access_key
        self._secret_access_key = secret_access_key
        self._session_token = session_token
        self._with_path_style = with_path_style
        self._region = region
        self._endpoint = endpoint

    @property
    def settings(self) -> api.AwsS3Settings:
        return api.AwsS3Settings(
            self._bucket_name,
            self._access_key,
            self._secret_access_key,
            self._with_path_style,
            self._region,
            self._endpoint,
            self._session_token,
        )

    @classmethod
    def new_from_path(cls, s3_path: str):
        """
        Constructs settings from S3 path. The engine will look for the credentials in
        environment variables and in local AWS profiles. The region of the bucket is
        detected automatically with an anonymous request, no credentials needed.

        This method may fail if the bucket does not exist or is unreachable.

        Args:
            s3_path: full path to the object in the form ``s3://<bucket_name>/<path>``.

        Returns:
            Configuration object.
        """
        for s3_path_prefix in S3_PATH_PREFIXES:
            starts_with_prefix = s3_path.startswith(s3_path_prefix)
            has_extra_chars = len(s3_path) > len(s3_path_prefix)
            if not starts_with_prefix or not has_extra_chars:
                continue
            bucket = s3_path[len(s3_path_prefix) :].split("/")[0]

            # the crate we use on the Rust-engine side can't detect the location of a
            # bucket, so it's done here; S3 reports the region of a bucket in a response
            # header, even for anonymous requests and even when it replies with a
            # redirect or an access denial
            import requests

            response = requests.head(
                f"https://s3.amazonaws.com/{bucket}",
                allow_redirects=False,
                timeout=S3_REGION_DETECTION_TIMEOUT_S,
            )
            region = response.headers.get("x-amz-bucket-region")
            if region is None:
                raise ValueError(
                    f"Failed to detect the region of S3 bucket {bucket!r} "
                    f"(HTTP status {response.status_code}): the bucket may not exist. "
                    "If it does, pass AwsS3Settings with an explicit region"
                )

            return cls(
                bucket_name=bucket,
                region=region,
            )

        # If it doesn't start with a valid S3 prefix, it's not a full S3 path
        raise ValueError(f"Incorrect S3 path: {s3_path}")

    def authorize(self):
        """Fills in the credentials that the downstream libraries can't deduce.

        The DeltaLake library resolves environment variables and instance
        credentials on its own, but does not read AWS profile files — those are
        resolved here, with the official AWS SDK chain (environment, profile
        files, SSO, assume-role, IMDS) built into the engine.
        """
        if self._access_key is not None and self._secret_access_key is not None:
            return
        env_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
        env_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        if env_access_key and env_secret_access_key:
            # pinned explicitly instead of left for the engine to rediscover:
            # this snapshot is immune to the credential vars that delta-rs
            # writes into the process env for previously opened tables
            self._access_key = env_access_key
            self._secret_access_key = env_secret_access_key
            session_token = os.environ.get("AWS_SESSION_TOKEN")
            if session_token:
                self._session_token = session_token
            return
        # boto3 also honored the legacy AWS_DEFAULT_PROFILE, at a lower precedence
        resolved = api.resolve_aws_credentials(
            os.environ.get("AWS_PROFILE") or os.environ.get("AWS_DEFAULT_PROFILE")
        )
        if resolved is None:
            return
        access_key, secret_access_key, session_token = resolved
        self._access_key = access_key
        self._secret_access_key = secret_access_key
        if session_token:
            self._session_token = session_token


@dataclasses.dataclass(frozen=True)
class SchemaRegistryHeader:
    """
    Represents an additional header to be used in Confluent Schema Registry HTTP requests.

    Args:
        key: The header key.
        value: The header value.

    Returns:
        The constructed header object
    """

    key: str
    value: str

    def __post_init__(self):
        if not isinstance(self.key, str):
            raise TypeError(
                f"SchemaRegistryHeader.key must be a str, got "
                f"{type(self.key).__name__}."
            )
        if not isinstance(self.value, str):
            raise TypeError(
                f"SchemaRegistryHeader.value must be a str, got "
                f"{type(self.value).__name__}."
            )


@dataclasses.dataclass(frozen=True)
class SchemaRegistrySettings:
    """
    Connection settings for the Confluent Schema Registry.

    Args:
        urls: A list of URLs for connecting to the schema registry. If multiple URLs
            are provided, they will be used in the specified order.
        token_authorization: Token used for token-based authorization.
        username: Username for simple authorization.
        password: Password for simple authorization. If specified, a username
            must also be provided.
        headers: Additional headers to include in HTTP requests to the schema registry.
        proxy: Proxy address for registry requests.
        timeout: Timeout duration for network requests, in seconds.

    Returns:
        The configuration object.
    """

    urls: list[str]
    token_authorization: str | None = None
    username: str | None = None
    password: str | None = None
    headers: list[SchemaRegistryHeader] | None = None
    proxy: str | None = None
    timeout: datetime.timedelta | None = None

    def __post_init__(self):
        if not isinstance(self.urls, (list, tuple)):
            raise TypeError(
                f"SchemaRegistrySettings.urls must be a list of strings, "
                f"got {type(self.urls).__name__}. Wrap a single URL in a "
                f"list: urls=['http://...']."
            )
        if not self.urls:
            raise ValueError(
                "SchemaRegistrySettings requires at least one entry in 'urls'; "
                "got an empty list."
            )
        for i, url in enumerate(self.urls):
            if not isinstance(url, str) or not url:
                raise ValueError(
                    f"SchemaRegistrySettings.urls[{i}] must be a non-empty "
                    f"string; got {url!r}."
                )
        for field_name in ("token_authorization", "username", "password", "proxy"):
            value = getattr(self, field_name)
            if value is not None and not isinstance(value, str):
                raise TypeError(
                    f"SchemaRegistrySettings.{field_name} must be a str, "
                    f"got {type(value).__name__}."
                )
        if self.password is not None and self.username is None:
            raise ValueError(
                "SchemaRegistrySettings: 'password' was provided without "
                "'username'. Both are needed for username/password "
                "authentication."
            )
        if self.token_authorization is not None and (
            self.username is not None or self.password is not None
        ):
            raise ValueError(
                "SchemaRegistrySettings: 'token_authorization' is mutually "
                "exclusive with 'username'/'password'. Pick one "
                "authentication method."
            )
        if self.headers is not None:
            for i, header in enumerate(self.headers):
                if not isinstance(header, SchemaRegistryHeader):
                    raise TypeError(
                        f"SchemaRegistrySettings.headers[{i}] must be a "
                        f"SchemaRegistryHeader instance, got "
                        f"{type(header).__name__}. Use "
                        f"pw.io.kafka.SchemaRegistryHeader(key=..., value=...)."
                    )
        if self.timeout is not None:
            if not isinstance(self.timeout, datetime.timedelta):
                raise TypeError(
                    f"SchemaRegistrySettings.timeout must be a "
                    f"datetime.timedelta, got {type(self.timeout).__name__}."
                )
            if self.timeout <= datetime.timedelta(0):
                raise ValueError(
                    f"SchemaRegistrySettings: 'timeout' must be a positive "
                    f"duration; got {self.timeout!r}."
                )

    @property
    def to_engine(self):
        return api.SchemaRegistrySettings(
            self.urls,
            token_authorization=self.token_authorization,
            username=self.username,
            password=self.password,
            headers=[(header.key, header.value) for header in self.headers or []],
            proxy=self.proxy,
            timeout=self.timeout,
        )


def is_s3_path(path: str) -> bool:
    for s3_path_prefix in S3_PATH_PREFIXES:
        if path.startswith(s3_path_prefix):
            return True
    return False


def _format_output_value_fields(table: Table) -> list[api.ValueField]:
    value_fields = []
    for column_name, column_data in table.schema.columns().items():
        value_field = api.ValueField(
            column_name,
            column_data.dtype.to_engine(),
            source=column_data.engine_field_source,
        )
        value_field.set_metadata(
            json.dumps(column_data.to_json_serializable_dict(), sort_keys=True)
        )
        value_fields.append(value_field)

    return value_fields


def _form_value_fields(schema: type[schema.Schema]) -> list[api.ValueField]:
    schema.default_values()
    default_values = schema.default_values()
    result = []

    columns = schema.columns()
    for f in schema.column_names():
        item = columns.get(f)
        if item is None:
            value_field = api.ValueField(
                f,
                api.PathwayType.ANY,
                source=api.FieldSource.PAYLOAD,
            )
        else:
            value_field = api.ValueField(
                f,
                item.dtype.to_engine(),
                source=item.engine_field_source,
            )
        if f in default_values:
            value_field.set_default(default_values[f])
        result.append(value_field)

    return result
