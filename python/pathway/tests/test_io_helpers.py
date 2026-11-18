# Copyright © 2026 Pathway

import ctypes
import os

from pathway.internals._io_helpers import AwsS3Settings


def _write_credentials_file(path, section, key="AKID", secret="SECRET", token=None):
    lines = [
        f"[{section}]",
        f"aws_access_key_id = {key}",
        f"aws_secret_access_key = {secret}",
    ]
    if token is not None:
        lines.append(f"aws_session_token = {token}")
    path.write_text("\n".join(lines) + "\n")


def _point_aws_files_at(monkeypatch, tmp_path, credentials=None, config=None):
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.setenv(
        "AWS_SHARED_CREDENTIALS_FILE", str(credentials or tmp_path / "missing-creds")
    )
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config or tmp_path / "missing-config"))
    # keep the resolver chain away from the instance metadata endpoint
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")


def test_authorize_prefers_explicit_keys(monkeypatch):
    settings = AwsS3Settings(
        bucket_name="b", region="us-east-1", access_key="A", secret_access_key="S"
    )
    settings.authorize()
    assert settings._access_key == "A"
    assert settings._secret_access_key == "S"


def test_authorize_pins_env_credentials_explicitly(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "ENVKEY")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "ENVSECRET")
    settings = AwsS3Settings(bucket_name="b", region="us-east-1")
    settings.authorize()
    assert settings._access_key == "ENVKEY"
    assert settings._secret_access_key == "ENVSECRET"


def test_authorize_ignores_credentials_leaked_into_process_env(tmp_path, monkeypatch):
    # delta-rs copies the credentials of every opened table into the process
    # env (S3StorageOptions::from_map), invisibly to os.environ; the resolution
    # must not pick those up
    libc = ctypes.CDLL(None)
    for var, val in [
        ("AWS_ACCESS_KEY_ID", b"leaked-minio-key"),
        ("AWS_SECRET_ACCESS_KEY", b"leaked-minio-secret"),
    ]:
        assert var not in os.environ
        libc.setenv(var.encode(), val, 1)
    try:
        creds_file = tmp_path / "credentials"
        _write_credentials_file(
            creds_file, "default", key="REALKEY", secret="REALSECRET"
        )
        _point_aws_files_at(monkeypatch, tmp_path, credentials=creds_file)
        monkeypatch.delenv("AWS_PROFILE", raising=False)

        settings = AwsS3Settings(bucket_name="b", region="us-east-1")
        settings.authorize()
        assert settings._access_key == "REALKEY"
        assert settings._secret_access_key == "REALSECRET"
    finally:
        libc.unsetenv(b"AWS_ACCESS_KEY_ID")
        libc.unsetenv(b"AWS_SECRET_ACCESS_KEY")


def test_authorize_fills_profile_credentials(tmp_path, monkeypatch):
    creds_file = tmp_path / "credentials"
    _write_credentials_file(creds_file, "my-profile", token="TOKEN")
    _point_aws_files_at(monkeypatch, tmp_path, credentials=creds_file)
    monkeypatch.setenv("AWS_PROFILE", "my-profile")

    settings = AwsS3Settings(bucket_name="b", region="us-east-1")
    settings.authorize()
    assert settings._access_key == "AKID"
    assert settings._secret_access_key == "SECRET"
    assert settings._session_token == "TOKEN"


def test_authorize_honors_legacy_default_profile_variable(tmp_path, monkeypatch):
    creds_file = tmp_path / "credentials"
    _write_credentials_file(creds_file, "legacy-profile")
    _point_aws_files_at(monkeypatch, tmp_path, credentials=creds_file)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.setenv("AWS_DEFAULT_PROFILE", "legacy-profile")

    settings = AwsS3Settings(bucket_name="b", region="us-east-1")
    settings.authorize()
    assert settings._access_key == "AKID"
    assert settings._secret_access_key == "SECRET"


def test_authorize_fills_default_profile_from_config_file(tmp_path, monkeypatch):
    config_file = tmp_path / "config"
    _write_credentials_file(config_file, "default")
    _point_aws_files_at(monkeypatch, tmp_path, config=config_file)
    monkeypatch.delenv("AWS_PROFILE", raising=False)

    settings = AwsS3Settings(bucket_name="b", region="us-east-1")
    settings.authorize()
    assert settings._access_key == "AKID"
    assert settings._secret_access_key == "SECRET"


def test_authorize_without_any_credentials_leaves_settings_intact(
    tmp_path, monkeypatch
):
    _point_aws_files_at(monkeypatch, tmp_path)
    monkeypatch.delenv("AWS_PROFILE", raising=False)

    settings = AwsS3Settings(bucket_name="b", region="us-east-1")
    settings.authorize()
    assert settings._access_key is None
    assert settings._secret_access_key is None
