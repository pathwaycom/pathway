import os
import re

import yaml
import jinja2

from .sources import DockerAirbyteSource


CONNECTION_CONFIG_TEMPLATE = jinja2.Template(
    """
source:
  {{ source.yaml_definition_example | indent(2, False) }}
"""
)


SECRETS = {}


def replace_secrets(yaml_config):
    google_secrets = re.findall(r"GCP_SECRET\([^\)]*\)", yaml_config)
    if google_secrets:
        import google.cloud.secretmanager

        secret_manager = google.cloud.secretmanager.SecretManagerServiceClient()
        for google_secret in google_secrets:
            if google_secret not in SECRETS:
                secret_name = (
                    google_secret.replace("GCP_SECRET(", "")
                    .replace(")", "")
                    .replace('"', "")
                    .replace("'", "")
                )
                secret = secret_manager.access_secret_version(name=secret_name)
                secret_decoded = secret.payload.data.decode("UTF-8")
                SECRETS[google_secret] = secret_decoded
            yaml_config = yaml_config.replace(google_secret, SECRETS[google_secret])
    return yaml_config


class Connection:
    """
    A `Connection` instance:
    - instantiates a `source` and a `destination` from provided `yaml_config`
    - has a `run` method to perform extract-load from `source` to `destination`

    Both `Connection` and `ConnectionFromFile` are only used in Pathway CLI to generate
    config templates based on the connector name.
    """

    def __init__(self, yaml_config=None):
        self.yaml_config = yaml_config

    def init_yaml_config(self, source):
        source = DockerAirbyteSource(source)
        self.yaml_config = CONNECTION_CONFIG_TEMPLATE.render(source=source)

    @property
    def config(self):
        yaml_config = self.yaml_config
        yaml_config = replace_secrets(yaml_config)
        assert (
            yaml_config
        ), "connection `yaml_config` does not exist. Please re-create connection"
        return yaml.safe_load(yaml_config)


class ConnectionFromFile(Connection):
    """
    A `ConnectionFromFile` extends a `Connection` with
    `yaml_config` stored in a configuration file instead of in a variable
    """

    CONNECTIONS_FOLDER = "connections"

    def __init__(self, name):
        self.name = name
        self.config_filename = f"{self.CONNECTIONS_FOLDER}/{self.name}.yaml"

    def init_yaml_config(self, source):
        assert not self.yaml_config, (
            f"Connection `{self.name}` already exists. "
            f"If you want to re-init it, delete the file `{self.config_filename}`"
            " and run this command again"
        )
        super().init_yaml_config(source)

    @property
    def yaml_config(self):
        if not os.path.isfile(self.config_filename):
            return ""
        return open(self.config_filename, encoding="utf-8").read()

    @yaml_config.setter
    def yaml_config(self, yaml_config):
        os.makedirs(self.CONNECTIONS_FOLDER, exist_ok=True)
        return open(self.config_filename, "w", encoding="utf-8").write(yaml_config)

    @classmethod
    def list_connections(cls):
        return [
            filename.replace(".yaml", "")
            for filename in os.listdir(cls.CONNECTIONS_FOLDER)
            if filename.endswith(".yaml")
        ]
