import os
import re

import yaml
import jinja2

from .sources import Source
from .destinations import Destination
from .runners import Runner


CONNECTION_CONFIG_TEMPLATE = jinja2.Template(
    """
source:
  {{ source.yaml_definition_example | indent(2, False) }}

destination:
  {{ destination.yaml_definition_example | indent(2, False) }}

remote_runner:
  {{ remote_runner.yaml_definition_example | indent(2, False) }}
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
    """

    def __init__(self, yaml_config=None):
        self.yaml_config = yaml_config

    def init_yaml_config(self, source, destination, remote_runner):
        source = Source(source)
        destination = Destination(destination)
        remote_runner = Runner(remote_runner, self)
        self.yaml_config = CONNECTION_CONFIG_TEMPLATE.render(
            source=source,
            destination=destination,
            remote_runner=remote_runner,
        )

    @property
    def config(self):
        yaml_config = self.yaml_config
        yaml_config = replace_secrets(yaml_config)
        assert (
            yaml_config
        ), "connection `yaml_config` does not exist. Please re-create connection"
        return yaml.safe_load(yaml_config)

    @property
    def available_streams(self):
        return self.source.available_streams

    def set_streams(self, streams):
        assert streams, "`streams` variable must be defined"
        self.yaml_config = re.sub(
            r"streams:[^#]*(#*.*)",
            f"streams: {streams} \g<1>",  # noqa
            self.yaml_config,
        )

    @property
    def source(self):
        return Source(**self.config["source"])

    @property
    def destination(self):
        return Destination(**self.config["destination"])

    @property
    def remote_runner(self):
        return Runner(self.config["remote_runner"]["type"], self)

    def run(self):
        Runner("direct", self).run()

    def remote_run(self):
        self.remote_runner.run()


class ConnectionFromFile(Connection):
    """
    A `ConnectionFromFile` extends a `Connection` with
    `yaml_config` stored in a configuration file instead of in a variable
    """

    CONNECTIONS_FOLDER = "connections"

    def __init__(self, name):
        self.name = name
        self.config_filename = f"{self.CONNECTIONS_FOLDER}/{self.name}.yaml"

    # TODO: make config consist only of a single source parameter
    def init_yaml_config(self, source, destination, remote_runner):
        assert not self.yaml_config, (
            f"Connection `{self.name}` already exists. "
            f"If you want to re-init it, delete the file `{self.config_filename}`"
            " and run this command again"
        )
        super().init_yaml_config(source, destination, remote_runner)

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
