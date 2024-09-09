import os
import sys
import time

from azure.core.credentials import AccessToken
from azure.core.exceptions import HttpResponseError
from azure.mgmt.containerinstance import ContainerInstanceManagementClient
from azure.mgmt.containerinstance.models import (
    Container,
    ContainerGroup,
    ContainerGroupRestartPolicy,
    ContainerPort,
    EnvironmentVariable,
    ImageRegistryCredential,
    IpAddress,
    OperatingSystemTypes,
    Port,
    ResourceRequests,
    ResourceRequirements,
)
from deltalake import DeltaTable

AZURE_SUBSCRIPTION_ID = "YOUR_AZURE_SUBSCRIPTION_ID"
AZURE_TOKEN_CREDENTIAL = "YOUR_AZURE_TOKEN_CREDENTIAL"
AZURE_RESOURCE_GROUP = "YOUR_AZURE_RESOURCE_GROUP"
AZURE_CONTAINER_GROUP_NAME = "pathway-test-container-group"
AZURE_CONTAINER_NAME = "pathway-test-container"
AZURE_LOCATION = "eastus"

DOCKER_REGISTRY_USER = "YOUR_DOCKER_REGISTRY_USER"
DOCKER_REGISTRY_TOKEN = "YOUR_DOCKER_REGISTRY_TOKEN"
DOCKER_IMAGE_NAME = "pathwaycom/pathway:latest"

AWS_S3_OUTPUT_PATH = "YOUR_AWS_S3_OUTPUT_PATH"
AWS_S3_ACCESS_KEY = "YOUR_AWS_S3_ACCESS_KEY"
AWS_S3_SECRET_ACCESS_KEY = "YOUR_AWS_S3_SECRET_ACCESS_KEY"
AWS_BUCKET_NAME = "YOUR_AWS_BUCKET_NAME"
AWS_REGION = "YOUR_AWS_REGION"

PATHWAY_LICENSE_KEY = "YOUR_PATHWAY_LICENSE_KEY"
GITHUB_PERSONAL_ACCESS_TOKEN = "YOUR_GITHUB_PERSONAL_ACCESS_TOKEN"


class TokenCredential:
    def __init__(self, token: str):
        self.token = token

    def get_token(self, *args, **kwargs):
        return AccessToken(self.token, 3600)


def get_environment_variable_overrides():
    environment_vars = [
        EnvironmentVariable(
            name="AWS_S3_OUTPUT_PATH",
            value=AWS_S3_OUTPUT_PATH,
        ),
        EnvironmentVariable(
            name="AWS_S3_ACCESS_KEY",
            value=AWS_S3_ACCESS_KEY,
        ),
        EnvironmentVariable(
            name="AWS_S3_SECRET_ACCESS_KEY",
            value=AWS_S3_SECRET_ACCESS_KEY,
        ),
        EnvironmentVariable(
            name="AWS_BUCKET_NAME",
            value=AWS_BUCKET_NAME,
        ),
        EnvironmentVariable(
            name="AWS_REGION",
            value=AWS_REGION,
        ),
        EnvironmentVariable(
            name="PATHWAY_LICENSE_KEY",
            value=PATHWAY_LICENSE_KEY,
        ),
        EnvironmentVariable(
            name="GITHUB_PERSONAL_ACCESS_TOKEN",
            value=GITHUB_PERSONAL_ACCESS_TOKEN,
        ),
        EnvironmentVariable(
            name="PATHWAY_SPAWN_ARGS",
            value="--repository-url https://github.com/pathway-labs/airbyte-to-deltalake python main.py",
        ),
    ]
    return environment_vars


def wait_for_container_completion(client, resource_group, container_group_name):
    while True:
        try:
            container_group = client.container_groups.get(
                resource_group, container_group_name
            )
            container = container_group.containers[0]
            instance_view = container.instance_view
            if instance_view is None or instance_view.current_state is None:
                print(
                    "Container instance view not yet available. "
                    "Waiting for initialization..."
                )
                time.sleep(10)
                continue
            container_state = instance_view.current_state.state
            print(f"Container state: {container_state}")
            if container_state == "Terminated":
                print("Container has terminated.")
                exit_code = container.instance_view.current_state.exit_code
                if exit_code == 0:
                    print("Container completed successfully.")
                else:
                    print(f"Container failed with exit code {exit_code}.")
                break
        except HttpResponseError as e:
            print(f"Error fetching container status: {e.message}")
            break
        time.sleep(10.0)


if __name__ == "__main__":
    sys.stdout = os.fdopen(sys.stdout.fileno(), "w", buffering=1)

    client = ContainerInstanceManagementClient(
        TokenCredential(AZURE_TOKEN_CREDENTIAL), AZURE_SUBSCRIPTION_ID
    )
    container = Container(
        name=AZURE_CONTAINER_NAME,
        image=DOCKER_IMAGE_NAME,
        resources=ResourceRequirements(
            requests=ResourceRequests(cpu=1, memory_in_gb=1.5)
        ),
        ports=[ContainerPort(port=80)],
        environment_variables=get_environment_variable_overrides(),
    )

    container_group = ContainerGroup(
        location=AZURE_LOCATION,
        containers=[container],
        os_type=OperatingSystemTypes.linux,
        ip_address=IpAddress(ports=[Port(protocol="TCP", port=80)], type="Public"),
        restart_policy=ContainerGroupRestartPolicy.never,
        image_registry_credentials=[
            ImageRegistryCredential(
                server="index.docker.io",
                username=DOCKER_REGISTRY_USER,
                password=DOCKER_REGISTRY_TOKEN,
            )
        ],
    )

    try:
        print(f"Deleting existing container group '{AZURE_CONTAINER_GROUP_NAME}'...")
        client.container_groups.begin_delete(
            AZURE_RESOURCE_GROUP, AZURE_CONTAINER_GROUP_NAME
        ).wait()
        print(f"Container group '{AZURE_CONTAINER_GROUP_NAME}' deleted.")
    except Exception:
        print(
            f"Container group '{AZURE_CONTAINER_GROUP_NAME}' does not exist, "
            "skipping deletion."
        )

    client.container_groups.begin_create_or_update(
        resource_group_name=AZURE_RESOURCE_GROUP,
        container_group_name=AZURE_CONTAINER_GROUP_NAME,
        container_group=container_group,
    )

    print(f"Deployment of container {AZURE_CONTAINER_NAME} started successfully.")

    wait_for_container_completion(
        client, AZURE_RESOURCE_GROUP, AZURE_CONTAINER_GROUP_NAME
    )

    storage_options = {
        "AWS_ACCESS_KEY_ID": AWS_S3_ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": AWS_S3_SECRET_ACCESS_KEY,
        "AWS_REGION": AWS_REGION,
        "AWS_BUCKET_NAME": AWS_BUCKET_NAME,
        # Disabling DynamoDB sync since there are no parallel writes into this Delta Lake
        "AWS_S3_ALLOW_UNSAFE_RENAME": "True",
    }
    delta_table = DeltaTable(
        AWS_S3_OUTPUT_PATH,
        storage_options=storage_options,
    )
    pd_table_from_delta = delta_table.to_pandas()
    print("Entries read and parsed: ", pd_table_from_delta.shape[0])
