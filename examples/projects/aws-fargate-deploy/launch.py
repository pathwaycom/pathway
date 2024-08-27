import time

import boto3
from deltalake import DeltaTable

S3_OUTPUT_PATH = "YOUR_S3_OUTPUT_PATH"  # Example: "s3://aws-demo/runs/16.08.2024/1/"
S3_BUCKET_NAME = "YOUR_S3_BUCKET_NAME"  # Example: "aws-demo"
S3_REGION = "YOUR_S3_REGION"  # Example: "eu-central-1"
S3_ACCESS_KEY = "YOUR_AWS_S3_ACCESS_KEY"
S3_SECRET_ACCESS_KEY = "YOUR_AWS_S3_SECRET_ACCESS_KEY"
PATHWAY_LICENSE_KEY = (
    "YOUR_PATHWAY_LICENSE_KEY"  # You can get it at https://pathway.com/features
)
GITHUB_PERSONAL_ACCESS_TOKEN = (
    # You can get it at https://github.com/settings/tokens
    "YOUR_GITHUB_PERSONAL_ACCESS_TOKEN"
)


def get_environment_variable_overrides():
    environment_vars = [
        {
            "name": "AWS_S3_OUTPUT_PATH",
            "value": S3_OUTPUT_PATH,
        },
        {
            "name": "AWS_S3_ACCESS_KEY",
            "value": S3_ACCESS_KEY,
        },
        {
            "name": "AWS_S3_SECRET_ACCESS_KEY",
            "value": S3_SECRET_ACCESS_KEY,
        },
        {
            "name": "AWS_BUCKET_NAME",
            "value": S3_BUCKET_NAME,
        },
        {
            "name": "AWS_REGION",
            "value": S3_REGION,
        },
        {
            "name": "PATHWAY_LICENSE_KEY",
            "value": PATHWAY_LICENSE_KEY,
        },
        {
            "name": "GITHUB_PERSONAL_ACCESS_TOKEN",
            "value": GITHUB_PERSONAL_ACCESS_TOKEN,
        },
        {
            # Doesn't need to be changed
            "name": "PATHWAY_SPAWN_ARGS",
            "value": "--repository-url https://github.com/pathway-labs/airbyte-to-deltalake python main.py",
        },
    ]

    return environment_vars


def wait_for_completion(cluster_name, task_arn):
    while True:
        response = ecs_client.describe_tasks(cluster=cluster_name, tasks=[task_arn])
        task = response["tasks"][0]
        last_status = task["lastStatus"]
        if last_status == "STOPPED":
            print(f"Task {task_arn} is completed.")
            break
        print(f"Task status: {last_status}. Waiting 10 more seconds...")
        time.sleep(10)


if __name__ == "__main__":
    # Connect to AWS services ECS and EKS
    ecs_client = boto3.client("ecs", region_name=S3_REGION)
    ecr_client = boto3.client("ecr", region_name=S3_REGION)
    try:
        auth_token = ecr_client.get_authorization_token()
    except Exception:
        print("AWS connection failed")
        exit(1)

    # Create task definition
    task_definition = {
        "family": "pathway-container-test",
        "networkMode": "awsvpc",
        "requiresCompatibilities": ["FARGATE"],
        "cpu": "2048",
        "memory": "8192",
        "executionRoleArn": "arn:aws:iam::YOUR_ACCOUNT_ID:role/ecsTaskExecutionRole",
        "containerDefinitions": [
            {
                "name": "pathway-container-test-definition",
                "image": "PATH_TO_YOUR_IMAGE",
                "essential": True,
            }
        ],
    }
    response = ecs_client.register_task_definition(**task_definition)
    task_definition_arn = response["taskDefinition"]["taskDefinitionArn"]

    # Create cluster to execute the task
    cluster_name = "pathway-test-cluster"
    response = ecs_client.create_cluster(clusterName=cluster_name)

    # Run the task in Fargate
    task = {
        "taskDefinition": task_definition_arn,
        "cluster": cluster_name,
        "launchType": "FARGATE",
        "networkConfiguration": {
            "awsvpcConfiguration": {
                "subnets": ["REPLACE_WITH_YOUR_SUBNET_ID"],
                "assignPublicIp": "ENABLED",
            }
        },
        "count": 1,
        "overrides": {
            "containerOverrides": [
                {
                    "name": "pathway-container-test-definition",
                    "environment": get_environment_variable_overrides(),
                }
            ]
        },
    }
    task_arn = ecs_client.run_task(**task)["tasks"][0]["containers"][0]["taskArn"]

    print("Task ARN:", task_arn)
    wait_for_completion(cluster_name, task_arn)

    # Check execution results
    storage_options = {
        "AWS_ACCESS_KEY_ID": S3_ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": S3_SECRET_ACCESS_KEY,
        "AWS_REGION": S3_REGION,
        "AWS_BUCKET_NAME": S3_BUCKET_NAME,
        # Disabling DynamoDB sync since there are no parallel writes into this Delta Lake
        "AWS_S3_ALLOW_UNSAFE_RENAME": "True",
    }
    delta_table = DeltaTable(
        S3_OUTPUT_PATH,
        storage_options=storage_options,
    )
    pd_table_from_delta = delta_table.to_pandas()
    print("Entries read and parsed: ", pd_table_from_delta.shape[0])
