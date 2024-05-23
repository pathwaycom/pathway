import base64


class BaseRunner:

    yaml_definition_example = ""

    def __init__(self, connection):
        self.connection = connection

    def run(self):
        raise NotImplementedError()


class DirectRunner(BaseRunner):

    def run(self):
        state = self.connection.destination.get_state()
        messages = self.connection.source.extract(state=state)
        self.connection.destination.load(messages)


class CloudRunJobRunner(BaseRunner):

    yaml_definition_example = "\n".join(
        [
            "project:  # REQUIRED | string | GCP Project where cloud run job will be deployed",
            'region: "europe-west1" # REQUIRED | string | Region where cloud run job will be deployed',
            'service_account: "" # OPTIONAL | string | Service account email used bu Cloud Run Job. '
            "If empty default compute service account will be used",
            "env_vars:  # OPTIONAL | dict | Environements Variables",
        ]
    )

    def run(self):
        import google.cloud.run_v2
        import google.api_core.exceptions

        cloud_run = google.cloud.run_v2.JobsClient()

        docker_image = self.connection.config["source"]["docker_image"]
        runner_config = self.connection.config["remote_runner"]["config"]
        project = runner_config["project"]
        region = runner_config["region"]
        service_account = runner_config.get("service_account")
        env_vars = runner_config.get("env_vars")
        env = []
        if env_vars:
            assert isinstance(
                env_vars, dict
            ), "Given env_vars argument should be a dict"
            env = [{"name": k, "value": v} for k, v in env_vars.items()]
        yaml_config_b64 = base64.b64encode(
            self.connection.yaml_config.encode("utf-8")
        ).decode("utf-8")

        location = f"projects/{project}/locations/{region}"
        job_id = f"abs-{self.connection.name}".lower().replace("_", "-")
        job_name = f"{location}/jobs/{job_id}"

        container = {
            "image": docker_image,
            "command": ["/bin/sh"],
            "args": ["-c", "pip install airbyte-serverless && abs run-env-vars"],
            "env": [{"name": "YAML_CONFIG", "value": yaml_config_b64}] + env,
            "resources": {
                "limits": {
                    "memory": "512Mi",
                    "cpu": "1",
                }
            },
        }
        job_config = {
            "containers": [container],
            "timeout": "3600s",
            "max_retries": 0,
        }
        if service_account:
            job_config["service_account"] = service_account

        try:
            cloud_run.delete_job(name=job_name).result()
        except google.api_core.exceptions.NotFound:
            pass
        cloud_run.create_job(
            job={"template": {"template": job_config}},  # type: ignore
            job_id=job_id,
            parent=location,
        ).result()
        operation = cloud_run.run_job({"name": job_name})
        execution_id = operation.metadata.name.split("/")[-1]
        execution_url = f"https://console.cloud.google.com/run/jobs/executions/details/{region}/{execution_id}/logs?project={project}"  # noqa
        print("Launched Job. See details at", execution_url)
        operation.result()


RUNNER_CLASS_MAP = {
    "direct": DirectRunner,
    "cloud_run_job": CloudRunJobRunner,
}


class Runner:

    def __init__(self, runner_type, connection):
        Runner = RUNNER_CLASS_MAP.get(runner_type)
        assert Runner, f"`runner_type` should be among {list(RUNNER_CLASS_MAP.keys())}"
        self.runner = Runner(connection)
        self.yaml_definition_example = "\n".join(
            [
                f'type: "{runner_type}" # GENERATED | string | Runner Type. '
                f"Must be one of {list(RUNNER_CLASS_MAP.keys())}",
                "config: # PREGENERATED | object | PLEASE UPDATE this pre-generated config",
                "  " + self.runner.yaml_definition_example.replace("\n", "\n  "),
            ]
        )

    def __getattr__(self, name):
        return getattr(self.runner, name)
