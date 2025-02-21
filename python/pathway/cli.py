# Copyright © 2024 Pathway

import logging
import os
import pathlib
import subprocess
import sys
import tempfile
import uuid
import venv
from typing import NoReturn, Tuple

import click

import pathway as pw
from pathway.optional_import import optional_imports


def plural(n, singular, plural):
    if n == 1:
        return f"1 {singular}"
    return f"{n} {plural}"


def get_temporary_paths(
    temp_root_directory: tempfile.TemporaryDirectory,
) -> Tuple[pathlib.Path, pathlib.Path]:
    temp_root_path = pathlib.Path(temp_root_directory.name)
    repository_path = temp_root_path / "repository"
    venv_path = temp_root_path / "venv"
    return (repository_path, venv_path)


def checkout_repository(
    repository_url: str | None, branch: str | None
) -> tempfile.TemporaryDirectory | None:
    if repository_url is None:
        return None
    try:
        import git
    except ImportError:
        logging.error("To run the code from Git repository please have git installed")
        exit(1)
    temp_root_directory = tempfile.TemporaryDirectory()
    repository_path, venv_path = get_temporary_paths(temp_root_directory)
    repository = git.Repo.clone_from(repository_url, repository_path)
    if branch is not None:
        repository.git.checkout(branch)
    venv.create(venv_path, with_pip=True)
    return temp_root_directory


def spawn_program(
    *,
    threads,
    processes,
    first_port,
    repository_url,
    branch,
    program,
    arguments,
    env_base,
):
    temp_root_directory = checkout_repository(repository_url, branch)
    if temp_root_directory is not None:
        repository_path, venv_path = get_temporary_paths(temp_root_directory)
        requirements_path = repository_path / "requirements.txt"
        if program.startswith("python"):
            program = venv_path / "bin" / program
        if requirements_path.exists():
            pip_path = venv_path / "bin" / "pip"
            command = [
                os.fspath(pip_path),
                "install",
                "-r",
                os.fspath(requirements_path),
            ]
            pip_handle = subprocess.run(
                " ".join(command),
                stderr=subprocess.STDOUT,
                shell=True,
            )
            if pip_handle.returncode != 0:
                process_stdout = pip_handle.stdout.decode("utf-8")
                logging.error(f"Failed to install requirements:\n{process_stdout}")
                raise RuntimeError("Failed to install dependencies")
        os.chdir(repository_path)

    processes_str = plural(processes, "process", "processes")
    workers_str = plural(processes * threads, "total worker", "total workers")
    click.echo(f"Preparing {processes_str} ({workers_str})", err=True)
    run_id = uuid.uuid4()
    process_handles = []
    try:
        for process_id in range(processes):
            env = env_base.copy()
            env["PATHWAY_THREADS"] = str(threads)
            env["PATHWAY_PROCESSES"] = str(processes)
            env["PATHWAY_FIRST_PORT"] = str(first_port)
            env["PATHWAY_PROCESS_ID"] = str(process_id)
            env["PATHWAY_RUN_ID"] = str(run_id)
            env["PATHWAY_SUPPRESS_OTHER_WORKER_ERRORS"] = "1"
            handle = subprocess.Popen([program] + list(arguments), env=env)
            process_handles.append(handle)
        for handle in process_handles:
            handle.wait()
    finally:
        for handle in process_handles:
            handle.terminate()
    sys.exit(max(handle.returncode for handle in process_handles))


@click.group
@click.version_option(version=pw.__version__, prog_name="pathway")
def cli() -> None:
    pass


@cli.command(
    context_settings={
        "allow_interspersed_args": False,
        "show_default": True,
    }
)
@click.option(
    "-t",
    "--threads",
    metavar="N",
    type=int,
    default=1,
    help="number of threads per process",
)
@click.option(
    "-n",
    "--processes",
    metavar="N",
    type=int,
    default=1,
    help="number of processes",
)
@click.option(
    "--first-port",
    type=int,
    metavar="PORT",
    default=10000,
    help="first port to use for communication",
)
@click.option("--record", is_flag=True, help="record data in the input connectors")
@click.option(
    "--record-path",
    type=str,
    default="record",
    help="directory in which record will be saved",
)
@click.option(
    "--repository-url",
    type=str,
    help="github repository path if the program is spawned from a repository",
)
@click.option(
    "--branch",
    type=str,
    help="branch if different from the default branch",
)
@click.argument("program")
@click.argument("arguments", nargs=-1)
def spawn(
    threads,
    processes,
    first_port,
    record,
    record_path,
    repository_url,
    branch,
    program,
    arguments,
):
    env = os.environ.copy()
    if record:
        env["PATHWAY_REPLAY_STORAGE"] = record_path
        env["PATHWAY_SNAPSHOT_ACCESS"] = "record"
        env["PATHWAY_CONTINUE_AFTER_REPLAY"] = "true"
    spawn_program(
        threads=threads,
        processes=processes,
        first_port=first_port,
        repository_url=repository_url,
        branch=branch,
        program=program,
        arguments=arguments,
        env_base=env,
    )


@cli.command(
    context_settings={
        "allow_interspersed_args": False,
        "show_default": True,
    }
)
@click.option(
    "-t",
    "--threads",
    metavar="N",
    type=int,
    default=1,
    help="number of threads per process",
)
@click.option(
    "-n",
    "--processes",
    metavar="N",
    type=int,
    default=1,
    help="number of processes",
)
@click.option(
    "--first-port",
    type=int,
    metavar="PORT",
    default=10000,
    help="first port to use for communication",
)
@click.option(
    "--record-path",
    type=str,
    default="record",
    help="directory in which recording is stored",
)
@click.option(
    "--mode",
    type=click.Choice(["batch", "speedrun"], case_sensitive=False),
    help="mode of replaying data",
)
@click.option(
    "--continue",
    "continue_after_replay",
    is_flag=True,
    help="continue with realtime data from connectors after stored recording is replayed",
)
@click.option(
    "--repository-url",
    type=str,
    help="github repository path if the program is spawned from a repository",
)
@click.option(
    "--branch",
    type=str,
    help="branch if different from the default branch",
)
@click.argument("program")
@click.argument("arguments", nargs=-1)
def replay(
    threads,
    processes,
    first_port,
    record_path,
    mode,
    continue_after_replay,
    repository_url,
    branch,
    program,
    arguments,
):
    env = os.environ.copy()
    env["PATHWAY_REPLAY_STORAGE"] = record_path
    env["PATHWAY_SNAPSHOT_ACCESS"] = "replay"
    env["PATHWAY_PERSISTENCE_MODE"] = mode
    env["PATHWAY_REPLAY_MODE"] = mode
    if continue_after_replay:
        env["PATHWAY_CONTINUE_AFTER_REPLAY"] = "true"
    spawn_program(
        threads=threads,
        processes=processes,
        first_port=first_port,
        repository_url=repository_url,
        branch=branch,
        program=program,
        arguments=arguments,
        env_base=env,
    )


@cli.command()
def spawn_from_env():
    cli_spawn_arguments = os.environ.get("PATHWAY_SPAWN_ARGS")
    if cli_spawn_arguments is not None:
        args = ["spawn"] + cli_spawn_arguments.split(" ")
        os.execl(sys.executable, sys.executable, sys.argv[0], *args)
    else:
        logging.warning("PATHWAY_SPAWN_ARGS variable is unspecified, exiting...")


@cli.group()
def airbyte() -> None:
    pass


@airbyte.command()
@click.argument("connection")
@click.option(
    "--image",
    default="airbyte/source-faker:0.1.4",
    help="Any Public Docker Airbyte Source. Example: `airbyte/source-faker:0.1.4`. "
    '(see connectors list at: "https://hub.docker.com/search?q=airbyte%2Fsource-" )',
)
def create_source(connection, image):
    with optional_imports("airbyte"):
        from pathway.third_party.airbyte_serverless.connections import (
            ConnectionFromFile,
        )

    connection = ConnectionFromFile(connection)
    connection.init_yaml_config(image)
    click.echo(
        f"Connection `{connection.name}` with source `{image}` created successfully"
    )


def main() -> NoReturn:
    cli.main()
