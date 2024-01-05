# Copyright © 2024 Pathway

import os
import subprocess
import sys
import uuid
from typing import NoReturn

import click

import pathway as pw


def plural(n, singular, plural):
    if n == 1:
        return f"1 {singular}"
    return f"{n} {plural}"


def spawn_program(threads, processes, first_port, program, arguments, env_base):
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
    "--record_path",
    type=str,
    default="record",
    help="directory in which record will be saved",
)
@click.argument("program")
@click.argument("arguments", nargs=-1)
def spawn(threads, processes, first_port, record, record_path, program, arguments):
    env = os.environ.copy()
    if record:
        env["PATHWAY_REPLAY_STORAGE"] = record_path
        env["PATHWAY_SNAPSHOT_ACCESS"] = "record"
        env["PATHWAY_CONTINUE_AFTER_REPLAY"] = "true"
    spawn_program(threads, processes, first_port, program, arguments, env)


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
    "--record_path",
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
@click.argument("program")
@click.argument("arguments", nargs=-1)
def replay(
    threads,
    processes,
    first_port,
    record_path,
    mode,
    continue_after_replay,
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
    spawn_program(threads, processes, first_port, program, arguments, env)


def main() -> NoReturn:
    cli.main()
