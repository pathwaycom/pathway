# Copyright © 2023 Pathway

import os
import subprocess
import sys
from typing import NoReturn

import click

import pathway as pw


def plural(n, singular, plural):
    if n == 1:
        return f"1 {singular}"
    return f"{n} {plural}"


@click.version_option(version=pw.__version__, prog_name="pathway")
@click.group()
def cli():
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
@click.option(
    "--persistent-storage-path",
    type=str,
    default=None,
    help="directory path for persistent storage",
)
@click.argument("program")
@click.argument("arguments", nargs=-1)
def spawn(threads, processes, first_port, program, arguments, persistent_storage_path):
    processes_str = plural(processes, "process", "processes")
    workers_str = plural(processes * threads, "total worker", "total workers")
    click.echo(f"Preparing {processes_str} ({workers_str})", err=True)
    process_handles = []
    try:
        for process_id in range(processes):
            env = os.environ.copy()
            env["PATHWAY_THREADS"] = str(threads)
            env["PATHWAY_PROCESSES"] = str(processes)
            env["PATHWAY_FIRST_PORT"] = str(first_port)
            env["PATHWAY_PROCESS_ID"] = str(process_id)
            if persistent_storage_path:
                env["PATHWAY_PERSISTENT_STORAGE"] = persistent_storage_path
            handle = subprocess.Popen([program] + list(arguments), env=env)
            process_handles.append(handle)
        for handle in process_handles:
            handle.wait()
    finally:
        for handle in process_handles:
            handle.terminate()
    sys.exit(max(handle.returncode for handle in process_handles))


def main() -> NoReturn:
    cli.main()
