# Copyright © 2026 Pathway

"""Tests of the wordcount harness itself."""

import contextlib
import multiprocessing
import os
import pathlib
import socket
import time

import pytest

from pathway.tests.utils import PortBlockRegistry

from .base import (
    FS_STORAGE_NAME,
    INPUT_PERSISTENCE_MODE_NAME,
    MAX_RUN_WAIT_SEC,
    STREAMING_MODE_NAME,
    generate_next_input,
    get_pw_program_run_time,
    start_pw_computation,
)


@contextlib.contextmanager
def occupied_port(port: int):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", port))
        listener.listen(1)
        yield


def test_port_registry_hands_out_blocks_nobody_else_holds(tmp_path: pathlib.Path):
    """Every handed-out block must be free and disjoint from the live ones."""
    registry = PortBlockRegistry(
        db_path=str(tmp_path / "ports.sqlite"),
        range_start=21000,
        range_end=21050,
        block_size=5,
    )
    with occupied_port(21000):
        blocks = [registry.acquire() for _ in range(9)]
        assert len(set(blocks)) == len(blocks), "a block was handed out twice"
        assert 21000 not in blocks, "a block with an occupied port was handed out"

        # The range holds ten blocks, one of which is occupied and nine are
        # reserved, so there is nothing left to hand out.
        with pytest.raises(RuntimeError, match="No free block"):
            registry.acquire()

        registry.release(blocks[0])
        assert registry.acquire() == blocks[0]


def acquire_blocks(args) -> list[int]:
    db_path, n_blocks = args
    registry = PortBlockRegistry(
        db_path=db_path, range_start=22000, range_end=23000, block_size=5
    )
    return [registry.acquire() for _ in range(n_blocks)]


def churn_blocks(args) -> int:
    """Take and give back a block repeatedly, as a worker running tests does."""
    db_path, n_rounds = args
    registry = PortBlockRegistry(
        db_path=db_path, range_start=24000, range_end=25000, block_size=5
    )
    for _ in range(n_rounds):
        registry.release(registry.acquire())
    return n_rounds


def test_port_registry_gives_no_block_to_two_processes_at_once(
    tmp_path: pathlib.Path,
):
    """Concurrent processes must never be handed the same block.

    Tests are run by a dozen xdist workers - separate processes - and several
    such runs share a machine, so the registry is only worth anything if its
    check-then-claim is atomic across processes. The database does not exist
    beforehand here: the workers of a run reach the registry simultaneously,
    so creating it is itself contended.
    """
    db_path = str(tmp_path / "ports.sqlite")

    n_processes = 8
    blocks_per_process = 20
    with multiprocessing.Pool(n_processes) as pool:
        results = pool.map(
            acquire_blocks, [(db_path, blocks_per_process)] * n_processes
        )

    handed_out = [block for result in results for block in result]
    assert len(handed_out) == n_processes * blocks_per_process
    assert len(set(handed_out)) == len(handed_out), "a block was handed out twice"


def test_port_registry_survives_many_workers_taking_turns(tmp_path: pathlib.Path):
    """Handing blocks out must not stall the workers queuing behind it.

    A whole xdist run asks this registry for ports, and every wait is bounded
    by the connection timeout: an acquire that holds the write lock while it
    probes the kernel makes the others queue until they time out and fail with
    "database is locked".
    """
    db_path = str(tmp_path / "ports.sqlite")

    with multiprocessing.Pool(16) as pool:
        results = pool.map(churn_blocks, [(db_path, 40)] * 16)

    assert results == [40] * 16


def test_run_with_a_failing_process_is_given_up_on_promptly(
    tmp_path: pathlib.Path, port: int
):
    """A run in which a process dies must be abandoned as soon as that happens.

    The processes of a multiprocess run find each other over TCP, retrying
    forever, so the ones that did start keep waiting for the dead one and
    produce no output. Waiting for the streaming timeout instead would multiply
    every such failure by 15 minutes, once per retry.
    """
    inputs_path = tmp_path / "inputs"
    os.makedirs(inputs_path)
    generate_next_input(inputs_path, input_size=1000, dictionary=["a", "b", "c"])

    time_start = time.time()
    with occupied_port(port):
        with pytest.raises(AssertionError, match="failed .* times in a row"):
            get_pw_program_run_time(
                n_threads=1,
                n_processes=2,
                input_path=inputs_path,
                output_path=tmp_path / "table.csv",
                pstorage_path=str(tmp_path / "pstorage"),
                mode=STREAMING_MODE_NAME,
                pstorage_type=FS_STORAGE_NAME,
                persistence_mode=INPUT_PERSISTENCE_MODE_NAME,
                first_port=port,
            )
    elapsed = time.time() - time_start

    assert elapsed < MAX_RUN_WAIT_SEC, (
        f"The failed run took {elapsed} seconds to be given up on, which means "
        "a timeout, rather than the failure itself, ended it"
    )


def test_killed_process_does_not_leave_snapshot_caches(tmp_path, monkeypatch, port):
    monkeypatch.setenv("TMPDIR", str(tmp_path))
    inputs_path = tmp_path / "inputs"
    inputs_path.mkdir()
    generate_next_input(inputs_path, input_size=1000, dictionary=["a", "b", "c"])
    process_group = start_pw_computation(
        n_threads=1,
        n_processes=1,
        input_path=inputs_path,
        output_path=tmp_path / "table.csv",
        pstorage_path=str(tmp_path / "pstorage"),
        mode=STREAMING_MODE_NAME,
        pstorage_type=FS_STORAGE_NAME,
        persistence_mode=INPUT_PERSISTENCE_MODE_NAME,
        first_port=port,
    )
    try:
        deadline = time.monotonic() + 30
        while not list(tmp_path.rglob("snapshot.sqlite3")):
            assert not process_group.dead_processes(), "the pipeline exited early"
            assert time.monotonic() < deadline, "the snapshot cache was not created"
            time.sleep(0.1)
    finally:
        process_group.kill()

    # SIGKILL bypasses the engine's TempDir destructor. The harness must remove
    # these disposable caches after reaping the process, before the next run.
    assert not list(tmp_path.rglob("snapshot.sqlite3"))
