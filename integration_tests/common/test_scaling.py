import os
import pathlib
import uuid

from pathway.cli import (
    ProcessHandlesState,
    create_process_handles,
    terminate_process_handles,
    wait_for_process_handles,
)


def wait_for_scaling_event(
    process_handles: list,
) -> ProcessHandlesState:
    state = ProcessHandlesState()
    try:
        for _ in range(300):
            state = wait_for_process_handles(process_handles, timeout=1.0)
            if (
                state.needs_upscaling
                or state.needs_downscaling
                or state.has_process_with_error
                or not state.has_working_process
            ):
                break
    finally:
        terminate_process_handles(process_handles)
    return state


def run_test(
    *,
    tmp_path: pathlib.Path,
    port: int,
    rate: int,
    processes: int,
    expected_upscaling: bool = False,
    expected_downscaling: bool = False,
    expected_stable: bool = False,
):
    pstorage = str(tmp_path / "PStorage")
    program_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "example_scaling.py"
    )
    env_base = os.environ.copy()

    process_handles = create_process_handles(
        processes=processes,
        threads=1,
        first_port=port,
        run_id=str(uuid.uuid4()),
        env_base=env_base,
        program="python",
        arguments=[
            program_path,
            "--rate",
            str(rate),
            "--persistent-storage-path",
            pstorage,
        ],
    )

    state = wait_for_scaling_event(process_handles)

    assert state.needs_downscaling == expected_downscaling
    assert state.needs_upscaling == expected_upscaling
    if expected_stable:
        assert not state.needs_downscaling
        assert not state.needs_upscaling
        assert not state.has_process_with_error
        assert state.has_working_process


def test_downscaling(tmp_path: pathlib.Path, port: int):
    run_test(
        tmp_path=tmp_path,
        port=port,
        rate=5,
        processes=2,
        expected_downscaling=True,
    )


def test_no_downscaling_with_fixed_address_pool(
    tmp_path: pathlib.Path, two_free_ports: list[int]
):
    # Same conditions as test_downscaling (low rate, 2 processes), but the worker
    # pool is given as explicit addresses. Downscaling must not happen because the
    # address pool is fixed for the entire run.
    port0, port1 = two_free_ports
    pstorage = str(tmp_path / "PStorage")
    program_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "example_scaling.py"
    )
    addresses = f"127.0.0.1:{port0},127.0.0.1:{port1}"
    env_base = os.environ.copy()
    arguments = [program_path, "--rate", "5", "--persistent-storage-path", pstorage]

    def make_handles(process_id: int) -> list:
        return create_process_handles(
            processes=2,
            threads=1,
            first_port=port0,
            addresses=addresses,
            process_id=process_id,
            run_id=str(uuid.uuid4()),
            env_base=env_base,
            program="python",
            arguments=arguments,
        )

    process_handles = make_handles(0) + make_handles(1)

    state = wait_for_scaling_event(process_handles)

    assert not state.needs_downscaling
    assert not state.needs_upscaling
    assert not state.has_process_with_error
    assert state.has_working_process


def test_upscaling(tmp_path: pathlib.Path, port: int):
    run_test(
        tmp_path=tmp_path,
        port=port,
        rate=400,
        processes=1,
        expected_upscaling=True,
    )


def test_already_one_worker_no_downscaling(tmp_path: pathlib.Path, port: int):
    run_test(
        tmp_path=tmp_path,
        port=port,
        rate=5,
        processes=1,
        expected_stable=True,
    )
