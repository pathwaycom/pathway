import json
import os
import time
import uuid

import pytest

from pathway.cli import create_process_handles, terminate_process_handles

IDENTITY_PROGRAM = os.path.join(os.path.dirname(__file__), "identity.py")


def test_two_machine_identity(tmp_path, two_free_ports):
    port1, port2 = two_free_ports
    addresses = f"127.0.0.1:{port1},127.0.0.1:{port2}"
    input_path = tmp_path / "input.txt"
    output_path = tmp_path / "output.jsonl"
    input_path.write_text("hello world\n")

    env_base = os.environ.copy()
    common_args = dict(
        processes=2,
        threads=1,
        first_port=port1,
        addresses=addresses,
        env_base=env_base,
        program="python",
        arguments=[IDENTITY_PROGRAM, str(input_path), str(output_path)],
    )

    process0 = create_process_handles(
        **common_args, process_id=0, run_id=str(uuid.uuid4())
    )[0]
    try:
        time.sleep(15)
        assert process0.poll() is None, "Process 0 exited before process 1 was launched"
        assert not output_path.exists(), "Output appeared before process 1 was launched"

        time.sleep(15)
        assert process0.poll() is None, "Process 0 exited before process 1 was launched"
        assert not output_path.exists(), "Output appeared before process 1 was launched"

        process1 = create_process_handles(
            **common_args, process_id=1, run_id=str(uuid.uuid4())
        )[0]
        try:
            deadline = time.time() + 60
            while time.time() < deadline:
                if process0.poll() is not None and process1.poll() is not None:
                    break
                time.sleep(0.5)
            else:
                pytest.fail("Processes did not complete within 60 seconds")

            assert (
                process0.returncode == 0
            ), f"Process 0 exited with code {process0.returncode}"
            assert (
                process1.returncode == 0
            ), f"Process 1 exited with code {process1.returncode}"
        finally:
            terminate_process_handles([process1])
    finally:
        terminate_process_handles([process0])

    assert output_path.exists(), "Output file was not created"
    rows = [
        json.loads(line)
        for line in output_path.read_text().splitlines()
        if line.strip()
    ]
    assert any(row.get("data") == "hello world" for row in rows)
