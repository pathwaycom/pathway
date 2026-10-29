#!/usr/bin/env python

# Copyright © 2026 Pathway

import json
import os
import pathlib
import random
import shutil
import socket
import subprocess
import threading
import time
import uuid
import warnings

import boto3
from azure.storage.blob import BlobServiceClient

DEFAULT_INPUT_SIZE = 5000000
COMMIT_LINE = "*COMMIT*\n"
# How long a run may go without any sign of life before it's declared stuck.
# The engine logs at least every `snapshot_interval_ms`, so a live process
# always keeps its log growing - including one that is busy re-reading a
# snapshot and therefore writes no output at all for a while.
NO_PROGRESS_TIMEOUT_SEC = 300
# The bound for a run that does keep making progress but never gets anywhere.
MAX_RUN_WAIT_SEC = 2400
POLL_INTERVAL_SEC = 10
PROCESS_HEALTH_CHECK_INTERVAL_SEC = 0.5
MAX_PW_PROGRAM_RETRIES = 3
PORT_RELEASE_WAIT_SEC = 30
LOG_TAIL_LINES = 60
# Kept from the whole log rather than only its tail: a process usually dies a
# while after the line that explains why.
LOG_KEYWORDS = ("panicked", "ERROR", "retrying")
MAX_KEYWORD_LOG_LINES = 40
MAX_LISTED_OBJECTS = 40
STATIC_MODE_NAME = "static"
STREAMING_MODE_NAME = "streaming"
AZURE_STORAGE_NAME = "azure"
FS_STORAGE_NAME = "fs"
S3_STORAGE_NAME = "s3"
INPUT_PERSISTENCE_MODE_NAME = "PERSISTING"
OPERATOR_PERSISTENCE_MODE_NAME = "OPERATOR_PERSISTING"


class PStoragePath:
    def __init__(self, pstorage_type, local_tmp_path: pathlib.Path):
        self._pstorage_type = pstorage_type
        self._pstorage_path = self._get_pstorage_path(pstorage_type, local_tmp_path)

    def __enter__(self):
        return self._pstorage_path

    def __exit__(self, exc_type, exc_value, traceback):
        if self._pstorage_type == "s3":
            self._clean_s3_prefix(self._pstorage_path)
        elif self._pstorage_type == "fs":
            shutil.rmtree(self._pstorage_path)
        elif self._pstorage_type == "azure":
            self._clean_azure_prefix(self._pstorage_path)
        else:
            raise NotImplementedError(
                f"method not implemented for storage {self._pstorage_type}"
            )

    @staticmethod
    def _get_pstorage_path(pstorage_type, local_tmp_path: pathlib.Path):
        if pstorage_type == "fs":
            return str(local_tmp_path / "pstorage")
        elif pstorage_type in ("azure", "s3"):
            return f"wordcount-integration-tests/pstorages/{time.time()}-{str(uuid.uuid4())}"
        else:
            raise NotImplementedError(
                f"method not implemented for storage {pstorage_type}"
            )

    @staticmethod
    def _clean_azure_prefix(prefix):
        account_name = os.environ["AZURE_BLOB_STORAGE_ACCOUNT"]
        account_key = os.environ["AZURE_BLOB_STORAGE_PASSWORD"]
        container_name = os.environ["AZURE_BLOB_STORAGE_CONTAINER"]
        blob_service_client = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=account_key,
        )
        container_client = blob_service_client.get_container_client(container_name)
        blobs_to_delete = container_client.list_blobs(name_starts_with=prefix)
        for blob in blobs_to_delete:
            container_client.delete_blob(blob.name)

    @staticmethod
    def _clean_s3_prefix(prefix):
        s3 = boto3.client(
            "s3",
            aws_access_key_id=os.environ["AWS_S3_ACCESS_KEY"],
            aws_secret_access_key=os.environ["AWS_S3_SECRET_ACCESS_KEY"],
        )

        while True:
            objects_to_delete = s3.list_objects_v2(
                Bucket="aws-integrationtest", Prefix=prefix
            )
            contents = objects_to_delete.get("Contents")
            if not contents:
                break
            objects = [{"Key": obj["Key"]} for obj in contents]
            response = s3.delete_objects(
                Bucket="aws-integrationtest", Delete={"Objects": objects}
            )
            # Objects that stay undeleted would be listed again on the next
            # iteration, making this loop spin forever.
            errors = response.get("Errors")
            assert not errors, f"Failed to remove objects from S3: {errors}"


def try_read_output_word_counts(output_path) -> dict[str, int] | None:
    """Read the latest per-word counts from the output CSV.

    Returns None if the file can't be interpreted yet, e.g. the header hasn't
    been written. Rows that can't be parsed (e.g. the trailing line of a write
    that is still in progress) are skipped: they will be re-read complete on
    the next poll.
    """
    word_counts: dict[str, int] = {}
    try:
        with open(output_path) as f:
            header = f.readline()
            if not header.endswith("\n"):
                return None
            column_names = header.strip().replace('"', "").split(",")
            try:
                word_column_index = column_names.index("word")
                count_column_index = column_names.index("count")
            except ValueError:
                return None
            for row in f:
                tokens = row.strip().replace('"', "").split(",")
                try:
                    word = tokens[word_column_index].strip('"')
                    count = int(tokens[count_column_index])
                except (IndexError, ValueError):
                    continue
                word_counts[word] = count
    except FileNotFoundError:
        return None
    return word_counts


def check_output_correctness(
    latest_input_file, input_path, output_path, interrupted_run=False
):
    input_word_counts = {}
    old_input_word_counts = {}
    new_file_lines = set()
    distinct_new_words = set()

    input_file_list = os.listdir(input_path)
    for processing_old_files in (True, False):
        for file in input_file_list:
            path = os.path.join(input_path, file)
            if not os.path.isfile(path):
                continue

            on_old_file = path != latest_input_file
            if on_old_file != processing_old_files:
                continue
            with open(path) as f:
                for row in f:
                    if not row.strip() or row.strip() == "*COMMIT*":
                        continue
                    json_payload = json.loads(row.strip())
                    word = json_payload["word"]
                    if word not in input_word_counts:
                        input_word_counts[word] = 0
                    input_word_counts[word] += 1

                    if on_old_file:
                        if word not in old_input_word_counts:
                            old_input_word_counts[word] = 0
                        old_input_word_counts[word] += 1
                    else:
                        new_file_lines.add((word, input_word_counts[word]))
                        distinct_new_words.add(word)

    print("  New file lines:", len(new_file_lines))

    n_rows = 0
    n_old_lines = 0
    output_word_counts = {}
    try:
        with open(output_path) as f:
            is_first_row = True
            word_column_index = None
            count_column_index = None
            diff_column_index = None
            for row in f:
                n_rows += 1
                if is_first_row:
                    column_names = row.strip().replace('"', "").split(",")
                    for col_idx, col_name in enumerate(column_names):
                        if col_name == "word":
                            word_column_index = col_idx
                        elif col_name == "count":
                            count_column_index = col_idx
                        elif col_name == "diff":
                            diff_column_index = col_idx
                    is_first_row = False
                    assert (
                        word_column_index is not None
                    ), "'word' is absent in CSV header"
                    assert (
                        count_column_index is not None
                    ), "'count' is absent in CSV header"
                    assert (
                        diff_column_index is not None
                    ), "'diff' is absent in CSV header"
                    continue

                assert word_column_index is not None
                assert count_column_index is not None
                assert diff_column_index is not None
                tokens = row.strip().replace('"', "").split(",")
                try:
                    word = tokens[word_column_index].strip('"')
                    count = int(tokens[count_column_index])
                    diff = int(tokens[diff_column_index])
                    output_word_counts[word] = int(count)
                except (IndexError, ValueError):
                    # line split in two chunks, one fsynced, another did not
                    print(f"Broken row: {row}")
                    print(f"Tokens: {tokens}")
                    print(
                        f"Indices (word, count, diff): {word_column_index}, {count_column_index}, {diff_column_index}"
                    )
                    if not interrupted_run:
                        raise

                if diff == 1:
                    if (word, count) not in new_file_lines:
                        n_old_lines += 1
                elif diff == -1:
                    new_line_update = (word, count) in new_file_lines
                    old_line_update = old_input_word_counts.get(word) == count
                    if not (new_line_update or old_line_update):
                        n_old_lines += 1
                else:
                    raise ValueError("Incorrect diff value: {diff}")
    except FileNotFoundError:
        if interrupted_run:
            return False
        raise

    assert len(input_word_counts) >= len(output_word_counts), (
        "There are some new words on the output. "
        + f"Input dict: {len(input_word_counts)} Output dict: {len(output_word_counts)}"
    )

    for word, output_count in output_word_counts.items():
        if interrupted_run:
            assert input_word_counts[word] >= output_count
        else:
            assert (
                input_word_counts[word] == output_count
            ), f"Word: {word} Output count: {output_count} Input count: {input_word_counts.get(word)}"

    if not interrupted_run:
        assert latest_input_file is None or n_old_lines < DEFAULT_INPUT_SIZE / 10, (
            f"Output contains too many old lines: {n_old_lines} while 1/10 of the input size "
            + f"is {DEFAULT_INPUT_SIZE / 10}"
        )
        assert n_rows >= len(
            distinct_new_words
        ), f"Output contains only {n_rows} lines, while there should be at least {len(distinct_new_words)}"

    print("  Total rows on the output:", n_rows)
    print("  Total old lines:", n_old_lines)

    return input_word_counts == output_word_counts


def get_pinned_cpu_list(n_cpus: int) -> str:
    """CPU list for taskset, offset by the pytest-xdist worker index.

    Every test pins its pathway processes to exactly `n_cpus` cores so that
    the multi-worker configurations stay meaningful. The offset spreads
    concurrently running tests over the machine: without it every parallel
    xdist worker pins its pathway processes to the same cores 0..n_cpus-1,
    where they serialize (a single core ends up time-slicing all the "1-1"
    runs of the suite) while the rest of the machine stays idle. That both
    inflates the suite duration and stretches every timing window the
    harness relies on (output stability, static-mode wait).
    """
    worker = os.environ.get("PYTEST_XDIST_WORKER", "gw0")
    try:
        worker_id = int(worker.removeprefix("gw"))
    except ValueError:
        worker_id = 0
    total_cpus = os.cpu_count() or n_cpus
    start = worker_id * n_cpus
    return ",".join(str((start + i) % total_cpus) for i in range(n_cpus))


class PwProcessGroup:
    """The pathway processes of a single run, along with their logs.

    A multiprocess run is a rendezvous: every process connects to its peers over
    TCP and retries forever. So when one process dies - it fails to bind its
    port, the engine returns an error, the kernel OOM-kills it - the survivors
    never make progress and never exit. A harness that only watches the output
    file can't tell that apart from a slow run, so every wait below also watches
    the processes themselves and stops the run as soon as one of them is gone.
    """

    def __init__(self, handles, log_paths, pstorage_type, pstorage_path, temp_path):
        self._handles = handles
        self._log_paths = log_paths
        self._pstorage_type = pstorage_type
        self._pstorage_path = pstorage_path
        self._temp_path = temp_path

    def dead_processes(self) -> list[tuple[int, int]]:
        """`(process_id, exit_code)` of the processes that have already finished."""
        result = []
        for process_id, handle in enumerate(self._handles):
            exit_code = handle.poll()
            if exit_code is not None:
                result.append((process_id, exit_code))
        return result

    def kill(self) -> int:
        """Kill whatever is still running and return the worst exit code.

        Being killed is how a streaming run is expected to end, so it doesn't
        count as a failure. The processes are also reaped: an un-reaped process
        may still hold the rendezvous port that the next run binds.
        """
        worst_exit_code = 0
        for handle in self._handles:
            exit_code = handle.poll()
            if exit_code is None:
                handle.kill()
                exit_code = 0
            worst_exit_code = _worse_exit_code(worst_exit_code, exit_code)
        self._reap()
        return worst_exit_code

    def seconds_since_progress(self) -> float:
        """For how long the processes have been writing nothing to their logs."""
        last_progress = 0.0
        for log_path in self._log_paths:
            try:
                last_progress = max(last_progress, os.path.getmtime(log_path))
            except FileNotFoundError:
                continue
        return time.time() - last_progress

    def wait(self, timeout: float) -> tuple[int, bool]:
        """Wait for all the processes to finish.

        Returns the worst exit code and whether the wait was given up on. A
        run that is given up on isn't worth retrying: it was making no
        progress, and a restart would only repeat that from the beginning.
        """
        deadline = time.time() + timeout
        while True:
            exit_codes = [handle.poll() for handle in self._handles]
            if all(exit_code is not None for exit_code in exit_codes):
                self._reap()
                return _worst_exit_code(exit_codes), False
            failed = [(i, code) for i, code in enumerate(exit_codes) if code]
            if failed:
                # The processes that are still running wait for the failed one
                # in the rendezvous and would never finish on their own.
                self.dump_logs(f"processes exited with an error: {failed}")
                self.kill()
                return _worst_exit_code(exit_codes), False
            no_progress_for = self.seconds_since_progress()
            if no_progress_for > NO_PROGRESS_TIMEOUT_SEC:
                self.dump_logs(f"no progress in {no_progress_for:.0f} seconds")
                break
            if time.time() > deadline:
                self.dump_logs(f"the program hasn't finished in {timeout} seconds")
                break
            time.sleep(1)

        self.kill()
        return 0, True

    def dump_logs(self, header: str) -> None:
        print(f"Pathway program failure: {header}")
        print(f"  Disk: {describe_disk_space(self._log_paths[0])}")
        oom_kills = _cgroup_oom_kill_count()
        if oom_kills:
            print(f"  The cgroup has OOM-killed {oom_kills} processes so far")
        print(f"  Pathway programs running: {describe_running_pw_programs()}")
        print(f"  Persistent storage: {self._describe_pstorage()}")
        for process_id, log_path in enumerate(self._log_paths):
            try:
                with open(log_path) as f:
                    lines = f.readlines()
            except FileNotFoundError:
                lines = ["<no log file>"]
            print(f"  Persistence lines of the log of process {process_id}:")
            keyword_lines = [
                line for line in lines if any(word in line for word in LOG_KEYWORDS)
            ]
            for line in keyword_lines[-MAX_KEYWORD_LOG_LINES:]:
                print(f"    {line.rstrip()}")
            print(f"  Last lines of the log of process {process_id}:")
            for line in lines[-LOG_TAIL_LINES:]:
                print(f"    {line.rstrip()}")

    def _describe_pstorage(self) -> str:
        """What the persistent storage of this run holds, with object sizes.

        A chunk that can't be deserialized is only explainable together with
        what is actually stored: its size here versus the length its name
        claims tells a truncated object from an unrelated one.
        """
        try:
            if self._pstorage_type == S3_STORAGE_NAME:
                return describe_s3_prefix(self._pstorage_path)
            if self._pstorage_type == FS_STORAGE_NAME:
                return describe_local_directory(self._pstorage_path)
        except Exception as e:  # diagnostics must never mask the failure
            return f"could not be listed: {e}"
        return f"not listed for {self._pstorage_type}"

    def _reap(self) -> None:
        for handle in self._handles:
            try:
                handle.wait(timeout=60)
            except subprocess.TimeoutExpired:
                warnings.warn(f"Process {handle.pid} didn't react to SIGKILL")
        # SIGKILL bypasses Rust's TempDir destructors. Discard each run's local
        # snapshot caches after all processes exit; persistence lives elsewhere.
        if all(handle.poll() is not None for handle in self._handles):
            if self._temp_path.exists():
                shutil.rmtree(self._temp_path)


def _cgroup_oom_kill_count() -> int | None:
    """How many processes the container's cgroup has OOM-killed.

    A process killed this way exits with SIGKILL and an empty log, which is
    otherwise indistinguishable from any other abrupt death.
    """
    try:
        with open("/sys/fs/cgroup/memory.events") as f:
            for line in f:
                key, _, value = line.partition(" ")
                if key == "oom_kill":
                    return int(value)
    except OSError:
        pass
    return None


def _worse_exit_code(lhs: int, rhs: int) -> int:
    """Of two exit codes, the one describing a failure. Signals are negative."""
    if lhs == 0:
        return rhs
    if rhs == 0:
        return lhs
    return max(abs(lhs), abs(rhs))


def _worst_exit_code(exit_codes: list[int | None]) -> int:
    result = 0
    for exit_code in exit_codes:
        result = _worse_exit_code(result, exit_code or 0)
    return result


def describe_s3_prefix(prefix: str) -> str:
    """Every object under the prefix, with its size and last modification."""
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.environ["AWS_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["AWS_S3_SECRET_ACCESS_KEY"],
    )
    descriptions = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket="aws-integrationtest", Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"].removeprefix(prefix).lstrip("/")
            descriptions.append(f"{key} ({obj['Size']}B)")
    return _describe_objects(descriptions)


def describe_local_directory(path: str) -> str:
    descriptions = []
    for root, _dirs, files in os.walk(path):
        for file in files:
            full_path = os.path.join(root, file)
            relative_path = os.path.relpath(full_path, path)
            descriptions.append(f"{relative_path} ({os.path.getsize(full_path)}B)")
    return _describe_objects(descriptions)


def _describe_objects(descriptions: list[str]) -> str:
    listed = sorted(descriptions)[:MAX_LISTED_OBJECTS]
    suffix = "" if len(listed) == len(descriptions) else ", ..."
    return f"{len(descriptions)} objects: " + ", ".join(listed) + suffix


def describe_disk_space(path: str) -> str:
    """Free space and inodes of the filesystem the run writes to.

    A full partition breaks a run in ways that surface far from the cause -
    a half-written file, a snapshot chunk that can't be deserialized - so the
    state of the disk is reported next to every failure.
    """
    directory = os.path.dirname(path) or "."
    try:
        usage = shutil.disk_usage(directory)
        stat = os.statvfs(directory)
    except OSError as e:
        return f"unavailable: {e}"
    return (
        f"{usage.free // 2**20} MiB free of {usage.total // 2**20} MiB, "
        f"{stat.f_favail} inodes free"
    )


def describe_running_pw_programs() -> str:
    """The pathway processes running on the machine, with the ports they use.

    Printed when a port can't be bound: the ports of a run belong to its xdist
    worker alone, so an occupied one is almost always held by a program of this
    suite that outlived the run that started it.
    """
    descriptions = []
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/cmdline") as f:
                cmdline = f.read()
            if "pw_wordcount.py" not in cmdline:
                continue
            with open(f"/proc/{entry}/environ") as f:
                environ = dict(
                    var.split("=", 1) for var in f.read().split("\0") if "=" in var
                )
        except OSError:
            continue
        descriptions.append(
            f"pid={entry} "
            f"first_port={environ.get('PATHWAY_FIRST_PORT')} "
            f"process_id={environ.get('PATHWAY_PROCESS_ID')} "
            f"run_id={environ.get('PATHWAY_RUN_ID')}"
        )
    return "; ".join(descriptions) or "none"


def wait_for_free_ports(first_port: int, n_processes: int) -> None:
    """Wait until the rendezvous ports of the run can be bound.

    The previous run of the same test used the same ports and was SIGKILLed a
    moment ago; if the kernel hasn't released them yet, the process that binds
    them fails and the rest of the group hangs waiting for it.
    """
    if n_processes == 1:
        return  # a single-process run doesn't do the rendezvous at all
    deadline = time.time() + PORT_RELEASE_WAIT_SEC
    for port in range(first_port, first_port + n_processes):
        while True:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
                # matches what Rust's TcpListener::bind does, so that a socket
                # left in TIME_WAIT isn't reported as occupied
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                try:
                    probe.bind(("127.0.0.1", port))
                    break
                except OSError as e:
                    if time.time() > deadline:
                        warnings.warn(
                            f"Port {port} is still occupied: {e}. "
                            f"Pathway programs running: "
                            f"{describe_running_pw_programs()}"
                        )
                        break
            time.sleep(0.5)


def start_pw_computation(
    *,
    n_threads,
    n_processes,
    input_path,
    output_path,
    pstorage_path,
    mode,
    pstorage_type,
    persistence_mode,
    first_port,
) -> PwProcessGroup:
    pw_wordcount_path = (
        "/".join(os.path.abspath(__file__).split("/")[:-1])
        + f"/pw_wordcount.py --input {input_path} --output {output_path} --pstorage {pstorage_path} "
        + f"--mode {mode} --pstorage-type {pstorage_type} --persistence_mode {persistence_mode}"
    )
    n_cpus = n_threads * n_processes
    cpu_list = get_pinned_cpu_list(n_cpus)
    command = f"taskset --cpu-list {cpu_list} python {pw_wordcount_path}"
    run_args = command.split()

    run_id = uuid.uuid4()
    # Computed once in the parent and shared with every process, mirroring
    # `pathway spawn`, so all processes of the run agree on the start-up-batch
    # timestamp. Without this each process would pick its own wall-clock value
    # and the parallel readers would disagree on the boundary of the initial
    # snapshot (see the connector's `timestamp_at_start` handling).
    start_timestamp_ms = str(int(time.time() * 1000))
    wait_for_free_ports(first_port, n_processes)

    log_dir = os.path.dirname(os.path.abspath(output_path))
    temp_path = pathlib.Path(log_dir) / f"pw-tmp-{run_id}"
    temp_path.mkdir()
    process_handles = []
    log_paths = []
    for process_id in range(n_processes):
        env = os.environ.copy()
        env["TMPDIR"] = str(temp_path)
        env["PATHWAY_THREADS"] = str(n_threads)
        env["PATHWAY_PROCESSES"] = str(n_processes)
        env["PATHWAY_FIRST_PORT"] = str(first_port)
        env["PATHWAY_PROCESS_ID"] = str(process_id)
        env["PATHWAY_RUN_ID"] = str(run_id)
        env["PATHWAY_START_TIMESTAMP_MS"] = start_timestamp_ms
        # A panic without a backtrace only says which line gave up, not which
        # path led there.
        env["RUST_BACKTRACE"] = "1"
        # Kept per run so that a failure can be explained afterwards: without it
        # the reason a process died is lost among the output of the other runs.
        log_path = os.path.join(log_dir, f"pw-log-{run_id}-{process_id}.txt")
        with open(log_path, "wb") as log_file:
            handle = subprocess.Popen(
                run_args, env=env, stdout=log_file, stderr=subprocess.STDOUT
            )
        process_handles.append(handle)
        log_paths.append(log_path)

    return PwProcessGroup(
        process_handles, log_paths, pstorage_type, pstorage_path, temp_path
    )


def get_pw_program_run_time(
    *,
    n_threads,
    n_processes,
    input_path,
    output_path,
    pstorage_path,
    mode,
    pstorage_type,
    persistence_mode,
    first_port,
    expected_word_counts=None,
):
    needs_pw_program_launch = True
    n_retries = 0
    while needs_pw_program_launch:
        needs_pw_program_launch = False
        time_start = time.time()
        process_group = start_pw_computation(
            n_threads=n_threads,
            n_processes=n_processes,
            input_path=input_path,
            output_path=output_path,
            pstorage_path=pstorage_path,
            mode=mode,
            pstorage_type=pstorage_type,
            persistence_mode=persistence_mode,
            first_port=first_port,
        )
        try:
            needs_polling = mode == STREAMING_MODE_NAME
            while needs_polling:
                print("Waiting for 10 seconds...")
                time.sleep(POLL_INTERVAL_SEC)

                # A streaming program only stops when it's killed, so a process
                # that has exited on its own has failed. Its peers then wait for
                # it forever, hence the run is over: waiting for the deadline
                # below would only delay the retry.
                dead_processes = process_group.dead_processes()
                if dead_processes:
                    process_group.dump_logs(
                        f"processes exited before the run was over: {dead_processes}"
                    )
                    break

                no_progress_for = process_group.seconds_since_progress()
                is_stuck = no_progress_for > NO_PROGRESS_TIMEOUT_SEC
                timed_out = is_stuck or time.time() - time_start > MAX_RUN_WAIT_SEC
                if is_stuck:
                    process_group.dump_logs(
                        f"no progress in {no_progress_for:.0f} seconds"
                    )

                try:
                    modified_at = os.path.getmtime(output_path)
                    file_size = os.path.getsize(output_path)
                except FileNotFoundError:
                    if time.time() - time_start > 180:
                        raise
                    modified_at = None
                    file_size = 0

                if file_size == 0:
                    # No output yet; the deadlines above are the only thing that
                    # bounds this loop.
                    if timed_out:
                        break
                    continue

                assert modified_at is not None
                is_output_stable = (
                    modified_at > time_start and time.time() - modified_at > 60
                )

                # The output being stable for a while is not enough to conclude
                # that the computation has finished: the engine may legitimately
                # stay silent for longer than the stability window, e.g. while it
                # re-ingests a whole input file after a recovery — a single
                # atomic block that produces no output until it's complete. When
                # the expected counts are known, also require the output to
                # converge to them before stopping the program. That check reads
                # the whole output back, so it waits for the output to be stable.
                has_finished = is_output_stable and (
                    expected_word_counts is None
                    or try_read_output_word_counts(output_path) == expected_word_counts
                )
                if timed_out and not has_finished:
                    warnings.warn(
                        "The output hasn't converged to the expected word counts "
                        f"in {time.time() - time_start:.0f} seconds; "
                        "stopping the program anyway"
                    )
                if has_finished or timed_out:
                    needs_polling = False
        finally:
            if mode == STATIC_MODE_NAME:
                pw_exit_code, gave_up = process_group.wait(timeout=MAX_RUN_WAIT_SEC)
            else:
                pw_exit_code, gave_up = process_group.kill(), False

            if gave_up:
                warnings.warn("Gave up on the pw program: it was making no progress")
            elif pw_exit_code != 0:
                # Restarting only makes sense for a process that died: on S3 or
                # Azure that's usually a connection that can be re-established.
                warnings.warn(
                    f"Warning: pw program terminated with non zero exit code: {pw_exit_code}"
                )
                assert (
                    n_retries < MAX_PW_PROGRAM_RETRIES
                ), f"The pathway program failed {n_retries + 1} times in a row"
                needs_pw_program_launch = True
                n_retries += 1

    return time.time() - time_start


def run_pw_program_suddenly_terminate(
    *,
    n_threads,
    n_processes,
    input_path,
    output_path,
    pstorage_path,
    min_work_time,
    max_work_time,
    pstorage_type,
    persistence_mode,
    first_port,
):
    for n_retry in range(MAX_PW_PROGRAM_RETRIES + 1):
        process_group = start_pw_computation(
            n_threads=n_threads,
            n_processes=n_processes,
            input_path=input_path,
            output_path=output_path,
            pstorage_path=pstorage_path,
            mode=STREAMING_MODE_NAME,
            pstorage_type=pstorage_type,
            persistence_mode=persistence_mode,
            first_port=first_port,
        )
        try:
            # The program is meant to be killed while it works, so a process
            # that exits on its own has failed: its peers are then stuck in the
            # rendezvous and this run tests nothing. Restart it instead.
            deadline = time.time() + random.uniform(min_work_time, max_work_time)
            dead_processes = []
            while time.time() < deadline:
                time.sleep(
                    max(
                        0.0,
                        min(PROCESS_HEALTH_CHECK_INTERVAL_SEC, deadline - time.time()),
                    )
                )
                dead_processes = process_group.dead_processes()
                if dead_processes:
                    process_group.dump_logs(
                        f"processes exited before being killed: {dead_processes}"
                    )
                    break
        finally:
            process_group.kill()
        if not dead_processes:
            return
        assert (
            n_retry < MAX_PW_PROGRAM_RETRIES
        ), f"The pathway program failed {n_retry + 1} times in a row"


def reset_runtime(inputs_path, output_path, pstorage_path, pstorage_type):
    if pstorage_type == "fs":
        try:
            shutil.rmtree(pstorage_path)
        except FileNotFoundError:
            print("There is no persistent storage to remove")
        except Exception:
            print("Failed to clean persistent storage")
            raise

    try:
        shutil.rmtree(inputs_path)
        os.remove(output_path)
    except FileNotFoundError:
        print("There is no inputs directory to remove")
    except Exception:
        print("Failed to clean inputs directory")
        raise

    os.makedirs(inputs_path)
    print("State successfully re-set")


def generate_word() -> str:
    word_chars = []
    for _ in range(10):
        word_chars.append(random.choice("abcdefghijklmnopqrstuvwxyz"))
    return "".join(word_chars)


def generate_dictionary(dict_size: int) -> list[str]:
    result_as_set = set()
    for _ in range(dict_size):
        word = generate_word()
        while word in result_as_set:
            word = generate_word()
        result_as_set.add(word)
    return list(result_as_set)


DICTIONARY: list[str] = generate_dictionary(10000)


def generate_input(
    file_name: pathlib.Path | str,
    input_size: int,
    commit_frequency: int,
    dictionary: list[str],
) -> dict[str, int]:
    word_counts: dict[str, int] = {}
    with open(file_name, "w") as fw:
        for seq_line_id in range(input_size):
            word = random.choice(dictionary)
            word_counts[word] = word_counts.get(word, 0) + 1
            dataset_line_dict = {"word": word}
            dataset_line = json.dumps(dataset_line_dict)
            fw.write(dataset_line + "\n")
            if (seq_line_id + 1) % commit_frequency == 0:
                # fw.write(COMMIT_LINE)
                pass
    return word_counts


def generate_next_input(
    inputs_path: pathlib.Path,
    *,
    input_size: int | None = None,
    dictionary: list[str] | None = None,
    commit_frequency: int | None = None,
) -> tuple[str, dict[str, int]]:
    file_name = os.path.join(inputs_path, str(time.time()))

    word_counts = generate_input(
        file_name=file_name,
        input_size=input_size or DEFAULT_INPUT_SIZE,
        commit_frequency=commit_frequency or 100000,
        dictionary=dictionary or DICTIONARY,
    )

    return file_name, word_counts


def do_test_persistent_wordcount(
    *,
    n_backfilling_runs,
    n_threads,
    n_processes,
    tmp_path,
    mode,
    pstorage_type,
    persistence_mode,
    first_port,
):
    inputs_path = tmp_path / "inputs"
    output_path = tmp_path / "table.csv"

    with PStoragePath(pstorage_type, tmp_path) as pstorage_path:
        reset_runtime(inputs_path, output_path, pstorage_path, pstorage_type)
        expected_word_counts: dict[str, int] = {}
        for n_run in range(n_backfilling_runs):
            print(f"Run {n_run}: generating input")
            latest_input_name, input_word_counts = generate_next_input(inputs_path)
            for word, count in input_word_counts.items():
                expected_word_counts[word] = expected_word_counts.get(word, 0) + count

            print(f"Run {n_run}: running pathway program")
            elapsed = get_pw_program_run_time(
                n_threads=n_threads,
                n_processes=n_processes,
                input_path=inputs_path,
                output_path=output_path,
                pstorage_path=pstorage_path,
                mode=mode,
                pstorage_type=pstorage_type,
                persistence_mode=persistence_mode,
                first_port=first_port,
                expected_word_counts=expected_word_counts,
            )
            print(f"Run {n_run}: pathway time elapsed {elapsed}")

            print(f"Run {n_run}: checking output correctness")
            check_output_correctness(latest_input_name, inputs_path, output_path)
            print(f"Run {n_run}: finished")


class InputGenerator:
    def __init__(
        self,
        inputs_path: pathlib.Path,
        input_size: int,
        max_files: int,
        waiting_time: float,
        dictionary_size: int,
        commit_frequency: int,
    ) -> None:
        self.inputs_path = inputs_path
        self.input_size = input_size
        self.max_files = max_files
        self.waiting_time = waiting_time
        self.should_stop = False
        self.dictionary = generate_dictionary(dictionary_size)
        self.commit_frequency = commit_frequency

    def start(self) -> None:
        def run() -> None:
            for _ in range(self.max_files):
                print(f"generating input of size {self.input_size}")
                generate_next_input(
                    self.inputs_path,
                    input_size=self.input_size,
                    dictionary=self.dictionary,
                    commit_frequency=self.commit_frequency,
                )
                time.sleep(self.waiting_time)
                if self.should_stop:
                    break

        self.thread = threading.Thread(target=run, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.should_stop = True
        self.thread.join()


def do_test_failure_recovery(
    *,
    n_backfilling_runs,
    n_threads,
    n_processes,
    tmp_path,
    min_work_time,
    max_work_time,
    pstorage_type,
    persistence_mode,
    first_port,
):
    inputs_path = tmp_path / "inputs"
    output_path = tmp_path / "table.csv"

    with PStoragePath(pstorage_type, tmp_path) as pstorage_path:
        reset_runtime(inputs_path, output_path, pstorage_path, pstorage_type)

        input_generator = InputGenerator(
            inputs_path,
            input_size=100_000,
            max_files=n_backfilling_runs * 5,
            waiting_time=min_work_time / 5,
            dictionary_size=100_000,
            commit_frequency=1_000_000,  # don't do manual commits
        )
        input_generator.start()
        for n_run in range(n_backfilling_runs):
            print(f"Run {n_run}: running pathway program")
            run_pw_program_suddenly_terminate(
                n_threads=n_threads,
                n_processes=n_processes,
                input_path=inputs_path,
                output_path=output_path,
                pstorage_path=pstorage_path,
                min_work_time=min_work_time,
                max_work_time=max_work_time,
                pstorage_type=pstorage_type,
                persistence_mode=persistence_mode,
                first_port=first_port,
            )

            check_output_correctness(
                None, inputs_path, output_path, interrupted_run=True
            )

        input_generator.stop()
        elapsed = get_pw_program_run_time(
            n_threads=n_threads,
            n_processes=n_processes,
            input_path=inputs_path,
            output_path=output_path,
            pstorage_path=pstorage_path,
            mode=STATIC_MODE_NAME,
            pstorage_type=pstorage_type,
            persistence_mode=persistence_mode,
            first_port=first_port,
        )
        print("Time elapsed for non-interrupted run:", elapsed)
        print("Checking correctness at the end")
        check_output_correctness(None, inputs_path, output_path)
