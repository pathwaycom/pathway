#!/usr/bin/env python

# Copyright © 2024 Pathway

import json
import os
import pathlib
import random
import shutil
import subprocess
import threading
import time
import uuid
import warnings

import boto3
from azure.storage.blob import BlobServiceClient

DEFAULT_INPUT_SIZE = 5000000
COMMIT_LINE = "*COMMIT*\n"
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
            if "Contents" in objects_to_delete and objects_to_delete["Contents"]:
                objects = [{"Key": obj["Key"]} for obj in objects_to_delete["Contents"]]
                s3.delete_objects(
                    Bucket="aws-integrationtest", Delete={"Objects": objects}
                )
            else:
                break


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
                except IndexError:
                    # line split in two chunks, one fsynced, another did not
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
):
    pw_wordcount_path = (
        "/".join(os.path.abspath(__file__).split("/")[:-1])
        + f"/pw_wordcount.py --input {input_path} --output {output_path} --pstorage {pstorage_path} "
        + f"--mode {mode} --pstorage-type {pstorage_type} --persistence_mode {persistence_mode}"
    )
    n_cpus = n_threads * n_processes
    cpu_list = ",".join([str(x) for x in range(n_cpus)])
    command = f"taskset --cpu-list {cpu_list} python {pw_wordcount_path}"
    run_args = command.split()

    run_id = uuid.uuid4()
    process_handles = []
    for process_id in range(n_processes):
        env = os.environ.copy()
        env["PATHWAY_THREADS"] = str(n_threads)
        env["PATHWAY_PROCESSES"] = str(n_processes)
        env["PATHWAY_FIRST_PORT"] = str(first_port)
        env["PATHWAY_PROCESS_ID"] = str(process_id)
        env["PATHWAY_RUN_ID"] = str(run_id)
        handle = subprocess.Popen(run_args, env=env)
        process_handles.append(handle)

    return process_handles


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
):
    needs_pw_program_launch = True
    n_retries = 0
    while needs_pw_program_launch:
        needs_pw_program_launch = False
        time_start = time.time()
        process_handles = start_pw_computation(
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
                time.sleep(10)

                # Insert file size check here

                try:
                    modified_at = os.path.getmtime(output_path)
                    file_size = os.path.getsize(output_path)
                    if file_size == 0:
                        continue
                except FileNotFoundError:
                    if time.time() - time_start > 180:
                        raise
                    continue
                if modified_at > time_start and time.time() - modified_at > 60:
                    for process_handle in process_handles:
                        process_handle.kill()
                    needs_polling = False
        finally:
            pw_exit_code = 0
            for process_handle in process_handles:
                if mode == STATIC_MODE_NAME:
                    try:
                        local_exit_code = process_handle.wait(timeout=600)
                    except subprocess.TimeoutExpired:
                        process_handle.kill()
                        local_exit_code = 255
                else:
                    local_exit_code = process_handle.poll()
                    if local_exit_code is None:
                        # In streaming mode the code never ends, so it's the expected
                        # behavior
                        process_handle.kill()
                        local_exit_code = 0
                pw_exit_code = max(pw_exit_code, local_exit_code)

            if pw_exit_code is not None and pw_exit_code != 0:
                warnings.warn(
                    f"Warning: pw program terminated with non zero exit code: {pw_exit_code}"
                )
                assert n_retries < 3, "Number of retries for S3 reconnection exceeded"
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
    process_handles = start_pw_computation(
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
        wait_time = random.uniform(min_work_time, max_work_time)
        time.sleep(wait_time)
    finally:
        for process_handle in process_handles:
            process_handle.kill()


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
) -> None:
    with open(file_name, "w") as fw:
        for seq_line_id in range(input_size):
            word = random.choice(dictionary)
            dataset_line_dict = {"word": word}
            dataset_line = json.dumps(dataset_line_dict)
            fw.write(dataset_line + "\n")
            if (seq_line_id + 1) % commit_frequency == 0:
                # fw.write(COMMIT_LINE)
                pass


def generate_next_input(
    inputs_path: pathlib.Path,
    *,
    input_size: int | None = None,
    dictionary: list[str] | None = None,
    commit_frequency: int | None = None,
) -> str:
    file_name = os.path.join(inputs_path, str(time.time()))

    generate_input(
        file_name=file_name,
        input_size=input_size or DEFAULT_INPUT_SIZE,
        commit_frequency=commit_frequency or 100000,
        dictionary=dictionary or DICTIONARY,
    )

    return file_name


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
        for n_run in range(n_backfilling_runs):
            print(f"Run {n_run}: generating input")
            latest_input_name = generate_next_input(inputs_path)

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
