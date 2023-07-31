#!/usr/bin/env python

# Copyright © 2023 Pathway

import json
import os
import random
import shutil
import subprocess
import time

DEFAULT_INPUT_SIZE = 5000000
COMMIT_LINE = "*COMMIT*\n"
STATIC_MODE_NAME = "static"
STREAMING_MODE_NAME = "streaming"
DICTIONARY = None


def ensure_no_pstorage_present(pstorage_path):
    if os.path.exists(pstorage_path):
        shutil.rmtree(pstorage_path)


def check_output_correctness(
    latest_input_file, input_path, output_path, interrupted_run=False
):
    input_word_counts = {}
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
            with open(path, "r") as f:
                for row in f:
                    if not row.strip() or row.strip() == "*COMMIT*":
                        continue
                    json_payload = json.loads(row.strip())
                    word = json_payload["word"]
                    if word not in input_word_counts:
                        input_word_counts[word] = 0
                    input_word_counts[word] += 1

                    if not on_old_file:
                        new_file_lines.add((word, input_word_counts[word]))
                        distinct_new_words.add(word)

    print("  New file lines:", len(new_file_lines))

    n_rows = 0
    n_old_lines = 0
    output_word_counts = {}
    try:
        with open(output_path, "r") as f:
            is_first_row = True
            word_column_index = None
            count_column_index = None
            for row in f:
                n_rows += 1
                if is_first_row:
                    column_names = row.strip().split(",")
                    for col_idx, col_name in enumerate(column_names):
                        if col_name == "word":
                            word_column_index = col_idx
                        elif col_name == "count":
                            count_column_index = col_idx
                    is_first_row = False
                    assert (
                        word_column_index is not None
                    ), "'word' is absent in CSV header"
                    assert (
                        count_column_index is not None
                    ), "'count' is absent in CSV header"
                    continue

                tokens = row.strip().split(",")
                word = tokens[word_column_index].strip('"')
                count = tokens[count_column_index]
                output_word_counts[word] = int(count)

                if (word, int(count)) not in new_file_lines:
                    n_old_lines += 1
    except FileNotFoundError:
        if interrupted_run:
            return False
        raise

    assert len(input_word_counts) >= len(
        output_word_counts
    ), "There are some new words on the output. Input dict: {} Output dict: {}".format(
        len(input_word_counts), len(output_word_counts)
    )

    for word, output_count in output_word_counts.items():
        if interrupted_run:
            assert input_word_counts.get(word) >= output_count
        else:
            assert (
                input_word_counts.get(word) == output_count
            ), "Word: {} Output count: {} Input count: {}".format(
                word, output_count, input_word_counts.get(word)
            )

    if not interrupted_run:
        assert (
            n_old_lines < DEFAULT_INPUT_SIZE / 10
        ), "Output contains too many old lines: {} while 1/10 of the input size is {}".format(
            n_old_lines, DEFAULT_INPUT_SIZE / 10
        )
        assert n_rows >= len(
            distinct_new_words
        ), "Output contains only {} lines, while there should be at least {}".format(
            n_rows, len(distinct_new_words)
        )

    print("  Total rows on the output:", n_rows)
    print("  Total old lines:", n_old_lines)

    return input_word_counts == output_word_counts


def start_pw_computation(n_cpus, input_path, output_path, pstorage_path, mode):
    pw_wordcount_path = "/".join(
        os.path.abspath(__file__).split("/")[:-1]
    ) + "/pw_wordcount.py --input {} --output {} --pstorage {} --n-cpus {} --mode {}".format(
        input_path, output_path, pstorage_path, n_cpus, mode
    )

    cpu_list = ",".join([str(x) for x in range(n_cpus)])
    command = "taskset --cpu-list {} python {}".format(cpu_list, pw_wordcount_path)
    run_args = command.split()

    return subprocess.Popen(run_args)


def get_pw_program_run_time(n_cpus, input_path, output_path, pstorage_path, mode):
    time_start = time.time()
    popen = start_pw_computation(n_cpus, input_path, output_path, pstorage_path, mode)
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
                popen.kill()
                needs_polling = False
    finally:
        if mode == STREAMING_MODE_NAME:
            popen.kill()
        else:
            pw_exit_code = popen.poll()
            while pw_exit_code is None:
                pw_exit_code = popen.poll()
            assert pw_exit_code == 0, "Bad exit code for pw program: {}".format(
                pw_exit_code
            )

    return time.time() - time_start


def run_pw_program_suddenly_terminate(
    n_cpus, input_path, output_path, pstorage_path, min_work_time, max_work_time
):
    popen = start_pw_computation(
        n_cpus, input_path, output_path, pstorage_path, STATIC_MODE_NAME
    )
    try:
        wait_time = random.uniform(min_work_time, max_work_time)
        time.sleep(wait_time)
    finally:
        popen.kill()


def reset_runtime(inputs_path, pstorage_path):
    try:
        shutil.rmtree(pstorage_path)
    except FileNotFoundError:
        print("There is no persistent storage to remove")
    except Exception:
        print("Failed to clean persistent storage")
        raise

    try:
        shutil.rmtree(inputs_path)
    except FileNotFoundError:
        print("There is no inputs directory to remove")
    except Exception:
        print("Failed to clean inputs directory")
        raise

    os.makedirs(inputs_path)
    print("State successfully re-set")


def generate_word():
    word_chars = []
    for _ in range(10):
        word_chars.append(random.choice("abcdefghijklmnopqrstuvwxyz"))
    return "".join(word_chars)


def generate_dictionary(dict_size):
    result_as_set = set()
    for _ in range(dict_size):
        word = generate_word()
        while word in result_as_set:
            word = generate_word()
        result_as_set.add(word)
    return list(result_as_set)


DICTIONARY = generate_dictionary(10000)


def generate_input(file_name, input_size, commit_frequency):
    with open(file_name, "w") as fw:
        for seq_line_id in range(input_size):
            word = random.choice(DICTIONARY)
            dataset_line_dict = {"word": word}
            dataset_line = json.dumps(dataset_line_dict)
            fw.write(dataset_line + "\n")
            if (seq_line_id + 1) % commit_frequency == 0:
                fw.write(COMMIT_LINE)


def generate_next_input(inputs_path):
    file_name = os.path.join(inputs_path, str(time.time()))

    generate_input(
        file_name=file_name,
        input_size=DEFAULT_INPUT_SIZE,
        commit_frequency=100000,
    )

    return file_name


def do_test_persistent_wordcount(n_backfilling_runs, n_cpus, tmp_path, mode):
    pstorage_path = tmp_path / "pstorage"
    inputs_path = tmp_path / "inputs"
    output_path = tmp_path / "table.csv"

    reset_runtime(inputs_path, pstorage_path)
    for n_run in range(n_backfilling_runs):
        print("Run {}: generating input".format(n_run))
        latest_input_name = generate_next_input(inputs_path)

        print("Run {}: running pathway program".format(n_run))
        elapsed = get_pw_program_run_time(
            n_cpus, inputs_path, output_path, pstorage_path, mode
        )
        print("Run {}: pathway time elapsed {}".format(n_run, elapsed))

        print("Run {}: checking output correctness".format(n_run))
        check_output_correctness(latest_input_name, inputs_path, output_path)
        print("Run {}: finished".format(n_run))


def do_test_failure_recovery_static(
    n_backfilling_runs, n_cpus, tmp_path, min_work_time, max_work_time
):
    pstorage_path = tmp_path / "pstorage"
    inputs_path = tmp_path / "inputs"
    output_path = tmp_path / "table.csv"
    reset_runtime(inputs_path, pstorage_path)

    finished = False
    input_file_name = generate_next_input(inputs_path)
    for n_run in range(n_backfilling_runs):
        print("Run {}: generating input".format(n_run))

        print("Run {}: running pathway program".format(n_run))
        run_pw_program_suddenly_terminate(
            n_cpus,
            inputs_path,
            output_path,
            pstorage_path,
            min_work_time,
            max_work_time,
        )

        finished_in_this_run = check_output_correctness(
            input_file_name, inputs_path, output_path, interrupted_run=True
        )
        if finished_in_this_run:
            finished = True

    if finished:
        print("The program finished during one of interrupted runs")
    else:
        elapsed = get_pw_program_run_time(
            n_cpus, inputs_path, output_path, pstorage_path, STATIC_MODE_NAME
        )
        print("Time elapsed for non-interrupted run:", elapsed)
        print("Checking correctness at the end")
        check_output_correctness(input_file_name, inputs_path, output_path)
