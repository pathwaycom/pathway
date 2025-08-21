import json
import os

import pytest

import pathway as pw
from pathway.internals.parse_graph import G


def generate_test_scenarios(
    max_length: int, max_files: int, allow_replacements: bool, prefix: list[int] = []
) -> list[list[int]]:
    """
    Recursively generate all sequences of file actions of the given length with the
    given restriction on the number of files operated on. For each of these sequences,
    the method also generates all ways to split them into several persisted Pathway
    runs. It enables exhaustive end-to-end Pathway persistence testing, where all possible
    non-isomorphic usage patterns are checked.

    Each scenario token is a number. It can be positive, meaning that the corresponding
    file is upserted; negative, meaning that the corresponding file is deleted; or zero,
    meaning that a Pathway run takes place.

    Args:
        max_length: The number of file operations in the generated scenarios. Each operation
            can be a file insertion, deletion, or replacement (if enabled).
        max_files: The maximum number of files operated on in each scenario.
        allow_replacements: Whether the replacement operation is allowed in the generated
            scenarios.
        prefix: The scenario prefix. Prefixes like [1, 2] are quite good for breaking
            buggy code, as opposed to [1, 0], since, when the object numbered 1st is
            deleted, the corresponding persistence artifacts are cleaned up as obsolete,
            and the system returns to a starting, empty state.

    Returns:
        A list of test scenarios.
    """
    scenarios = []

    def construct(sequence: list[int], n_actions: int):
        if n_actions == max_length:
            scenarios.append(sequence + [0])
            return

        max_number = 0
        present_numbers = set()
        for number in sequence:
            max_number = max(max_number, abs(number))
            if number > 0:
                present_numbers.add(number)
            elif number < 0:
                present_numbers.remove(-number)

        # Add a totally new object. It is possible if it doesn't exceed the files limit.
        if max_number + 1 <= max_files:
            construct(sequence + [max_number + 1], n_actions + 1)

        # Update the state of something that was not touched in the current transaction:
        # either upsert an object (add it if it wasn't present or modify if it was), or
        # delete it.
        for number in range(max_number):
            candidate = number + 1
            if len(sequence) > 0 and candidate <= abs(sequence[-1]):
                # To exclude duplicates, we consider only increasing sequences of ids
                # within a single run of a Pathway program
                continue

            # Upsert an object. This is possible either if replacements are allowed or
            # if there is no such object.
            if allow_replacements or candidate not in present_numbers:
                construct(sequence + [candidate], n_actions + 1)

            # Remove an object. It's possible if it's present.
            if candidate in present_numbers:
                construct(sequence + [-candidate], n_actions + 1)

        # Don't add anything, but mark that the next action will go in the next pw run
        if len(sequence) > 0 and sequence[-1] != 0:
            construct(sequence + [0], n_actions)

    total_actions_in_prefix = 0
    for token in prefix:
        if token != 0:
            total_actions_in_prefix += 1
    construct(prefix, total_actions_in_prefix)
    return scenarios


TEST_SCENARIOS = (
    generate_test_scenarios(5, 3, allow_replacements=False)
    + generate_test_scenarios(4, 3, allow_replacements=True)
    + generate_test_scenarios(6, 4, allow_replacements=False, prefix=[1, 2])
)


@pytest.mark.parametrize("scenario", TEST_SCENARIOS, ids=lambda s: str(s))
def test_persistent_runs(tmp_path, scenario):
    inputs_path = tmp_path / "inputs"
    output_path = tmp_path / "output.jsonl"
    pstorage_path = tmp_path / "pstorage"
    persistence_config = pw.persistence.Config(
        backend=pw.persistence.Backend.filesystem(pstorage_path)
    )
    os.mkdir(inputs_path)
    n_total_upserts = 0
    current_objects_snapshot: dict[str, str] = {}
    expected_additions: list[list[str]] = []
    expected_deletions: list[list[str]] = []

    def run_pathway_program():
        G.clear()
        table = pw.io.plaintext.read(inputs_path, mode="static", with_metadata=True)
        pw.io.jsonlines.write(table, output_path)
        pw.run(persistence_config=persistence_config)
        factual_additions = []
        factual_deletions = []
        addition_bad_idx = []
        deletion_bad_idx = []
        with open(output_path, "r") as f:
            for row in f:
                parsed = json.loads(row)
                file_name = os.path.basename(parsed["_metadata"]["path"])
                contents = parsed["data"]
                if parsed["diff"] == 1:
                    factual_additions.append([file_name, contents])
                elif parsed["diff"] == -1:
                    factual_deletions.append([file_name, contents])
                else:
                    assert False, "incorrect diff: {}".format(parsed["diff"])

        for addition_idx, addition in enumerate(factual_additions):
            for deletion_idx, deletion in enumerate(factual_deletions):
                if addition == deletion:
                    # Owner changed from user to null or vice-versa, ignore such changes
                    addition_bad_idx.append(addition_idx)
                    deletion_bad_idx.append(deletion_idx)
        factual_additions = [
            item
            for i, item in enumerate(factual_additions)
            if i not in addition_bad_idx
        ]
        factual_deletions = [
            item
            for i, item in enumerate(factual_deletions)
            if i not in deletion_bad_idx
        ]

        factual_deletions.sort()
        factual_additions.sort()
        expected_additions.sort()
        expected_deletions.sort()
        assert factual_additions == expected_additions
        assert factual_deletions == expected_deletions

    # Check the states of all objects at the end, after the possible compression round
    all_object_ids = []
    for token in scenario:
        if token > 0 and token not in all_object_ids:
            all_object_ids.append(token)
    full_scenario = scenario + [0] + all_object_ids + [0]

    for token in full_scenario:
        file_name = str(abs(token))
        file_path = inputs_path / file_name
        if token > 0:
            n_total_upserts += 1
            file_contents = "a" * n_total_upserts

            old_contents = current_objects_snapshot.pop(file_name, None)
            if old_contents is not None:
                expected_deletions.append([file_name, old_contents])
            current_objects_snapshot[file_name] = file_contents
            expected_additions.append([file_name, file_contents])

            file_path.write_text(file_contents)
        elif token < 0:
            os.remove(file_path)
            old_contents = current_objects_snapshot.pop(file_name)
            expected_deletions.append([file_name, old_contents])
        else:
            run_pathway_program()
            expected_additions.clear()
            expected_deletions.clear()


if __name__ == "__main__":
    print(len(TEST_SCENARIOS))
