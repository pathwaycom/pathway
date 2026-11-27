---
name: bug-fix
description: >-
  Investigate and fix failing or flaky tests in the Pathway repository — both
  Python unit tests and integration tests that run against real Docker
  containers. Use this skill WHENEVER you are handed a CI failure (typically a
  Jenkins log snippet containing a test error), asked "why did this test fail",
  told a test is flaky / intermittent / passes-sometimes, or asked to reproduce,
  diagnose, or fix a red test. Use it even when the failure looks like a one-off:
  in this repo "it was just the machine" is never an acceptable conclusion. The
  skill covers routing unit vs integration failures, building a reliable
  reproduction, verification in the CI environment, and a final 20-run green
  streak scoped to the reported tests. Run full suites only for an explicitly
  requested stabilization effort aimed at finding other failures.
---

# Flaky Test Investigation (Pathway)

You are fixing a failing or intermittently-failing test in the Pathway
repository. This skill tells you how to route the failure, reproduce it
reliably, fix the *root cause* (never paper over it), and verify the fix the way
CI would.

Read the applicable project instructions (`AGENTS.md`, or `CLAUDE.md` where
the project keeps its build/testing conventions), plus `CONTRIBUTING.md`,
before changing code. Use the current build commands and required checks.

## Discovery and repository paths

The canonical skill lives in `.agents/skills/bug-fix/` in the Pathway project.
`.claude/skills` is a relative symlink to `.agents/skills`, sharing all skills
between Codex and Claude Code. Invoke this skill as `$bug-fix` in Codex or
`/bug-fix` in Claude Code; requests matching its description also apply.

In the IoT-Pathway checkout, the Pathway project is `public/pathway/`, while
Jenkins infrastructure is at the checkout root. Run Python test commands from
`public/pathway/`; run commands referencing `.jenkins/` from the IoT-Pathway
root. Resolve these locations from the actual checkout rather than assuming
that the current working directory is correct. A standalone Pathway checkout
may not contain the private Jenkins infrastructure; if it is unavailable,
report the resulting verification limit rather than inventing a CI-equivalent
run.

## Why tests here are non-deterministic

Two independent sources of non-determinism make failures here rare and hard to
catch, and you must keep both in mind the whole time:

1. **Time-slicing.** Data entering the engine is cut into mini-batches by
   wall-clock time in milliseconds (`autocommit_duration_ms`). Whether two
   records land in the same mini-batch or get split across two is *not*
   deterministic — it depends on timing. A test that assumes a particular
   batching will pass most of the time and fail occasionally.
2. **Integration tests run against real systems, in parallel.** Integration
   tests exercise connectors, persistence, etc. against real Docker containers
   (via `docker-compose`), not mocks. That external system is a second,
   independent source of randomness. On top of that, a whole *class* of systems
   is exercised at once (e.g. all database-connector tests, or all persistence
   behaviours) and the suite runs with high parallelism (≈24–48 workers). Both
   the shared external state and the parallelism add further non-determinism.

The practical consequence: a test may fail once in 10 runs, or once a week. That
is exactly the kind of failure you are here to fix — not to dismiss.

## Input

You will usually be given a slice of context from CI (most often a Jenkins log)
containing an error. It may be small. Your first job is to extract from it:

- **Which test** failed (file, test name, parametrization if any).
- **Whether it is a unit test or an integration test** — this determines your
  whole plan. Integration tests live under `integration_tests/`; unit tests live
  under `python/pathway/tests/` (and doctests). If the log path or suite name
  does not make it obvious, locate the test in the tree to decide.
- **The failure signature** — assertion, exception, timeout, hang, wrong row
  count/order, etc.

## Workflow

Follow these phases in order. Build a plan first, tailored to unit vs
integration, then execute.

### Choose the test scope first

- **Targeted fix (default):** when given one specific failing test or a small
  set, run only those tests, their relevant parametrizations, and regression
  tests directly needed to verify the fix. Apply this scope to reproduction,
  post-fix checks, and every repetition in the final green streak. Do not run
  unrelated tests, broaden to the containing suite, or investigate and fix
  unrelated failures encountered in existing logs.
- **Explicit stabilization:** when the user explicitly asks to stabilize a
  suite or area, run the entire suite or area they named to find additional
  failures. Finding and fixing failures within that scope is then part of the
  task. A report of a flaky test, or a request to fix it, does not by itself mean
  stabilization of the whole suite.

CI parity concerns the image, dependencies, initialization, pytest options,
and parallelism; it does not require running unrelated tests. Keep the chosen
test selection in every execution path below. Required formatters, linters,
and build checks still apply to the change.

### Phase 1 — Static analysis (always do this first)

Before running anything, read the code. Read the failing test, then read the
machinery it exercises — the connector, reducer, temporal operator, persistence
path, whatever it touches. Trace the code paths involved and try to find the
logical flaw by inspection: a batching assumption, an ordering assumption, an
unhandled deletion/retraction, a race between workers, a missing `cutoff`, a
container init step the test depends on, etc.

If static analysis alone reveals the defect, that's the best outcome — you can
go straight to a fix (Phase 4). Still prefer to add a reproduction (Phase 3)
when feasible, because it protects you and future changes.

### Phase 2 — Instrument *before* you run (when you'll need to run repeatedly)

If you anticipate having to run the test many times to catch a rare failure, do
**not** run first and add logging later. Read the code and add the debug output
you'll want *before* the first repeated run, so that when you finally catch a
failure the diagnostic information is already there. Think ahead about what would
let you understand the failure the moment it happens — mini-batch boundaries,
row diffs (`+1`/`-1`), timestamps, worker/shard ids, container state — and log
that. Adding logs after catching the failure means paying the (often very long)
reproduction cost twice.

### Phase 3 — Build a reliable reproduction

A stable reproduction is a large gift to yourself; invest in one.

- Write a test that **reliably** reproduces the problem so you can see clearly
  what it is.
- The test must describe the **desired, correct behaviour** — never encode the
  bug. Name and document it as what *should* happen, not as "X is broken". (See
  the project instructions (`AGENTS.md`, or `CLAUDE.md` if the project still keeps its build/testing conventions there): `test_csv_reader_preserves_all_rows_after_reconnect`, not
  `..._drops_rows_on_reconnect`.) This rule holds even though the test is being
  written to capture a bug.
- For time/batching-sensitive unit tests, use the static-mode testing pattern
  (`pw.debug.table_from_markdown` + `pw.debug.compute_and_print`) where you can,
  and make the assertion robust to non-deterministic batching rather than
  dependent on a lucky split.

If the context is too small to reproduce or even to understand the failure,
you have limited options — fall back to running the test many times to catch it
(having first instrumented it in Phase 2).

### Phase 4 — Fix the root cause

- If the fix is **trivial and unambiguous**, make it yourself.
- Resolve routine implementation choices using the requested behavior and
  existing project conventions. If a fix needs a product decision that the
  request does not settle (for example, changing a public-facing default),
  present the options and their trade-offs and ask for that decision.
- After any significant change, run the required checks from the applicable
  project instructions and contribution guide, in their documented order.
  Fix issues introduced by the change before moving on; report unrelated
  pre-existing failures separately.

## Running tests the way CI does

Run as close to CI as possible so your reproduction and verification are
trustworthy. Always read the actual files at runtime before relying on any
command below — they change often:

- Jenkins readme: `.jenkins/integration_tests/readme.md` (**all lowercase**).
- Driver script: `.jenkins/integration_tests/integration-tests.sh`.
- Compose file: `.jenkins/integration_tests/docker-compose-integration.yml`.

The compose file in particular is edited frequently, and its **inline comments
document previously-fixed flakiness** per service (Kafka rebalance delay, QuestDB
ILP commit lag, Iceberg catalog on SQLite→Postgres, RabbitMQ erlang-cookie race,
MySQL socket-vs-TCP healthcheck, Atlas `mongot` readiness, …). Read those
comments: they show the expected depth of root-causing and usually reveal the
readiness/healthcheck gotcha for the system your test talks to.

### Unit tests

Run the selected pytest node IDs using the environment documented for the
checkout, for example
`pytest python/pathway/tests/test_table.py::test_name -v` from the Pathway
project directory. Internal development environments may rebuild the Rust
extension on `import pathway`; standalone environments may require an explicit
wheel build. Follow the current project instructions and CI workflow rather
than assuming either behavior. Match the reported job's Python version,
engine thread count, runtime typechecking, and pytest options while retaining
the selected test scope.

### Integration tests — fast variant (preferred)

There is a fast variant and a light variant. The light variant is slower; **use
the fast variant.** The fast variant skips the slow engine-image build entirely
and runs the test process locally against the **minimal** set of containers the
specific failing test touches:

1. Identify only the containers your *specific* test needs — not the whole
   `depends_on:` of the suite. A suite like `db-connectors` declares every
   database dependency, but if you're chasing a Postgres-only failure you bring
   up **only Postgres**, not all of them. Use the suite's `depends_on:` as a
   menu to pick from, not a set to launch wholesale.
2. Stand up just those services, publishing/proxying their ports so they're
   reachable on `localhost`:
   `docker compose -f .jenkins/integration_tests/docker-compose-integration.yml up <only-what-you-need>`
   (many DB services declare no `ports:` — add a published port for the ones you
   need; this counts as temporary scaffolding to revert, see below.)
3. Wait for initialization to complete. Several services gate readiness through
   healthchecks *and* init containers that also perform crucial setup — do not
   skip them and do not start tests before they finish. Examples: `mssql-init`
   must reach `service_completed_successfully` (creates `testdb`, enables CDC);
   `mongodb` initiates its replica set inside the healthcheck; Atlas reports
   healthy only once `mongot` is queryable.
4. Run the selected tests locally, matching CI's pytest options
   (`--dist worksteal -n auto`, which is what `PYTEST_ADDOPTS` sets in compose):
   `pytest integration_tests/<suite>/test_file.py::test_name --dist worksteal -n auto`
   (or `-n 24` for a fixed count). Pass multiple node IDs for a small set. Use
   `integration_tests/<suite>/` without a filter only for explicit suite
   stabilization. The parallelism itself is a flakiness factor — keep it on
   when reproducing.
5. You will likely need to adjust port numbers in the test configs to match your
   local published ports. **Revert those edits before you finish.**

### Integration tests — full Jenkins-identical flow

Use this when the fast variant can't reproduce the failure, or for the final
green-streak where exact CI parity matters. Preserve the selected test scope:
inspect the current driver for test-selection support. If it always runs a
whole suite, use its built image and a temporary Compose command override to
pass the selected pytest node IDs, starting only their required services and
init containers. Check the collected tests to ensure the filter took effect,
and tear down the test stack with `down -v` between runs. Do not launch an
unfiltered suite merely because the wrapper has no selection option.

Use an isolated Compose project for the investigation and apply that same
project name to startup, test execution, and teardown. Never tear down another
checkout's or contributor's containers or volumes.

Follow `readme.md`; the current unfiltered flow below is for explicit
stabilization of the named suite:

```bash
export PATHWAY_SRC_IMAGE=integration_test_$(git rev-parse --short HEAD)
./.jenkins/integration_tests/integration-tests.sh build_docker_image   # slow
export DOCKER_BUILDKIT=1
./.jenkins/integration_tests/integration-tests.sh <suite>              # e.g. webserver
```

Two stages you depend on:

1. **`build_docker_image`** (underscores) — compiles the engine into the slim
   image `$PATHWAY_SRC_IMAGE` and then builds the `base-test` image. It is
   **slow**: budget ~30–40 min, up to an hour worst case.
   - Re-run it **every time you change the engine** — and "engine" means *both*
     its Rust parts and its Python parts. Any change to the engine's code,
     Rust **or** Python, requires a rebuild. Do not assume only `src/` / Rust
     edits matter.
   - Only when your change is confined to the **tests** (files under
     `integration_tests/`) can you skip the rebuild — `run_test` runs the suite
     with `docker compose run --rm --build`, so the per-suite `--build` picks up
     current test code against the existing engine image.
2. **Run one suite** — `integration-tests.sh <suite>` runs
   `integration-test-<suite>` and tears it down with `down -v` afterwards. Valid
   suite names come from the `TESTS` map in the script: `wordcount`, `s3`,
   `kafka`, `webserver`, `indexer`, `gdrive`, `sharepoint`, `airbyte`,
   `license-api`, `pathway-license`, `monitoring`,
   `pathway-enterprise-license`, `xpack`, `rag`, `common`, `stdlib`,
   `db-connectors`, `iceberg`.
   **Out of scope:** the `rag` suite (LLM/RAG evals — MLflow/OpenAI, runs with
   `-n 0`) is not something this skill handles; assume it won't be sent here.

**Credentials / Vault:** the driver's `load_env_vars` and
`checkout_test_postgres_certs` pull secrets from **Vault** (several suites also
have credential pre-hooks: gdrive/sharepoint/xpack/license, real-AWS
`s3`/`wordcount`). **You cannot authenticate to Vault yourself** — only the
developer can, by entering their password. Usually they log in before handing
you the investigation. If Vault auth is missing when you need it, do **not** try
to work around it or fake credentials. Use the repository's documented login
flow if available. If it requires human authentication, explain which step
needs it and ask the developer to authenticate. Continue independent work and
resume credential-dependent verification once access is available. Never print
secret values in logs or the final report.

Because this path is very time-expensive, make sure you have already done Phase 2
(instrumentation) — have the debug output in place *before* you kick off a long
run, so a caught failure is diagnosable and you don't pay the reproduction cost
twice.

## Hard prohibitions

These are never acceptable. Each one hides the problem instead of solving it.

- **Do not conclude "it's just a flaky machine."** Even if timing or the
  environment is the trigger, the correct response is to improve the test
  harness so the flap cannot happen — or happens orders of magnitude less often.
- **Do not mark the test `xfail`.** That sweeps the problem under the rug.
- **Do not add retries / reruns on failure.** Re-running a failing test until it
  passes does not fix anything; it only makes a real bug less visible, which is
  worse than leaving it visible.

If you find yourself reaching for any of these, go back to Phase 1.

## Clean up scaffolding before committing

Everything temporary you introduced to investigate must be removed before you
commit, so the committed state works correctly on its own. This includes:

- debug logging / print statements you added to catch the failure,
- port-proxying edits (added `ports:` entries, changed port numbers),
- any `localhost` / local-endpoint edits you made in test configs for the fast
  variant,
- any other scratch changes made only to reproduce.

Only the actual root-cause fix (and any legitimate new test) should remain. Do a
final diff review to confirm no scaffolding leaked into the commit.

## Final verification — the 20-run green streak

The guiding principle is **zero tolerance for flaky tests** — direct all your
effort at that. When you've fixed a flaky test, prove it stays green.

- Run the **selected tests** 20 times in a row using the verification path
  above. For a targeted fix this means the reported tests and directly relevant
  regressions, even when the full Jenkins image is already built. For explicit
  stabilization it means the entire suite or area the user named. Integration
  runs tear down their test stack with `down -v` between repetitions.
- If any selected test fails, the fix is not done. Analyse that failure, go
  through the repair procedure again from Phase 1, then restart the 20-run
  streak from scratch. An unrelated failure outside the selected scope is not
  a reason to expand the task; report it separately without chasing it.
- Only a clean, uninterrupted streak of 20 green runs counts as success.

**Keep verification proportional to the requested scope:**

- The apparent cost of a full suite is not a reason to expand a targeted fix
  into a full-suite run. Stop once the selected verification is complete.
- **`wordcount` is the one genuinely heavy suite.** A full 20-run streak on it
  can take ~12 hours, so run that only when the developer explicitly asks for it
  (e.g. leaving the agent to work overnight) — don't launch a wordcount
  green-streak unprompted.
- If your change was confined to test files (under `integration_tests/`), you
  don't owe the full heavy green-streak ritual for that alone; reserve the 20×
  streak for verifying an actual flaky fix, including fixes to its test harness.
  A green streak never broadens the selected test scope.

## Delivery and branch changes

Report the root cause, the fix, the selected tests and commands, and the actual
verification results, including the green-streak count and any limitations.
Do not claim CI parity or a completed streak when those checks did not run.

Follow the user's authorization and applicable project instructions for
committing or pushing. A dedicated branch alone does not authorize publishing
changes. Preserve other contributors' work and reconcile concurrent branch
updates without discarding their changes.
