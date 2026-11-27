# Integration Tests (Light)

This workflow collects integration suites and runs their light, local tests
without private credentials or external services. It runs for pull requests and
pushes to `main`, and can also be started manually from Actions in any fork with
Actions enabled.

The build job compiles a Python 3.10 ABI3 wheel from the checked-out commit.
Each suite gets an independent matrix job, installs that wheel in a Python 3.11
Docker image, and runs with external networking disabled. Public packages and
tokenizer data are downloaded while building the image. No container registry
login or repository secrets are required.

The matrix includes:

- `webserver`: all REST, vector-store, LangChain, LlamaIndex and RAG client tests,
  using local servers and fake LLMs/embeddings. These tests are unchanged.
- `common`: CLI validation, operator diagnostics/compaction, input synchronization,
  unused-column memory checks, and the negative monitoring-license test.

The runner passes `--integration-light`. Tests decorated with
`@pytest.mark.external_service` or `@pytest.mark.slow` are explicitly skipped in
this mode. Tests decorated with `@pytest.mark.requires_license` are skipped
when no Pathway license key is configured. Skip reasons are included in the
console output and JUnit reports, which are uploaded even if a test fails.

For `common`, this skips the external GitHub application, exhaustive persistence
matrix, and licensed tests. An ordinary pytest invocation without
`--integration-light` still runs external and slow tests; licensed tests run when
a license key is provided. Invalid or insufficient license keys fail normally.

The existing NLTK setup may log an offline download warning; its tokenizer data
are already installed in the image, so the tests can run without that download.

## Adding a suite

1. Add its directory name to `matrix.suite` in
   `.github/workflows/integration-tests-light.yml`.
2. Ensure its test dependencies and public data are prepared in `Dockerfile`.
3. Run the suite with no private credentials and `--network none`. Mark tests
   requiring a license, external services, or long-running workloads as above.
   Suites needing local dependency containers will also need an isolated
   Docker network and service startup/cleanup in their matrix jobs.

Suites run independently (`fail-fast: false`) and produce separate reports.
The native wheel is built once and shared across the matrix.

## Running locally

Build a wheel into `dist/` from the commit under test, then run:

```bash
docker build -f .github/ci/integration-tests/Dockerfile -t pathway-integration-light .
mkdir -p test-results
docker run --rm --network none \
  --mount "type=bind,source=$PWD/test-results,target=/test-results" \
  pathway-integration-light webserver
```
