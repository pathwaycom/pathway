# Integration Tests (Light)

This workflow runs complete integration suites that need no private credentials
or external services. It runs for pull requests and pushes to `main`, and can
also be started manually from Actions in any fork with Actions enabled.

The build job compiles a Python 3.10 ABI3 wheel from the checked-out commit.
Each suite gets an independent matrix job, installs that wheel in a Python 3.11
Docker image, and runs with external networking disabled. Public packages and
tokenizer data are downloaded while building the image. No container registry
login or repository secrets are required.

The initial suite is the complete `integration_tests/webserver` directory,
including the REST, vector-store, LangChain, LlamaIndex and RAG client tests.
These tests use local servers and fake LLMs/embeddings. The test files and
parameterizations are unchanged. Their JUnit reports are uploaded even if a
test fails.

The existing NLTK setup may log an offline download warning; its tokenizer data
are already installed in the image, so the tests can run without that download.

## Adding a suite

1. Add its directory name to `matrix.suite` in
   `.github/workflows/integration-tests-light.yml`.
2. Ensure its test dependencies and public data are prepared in `Dockerfile`.
3. Run the complete suite with no private credentials and `--network none`.
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
