# Ubuntu 24.04 builds the native wheel. Trixie's newer glibc can load it;
# Bookworm's older glibc cannot reliably load a native Ubuntu 24.04 wheel.
FROM python:3.11-slim-trixie

ENV PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    PATHWAY_THREADS=1 \
    OMP_NUM_THREADS=1 \
    OPENBLAS_NUM_THREADS=1 \
    NLTK_DATA=/opt/nltk_data \
    TIKTOKEN_CACHE_DIR=/opt/tiktoken-cache

RUN apt-get update \
    && apt-get install -y --no-install-recommends libmagic1t64 libpq5 \
    && rm -rf /var/lib/apt/lists/*

COPY dist/pathway-*.whl /tmp/wheels/
RUN pip install --no-cache-dir uv \
    && for wheel in /tmp/wheels/pathway-*.whl; do \
        uv pip install --system --no-cache \
            "${wheel}[tests-core,xpack-llm]" 'nltk>=3.9,<4'; \
       done \
    && rm -rf /tmp/wheels

# Prepare public tokenizer data before the test container loses network access.
RUN python -m nltk.downloader -d /opt/nltk_data punkt punkt_tab \
    && python -c 'import tiktoken; tiktoken.get_encoding("cl100k_base")'

WORKDIR /tests
COPY integration_tests/ integration_tests/
COPY .github/ci/integration-tests/run.sh /usr/local/bin/run-integration-tests
ENTRYPOINT ["bash", "/usr/local/bin/run-integration-tests"]
