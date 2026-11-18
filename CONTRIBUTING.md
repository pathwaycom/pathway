# Contributing to the Pathway Live Data Framework

Welcome! This guide is intended to help developers that are new to the community
to make contributions to the Pathway Live Data Framework project. Please be sure to also read our [code of conduct](CODE_OF_CONDUCT.md). We work hard to make this a welcoming community for all, and we're excited to have you on board!

## The basics:

* Use the [Issue Tracker](https://github.com/pathwaycom/pathway/issues) to
  report bugs, crashes, performance issues, etc... Please include as much detail
  as possible.

* [Discord](https://discord.com/invite/pathway) is our main gathering place - jump in and ask us anything!

* [Our forum](https://discord.com/channels/1042405378304004156/1044276516290314381) is a great
  venue to ask questions, start a design discussion, or post an RFC.

## Running the PR Python tests

The **Python tests** workflow runs a core pytest subset on pull requests to
`main`, on branch pushes, and on manual dispatch. It builds the Rust engine from
the checked-out commit and uses Python 3.11 on Linux. The first build can take
considerably longer than later runs, which reuse the Cargo cache.

To run the same tests locally, install the native Rust build prerequisites
(including Clang/LLVM and PostgreSQL development libraries), then run:

```bash
uv venv --python 3.11 .venv
source .venv/bin/activate
uv pip install 'maturin>=1.4,<2'
maturin develop --locked --profile dev --uv --extras tests-core
python -m pytest -c pytest-pr.ini -n 2 --dist worksteal
```

The selected modules are listed in `pytest-pr.ini`. They cover schemas, types,
expressions, operators, joins, reducers, and SQL without requiring external
services or repository secrets. This subset complements the full package tests.
CI uploads a JUnit report as the `pytest-core-results` artifact even when pytest
fails. To try a change in a fork, push it to a branch in that fork and inspect
the **Python tests** run in its Actions tab, or open a PR against the fork's
`main` branch to see the check on the PR.

## Contributing Code

We are eager to build the Pathway Live Data Framework together with the community and are excited to have you here!

Important: Before you contribute any changes to our repositories (make a Pull Request) please get in touch with us and explain what you want to work on. You can do so by filing an issue, or asking us on Discord. That way you can get coding advice and speedier reviews. If you start to work on an issue before hearing back from a Pathway core team engineer there is a chance that we will not be able to accept your changes - something we all want to avoid!

We use a standard GitHub fork + pull request model for merging and reviewing
changes. New pull requests should be made against the main upstream branch.
After a pull request is opened it will be reviewed, and merged after
passing continuous integration tests and being accepted by a project or
sub-system maintainer.

We maintain a [Changelog](https://github.com/pathwaycom/pathway/blob/main/CHANGELOG.md) where all notable changes to this project are documented. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

We ask that developers sign our [contributor license
agreement](https://cla-assistant.io/pathwaycom/pathway). The
process of signing the CLA is automated, and you'll be prompted with instructions
the first time you submit a pull request to the project.
