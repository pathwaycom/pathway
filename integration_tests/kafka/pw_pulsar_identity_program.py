#!/usr/bin/env python
# Copyright © 2026 Pathway

"""Identity pipeline used by the Pulsar persistence tests.

Reads plaintext messages from a Pulsar topic and appends them to a jsonlines
file, with filesystem persistence enabled. Run as a subprocess so that the
harness can SIGKILL it at an arbitrary moment and restart it against the same
persistent storage.
"""

import argparse

import pathway as pw


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--pstorage", required=True)
    parser.add_argument("--autocommit-ms", type=int, default=100)
    parser.add_argument("--snapshot-interval-ms", type=int, default=1000)
    args = parser.parse_args()

    table = pw.io.pulsar.read(
        args.uri,
        args.topic,
        format="plaintext",
        autocommit_duration_ms=args.autocommit_ms,
        name="pulsar_source",
    )
    pw.io.jsonlines.write(table, args.output)

    persistence_config = pw.persistence.Config(
        pw.persistence.Backend.filesystem(args.pstorage),
        snapshot_interval_ms=args.snapshot_interval_ms,
    )
    pw.run(
        persistence_config=persistence_config,
        monitoring_level=pw.MonitoringLevel.NONE,
    )


if __name__ == "__main__":
    main()
