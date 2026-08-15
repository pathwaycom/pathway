#!/usr/bin/env python
# Copyright © 2026 Pathway

"""Writing pipeline used by the Pulsar output persistence test.

Streams the lines of a directory into a Pulsar topic, with filesystem
persistence enabled. Run as a subprocess so that the harness can SIGKILL it
in the middle of the writing and restart it against the same persistent
storage.
"""

import argparse

import pathway as pw


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--pstorage", required=True)
    parser.add_argument("--autocommit-ms", type=int, default=100)
    parser.add_argument("--snapshot-interval-ms", type=int, default=200)
    args = parser.parse_args()

    table = pw.io.fs.read(
        args.input_dir,
        format="plaintext",
        mode="streaming",
        autocommit_duration_ms=args.autocommit_ms,
        name="pulsar_writer_source",
    )
    pw.io.pulsar.write(table, args.uri, args.topic, format="plaintext")

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
