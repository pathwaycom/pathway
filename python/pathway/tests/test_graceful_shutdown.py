# Copyright © 2026 Pathway

import subprocess
import sys

_SIGTERM_SCRIPT = """
import os, signal, threading, time
import pathway as pw


class Gen(pw.io.python.ConnectorSubject):
    def run(self):
        i = 0
        while True:
            self.next(a=i)
            i += 1
            time.sleep(0.001)


class InSchema(pw.Schema):
    a: int


t = pw.io.python.read(Gen(), schema=InSchema, autocommit_duration_ms=50)
pw.io.null.write(t)


def kill_self():
    time.sleep(2)
    os.kill(os.getpid(), signal.SIGTERM)


threading.Thread(target=kill_self, daemon=True).start()
try:
    pw.run(monitoring_level=pw.MonitoringLevel.NONE)
except SystemExit as e:
    print("GRACEFUL", e.code, flush=True)
    raise
"""


def test_sigterm_graceful_shutdown():
    # SIGTERM during pw.run must unwind the engine gracefully (SystemExit
    # with the conventional 128+15 code) instead of killing the process on
    # the spot. Run in a subprocess so that a regression terminates the
    # child (returncode -SIGTERM), not the test runner.
    result = subprocess.run(
        [sys.executable, "-c", _SIGTERM_SCRIPT],
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert "GRACEFUL 143" in result.stdout, (result.stdout, result.stderr)
    assert result.returncode == 143, (result.returncode, result.stderr)
