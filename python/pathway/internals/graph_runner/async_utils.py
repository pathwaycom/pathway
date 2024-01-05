# Copyright © 2024 Pathway

from __future__ import annotations

import asyncio
import contextlib
import threading


@contextlib.contextmanager
def new_event_loop():
    event_loop = asyncio.new_event_loop()

    def target(event_loop: asyncio.AbstractEventLoop):
        try:
            event_loop.run_forever()
        finally:
            event_loop.close()

    thread = threading.Thread(target=target, args=(event_loop,))
    thread.start()

    try:
        yield event_loop
    finally:
        event_loop.call_soon_threadsafe(event_loop.stop)
        thread.join()
