#
# Copyright 2026 Tabs Data Inc.
#

"""Liveness file utilities.

Per `docs/01-CommandBasics.md`, every command must touch its
``--liveness`` file at least once a minute while it is making progress.
The :class:`Liveness` context manager runs a daemon thread that does so
on a configurable interval; :func:`touch` covers one-shot use.
"""

from __future__ import annotations

import threading
from pathlib import Path


def touch(path: Path) -> None:
    """Create the file at ``path`` or update its mtime.

    Raises:
        FileNotFoundError: if the parent directory does not exist.
    """
    path.touch()


class Liveness:
    """Periodically touches a liveness file from a daemon thread.

    Usage::

        with Liveness(path, interval=30.0):
            ... long-running work ...

    ``start()`` is idempotent (a second call is a no-op while running);
    ``stop()`` is also idempotent. Both ``__enter__``/``__exit__`` and
    explicit ``start()``/``stop()`` are supported, and ``__exit__`` is
    invoked on exceptions inside the ``with`` body.
    """

    def __init__(self, path: Path, interval: float = 30.0) -> None:
        self._path = path
        self._interval = interval
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        touch(self._path)
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._thread is None:
            return
        self._stop_event.set()
        self._thread.join()
        self._thread = None

    def _run(self) -> None:
        while not self._stop_event.wait(self._interval):
            touch(self._path)

    def __enter__(self) -> "Liveness":
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.stop()
