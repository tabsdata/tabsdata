#
# Copyright 2026 Tabs Data Inc.
#

"""Liveness file utilities.

Per `docs/01-CommandBasics.md`, every command must signal liveness via
its ``--liveness`` file at least once a minute while it is making
progress. The :class:`Liveness` context manager runs a daemon thread
that emits a heartbeat on a configurable interval; :func:`beat` covers
one-shot use.

If beating fails persistently (e.g. parent directory removed, disk
issue), the context manager records the failure and re-raises it from
``stop()`` / ``__exit__`` — see :class:`LivenessFailureError` and the
``TD_LIVENESS_MAX_FAILED_BEATS`` env var below.
"""

from __future__ import annotations

import logging
import os
import threading
from pathlib import Path

logger = logging.getLogger(__name__)


# How many consecutive beat failures we tolerate before flagging the
# liveness as stale. Reasonable default: 5 (at the recommended 30s
# interval that's 2.5 min of grace, comfortably past any transient
# filesystem hiccup). Overridable per-instance via the
# `max_failed_beats` constructor argument, or globally via the
# `TD_LIVENESS_MAX_FAILED_BEATS` env var.
_DEFAULT_MAX_FAILED_BEATS: int = 5
_MAX_FAILED_BEATS_ENV: str = "TD_LIVENESS_MAX_FAILED_BEATS"


class LivenessFailureError(RuntimeError):
    """A liveness beat failed too many consecutive times."""


def _max_failed_beats_from_env() -> int:
    raw = os.environ.get(_MAX_FAILED_BEATS_ENV)
    if raw is None:
        return _DEFAULT_MAX_FAILED_BEATS
    try:
        n = int(raw)
    except ValueError:
        logger.warning(
            "%s=%r is not an integer; falling back to default %d",
            _MAX_FAILED_BEATS_ENV,
            raw,
            _DEFAULT_MAX_FAILED_BEATS,
        )
        return _DEFAULT_MAX_FAILED_BEATS
    if n < 1:
        logger.warning(
            "%s=%d is below 1; falling back to default %d",
            _MAX_FAILED_BEATS_ENV,
            n,
            _DEFAULT_MAX_FAILED_BEATS,
        )
        return _DEFAULT_MAX_FAILED_BEATS
    return n


def beat(path: Path) -> None:
    """Emit one liveness heartbeat at ``path``.

    Semantically: "I'm still here". The mechanism (file mtime bump via
    :meth:`Path.touch`) is an implementation detail; callers should rely
    on the semantic, not the underlying syscall.

    Raises:
        FileNotFoundError: if the parent directory does not exist.
    """
    path.touch()


class Liveness:
    """Periodically emits a liveness heartbeat from a daemon thread.

    Used exclusively as a context manager::

        with Liveness(path, interval=30.0):
            ... long-running work ...

    ``__enter__`` starts a daemon thread that emits a beat at ``path``
    every ``interval`` seconds. ``__exit__`` signals the thread to
    stop and joins it; it runs whether the ``with`` body completes
    normally or raises.

    If beat failures recur consecutively past ``max_failed_beats``, the
    daemon thread records a :class:`LivenessFailureError` and exits.
    The error is re-raised from ``__exit__``. When the ``with`` body
    has already raised its own exception, the liveness failure is
    logged at ERROR and the body's exception is allowed to propagate —
    the user's real error is more useful, and the liveness failure
    remains visible in the log.
    """

    def __init__(
        self,
        path: Path,
        interval: float = 30.0,
        max_failed_beats: int | None = None,
    ) -> None:
        self._path = path
        self._interval = interval
        self._max_failed_beats = (
            max_failed_beats
            if max_failed_beats is not None
            else _max_failed_beats_from_env()
        )
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._failure: LivenessFailureError | None = None

    def _start(self) -> None:
        if self._thread is not None:
            return
        beat(self._path)
        self._stop_event.clear()
        self._failure = None
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _stop(self) -> None:
        if self._thread is None:
            return
        self._stop_event.set()
        self._thread.join()
        self._thread = None
        failure = self._failure
        self._failure = None
        if failure is not None:
            raise failure

    def _run(self) -> None:
        # A failed `beat` (e.g. parent dir deleted mid-run, filesystem
        # hiccup) must not kill the background thread silently;
        # otherwise the whole program continues running with liveness
        # silently dead. Below the threshold we log and continue;
        # at/over the threshold we record a LivenessFailureError that
        # stop() will re-raise. The pod-runner's external mtime-timeout
        # is the authoritative stale-detector in production; the
        # internal mechanism here is for local diagnostics and a
        # well-defined Python-side failure surface.
        consecutive_failures = 0
        while not self._stop_event.wait(self._interval):
            try:
                beat(self._path)
            except Exception as exc:
                consecutive_failures += 1
                if consecutive_failures >= self._max_failed_beats:
                    logger.error(
                        "liveness beat failed %d consecutive times at %s; "
                        "marking stale: %s",
                        consecutive_failures,
                        self._path,
                        exc,
                    )
                    self._failure = LivenessFailureError(
                        f"liveness beat failed {consecutive_failures} "
                        f"consecutive times at {self._path}: {exc}"
                    )
                    return
                logger.warning(
                    "liveness beat failed (attempt %d/%d) at %s: %s",
                    consecutive_failures,
                    self._max_failed_beats,
                    self._path,
                    exc,
                )
            else:
                consecutive_failures = 0

    def __enter__(self) -> "Liveness":
        self._start()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> bool | None:
        try:
            self._stop()
        except LivenessFailureError as exc:
            if exc_type is None:
                raise
            # The body already raised; its exception is more useful.
            # Log the liveness failure at ERROR so it stays visible.
            logger.error(
                "liveness failure suppressed by in-flight exception (%s): %s",
                exc_type.__name__,
                exc,
            )
