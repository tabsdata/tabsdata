#
# Copyright 2026 Tabsdata Inc.
#

"""Liveness signalling via a logging side-effect handler.

Per `docs/01-CommandBasics.md`, every command must signal liveness via
its ``--liveness`` file at least once a minute while it is making
progress. We piggyback on the standard ``logging`` framework: a
:class:`LivenessLogHandler` attached to the **root** logger touches
the configured file every time any logger in the process emits a
record. Each record is itself evidence of forward progress, so
"something logged" and "the process is alive" collapse into one
signal — no separate heartbeat thread.

Attaching at root (rather than ``tabsdatak``) means third-party
libraries used inside commands or plugins — ``sqlalchemy``,
``polars``, ``boto3``/``botocore``, etc. — also tick liveness when
they log. That's deliberate: an S3 download in a user ``read_in`` is
forward progress just as much as one of our own log lines, so we want
its records to count.

For external work that doesn't naturally log from the Python side
(subprocess output), pipe it through ``logging`` — see
``TransporterHandler.run`` (in :mod:`tabsdatak.common._transporter.handler`)
for the canonical pattern.

User-defined plugin code (``SrcPlugin.read_in`` / ``DestPlugin.write_out``
and the user transformer body) that doesn't log AND doesn't use any
``logging``-aware library is a known gap — a long silent ``read_in``
will not tick liveness. We accept that for now; the fix is to
instrument those call sites separately.
"""

from __future__ import annotations

import logging
from pathlib import Path


def beat(path: Path) -> None:
    """Emit one liveness signal at ``path``.

    Semantically: "I'm still here". Mechanism (``Path.touch``) is an
    implementation detail; callers depend on the semantic.

    Raises:
        FileNotFoundError: if the parent directory does not exist.
    """
    path.touch()


class LivenessLogHandler(logging.Handler):
    """Logging handler that touches a liveness file on every record.

    Side-effect only: it does not write the record anywhere — it just
    treats the act of emitting as evidence the process is making
    progress, and touches the configured file.

    ``Path.touch`` is a cheap syscall (microseconds on a local
    filesystem) and idempotent, so we touch unconditionally on every
    record rather than throttling or coordinating between threads.

    Errors during touch (parent dir removed, disk full, …) are routed
    through :meth:`logging.Handler.handleError` rather than re-raised.
    Raising from a handler would unwind whatever the command was doing,
    and the pod-runner's external mtime-timeout is the authoritative
    stale-detector in production anyway.
    """

    def __init__(self, path: Path) -> None:
        # NOTSET = no level filtering at the handler — the attached
        # logger decides what reaches us; we count all records.
        super().__init__(level=logging.NOTSET)
        self._path = path
        # Touch immediately on construction so the runner sees a fresh
        # file even before any log record fires. Construction-time
        # errors are swallowed; the next emit() will surface them via
        # handleError.
        try:
            beat(self._path)
        except Exception:
            pass

    def emit(self, record: logging.LogRecord) -> None:
        try:
            beat(self._path)
        except Exception:
            self.handleError(record)


def install_liveness_handler(path: Path) -> LivenessLogHandler:
    """Install a :class:`LivenessLogHandler` on the root logger.

    Idempotent: any pre-existing :class:`LivenessLogHandler` on the
    root logger is removed first. Returns the installed handler so
    callers can inspect it (e.g. tests) or remove it explicitly.

    Side effect: the root logger is bumped to DEBUG if it was at a
    stricter level than DEBUG. Otherwise an INFO/DEBUG record emitted
    by any logger in the process would be dropped at its emitting
    logger before reaching us (the default root level is WARNING and
    most library loggers inherit from root). Callers that want
    different verbosity can re-set the level after install.
    """
    target = logging.getLogger()  # root
    for h in list(target.handlers):
        if isinstance(h, LivenessLogHandler):
            target.removeHandler(h)
    handler = LivenessLogHandler(path)
    target.addHandler(handler)
    if target.level == logging.NOTSET or target.level > logging.DEBUG:
        target.setLevel(logging.DEBUG)
    return handler
