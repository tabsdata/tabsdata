#
# Copyright 2026 Tabsdata Inc.
#

"""Split a command's output into a *brief* console stream and a *detailed* log.

The tdx-system / tdx-domain mutating verbs are chatty — docker pulls, readiness
waits, SQL DDL. Run interactively that firehose buries the one thing the user
wants: is it working, and how far along is it.

So those verbs run under :func:`activate`, which opens a timestamped
``/tmp/<role>-<name>-<YYYYMMDD>T<hhmmss>.log`` and redirects fd 1 & 2 into it
(capturing every ``print`` and every subprocess line as detail), while keeping a
handle to the real terminal for brief, colored progress emitted through
:func:`out` / :func:`good` / :func:`bad` / :func:`warn`. The log lives in ``/tmp``
(not the descriptor store) so the store dir holds only descriptors.

When no verb is active (a unit test calling a verb function directly) the brief
helpers just ``print`` to stdout, so the output contract is unchanged.
"""

from __future__ import annotations

import os
import sys
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import TextIO

_RED = "\033[31m"
_GREEN = "\033[32m"
_YELLOW = "\033[33m"
_RESET = "\033[0m"


def prune_logs(log_dir: str | Path, max_age_days: float) -> None:
    """Delete ``*.log`` files under `log_dir` older than `max_age_days`
    (best-effort — a dedicated log dir, so this only touches our own files)."""
    d = Path(log_dir)
    if not d.is_dir():
        return
    cutoff = time.time() - max_age_days * 86400
    for p in d.glob("*.log"):
        try:
            if p.stat().st_mtime < cutoff:
                p.unlink()
        except OSError:  # racing run, permissions — skip
            pass


def _unique_log_path(log_dir: Path, name_prefix: str = "") -> Path:
    stamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    path = log_dir / f"{name_prefix}{stamp}.log"
    n = 1
    while path.exists():
        path = log_dir / f"{name_prefix}{stamp}-{n}.log"
        n += 1
    return path


class _Console:
    """Process-wide singleton (see module-level ``CONSOLE``). Inactive by
    default; :meth:`activate` turns on the log/terminal split for one verb."""

    def __init__(self) -> None:
        self.active = False
        self.log_path: Path | None = None
        self._term: TextIO | None = None
        self._term_tty = False
        self._quiet = False

    @contextmanager
    def activate(self, log_dir: str | Path, name_prefix: str = "", quiet: bool = False):
        """Split detail (a ``/tmp`` log) from brief terminal progress. In `quiet`
        mode the terminal gets neither the ``>>> detailed log`` banner nor the
        brief ``out``/``good``/… lines (they still go to the log) — only
        :meth:`result` reaches stdout, so ``--out json`` prints clean JSON. The
        banner goes to stderr instead, so a human still sees where the log is."""
        if self.active:
            yield self
            return
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        path = _unique_log_path(log_dir, name_prefix)

        sys.stdout.flush()
        sys.stderr.flush()
        term_fd = os.dup(1)
        err_fd = os.dup(2)
        term = os.fdopen(term_fd, "w", buffering=1, closefd=False)
        term_tty = os.isatty(term_fd)

        log_fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        os.dup2(log_fd, 1)
        os.dup2(log_fd, 2)
        os.close(log_fd)
        saved_out, saved_err = sys.stdout, sys.stderr
        sys.stdout = os.fdopen(1, "w", buffering=1, closefd=False)
        sys.stderr = os.fdopen(2, "w", buffering=1, closefd=False)

        self.active = True
        self.log_path = path
        self._term = term
        self._term_tty = term_tty
        self._quiet = quiet
        banner = f">>> detailed log: {path}\n"
        if quiet:
            os.write(err_fd, banner.encode())
        else:
            term.write(banner)
            term.flush()
        try:
            yield self
        finally:
            try:
                sys.stdout.flush()
                sys.stderr.flush()
            except Exception:  # noqa: BLE001 - restore must not be masked
                pass
            os.dup2(term_fd, 1)
            os.dup2(err_fd, 2)
            sys.stdout, sys.stderr = saved_out, saved_err
            self.active = False
            self._term = None
            self._quiet = False
            try:
                term.flush()
                term.close()
                os.close(err_fd)
            except OSError:
                pass

    def stream(self) -> TextIO:
        return self._term if (self.active and self._term) else sys.stdout

    def err_stream(self) -> TextIO:
        return self._term if (self.active and self._term) else sys.stderr

    def _paint(self, text: str, color: str | None, tty: bool) -> str:
        return f"{color}{text}{_RESET}" if (color and tty) else text

    def _write(self, text: str, color: str | None) -> None:
        if self.active and self._term is not None:
            if not self._quiet:
                self._term.write(self._paint(text, color, self._term_tty) + "\n")
                self._term.flush()
            try:
                sys.stdout.write(text + "\n")
                sys.stdout.flush()
            except Exception:  # noqa: BLE001 - the terminal line already landed
                pass
        else:
            tty = bool(getattr(sys.stdout, "isatty", lambda: False)())
            print(self._paint(text, color, tty))

    def result(self, text: str) -> None:
        """Machine-readable output (``--out json``): always to the real stdout —
        the terminal fd when active (bypassing the log), else stdout."""
        stream = self._term if (self.active and self._term is not None) else sys.stdout
        stream.write(text + "\n")
        stream.flush()

    def out(self, text: str = "") -> None:
        self._write(text, None)

    def good(self, text: str) -> None:
        self._write(text, _GREEN)

    def bad(self, text: str) -> None:
        self._write(text, _RED)

    def warn(self, text: str) -> None:
        self._write(text, _YELLOW)


CONSOLE = _Console()


def activate(log_dir: str | Path, name_prefix: str = "", quiet: bool = False):
    return CONSOLE.activate(log_dir, name_prefix, quiet)


def stream() -> TextIO:
    return CONSOLE.stream()


def err_stream() -> TextIO:
    return CONSOLE.err_stream()


def result(text: str) -> None:
    CONSOLE.result(text)


def out(text: str = "") -> None:
    CONSOLE.out(text)


def good(text: str) -> None:
    CONSOLE.good(text)


def bad(text: str) -> None:
    CONSOLE.bad(text)


def warn(text: str) -> None:
    CONSOLE.warn(text)
