#
# Copyright 2026 Tabsdata Inc.
#

"""Split a command's output into a *brief* console stream and a *detailed* log.

The `tdx-system` / `tdx-domain` verbs are chatty — docker pulls, per-container
readiness waits, SQL DDL, forked-worker logs. Run interactively that firehose
buries the one thing the user wants: is it working, and how far along is it.

So the mutating verbs run under :func:`activate`, which:

  * opens a timestamped ``<YYYYMMDD>T<hhmmss>.log`` in the store directory and
    redirects the process's fd 1 & 2 into it — so every ``print`` AND every
    subprocess (docker / uv) line is captured as *detail*, with no change to the
    dozens of existing detail ``print`` calls, and
  * keeps a handle to the real terminal for *brief* output — progress, colored
    success / failure, and counters — emitted through :func:`out` / :func:`good`
    / :func:`bad` / :func:`warn` / :func:`progress`.

Brief lines are mirrored (uncolored) into the log too, so the log is a complete
record. Color is emitted only to a TTY (mirrors ``errors._red``).

When no verb is active (the default — e.g. a unit test calling a verb function
directly), the brief helpers just ``print`` to stdout as before, so the CLI's
output contract is unchanged outside the redirect.
"""

from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

_RED = "\033[31m"
_GREEN = "\033[32m"
_YELLOW = "\033[33m"
_RESET = "\033[0m"


def _unique_log_path(log_dir: Path) -> Path:
    """``<log_dir>/<YYYYMMDD>T<hhmmss>.log``, with a ``-N`` suffix if a run in
    the same second already claimed the name (so a log is never clobbered)."""
    stamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    path = log_dir / f"{stamp}.log"
    n = 1
    while path.exists():
        path = log_dir / f"{stamp}-{n}.log"
        n += 1
    return path


class _Console:
    """Process-wide singleton (see the module-level `CONSOLE`). Inactive by
    default; :meth:`activate` turns on the log-file / terminal split for the
    duration of one verb."""

    def __init__(self) -> None:
        self.active = False
        self.log_path: Path | None = None
        self._term = None  # writable stream to the real terminal (brief output)
        self._term_tty = False

    @contextmanager
    def activate(self, log_dir: str | Path):
        """Redirect detail (fd 1 & 2) into a fresh timestamped log under
        `log_dir`, keeping the real terminal for brief output. Restores both on
        exit. Re-entrant: a nested `activate` is a no-op that reuses the log."""
        if self.active:
            yield self
            return
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        path = _unique_log_path(log_dir)

        sys.stdout.flush()
        sys.stderr.flush()
        term_fd = os.dup(1)  # the real stdout — brief output + restores fd 1
        err_fd = os.dup(2)  # the real stderr — restores fd 2
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
        term.write(f">>> detailed log: {path}\n")
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
            try:
                term.flush()
                term.close()
                os.close(err_fd)
            except OSError:
                pass

    def stream(self):
        """The stream user-facing text (help / usage) should go to: the real
        terminal while active (so it isn't swallowed by the detail redirect),
        else the current stdout."""
        return self._term if (self.active and self._term) else sys.stdout

    def err_stream(self):
        """Like :meth:`stream`, for errors: the real terminal while active, else
        the current stderr."""
        return self._term if (self.active and self._term) else sys.stderr

    def reset_for_child(self) -> None:
        """Drop the terminal handle after a fork + fd redirect (see
        `dispatch._child`), so brief output in the worker falls back to `print`
        (which the worker has redirected to its own capture file) rather than
        writing to the parent's real terminal via the inherited handle. The
        parent keeps owning the terminal and the log."""
        self.active = False
        self._term = None

    def _paint(self, text: str, color: str | None, tty: bool) -> str:
        return f"{color}{text}{_RESET}" if (color and tty) else text

    def _write(self, text: str, color: str | None) -> None:
        if self.active:
            self._term.write(self._paint(text, color, self._term_tty) + "\n")
            self._term.flush()
            try:  # mirror an uncolored copy into the log so it stays complete
                sys.stdout.write(text + "\n")
                sys.stdout.flush()
            except Exception:  # noqa: BLE001 - the terminal line already landed
                pass
        else:
            tty = bool(getattr(sys.stdout, "isatty", lambda: False)())
            print(self._paint(text, color, tty))

    def out(self, text: str = "") -> None:
        """A neutral brief line (or a blank line)."""
        self._write(text, None)

    def good(self, text: str) -> None:
        """A brief success line (green on a TTY)."""
        self._write(text, _GREEN)

    def bad(self, text: str) -> None:
        """A brief failure line (red on a TTY)."""
        self._write(text, _RED)

    def warn(self, text: str) -> None:
        """A brief caution line (yellow on a TTY)."""
        self._write(text, _YELLOW)

    def progress(
        self,
        done: int,
        total: int,
        ok: bool,
        mark: str,
        name: str,
        seconds: float,
        detail: str = "",
    ) -> None:
        """One live per-target line as a batch item completes: ``[k/N] <mark>
        name (1.2s): detail`` — green when `ok`, red otherwise. The counter is
        right-padded to the width of `total` so the lines stay column-aligned."""
        width = len(str(total))
        line = f"  [{done:>{width}}/{total}] {mark}  {name} ({seconds:.1f}s)"
        if detail:
            line += f": {detail}"
        (self.good if ok else self.bad)(line)


CONSOLE = _Console()


def activate(log_dir: str | Path):
    return CONSOLE.activate(log_dir)


def stream():
    return CONSOLE.stream()


def err_stream():
    return CONSOLE.err_stream()


def out(text: str = "") -> None:
    CONSOLE.out(text)


def good(text: str) -> None:
    CONSOLE.good(text)


def bad(text: str) -> None:
    CONSOLE.bad(text)


def warn(text: str) -> None:
    CONSOLE.warn(text)


def progress(
    done: int,
    total: int,
    ok: bool,
    mark: str,
    name: str,
    seconds: float,
    detail: str = "",
) -> None:
    CONSOLE.progress(done, total, ok, mark, name, seconds, detail)
