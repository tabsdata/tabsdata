#
# Copyright 2026 Tabsdata Inc.
#

"""Fan a per-target operation out across forked worker processes.

The `tdx-system` / `tdx-domain` batch verbs (`create --auto`, `destroy`,
`status`, `recreate`, the manifest create/sync paths) all run the *same*
per-target callable over many names. Provisioning is dominated by slow,
independent I/O — `docker pull`/`run`, cloud API calls, SQL over the wire — so
running the targets concurrently is a large wall-clock win.

Threads can't be used: the per-target work mutates process-global state
(`_setenv` bulk-updates ``os.environ``; the container backends export
``PORT`` / ``CONTAINER`` for the templated compose files and the shelled-out
clients to read). Two threads racing on ``os.environ`` would provision against
each other's values.

So each target runs in its own **forked** child instead. A fork gives the
child a private copy of ``os.environ`` — the existing worker mutates *its*
copy, never the parent's — so no backend needs to change. The worker callable
is inherited across the fork (no pickling), and each child's stdout+stderr
(Python prints *and* subprocess output, via an fd-level redirect) is captured
to a file so the parent can print each target's log in a stable order rather
than interleaved.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import pickle
import select
import sys
import tempfile
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path

from . import console

DEFAULT_MAX_WORKERS = 8
_MAX_WORKERS_ENV = "TDX_PARALLELISM"

type OnDone = Callable[[int, int, object, "Outcome"], None]


@dataclass
class Outcome:
    """The result of one forked worker: its return `value` (None if it raised
    or the process died), the `error` string (None on success), the child's
    captured `stdout` / `stderr` (kept apart so the parent can replay each to the
    matching stream), and its wall-clock `seconds`."""

    value: object
    error: str | None
    stdout: str
    stderr: str
    seconds: float = 0.0

    @property
    def ok(self) -> bool:
        return self.error is None


def max_workers(item_count: int, explicit: int | None = None) -> int:
    """Concurrency cap: an explicit value, else ``$TDX_PARALLELISM``, else
    `DEFAULT_MAX_WORKERS` — never more than the number of targets, never < 1."""
    chosen = explicit
    if chosen is None:
        env = os.environ.get(_MAX_WORKERS_ENV, "")
        chosen = int(env) if env.isdigit() and int(env) > 0 else DEFAULT_MAX_WORKERS
    return max(1, min(chosen, item_count)) if item_count else 0


def _redirect(fd_no: int, path: str):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.dup2(fd, fd_no)
    os.close(fd)
    return os.fdopen(fd_no, "w", buffering=1)


def _child(worker: Callable, item, paths: tuple[str, str, str]) -> None:
    """Runs in the forked child: redirect fd 1 & 2 (so Python prints *and* any
    subprocess output are captured, each to its own file), run ``worker(item)``
    against this process's private ``os.environ``, and pickle ``(value, error)``
    to the result file."""
    out_path, err_path, res_path = paths
    sys.stdout = _redirect(1, out_path)
    sys.stderr = _redirect(2, err_path)
    # The child inherited the parent's active console (its terminal handle is a
    # dup of the real stdout that survived the fork). Reset it so this worker's
    # brief output falls back to `print` — landing in the capture files above,
    # which the parent replays into the log — instead of racing straight to the
    # user's terminal.
    console.CONSOLE.reset_for_child()
    value = None
    error = None
    try:
        value = worker(item)
    except BaseException as exc:  # noqa: BLE001 - reported per target, never fatal
        error = str(exc) or exc.__class__.__name__
    sys.stdout.flush()
    sys.stderr.flush()
    try:
        payload = pickle.dumps((value, error))
    except Exception as exc:  # noqa: BLE001 - a non-picklable return is a bug here
        payload = pickle.dumps((None, f"unserializable worker result: {exc}"))
    Path(res_path).write_bytes(payload)


def _read(path: str) -> str:
    return Path(path).read_text() if os.path.exists(path) else ""


def _read_outcome(exitcode: int | None, paths: tuple[str, str, str]) -> Outcome:
    out_path, err_path, res_path = paths
    stdout, stderr = _read(out_path), _read(err_path)
    if os.path.exists(res_path):
        value, error = pickle.loads(Path(res_path).read_bytes())
        return Outcome(value, error, stdout, stderr)
    return Outcome(None, f"worker process died (exit code {exitcode})", stdout, stderr)


def _run_inline[I](
    items: list[I], worker: Callable[[I], object], on_done: OnDone | None
) -> list[Outcome]:
    """Sequential, in-process fallback: no fork, so a worker's ``os.environ``
    writes hit *this* process — safe only because nothing runs concurrently (it
    is exactly the old one-at-a-time behavior). Used when the concurrency limit
    collapses to 1 (a single target, or ``$TDX_PARALLELISM=1``); the latter lets
    a test / fork-hostile host opt out of forking entirely. The worker's own
    prints go straight to this process's streams, so `Outcome` carries none."""
    outcomes: list[Outcome] = []
    total = len(items)
    for i, item in enumerate(items):
        started = time.monotonic()
        value, error = None, None
        try:
            value = worker(item)
        except BaseException as exc:  # noqa: BLE001 - reported per target, never fatal
            error = str(exc) or exc.__class__.__name__
        outcome = Outcome(value, error, "", "", time.monotonic() - started)
        outcomes.append(outcome)
        if on_done is not None:
            on_done(i + 1, total, item, outcome)
    return outcomes


def run[I](
    items: Iterable[I],
    worker: Callable[[I], object],
    *,
    workers: int | None = None,
    on_done: OnDone | None = None,
) -> list[Outcome]:
    """Run ``worker(item)`` for each item, each in its own forked process, at
    most `workers` (see `max_workers`) at a time. Returns one `Outcome` per item
    in input order; a worker raising, or its process dying, becomes a failed
    `Outcome`, never stops the rest. With an effective limit of 1 the work runs
    inline (see `_run_inline`) — forking would buy no concurrency.

    `on_done(done, total, item, outcome)` is called in the parent as each target
    finishes — in *completion* order (fork) or input order (inline) — for live
    progress; `done` counts up to `total`."""
    items = list(items)
    if not items:
        return []
    total = len(items)
    limit = max_workers(total, workers)
    if limit <= 1:
        return _run_inline(items, worker, on_done)
    ctx = mp.get_context("fork")
    outcomes: list[Outcome | None] = [None] * total
    done = 0
    with tempfile.TemporaryDirectory(prefix="tdx-dispatch-") as tmp:
        pending = list(enumerate(items))
        Paths = tuple[str, str, str]
        running: dict[int, tuple[int, mp.process.BaseProcess, Paths, float]] = {}

        def fill() -> None:
            while pending and len(running) < limit:
                idx, item = pending.pop(0)
                paths = (
                    str(Path(tmp) / f"{idx}.out"),
                    str(Path(tmp) / f"{idx}.err"),
                    str(Path(tmp) / f"{idx}.res"),
                )
                sys.stdout.flush()
                sys.stderr.flush()
                proc = ctx.Process(target=_child, args=(worker, item, paths))
                proc.start()
                running[proc.sentinel] = (idx, proc, paths, time.monotonic())

        fill()
        while running:
            ready, _, _ = select.select(list(running), [], [])
            for fd in ready:
                idx, proc, paths, started = running.pop(fd)
                proc.join()
                outcome = _read_outcome(proc.exitcode, paths)
                outcome.seconds = time.monotonic() - started
                outcomes[idx] = outcome
                done += 1
                if on_done is not None:
                    on_done(done, total, items[idx], outcome)
            fill()
    return [o for o in outcomes if o is not None]
