#
# Copyright 2026 Tabsdata Inc.
#

"""Run a delegated verb against many targets in parallel.

The multi-target selector is ``--type`` for a system ``create`` (one system per
type, named after it), ``--sys`` for a domain ``create`` (one domain per system,
named after it), and ``--sys`` / ``--domain`` for every other verb (each stored
system / domain). Each accepts a single name, a comma-separated list, or ``ALL``;
``--sys-prefix P`` / ``--domain-prefix P`` prepends ``P_`` to the created names.
Any list / ALL (or a lone ``--report`` / ``--fail-fast``) puts the verb in batch
mode: each
target runs as its own ``python -m …`` child process — full isolation, safe with
the fork-/thread-unsafe native DB clients — with bounded concurrency, the
heaviest weights (see ``Backend.weight``) launched first. A system ``create``
batch also hands each child a *disjoint* pool of pre-found free ports
(``--ports``) so two parallel creates never pick the same port.

Per target the console shows a live ``▶ <verb> <name>`` line and an ``[i/N] … OK|FAIL
(Ns)`` line (green/red) with the child's output echoed; the run ends with an
``ok/failed in Ns`` summary. ``--fail-fast`` stops launching once a target
fails; otherwise every target is attempted. ``--report FILE`` additionally
writes a JSON record of the whole run.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

from tabsdatax.itest.tdksystems2 import (
    cli,
    console,
    constants,
    descriptor,
    store,
    systems as _systems,
)
from tabsdatax.itest.tdksystems2.errors import TdxError
from tabsdatax.itest.tdksystems2.shell import find_free_ports


@dataclass
class Task:
    name: str
    weight: int
    # one or more child invocations (each a full ``[verb, *args]`` argv) run in
    # order; a manifest domain is a create -> create-model -> create-data chain.
    steps: list[list[str]]
    status: str = "pending"  # pending | ok | fail | skipped
    seconds: float = 0.0
    returncode: int | None = None
    output: str = ""


def _selector(tool: str) -> str:
    return constants.OPT_DOMAIN if tool == constants.DOM_TOOL else constants.OPT_SYS


def _create_selector(tool: str) -> str:
    """The flag that drives a batch ``create``: ``--type`` (system — one system
    per type) or ``--sys`` (domain — one domain per system)."""
    return constants.OPT_SYS if tool == constants.DOM_TOOL else constants.OPT_TYPE


def _batch_selector(tool: str, verb: str) -> str:
    return _create_selector(tool) if verb == constants.V_CREATE else _selector(tool)


def _module(tool: str) -> str:
    name = "tdx_domain2" if tool == constants.DOM_TOOL else "tdx_system2"
    return f"tabsdatax.itest.tdksystems2.{name}"


def _parallelism() -> int:
    raw = os.environ.get(constants.ENV_PARALLELISM, "").strip()
    if raw.isdigit() and int(raw) >= 1:
        return int(raw)
    return constants.DEFAULT_PARALLELISM


def weight_for(slug: str) -> int:
    try:
        return int(getattr(_systems.get_class(slug), "weight", 1))
    except TdxError:
        return 1


def _names(sel: str) -> list[str]:
    return [n.strip() for n in sel.split(",") if n.strip()]


def port_pools(n: int) -> list[list[int]]:
    """`n` disjoint pools of ``PORTS_PER_SYSTEM`` free ports (all found at once so
    the pools never overlap), one per parallel system create."""
    if n <= 0:
        return []
    per = constants.PORTS_PER_SYSTEM
    ports = find_free_ports(n * per)
    return [ports[i * per : (i + 1) * per] for i in range(n)]


def _is_all(sel: str) -> bool:
    names = _names(sel)
    return len(names) == 1 and names[0].upper() == constants.TARGET_ALL


def is_batch(tool: str, verb: str, rest: list[str]) -> bool:
    """Whether the verb targets many systems/domains (comma list or ALL) or a
    report/fail-fast was requested (so even one target runs through here). The
    selector is ``--type`` for a system create, ``--sys`` for a domain create,
    and ``--sys``/``--domain`` for every other verb."""
    sel = cli.peek(rest, _batch_selector(tool, verb))
    if sel is None:
        return False
    if len(_names(sel)) > 1 or _is_all(sel):
        return True
    return cli.peek(rest, constants.OPT_REPORT) is not None or (
        constants.OPT_FAIL_FAST in rest
    )


def _stored(role: str) -> list[descriptor.SystemInfo | descriptor.DomainInfo]:
    store_dir = (
        store.domain_dir(None)
        if role == constants.ROLE_DOMAIN
        else (store.system_dir(None))
    )
    out: list[descriptor.SystemInfo | descriptor.DomainInfo] = []
    if not store_dir.is_dir():
        return out
    for path in sorted(p for p in store_dir.iterdir() if p.is_file()):
        if not descriptor.is_descriptor(path):
            continue
        try:
            info = descriptor.load(path)
        except TdxError:
            continue
        if info.role == role:
            out.append(info)
    return out


def weight_of_stored(role: str, name: str) -> int:
    for info in _stored(role):
        if info.name == name:
            return weight_for(info.system)
    return 1


def _strip(argv: list[str], value_flags: set[str], bool_flags: set[str]) -> list[str]:
    """`argv` with the given value flags (``--flag V`` / ``--flag=V``) and bare
    bool flags removed."""
    out: list[str] = []
    i = 0
    while i < len(argv):
        tok = argv[i]
        if tok in value_flags:
            i += 2
            continue
        if any(tok.startswith(f"{vf}=") for vf in value_flags):
            i += 1
            continue
        if tok in bool_flags:
            i += 1
            continue
        out.append(tok)
        i += 1
    return out


_BATCH_ONLY = {constants.OPT_FAIL_FAST}
_BATCH_VALUE = {constants.OPT_REPORT}


def _expand(tool: str, verb: str, rest: list[str]) -> list[Task]:
    """The per-target Tasks for a batch verb (each carries the child's argv).

    ``create`` iterates the create selector (``--type`` for a system, one system
    per type named after it; ``--sys`` for a domain, one domain per system named
    after it). The create command applies ``--sys-prefix`` / ``--domain-prefix``
    itself, so it is passed straight through to each child (unprefixed base name
    in the argv); the display / report name is the prefixed one. Other verbs
    iterate the stored ``--sys`` / ``--domain`` targets."""
    tasks: list[Task] = []
    if verb == constants.V_CREATE and tool == constants.SYS_TOOL:
        prefix = cli.peek(rest, constants.OPT_SYS_PREFIX)
        sel = cli.peek(rest, constants.OPT_TYPE) or ""
        types = _systems.all_slugs() if _is_all(sel) else _names(sel)
        passthrough = _strip(
            rest,
            {constants.OPT_TYPE, constants.OPT_SYS, constants.OPT_PORTS, *_BATCH_VALUE},
            _BATCH_ONLY,
        )
        pools = port_pools(len(types))
        for typ, pool in zip(types, pools):
            ports = [constants.OPT_PORTS, ",".join(str(p) for p in pool)]
            argv = [
                constants.V_CREATE,
                constants.OPT_TYPE,
                typ,
                constants.OPT_SYS,
                typ,
                *ports,
                *passthrough,
            ]
            tasks.append(Task(cli.apply_prefix(typ, prefix), weight_for(typ), [argv]))
        return tasks
    if verb == constants.V_CREATE and tool == constants.DOM_TOOL:
        prefix = cli.peek(rest, constants.OPT_DOMAIN_PREFIX)
        sel = cli.peek(rest, constants.OPT_SYS) or ""
        sysnames = (
            [s.name for s in _stored(constants.ROLE_SYSTEM)]
            if _is_all(sel)
            else _names(sel)
        )
        passthrough = _strip(
            rest, {constants.OPT_SYS, constants.OPT_DOMAIN, *_BATCH_VALUE}, _BATCH_ONLY
        )
        for sysn in sysnames:
            argv = [
                constants.V_CREATE,
                constants.OPT_SYS,
                sysn,
                constants.OPT_DOMAIN,
                sysn,
                *passthrough,
            ]
            tasks.append(
                Task(
                    cli.apply_prefix(sysn, prefix),
                    weight_of_stored(constants.ROLE_SYSTEM, sysn),
                    [argv],
                )
            )
        return tasks
    # non-create verbs: ALL = every stored target, else the named ones
    selector = _selector(tool)
    sel = cli.peek(rest, selector) or ""
    role = (
        constants.ROLE_DOMAIN if tool == constants.DOM_TOOL else constants.ROLE_SYSTEM
    )
    passthrough = _strip(rest, {selector, *_BATCH_VALUE}, _BATCH_ONLY)
    target_names = [i.name for i in _stored(role)] if _is_all(sel) else _names(sel)
    for name in target_names:
        tasks.append(
            Task(
                name,
                weight_of_stored(role, name),
                [[verb, selector, name, *passthrough]],
            )
        )
    return tasks


def _write_report(
    path: str, tool: str, verb: str, parallelism: int, total: float, tasks: list[Task]
) -> None:
    report = {
        "tool": tool,
        "verb": verb,
        "started": descriptor.now_stamp(),
        "parallelism": parallelism,
        "total_seconds": round(total, 2),
        "ok": sum(1 for t in tasks if t.status == "ok"),
        "failed": sum(1 for t in tasks if t.status == "fail"),
        "skipped": sum(1 for t in tasks if t.status == "skipped"),
        "tasks": [
            {
                "name": t.name,
                "weight": t.weight,
                "status": t.status,
                "seconds": t.seconds,
                "returncode": t.returncode,
                "output": t.output,
            }
            for t in tasks
        ],
    }
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(report, indent=2), encoding="utf-8")
    console.out(f">>> wrote run report to {path}")


def run(tool: str, verb: str, rest: list[str]) -> int:
    tasks = _expand(tool, verb, rest)
    role = "domain" if tool == constants.DOM_TOOL else "system"
    if not tasks:
        console.out(f"no {role}s matched — nothing to do")
        return 0
    return execute(
        tool,
        verb,
        tasks,
        report_path=cli.peek(rest, constants.OPT_REPORT),
        fail_fast=constants.OPT_FAIL_FAST in rest,
    )


def execute(
    tool: str,
    label: str,
    tasks: list[Task],
    *,
    report_path: str | None = None,
    fail_fast: bool = False,
) -> int:
    """Run `tasks` in parallel (heaviest ``weight`` first, bounded by
    ``$TD_BACKENDS_PARALLELISM``), each task's ``steps`` as child processes in
    order (stopping the task on the first failing step). Returns 0 iff every task
    succeeded. `label` names the run in the progress/summary lines."""
    role = "domain" if tool == constants.DOM_TOOL else "system"
    parallelism = _parallelism()
    tasks.sort(key=lambda t: (-t.weight, t.name))
    console.out(
        f">>> {label}: {len(tasks)} {role}(s), parallel x{parallelism}, heaviest first"
    )
    module = _module(tool)
    lock = threading.Lock()
    abort = threading.Event()
    done = 0

    def work(task: Task) -> Task:
        if abort.is_set():
            task.status = "skipped"
            return task
        with lock:
            console.out(f"  ▶ {label} {task.name} (weight {task.weight})")
        t0 = time.monotonic()
        out: list[str] = []
        rc = 0
        for step in task.steps:
            cp = subprocess.run(
                [sys.executable, "-m", module, *step],
                capture_output=True,
                text=True,
            )
            out.append((cp.stdout + cp.stderr).strip())
            rc = cp.returncode
            if rc != 0:
                break  # stop this task's chain on the first failing step
        task.seconds = round(time.monotonic() - t0, 2)
        task.returncode = rc
        task.output = "\n".join(o for o in out if o)
        task.status = "ok" if rc == 0 else "fail"
        nonlocal done
        with lock:
            done += 1
            head = (
                f"  [{done}/{len(tasks)}] {task.name} {task.status.upper()} "
                f"({task.seconds}s)"
            )
            (console.good if task.status == "ok" else console.bad)(head)
            for line in task.output.splitlines():
                console.out(f"        {line}")
            if task.status == "fail" and fail_fast:
                abort.set()
        return task

    t_start = time.monotonic()
    with ThreadPoolExecutor(max_workers=parallelism) as ex:
        done_tasks = list(ex.map(work, tasks))
    total = time.monotonic() - t_start
    ok = sum(1 for t in done_tasks if t.status == "ok")
    failed = sum(1 for t in done_tasks if t.status == "fail")
    skipped = sum(1 for t in done_tasks if t.status == "skipped")
    tail = f", {skipped} skipped" if skipped else ""
    summary = f">>> {label}: {ok} ok, {failed} failed{tail} in {round(total, 1)}s"
    (console.good if failed == 0 else console.bad)(summary)
    for task in done_tasks:  # recap each failure under the summary
        if task.status == "fail":
            last = task.output.splitlines()[-1] if task.output else ""
            console.bad(f"    FAILED {task.name}{f'  {last}' if last else ''}")
    if report_path:
        _write_report(report_path, tool, label, parallelism, total, done_tasks)
    return 0 if failed == 0 else 1
