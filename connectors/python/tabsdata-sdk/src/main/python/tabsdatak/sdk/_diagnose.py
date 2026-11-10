#
# Copyright 2026 Tabsdata Inc.
#

"""Worker logs read for what they explain: the error signature of a log (the
exception, its message and the user-code frame that raised it) and its
category, the log entries the ``logs`` commands emit, and the diagnosis of a
plan or function run built from them."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Iterable, List

from tabsdatak.sdk._apiserver import APIServerError
from tabsdatak.sdk.status_utils.groups import (
    Entity,
    Health,
    annotate,
    health,
    status_name,
)

if TYPE_CHECKING:
    from tabsdatak.sdk.tabsdata_server import TabsdataServer, Worker

MESSAGE_CHARS = 300
EXCERPT_CHARS = 4000
EXCERPT_CONTEXT = 20
REPEAT_LINES = 3
_REPEAT_CANDIDATES = 16
FORBIDDEN_LOG = "forbidden (DevelopProject required)"

_TRACEBACK = "Traceback (most recent call last):"
_CHAINS = (
    "During handling of the above exception, another exception occurred:",
    "The above exception was the direct cause of the following exception:",
)
_FRAME = re.compile(r'File "(?P<path>[^"]+)", line (?P<line>\d+), in (?P<function>\S+)')
_EXCEPTION = re.compile(r"(?P<exception>[A-Za-z_][\w.]*)(?::\s?(?P<detail>.*))?")
_ERROR_LINE = re.compile(r"\bERROR\b")
_LOG_RECORD = re.compile(
    r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?Z "
    r"(TRACE|DEBUG|INFO|WARNING|ERROR|CRITICAL) "
)
_CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")


class Category(str, Enum):
    MISSING_DEPENDENCY = "missing_dependency"
    SCHEMA_MISMATCH = "schema_mismatch"
    AUTH = "auth"
    CONNECTIVITY = "connectivity"
    RESOURCE = "resource"
    USER_CODE = "user_code"
    PLATFORM = "platform"
    UNKNOWN = "unknown"
    ON_HOLD = "on_hold"


_MATCHERS: tuple[tuple[Category, re.Pattern[str]], ...] = (
    (Category.MISSING_DEPENDENCY, re.compile(r"ModuleNotFoundError|ImportError")),
    (
        Category.SCHEMA_MISMATCH,
        re.compile(r"ColumnNotFound|SchemaError|SchemaFieldNotFound|\bKeyError\b"),
    ),
    (
        Category.AUTH,
        re.compile(
            r"permission denied|access denied|authentication|unauthori[sz]ed"
            r"|\b401\b|\b403\b",
            re.IGNORECASE,
        ),
    ),
    (
        Category.CONNECTIVITY,
        re.compile(
            r"connection refused|could not connect|timed? ?out|unreachable"
            r"|name or service not known|name resolution|nodename nor servname",
            re.IGNORECASE,
        ),
    ),
    (
        Category.RESOURCE,
        re.compile(r"MemoryError|out of memory|OOMKilled", re.IGNORECASE),
    ),
)

HINTS: dict[Category, str] = {
    Category.MISSING_DEPENDENCY: (
        "Add the package to the function's requirements, patch it with "
        "'tdk fn patch' and replay the run."
    ),
    Category.SCHEMA_MISMATCH: (
        "Compare the input schema with 'tdk table schema', patch the function "
        "and replay the run."
    ),
    Category.AUTH: "Check the connection's credentials, then recover the plan.",
    Category.CONNECTIVITY: (
        "Check that the external system is reachable, then recover the plan."
    ),
    Category.RESOURCE: "Reduce the data volume per run or the worker's memory use.",
    Category.USER_CODE: (
        "Read the function with 'tdk fn bundle', patch it and replay the run."
    ),
    Category.PLATFORM: "The failure is outside the function's code; escalate it "
    "with the worker id.",
    Category.UNKNOWN: "Read the worker's log with 'tdk worker logs'.",
    Category.ON_HOLD: "Repair the blocking runs; the held ones resume on their own.",
}


@dataclass(frozen=True)
class UserFrame:
    module: str
    line: int
    function: str

    def to_dict(self) -> dict:
        return {"module": self.module, "line": self.line, "function": self.function}


@dataclass(frozen=True)
class ErrorSignature:
    """What a log says went wrong: ``code`` is the snake-cased exception class
    (``error_log_line`` when only an ERROR line was found), ``traceback``
    the last traceback block, when there is one, and ``raised`` the whole
    exception lines of that traceback and of those chained into it."""

    code: str | None
    message: str
    user_frame: UserFrame | None = None
    traceback: str | None = None
    raised: tuple[str, ...] = ()


def extract_error_signature(text: str | None) -> ErrorSignature | None:
    """The signature of the last traceback in a log, else of its last
    ERROR-level line; ``None`` when nothing error-like is found."""
    lines = (text or "").splitlines()
    starts = [i for i, line in enumerate(lines) if _TRACEBACK in line]
    end = _raised(lines, starts[-1]) if starts else None
    if end is not None:
        start = starts[-1]
        line = lines[end].strip()
        match = _EXCEPTION.fullmatch(line)
        if match and match.group("exception"):
            exception = match.group("exception").split(".")[-1]
            detail = (match.group("detail") or "").strip()
            message = f"{exception}: {detail}" if detail else exception
            code = _snake_case(exception)
        else:
            message, code = line, None
        frames = [_FRAME.search(row) for row in lines[start + 1 : end]]
        return ErrorSignature(
            code=code,
            message=message[:MESSAGE_CHARS],
            user_frame=_user_frame(frame for frame in frames if frame),
            traceback="\n".join(lines[start : end + 1]),
            raised=(*_chained(lines, starts, len(starts) - 1)[1], line),
        )
    for line in reversed(lines):
        if _ERROR_LINE.search(line):
            return ErrorSignature(
                code="error_log_line", message=line.strip()[:MESSAGE_CHARS]
            )
    return None


def categorize(signature: ErrorSignature | None) -> Category:
    """The first category whose pattern the exception lines of the
    signature (its chained exceptions included) or its ERROR line match;
    frames and source lines are never matched. A traceback nothing matches
    is ``user_code`` when user code raised it and ``platform`` otherwise."""
    if signature is None:
        return Category.UNKNOWN
    haystack = "\n".join(signature.raised or (signature.message,))
    for category, pattern in _MATCHERS:
        if pattern.search(haystack):
            return category
    if signature.user_frame is not None:
        return Category.USER_CODE
    if signature.traceback is not None:
        return Category.PLATFORM
    return Category.UNKNOWN


def signature_document(text: str | None) -> dict | None:
    signature = extract_error_signature(text)
    if signature is None:
        return None
    return {
        "category": categorize(signature).value,
        "code": signature.code,
        "message": signature.message,
        "user_frame": signature.user_frame.to_dict() if signature.user_frame else None,
    }


def _snake_case(name: str) -> str:
    return _CAMEL.sub("_", name).lower()


def _user_frame(frames: Iterable[re.Match]) -> UserFrame | None:
    from tabsdatak.common._archive.bundle import BUNDLE_USER_CODE_DIRNAME

    user_code = f"/{BUNDLE_USER_CODE_DIRNAME}/"
    for frame in reversed(list(frames)):
        path = frame.group("path")
        if user_code in path:
            return UserFrame(
                module=path.split(user_code, 1)[1],
                line=int(frame.group("line")),
                function=frame.group("function"),
            )
    return None


def _raised(lines: list[str], start: int) -> int | None:
    """The line of the exception the traceback at ``start`` raised: its first
    line that is no frame, no source line and no log record, which the
    worker's stdout interleaves with its stderr."""
    for index in range(start + 1, len(lines)):
        line = lines[index]
        if (
            _FRAME.search(line)
            or not line.strip()
            or line[0].isspace()
            or _LOG_RECORD.match(line)
        ):
            continue
        return index
    return None


def _chained(lines: list[str], starts: list[int], last: int) -> tuple[int, list[str]]:
    """Where the chain of tracebacks ending in the one starting at
    ``starts[last]`` begins, and the exception lines chained into it,
    oldest first."""
    found = []
    following = starts[last]
    for start in reversed(starts[:last]):
        end = _raised(lines, start)
        if end is None or end >= following:
            break
        between = lines[end + 1 : following]
        if not any(chain in line for line in between for chain in _CHAINS):
            break
        found.insert(0, lines[end].strip())
        following = start
    return following, found


_ERROR_START = re.compile(r"Traceback \(most recent call last\):|\bERROR\b")


def select_lines(
    text: str,
    tail: int | None = None,
    grep: str | None = None,
    from_error: bool = False,
) -> list[tuple[int, str]]:
    """The lines of a log a reader asked for, each with its 1-based number:
    with ``from_error``, from the first traceback or ``ERROR`` line on (all
    of them when there is none); with ``grep``, those matching that regular
    expression; with ``tail``, the last ``tail`` of those."""
    numbered = list(enumerate(text.splitlines(), start=1))
    if from_error:
        start = next(
            (
                index
                for index, (_, row) in enumerate(numbered)
                if _ERROR_START.search(row)
            ),
            0,
        )
        numbered = numbered[start:]
    if grep is not None:
        pattern = re.compile(grep)
        numbered = [(n, row) for n, row in numbered if pattern.search(row)]
    if tail is not None:
        numbered = numbered[-tail:] if tail > 0 else []
    return numbered


def cut_ranges(kept: Iterable[int], total: int) -> list[list[int]]:
    """The ``[first, last]`` line ranges of a ``total``-line log not in
    ``kept``."""
    shown = set(kept)
    ranges: list[list[int]] = []
    for number in range(1, total + 1):
        if number in shown:
            continue
        if ranges and ranges[-1][1] == number - 1:
            ranges[-1][1] = number
        else:
            ranges.append([number, number])
    return ranges


def _chain_span(lines: list[str]) -> tuple[int, int] | None:
    """The first and last line of the last traceback with those chained
    into it."""
    starts = [i for i, line in enumerate(lines) if _TRACEBACK in line]
    end = _raised(lines, starts[-1]) if starts else None
    if end is None:
        return None
    return _chained(lines, starts, len(starts) - 1)[0], end


def excerpt(text: str, lines: int) -> tuple[str, bool]:
    """The last ``lines`` lines of a log, headed by the failing traceback
    chain and the ``EXCERPT_CONTEXT`` lines before it when they start
    earlier, with each repeated run of lines kept once and the whole bounded
    to ``EXCERPT_CHARS``; and whether anything was left out. Over the bound,
    the lines before that context (a pod's preamble) go first, then the
    middle of what is left."""
    rows = text.splitlines()
    first = max(len(rows) - lines, 0)
    body = rows[first:]
    span = _chain_span(rows)
    context = None if span is None else max(span[0] - EXCERPT_CONTEXT, 0)
    if span is not None and context < first:
        end = span[1]
        gap = ["..."] if end + 1 < first else []
        body = rows[context : end + 1] + gap + rows[max(first, end + 1) :]
        context = first
    collapsed, repeated = _collapse_repeats(body)
    if first == 0 and not repeated and _size(collapsed) <= EXCERPT_CHARS:
        return text, False
    if _size(collapsed) <= EXCERPT_CHARS or context is None:
        return "\n".join(_middle_cut(collapsed, EXCERPT_CHARS)), True
    preamble = context - first
    rest, _ = _collapse_repeats(body[preamble:])
    cut = [f"[... {preamble} lines cut]"] if preamble else []
    if _size(cut + rest) <= EXCERPT_CHARS:
        return "\n".join(cut + rest), True
    return "\n".join(cut + _middle_cut(rest, EXCERPT_CHARS - _size(cut) - 1)), True


def _collapse_repeats(rows: list[str]) -> tuple[list[str], bool]:
    """Each run of ``REPEAT_LINES`` or more lines that repeats an earlier
    run replaced by one marker line."""
    seen: dict[str, list[int]] = {}
    kept: list[str] = []
    repeated = False
    index = 0
    while index < len(rows):
        longest = 0
        for earlier in seen.get(rows[index], ())[-_REPEAT_CANDIDATES:]:
            length = 0
            while (
                index + length < len(rows)
                and rows[earlier + length] == rows[index + length]
            ):
                length += 1
            longest = max(longest, length)
        run = rows[index : index + longest]
        if longest >= REPEAT_LINES and any(row.strip() for row in run):
            kept.append(f"[... {longest} lines repeated from above]")
            repeated = True
        else:
            longest = 1
            kept.append(rows[index])
        for offset in range(longest):
            seen.setdefault(rows[index + offset], []).append(index + offset)
        index += longest
    return kept, repeated


def _size(rows: list[str]) -> int:
    return sum(len(row) + 1 for row in rows) - 1 if rows else 0


def _middle_cut(rows: list[str], budget: int) -> list[str]:
    """``rows`` within ``budget`` chars, their middle replaced by a marker
    line."""
    if _size(rows) <= budget:
        return rows
    room = max(budget - len(f"[... {len(rows)} lines cut]") - 2, 0) // 2
    head: list[str] = []
    used = 0
    for row in rows:
        used += len(row) + 1
        if used > room:
            break
        head.append(row)
    tail: list[str] = []
    used = 0
    for row in reversed(rows[len(head) :]):
        used += len(row) + 1
        if used > room:
            break
        tail.insert(0, row)
    dropped = len(rows) - len(head) - len(tail)
    return [*head, f"[... {dropped} lines cut]", *tail]


def attempts(workers: Iterable[Worker]) -> dict[str, int]:
    """Each worker's attempt number within its function run, 1 for the
    first; worker ids are time-ordered."""
    by_run: dict[str | None, list[str]] = {}
    for worker in workers:
        run = worker.kwargs.get("logical_function_run_id")
        by_run.setdefault(run, []).append(worker.id)
    return {
        worker_id: position
        for ids in by_run.values()
        for position, worker_id in enumerate(sorted(ids, key=str.lower), start=1)
    }


def needs_attention(code: str | None) -> bool:
    return health(Entity.WORKER, code) in (Health.WARNING, Health.ERROR)


def _read_log(server: TabsdataServer, worker_id: str) -> tuple[str | None, str | None]:
    """A worker's log, or why it cannot be read."""
    try:
        return server.get_worker_log(worker_id), None
    except APIServerError as error:
        if error.status != 403:
            raise
        return None, FORBIDDEN_LOG


def log_entry(
    server: TabsdataServer,
    worker: Worker,
    tail: int | None = None,
    attempt: int | None = None,
    grep: str | None = None,
    from_error: bool = False,
) -> dict:
    """One worker's log as the ``logs`` commands emit it: ``log`` the lines
    :func:`select_lines` keeps, and ``lines`` the same as ``[number, text]``
    pairs. A log the caller may not read is reported in ``log_unavailable``
    instead of failing."""
    kwargs = worker.kwargs
    code = kwargs.get("status")
    group = health(Entity.WORKER, code)
    entry = {
        "worker_id": worker.id,
        "function_run_id": kwargs.get("logical_function_run_id"),
        "collection": kwargs.get("collection"),
        "function": kwargs.get("function"),
        "status": code,
        "status_name": status_name(Entity.WORKER, code),
        "health": group.value if group is not None else None,
        "attempt": attempt,
        "lines_total": 0,
        "log_truncated": False,
        "log_unavailable": None,
        "signature": None,
        "log": None,
        "lines": None,
    }
    text, entry["log_unavailable"] = _read_log(server, worker.id)
    if text is None:
        return entry
    lines = select_lines(text, tail, grep, from_error)
    entry["lines_total"] = len(text.splitlines())
    entry["log_truncated"] = len(lines) < entry["lines_total"]
    entry["log"] = (
        text if not entry["log_truncated"] else "\n".join(row for _, row in lines)
    )
    entry["lines"] = [[number, row] for number, row in lines]
    if needs_attention(code):
        entry["signature"] = signature_document(text)
    return entry


def scope_filter(
    plan: str | None = None, trx: str | None = None, fnrun: str | None = None
) -> List[str]:
    scope = [
        f"execution_id:eq:{plan}" if plan else None,
        f"transaction_id:eq:{trx}" if trx else None,
        f"logical_function_run_id:eq:{fnrun}" if fnrun else None,
    ]
    conditions = [condition for condition in scope if condition]
    if not conditions:
        raise ValueError("Give a plan, a transaction or a function run.")
    return conditions


def scope_logs(
    server: TabsdataServer,
    plan: str | None = None,
    trx: str | None = None,
    fnrun: str | None = None,
    tail: int | None = None,
    failed_only: bool = False,
    max_workers: int | None = 20,
    grep: str | None = None,
    from_error: bool = False,
) -> tuple[list[dict], bool]:
    """The logs of every worker of a plan, transaction or function run,
    newest first, and whether ``max_workers`` cut the list short."""
    with server._spanning():
        workers = server.list_workers(filter=scope_filter(plan, trx, fnrun))
    numbers = attempts(workers)
    if failed_only:
        workers = [w for w in workers if needs_attention(w.kwargs.get("status"))]
    workers.sort(key=lambda w: w.id.lower(), reverse=True)
    truncated = bool(max_workers) and len(workers) > max_workers
    if truncated:
        workers = workers[:max_workers]
    return [
        log_entry(server, w, tail, numbers.get(w.id), grep, from_error) for w in workers
    ], truncated


@dataclass(frozen=True)
class Diagnosis:
    """Why a plan or a function run did not succeed: its failed runs with
    their repair handles and log signatures, the held runs with what blocks
    them, and the root cause."""

    kind: str
    subject: dict
    summary: dict
    root_cause: dict | None
    failed_runs: tuple[dict, ...]
    held_runs: tuple[dict, ...]
    failed_runs_total: int

    def to_dict(self) -> dict:
        return {
            "kind": self.kind,
            self.kind: self.subject,
            "summary": self.summary,
            "root_cause": self.root_cause,
            "failed_runs": list(self.failed_runs),
            "held_runs": list(self.held_runs),
            "truncated": {
                "failed_runs_shown": len(self.failed_runs),
                "failed_runs_total": self.failed_runs_total,
            },
        }


def diagnose_execution(
    server: TabsdataServer,
    execution_id: str,
    max_runs: int = 5,
    log_lines: int = 200,
    logs: bool = True,
) -> Diagnosis:
    with server._spanning():
        execution = server.get_execution(execution_id)
        plan = {"id": execution.id, **execution.kwargs}
        runs = _plan_runs(server, execution_id)
        group = health(Entity.EXECUTION, plan.get("status"))
        return _diagnose(
            server,
            kind="plan",
            subject={
                "id": plan["id"],
                "name": plan.get("name"),
                "project": plan.get("project"),
                **_annotated(Entity.EXECUTION, plan),
            },
            runs=runs,
            plan_runs=runs,
            plan_id=execution_id,
            recover=["plan", "recover", "--plan", execution_id],
            can_recover=group in (Health.WARNING, Health.ERROR),
            max_runs=max_runs,
            log_lines=log_lines,
            logs=logs,
        )


def diagnose_function_run(
    server: TabsdataServer,
    function_run_id: str,
    log_lines: int = 200,
    logs: bool = True,
) -> Diagnosis:
    with server._spanning():
        run = server.get_function_run(function_run_id).kwargs
        plan_id = run.get("execution_id")
        plan_runs = _plan_runs(server, plan_id) if run.get("status") == "H" else [run]
        return _diagnose(
            server,
            kind="function_run",
            subject={
                "id": run["id"],
                "project": run.get("project"),
                "collection": run.get("collection"),
                "function": run.get("name"),
                "plan_id": plan_id,
                "transaction_id": run.get("transaction_id"),
                **_annotated(Entity.FUNCTION_RUN, run),
            },
            runs=[run],
            plan_runs=plan_runs,
            plan_id=plan_id,
            recover=["fnrun", "recover", "--fnrun", function_run_id],
            can_recover=True,
            max_runs=1,
            log_lines=log_lines,
            logs=logs,
        )


def _annotated(entity: Entity, row: dict) -> dict:
    return annotate(entity, {"status": row.get("status")})


def _run_health(run: dict) -> Health | None:
    return health(Entity.FUNCTION_RUN, run.get("status"))


def _summary(runs: list[dict]) -> dict:
    by_health = {group.value: 0 for group in Health}
    for run in runs:
        group = _run_health(run)
        if group is not None:
            by_health[group.value] += 1
    held = sum(1 for run in runs if run.get("status") == "H")
    return {"runs": len(runs), "by_health": by_health, "held": held}


def _ended_order(run: dict):
    ended = run.get("ended_on")
    return (ended is None, ended or 0, run.get("id") or "")


def _latest_worker(server: TabsdataServer, run_id: str):
    workers = server.list_workers(filter=[f"logical_function_run_id:eq:{run_id}"])
    if not workers:
        return None, 0
    return max(workers, key=lambda w: w.id.lower()), len(workers)


def _failed_run(
    server: TabsdataServer,
    run: dict,
    recover: list[str],
    can_recover: bool,
    log_lines: int,
    logs: bool,
) -> dict:
    run_id = run["id"]
    failed = _run_health(run) is Health.ERROR
    entry = {
        "function_run_id": run_id,
        "collection": run.get("collection"),
        "function": run.get("name"),
        **_annotated(Entity.FUNCTION_RUN, run),
        "function_version_id": run.get("function_version_id"),
        "ran_patch_id": run.get("function_version_patch_id"),
        "physical_function_run_id": run.get("physical_function_run_id"),
        "transaction_id": run.get("transaction_id"),
        "worker_id": None,
        "attempts": None,
        "signature": None,
        "log_excerpt": None,
        "log_truncated": False,
        "log_unavailable": None,
        "can_replay": failed and not run.get("collection_deleted"),
        "can_recover": failed and can_recover,
        "argv": None,
    }
    if failed:
        entry["argv"] = {
            "replay": ["fnrun", "replay", "--fnrun", run_id, "--wait"],
            "recover": recover,
            "patch": [
                "fn",
                "patch",
                "--coll",
                run.get("collection"),
                "--name",
                run.get("name"),
                "--fn-version",
                run.get("function_version_id"),
                "--source",
                "<file>",
            ],
        }
    if not logs:
        return entry
    worker, count = _latest_worker(server, run_id)
    entry["attempts"] = count
    if worker is None:
        return entry
    entry["worker_id"] = worker.id
    text, entry["log_unavailable"] = _read_log(server, worker.id)
    if text is not None:
        entry["signature"] = signature_document(text)
        entry["log_excerpt"], entry["log_truncated"] = excerpt(text, log_lines)
        if entry["log_truncated"]:
            entry["log_argv"] = [
                "worker",
                "logs",
                "--worker",
                worker.id,
                "--from-error",
            ]
    return entry


def _edge_kind(edge) -> str | None:
    if isinstance(edge, dict) and len(edge) == 1:
        return next(iter(edge))
    return None


def _blockers(graph: dict, runs: list[dict], held: dict) -> tuple[list[str], bool]:
    """The failed runs upstream of a held run, found by walking the plan's
    function -> table -> function edges; and, when none is in the plan,
    whether the chain reaches a producer outside it."""
    run_by_version = {run.get("function_version_id"): run for run in runs}
    inputs: dict[str, set[str]] = {}
    for relation in graph.get("relations") or []:
        if _edge_kind(relation.get("edge")) in ("Dependency", "Trigger"):
            inputs.setdefault(relation.get("function_version_id"), set()).add(
                relation.get("table_version_id")
            )
    tables = graph.get("all_tables") or {}
    blockers: list[str] = []
    outside = False
    seen = {held.get("function_version_id")}
    pending = [held.get("function_version_id")]
    while pending:
        version = pending.pop()
        for table_version in sorted(inputs.get(version, ())):
            producer = (tables.get(table_version) or {}).get("function_version_id")
            if producer is None or producer in seen:
                continue
            seen.add(producer)
            upstream = run_by_version.get(producer)
            if upstream is None:
                outside = True
            elif _run_health(upstream) is Health.ERROR:
                blockers.append(upstream["id"])
            elif upstream.get("status") == "H":
                pending.append(producer)
    return blockers, outside and not blockers


def _earlier_runs_held_on(server: TabsdataServer, run: dict) -> list[dict]:
    """The failed or held earlier runs of ``run``'s own function: each output
    version builds on the one before, so a later run of the function waits on
    them even when nothing in its plan feeds it."""
    function_id = run.get("function_id")
    if not function_id:
        return []
    try:
        listing = server.list_function_runs_generator(
            filter=[f"function_id:eq:{function_id}"]
        )
        earlier = [entry.kwargs for entry in listing]
    except APIServerError:
        return []
    triggered = run.get("triggered_on") or 0
    return sorted(
        (
            entry
            for entry in earlier
            if entry["id"] != run["id"]
            and (entry.get("triggered_on") or 0) < triggered
            and (_run_health(entry) is Health.ERROR or entry.get("status") == "H")
        ),
        key=lambda entry: entry.get("triggered_on") or 0,
    )


def _outside_blocker(run: dict) -> dict:
    code = run.get("status")
    return {
        "plan": run.get("execution_id"),
        "function_run": run["id"],
        "status_name": status_name(Entity.FUNCTION_RUN, code) if code else None,
    }


def _held_runs(
    server: TabsdataServer, plan_id: str, runs: list[dict], held: list[dict]
) -> list[dict]:
    if not held:
        return []
    try:
        graph = server.get_execution_graph(plan_id)
    except APIServerError:
        graph = None
    entries = []
    for run in held:
        blocked_by, outside = _blockers(graph, runs, run) if graph else (None, None)
        elsewhere: list[dict] = []
        if not blocked_by:
            earlier = _earlier_runs_held_on(server, run)
            here = [e["id"] for e in earlier if e.get("execution_id") == plan_id]
            elsewhere = [
                _outside_blocker(e) for e in earlier if e.get("execution_id") != plan_id
            ]
            blocked_by = here or blocked_by
            outside = bool(elsewhere) or outside
        entries.append({
            "function_run_id": run["id"],
            "collection": run.get("collection"),
            "function": run.get("name"),
            "blocked_by": blocked_by,
            "blocked_by_outside_plan": outside,
            "blocked_by_outside": elsewhere,
        })
    return entries


def _on_hold_hint(outside: list[dict]) -> str:
    """How to release held runs: repair the blockers in the plan, or recover or
    cancel the plans outside it that hold them."""
    plans = list(dict.fromkeys(b["plan"] for b in outside if b.get("plan")))
    if not plans:
        return HINTS[Category.ON_HOLD]
    named = ", ".join(f"'{plan}'" for plan in plans)
    first = plans[0]
    return (
        f"Held by earlier runs of the same function in plan {named}. Fix the "
        f"cause and recover it ('tdk plan recover --plan {first}'), and the held "
        f"runs resume; or cancel it ('tdk plan cancel --plan {first}'), which "
        "also cancels the held runs, then trigger again."
    )


def _root_cause(first: dict | None, held: list[dict]) -> dict | None:
    if first is not None:
        signature = first.get("signature") or {}
        category = Category(signature.get("category") or Category.UNKNOWN.value)
        return {
            "function_run_id": first["function_run_id"],
            "collection": first["collection"],
            "function": first["function"],
            "category": category.value,
            "code": signature.get("code"),
            "message": signature.get("message"),
            "user_frame": signature.get("user_frame"),
            "hint": HINTS[category],
        }
    if held:
        blockers = sorted({b for run in held for b in (run["blocked_by"] or ())})
        outside = list(
            {
                b["function_run"]: b
                for run in held
                for b in run.get("blocked_by_outside") or ()
            }.values()
        )
        return {
            "function_run_id": None,
            "collection": None,
            "function": None,
            "category": Category.ON_HOLD.value,
            "code": None,
            "message": f"{len(held)} function run(s) on hold.",
            "user_frame": None,
            "blocked_by": blockers,
            "blocked_by_outside": outside,
            "hint": HINTS[Category.ON_HOLD] if blockers else _on_hold_hint(outside),
        }
    return None


def _plan_runs(server: TabsdataServer, plan_id: str) -> list[dict]:
    listing = server.list_function_runs_generator(filter=[f"execution_id:eq:{plan_id}"])
    return [run.kwargs for run in listing]


def _diagnose(
    server: TabsdataServer,
    *,
    kind: str,
    subject: dict,
    runs: list[dict],
    plan_runs: list[dict],
    plan_id: str,
    recover: list[str],
    can_recover: bool,
    max_runs: int,
    log_lines: int,
    logs: bool,
) -> Diagnosis:
    failing = sorted(
        (
            run
            for run in runs
            if _run_health(run) is Health.ERROR or run.get("status") == "E"
        ),
        key=_ended_order,
    )
    held = [run for run in runs if run.get("status") == "H"]
    shown = failing[:max_runs] if max_runs else failing
    failed_runs = [
        _failed_run(server, run, recover, can_recover, log_lines, logs) for run in shown
    ]
    root = next((r for r in failing if _run_health(r) is Health.ERROR), None)
    root = root or (failing[0] if failing else None)
    first = next(
        (e for e in failed_runs if root and e["function_run_id"] == root["id"]), None
    )
    if root is not None and first is None:
        first = _failed_run(server, root, recover, can_recover, log_lines, logs)
    held_runs = _held_runs(server, plan_id, plan_runs, held)
    return Diagnosis(
        kind=kind,
        subject=subject,
        summary=_summary(runs),
        root_cause=_root_cause(first, held_runs),
        failed_runs=tuple(failed_runs),
        held_runs=tuple(held_runs),
        failed_runs_total=len(failing),
    )
