#
# Copyright 2026 Tabsdata Inc.
#

"""Shared helpers for the execution-family command groups
(``plan``, ``trx``, ``run``, ``worker``)."""

from __future__ import annotations

import os
from itertools import islice
from typing import TYPE_CHECKING, List

import rich_click as click
from rich.table import Table

from tabsdatak.cli._errors import TdkException
from tabsdatak.cli._machine import machine
from tabsdatak.cli.cli_utils import (
    beautify_time,
    collection_label,
    is_valid_id,
    list_params,
    logical_prompt,
    make_console,
    max_result,
    output_format,
    present,
    render,
    render_list,
    take,
)
from tabsdatak.common._tabular import Column, Kind, TableView, show
from tabsdatak.sdk._diagnose import attempts, needs_attention, scope_filter
from tabsdatak.sdk.status_utils.execution import (
    EXECUTION_STATUS_MAPPING,
    EXECUTION_VALID_USER_PROVIDED_STATUSES,
    user_execution_status_to_api,
)
from tabsdatak.sdk.status_utils.function_run import (
    FUNCTION_RUN_STATUS_MAPPING,
    FUNCTION_RUN_VALID_USER_PROVIDED_STATUSES,
    user_function_run_status_to_api,
)
from tabsdatak.sdk.status_utils.groups import STATUS_GROUPS, Entity, Health
from tabsdatak.sdk.status_utils.transaction import (
    TRANSACTION_STATUS_MAPPING,
    TRANSACTION_VALID_USER_PROVIDED_STATUSES,
    user_transaction_status_to_api,
)
from tabsdatak.sdk.status_utils.worker import (
    WORKER_STATUS_MAPPING,
    WORKER_VALID_USER_PROVIDED_STATUSES,
    user_worker_status_to_api,
    worker_status_to_mapping,
)

if TYPE_CHECKING:
    from tabsdatak.sdk.tabsdata_server import TabsdataServer

PLAN_ID_HINT = "'tdk plan list' lists plan ids; 'tdk fn trigger' prints one."
TRX_ID_HINT = "'tdk trx list' lists transaction ids."
FNRUN_ID_HINT = "'tdk fnrun list' lists function run ids."
WORKER_ID_HINT = "'tdk worker list' and 'tdk plan get --plan PLAN' list worker ids."


_STATUSES = {
    Entity.EXECUTION: (
        "plans",
        EXECUTION_STATUS_MAPPING,
        EXECUTION_VALID_USER_PROVIDED_STATUSES,
        user_execution_status_to_api,
    ),
    Entity.TRANSACTION: (
        "transactions",
        TRANSACTION_STATUS_MAPPING,
        TRANSACTION_VALID_USER_PROVIDED_STATUSES,
        user_transaction_status_to_api,
    ),
    Entity.FUNCTION_RUN: (
        "function runs",
        FUNCTION_RUN_STATUS_MAPPING,
        FUNCTION_RUN_VALID_USER_PROVIDED_STATUSES,
        user_function_run_status_to_api,
    ),
    Entity.WORKER: (
        "workers",
        WORKER_STATUS_MAPPING,
        WORKER_VALID_USER_PROVIDED_STATUSES,
        user_worker_status_to_api,
    ),
}
HEALTHS = tuple(
    group.value for group in (Health.ERROR, Health.WARNING, Health.RUNNING, Health.OK)
)


def _healths(entity: Entity) -> list[str]:
    return [word for word in HEALTHS if STATUS_GROUPS[entity][Health(word)]]


def _health_groups(entity: Entity) -> str:
    _, names, _, _ = _STATUSES[entity]
    groups = STATUS_GROUPS[entity]
    return "; ".join(
        f"{word}: {', '.join(names[code] for code in sorted(groups[Health(word)]))}"
        for word in _healths(entity)
    )


class _Status(click.ParamType):
    name = "STATUS"

    def __init__(self, entity: Entity):
        self.entity = entity

    def convert(self, value, param, ctx):
        _, _, valid, to_api = _STATUSES[self.entity]
        try:
            return to_api(value)
        except ValueError:
            self.fail(
                f"'{value}' is not one of '{valid}'. To filter by outcome, use "
                f"'--health' ({_health_groups(self.entity)}).",
                param,
                ctx,
            )


def status_filter_options(entity: Entity):
    """``--status`` (the entity's own statuses, checked as they are parsed)
    and ``--health`` (every status of a health group)."""
    noun, _, valid, _ = _STATUSES[entity]

    def decorate(command):
        command = click.option(
            "--health",
            "healths",
            type=click.Choice(_healths(entity), case_sensitive=False),
            multiple=True,
            help=(
                f"Filter {noun} by health, the 'health' of each listed row: "
                f"{_health_groups(entity)}. Not together with '--status'."
            ),
        )(command)
        return click.option(
            "--status",
            type=_Status(entity),
            multiple=True,
            help=(
                f"Filter {noun} by status: '{valid}' (case-insensitive). "
                "Not together with '--health'."
            ),
        )(command)

    return decorate


def status_codes(
    entity: Entity, status: tuple[str, ...], healths: tuple[str, ...]
) -> list[str]:
    """The status codes a listing filters by, from ``--status`` or the groups
    of ``--health``."""
    if status and healths:
        raise click.UsageError("Pass '--status' or '--health', not both.")
    groups = STATUS_GROUPS[entity]
    return sorted({code for word in healths for code in groups[Health(word)]}) or list(
        status
    )


def span_projects(server: TabsdataServer) -> None:
    """The execution-family commands are id-keyed and their listings always
    span every visible project; a pinned or default project must not narrow
    them (narrowing is done with an explicit ``project:eq:`` filter)."""
    server.connection.project = None


def _append_at_filter(request_filter: List[str], at: str) -> None:
    # noinspection PyProtectedMember
    from tabsdatak.sdk.tabsdata_server import _top_and_convert_to_timestamp

    try:
        at_timestamp = _top_and_convert_to_timestamp(at)
        request_filter.append(f"triggered_on:le:{at_timestamp}")
    except ValueError as e:
        raise click.ClickException(f"Invalid date-time format for 'at' option: {e}")


def obtain_list_exec_filters(
    status: List[str] = None,
    exe_name: str = None,
    fn: str = None,
    collection: str = None,
    at: str = None,
    project: str = None,
) -> List[str]:
    """
    Helper function to obtain the filters for listing plans.
    """
    request_filter = []
    if status:
        request_filter.extend(f"status:eq:{code}" for code in status)
    if exe_name:
        request_filter.append(f"name:lk:{exe_name}")
    if fn:
        request_filter.append(f"function:eq:{fn}")
    if collection:
        request_filter.append(f"collection:eq:{collection}")
    if at:
        _append_at_filter(request_filter, at)
    if project:
        request_filter.append(f"project:eq:{project}")
    return request_filter


def obtain_list_trx_filters(
    status: List[str] = None,
    exe_name: str = None,
    execution_id: str = None,
    collection: str = None,
    at: str = None,
    project: str = None,
) -> List[str]:
    """
    Helper function to obtain the filters for listing transactions.
    """
    request_filter = []
    if status:
        request_filter.extend(f"status:eq:{code}" for code in status)
    if exe_name:
        request_filter.append(f"execution:lk:{exe_name}")
    if execution_id:
        request_filter.append(f"execution_id:eq:{execution_id}")
    if collection:
        request_filter.append(f"collection:eq:{collection}")
    if at:
        _append_at_filter(request_filter, at)
    if project:
        request_filter.append(f"project:eq:{project}")
    return request_filter


def obtain_list_fn_run_filters(
    status: List[str] = None,
    exe_name: str = None,
    execution_id: str = None,
    fn: str = None,
    collection: str = None,
    trx: str = None,
    project: str = None,
) -> List[str]:
    """
    Helper function to obtain the filters for listing function runs.
    """
    request_filter = []
    if status:
        request_filter.extend(f"status:eq:{code}" for code in status)
    if exe_name:
        request_filter.append(f"execution:lk:{exe_name}")
    if execution_id:
        request_filter.append(f"execution_id:eq:{execution_id}")
    if fn:
        request_filter.append(f"name:eq:{fn}")
    if collection:
        request_filter.append(f"collection:eq:{collection}")
    if trx:
        request_filter.append(f"transaction_id:eq:{trx}")
    if project:
        request_filter.append(f"project:eq:{project}")
    return request_filter


def obtain_list_worker_filters(
    status: List[str] = None,
    exe_name: str = None,
    execution_id: str = None,
    fn: str = None,
    fnrun: str = None,
    collection: str = None,
    trx: str = None,
    worker_type: str = None,
    project: str = None,
) -> List[str]:
    """
    Helper function to obtain the filters for listing workers.
    """
    request_filter = []
    if status:
        request_filter.extend(f"status:eq:{code}" for code in status)
    if exe_name:
        request_filter.append(f"execution:lk:{exe_name}")
    if execution_id:
        request_filter.append(f"execution_id:eq:{execution_id}")
    if fn:
        request_filter.append(f"function:eq:{fn}")
    if fnrun:
        request_filter.append(f"logical_function_run_id:eq:{fnrun}")
    if collection:
        request_filter.append(f"collection:eq:{collection}")
    if trx:
        request_filter.append(f"transaction_id:eq:{trx}")
    if worker_type:
        request_filter.append(f"worker_type:eq:{worker_type}")
    if project:
        request_filter.append(f"project:eq:{project}")
    return request_filter


def obtain_execution_list(
    ctx: click.Context, server: TabsdataServer, request_filter, last: bool
) -> tuple[list, bool]:
    executions = server.list_executions_generator(
        **list_params(ctx, request_filter, order_by="triggered_on-")
    )
    # The listing is newest-first, so --last needs a single row, never a page.
    if last:
        return list(islice(executions, 1)), False
    return take(ctx, executions)


def obtain_transaction_list(
    ctx: click.Context, server: TabsdataServer, request_filter, last: bool
) -> tuple[list, bool]:
    transactions = server.list_transactions_generator(
        **list_params(ctx, request_filter, order_by="triggered_on-")
    )
    if last:
        return list(islice(transactions, 1)), False
    return take(ctx, transactions)


def status_context(
    server: TabsdataServer,
    plan: str | None = None,
    trx: str | None = None,
    fnrun: str | None = None,
) -> list[str]:
    """The status of the plan, transaction and function run a listing is
    narrowed to, for its title line."""
    context = []
    if plan:
        context.append(("Plan", f"{plan} ({server.get_execution(plan).status})"))
    if trx:
        context.append(("Transaction", f"{trx} ({server.get_transaction(trx).status})"))
    if fnrun:
        context.append((
            "Function run",
            f"{fnrun} ({server.get_function_run(fnrun).status})",
        ))
    return context


def patch_label(item) -> str | None:
    """The patch a run/worker executed (a function-version id), or None for head
    code. The patch id is surfaced onto the row as `function_version_patch_id`;
    absent/None means unpatched (head)."""
    return item.kwargs.get("function_version_patch_id")


ID = Column("ID", Kind.ID)
NAME = Column("Name", width=20)
STARTED = Column("Start time", Kind.TIME)
ENDED = Column("End time", Kind.TIME)
STATUS = Column("Status", Kind.STATUS)


def plans_view(executions, title: str = "Plans") -> TableView:
    return TableView(
        title=title,
        columns=[
            ID,
            NAME,
            Column("Project"),
            Column("Trigger"),
            Column("Triggered by"),
            STARTED,
            ENDED,
            STATUS,
        ],
        rows=[
            [
                execution.id,
                present(execution.name),
                present(execution.project),
                f"{collection_label(execution)} / {execution.function.name}",
                present(execution.triggered_by),
                present(execution.started_on_str),
                present(execution.ended_on_str),
                present(execution.status),
            ]
            for execution in executions
        ],
        count="plans",
    )


def transactions_view(transactions) -> TableView:
    return TableView(
        title="Transactions",
        columns=[
            ID,
            Column("Plan ID", Kind.ID),
            Column("Project"),
            Column("Collection"),
            Column("Triggered by"),
            STARTED,
            ENDED,
            STATUS,
        ],
        rows=[
            [
                transaction.id,
                transaction.execution.id,
                present(transaction.project),
                collection_label(transaction),
                present(transaction.triggered_by),
                present(transaction.started_on_str),
                present(transaction.ended_on_str),
                present(transaction.status),
            ]
            for transaction in transactions
        ],
        count="transactions",
    )


def function_runs_view(function_runs, title: str = "Function runs") -> TableView:
    return TableView(
        title=title,
        columns=[
            Column("Function run ID", Kind.ID),
            Column("Function"),
            Column("Plan ID", Kind.ID),
            Column("Transaction ID", Kind.ID),
            Column("Project"),
            Column("Collection"),
            STARTED,
            ENDED,
            Column("Patch", Kind.ID),
            STATUS,
        ],
        rows=[
            [
                fn_run.id,
                fn_run.function.name,
                fn_run.execution.id,
                fn_run.transaction.id,
                present(fn_run.project),
                collection_label(fn_run),
                present(fn_run.started_on_str),
                present(fn_run.ended_on_str),
                present(patch_label(fn_run)),
                present(fn_run.status),
            ]
            for fn_run in function_runs
        ],
        count="function runs",
    )


def workers_view(workers, title: str = "Workers", indexed: bool = False) -> TableView:
    columns = [Column("Index", Kind.NUMBER)] if indexed else []
    columns += [
        Column("Worker ID", Kind.ID),
        Column("Function run ID", Kind.ID),
        Column("Function"),
        Column("Plan ID", Kind.ID),
        Column("Transaction ID", Kind.ID),
        Column("Project"),
        Column("Collection"),
        STARTED,
        ENDED,
        Column("Patch", Kind.ID),
        STATUS,
    ]
    rows = []
    for position, worker in enumerate(workers, start=1):
        row = [position] if indexed else []
        row += [
            worker.id,
            present(worker.kwargs.get("logical_function_run_id")),
            worker.function.name,
            present(worker.kwargs.get("execution_id")),
            present(worker.kwargs.get("transaction_id")),
            present(worker.project),
            collection_label(worker),
            present(worker.started_on_str),
            present(worker.ended_on_str),
            present(patch_label(worker)),
            present(worker_status_label(worker)),
        ]
        rows.append(row)
    return TableView(title=title, columns=columns, rows=rows, count="workers")


def worker_status_label(worker) -> str:
    """A failed stream says when it restarts, or that it needs the user."""
    status = worker.status
    kwargs = getattr(worker, "kwargs", None) or {}
    stream = getattr(worker, "type", None) == "ST"
    if not stream or worker_status_to_mapping(status) != "Failed":
        return status
    retry_at = kwargs.get("retry_at")
    if retry_at is None:
        return f"{status} (needs action)"
    from tabsdatak.common._datetime import _convert_timestamp_to_string

    retrying_at = beautify_time(_convert_timestamp_to_string(retry_at))
    failures = kwargs.get("failures")
    return f"{status} (retrying at {retrying_at}, failure {failures} in a row)"


LOGS_MAX_WORKERS = 20


def tail_option(fn):
    return click.option(
        "--tail",
        type=click.IntRange(min=0),
        default=None,
        help="Show only the last N lines of each log.",
    )(fn)


def _apply_logs_max_result(ctx: click.Context, param, value):
    ctx.ensure_object(dict)
    ctx.obj["max_result"] = value
    return value


def all_workers_option(fn):
    fn = click.option(
        "--max-result",
        "max_result",
        type=click.IntRange(min=0),
        default=LOGS_MAX_WORKERS,
        show_default=True,
        expose_value=False,
        callback=_apply_logs_max_result,
        help="With '--all', the most workers to show. Use 0 for no limit.",
    )(fn)
    return click.option(
        "--all",
        "all_workers",
        is_flag=True,
        default=False,
        help="Show the logs of every worker, newest first, instead of picking one.",
    )(fn)


def failed_only_option(fn):
    return click.option(
        "--failed-only",
        is_flag=True,
        default=False,
        help="Only the workers whose health is warning or error.",
    )(fn)


def pick_worker(ctx: click.Context, workers: list, subject: str):
    """Resolve a worker to show logs for: the only one directly, or one the
    user picks from the rendered listing when several ran."""
    if not workers:
        raise click.ClickException(f"{subject} has no workers yet — nothing has run.")
    if len(workers) == 1:
        return workers[0]
    if output_format(ctx) == "table":
        show(workers_view(workers, indexed=True))
    if output_format(ctx) != "table" or ctx.obj["no_prompt"]:
        raise TdkException(
            "usage",
            f"{subject} has {len(workers)} workers. Pass '--all' for all their "
            "logs, or pick one and run 'tdk worker logs --worker <WORKER-ID>'.",
        )
    choice = logical_prompt(ctx, "Index or ID of the worker to show logs for")
    choice = (choice or "").strip()
    if is_valid_id(choice):
        picked = next((w for w in workers if w.id == choice), None)
        if picked is None:
            raise TdkException(
                "not_found", f"Worker '{choice}' is not one of {subject}'s workers."
            )
        return picked
    try:
        index = int(choice)
    except ValueError:
        raise TdkException(
            "usage", f"'{choice}' is neither a worker ID nor an index in the list."
        ) from None
    if not 1 <= index <= len(workers):
        raise TdkException(
            "usage", f"Index {index} is out of range (1-{len(workers)})."
        )
    return workers[index - 1]


def _capped(entries: list[dict]) -> list[dict]:
    """In a machine run, each log's tail up to its ``log_cap_bytes``, within
    its ``logs_budget_bytes`` over the whole document."""
    run = machine()
    if run is None or run.log_cap_bytes is None:
        return entries
    left = run.logs_budget_bytes or run.log_cap_bytes
    capped = []
    for entry in entries:
        log = entry.get("log")
        if log is None:
            capped.append(entry)
            continue
        encoded = log.encode()
        keep = min(len(encoded), run.log_cap_bytes, left)
        left -= keep
        if keep == len(encoded):
            capped.append(entry)
            continue
        tail = encoded[len(encoded) - keep :].decode(errors="ignore")
        capped.append({**entry, "log": tail, "log_truncated": True})
    return capped


def _entry_text(entry: dict) -> str:
    details = []
    if entry.get("collection") and entry.get("function"):
        details.append(f"{entry['collection']}/{entry['function']}")
    if entry.get("status_name"):
        details.append(entry["status_name"])
    if entry.get("attempt"):
        details.append(f"attempt {entry['attempt']}")
    lines = ["  ".join([f"Worker {entry['worker_id']}", *details])]
    if entry.get("log_unavailable"):
        lines.append(f"(log unavailable: {entry['log_unavailable']})")
    else:
        lines.append(entry.get("log") or "")
    signature = entry.get("signature")
    if signature:
        lines.append(f"Error [{signature['category']}]: {signature['message']}")
    return os.linesep.join(lines)


def _compat_logs(entries: list[dict]) -> dict:
    return {
        "logs": (os.linesep * 2).join(
            f"Worker {entry['worker_id']}{os.linesep}{entry.get('log') or ''}"
            for entry in entries
        )
    }


def emit_logs(ctx: click.Context, entries: list[dict], truncated: bool, file: str):
    """Render worker log entries: the logs themselves for humans, the
    ``{items, truncated, next}`` envelope in ``jsonb``, and ``{"logs": text}``
    in ``json`` and ``yaml``."""
    text = (os.linesep * 2).join(_entry_text(entry) for entry in entries)
    if file == "-":
        if machine() is not None:
            raise TdkException(
                "usage",
                "'--file -' writes raw logs and is not available in a machine run.",
            )
        click.echo(text, nl=False)
        return
    human = output_format(ctx) == "table"
    if file:
        if human:
            click.echo(f"Saving logs to file '{file}'")
        with open(file, "w") as f:
            f.write(text)
    if human:
        if file:
            click.echo("Logs saved successfully")
        elif entries:
            click.echo(text)
        else:
            click.echo("No worker logs to show.")
    render_list(
        ctx,
        items=lambda: _capped(entries),
        truncated=truncated,
        compat=lambda: _compat_logs(entries),
    )


def emit_worker_logs(
    ctx: click.Context,
    server: TabsdataServer,
    worker_id: str,
    tail: int | None,
    file: str,
) -> None:
    emit_logs(ctx, [server.worker_log_entry(worker_id, tail=tail)], False, file)


def emit_scope_logs(
    ctx: click.Context,
    server: TabsdataServer,
    subject: str,
    *,
    all_workers: bool,
    tail: int | None,
    file: str,
    failed_only: bool = False,
    **scope: str,
) -> None:
    """Show the logs of a plan's, transaction's or function run's workers:
    all of them with ``--all``, else the one worker picked."""
    span_projects(server)
    if all_workers:
        entries, truncated = server.scope_logs(
            **scope,
            tail=tail,
            failed_only=failed_only,
            max_workers=max_result(ctx),
        )
        emit_logs(ctx, entries, truncated, file)
        return
    workers = server.list_workers(filter=scope_filter(**scope))
    candidates = (
        [w for w in workers if needs_attention(w.kwargs.get("status"))]
        if failed_only
        else workers
    )
    if workers and not candidates:
        raise click.ClickException(
            f"{subject} has no workers whose health is warning or error."
        )
    worker = pick_worker(ctx, candidates, subject)
    entry = server.worker_log_entry(
        worker, tail=tail, attempt=attempts(workers)[worker.id]
    )
    emit_logs(ctx, [entry], False, file)


def confirm_cancel(ctx: click.Context, confirm: str, subject: str) -> None:
    confirm = confirm or logical_prompt(
        ctx,
        f"Please type 'cancel' to confirm canceling {subject}",
        option="--confirm cancel",
    )
    if confirm != "cancel":
        raise click.ClickException(
            "Cancellation not confirmed. The confirmation word is 'cancel'."
        )


def diagnose_options(fn):
    fn = click.option(
        "--no-logs",
        "no_logs",
        is_flag=True,
        default=False,
        help="Diagnose from statuses alone, without reading worker logs.",
    )(fn)
    return click.option(
        "--log-lines",
        type=click.IntRange(min=0),
        default=200,
        show_default=True,
        help=(
            "The last lines of each worker log to include, with repeated blocks "
            "kept once and the excerpt capped at 4000 characters."
        ),
    )(fn)


def _diagnosis_table(document: dict) -> None:
    kind = document["kind"]
    subject = document[kind]
    label = "Plan" if kind == "plan" else "Function run"
    name = subject.get("name") or subject.get("function") or ""
    click.echo(
        f"{label} {subject['id']} {name}: {subject.get('status_name')} "
        f"({subject.get('health')})"
    )
    summary = document["summary"]
    counts = "  ".join(f"{k} {v}" for k, v in summary["by_health"].items())
    click.echo(f"Runs: {summary['runs']}  {counts}  held {summary['held']}")
    root = document.get("root_cause")
    if root:
        click.echo(f"Root cause [{root['category']}]: {root.get('message') or '-'}")
        frame = root.get("user_frame")
        if frame:
            click.echo(f"  at {frame['module']}:{frame['line']} in {frame['function']}")
        click.echo(f"  {root['hint']}")
    else:
        click.echo("No failed or held runs.")
    console = make_console()
    failed = document["failed_runs"]
    if failed:
        table = Table(title="Failed runs")
        table.add_column("Function run ID", style="cyan", no_wrap=True)
        table.add_column("Collection")
        table.add_column("Function")
        table.add_column("Status")
        table.add_column("Attempts", justify="right")
        table.add_column("Category")
        table.add_column("Message")
        for run in failed:
            signature = run.get("signature") or {}
            table.add_row(
                run["function_run_id"],
                run.get("collection") or "-",
                run.get("function") or "-",
                run.get("status_name") or "-",
                str(run["attempts"]) if run.get("attempts") is not None else "-",
                signature.get("category") or "-",
                signature.get("message") or run.get("log_unavailable") or "-",
            )
        console.print(table)
    held = document["held_runs"]
    if held:
        table = Table(title="Held runs")
        table.add_column("Function run ID", style="cyan", no_wrap=True)
        table.add_column("Collection")
        table.add_column("Function")
        table.add_column("Blocked by")
        for run in held:
            blocked = ", ".join(run.get("blocked_by") or []) or "-"
            if run.get("blocked_by_outside_plan"):
                blocked += " (and outside the plan)"
            table.add_row(
                run["function_run_id"],
                run.get("collection") or "-",
                run.get("function") or "-",
                blocked,
            )
        console.print(table)
    for run in failed:
        if run.get("log_excerpt"):
            click.echo(f"\nLog of worker {run['worker_id']}:")
            click.echo(run["log_excerpt"])
        for action, argv in (run.get("argv") or {}).items():
            click.echo(f"{action}: tdk {' '.join(argv)}")
    truncated = document["truncated"]
    if truncated["failed_runs_shown"] < truncated["failed_runs_total"]:
        click.echo(
            f"Showing {truncated['failed_runs_shown']} of "
            f"{truncated['failed_runs_total']} failed runs; raise '--max-runs'."
        )


def render_diagnosis(ctx: click.Context, diagnosis) -> None:
    document = diagnosis.to_dict()
    if output_format(ctx) == "table":
        _diagnosis_table(document)
    render(ctx, data=document)
