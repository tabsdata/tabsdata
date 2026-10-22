#
# Copyright 2026 Tabsdata Inc.
#

"""Shared helpers for the execution-family command groups
(``plan``, ``trx``, ``run``, ``worker``)."""

from __future__ import annotations

import os
from itertools import islice
from time import monotonic, sleep
from typing import TYPE_CHECKING, Callable, List, Sequence

import rich_click as click
from rich.live import Live

from tabsdatak.cli.cli_utils import (
    beautify_time,
    collection_label,
    is_valid_id,
    logical_prompt,
    make_console,
    output_format,
    present,
    render,
    take,
)
from tabsdatak.common._tabular import (
    Column,
    Display,
    Kind,
    TableView,
    renderable,
    show,
)
from tabsdatak.sdk.status_utils.execution import user_execution_status_to_api
from tabsdatak.sdk.status_utils.function_run import user_function_run_status_to_api
from tabsdatak.sdk.status_utils.transaction import user_transaction_status_to_api
from tabsdatak.sdk.status_utils.worker import (
    user_worker_status_to_api,
    worker_status_to_mapping,
)

if TYPE_CHECKING:
    from tabsdatak.sdk.tabsdata_server import TabsdataServer


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
        request_filter.extend(
            f"status:eq:{user_execution_status_to_api(s)}" for s in status
        )
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
        request_filter.extend(
            f"status:eq:{user_transaction_status_to_api(s)}" for s in status
        )
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
        request_filter.extend(
            f"status:eq:{user_function_run_status_to_api(s)}" for s in status
        )
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
        request_filter.extend(
            f"status:eq:{user_worker_status_to_api(s)}" for s in status
        )
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
        filter=request_filter,
        order_by="triggered_on-",
    )
    # The listing is newest-first, so --last needs a single row, never a page.
    if last:
        return list(islice(executions, 1)), False
    return take(ctx, executions)


def obtain_transaction_list(
    ctx: click.Context, server: TabsdataServer, request_filter, last: bool
) -> tuple[list, bool]:
    transactions = server.list_transactions_generator(
        filter=request_filter, order_by="triggered_on-"
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
    from tabsdatak.sdk.tabsdata_server import _convert_timestamp_to_string

    retrying_at = beautify_time(_convert_timestamp_to_string(retry_at))
    failures = kwargs.get("failures")
    return f"{status} (retrying at {retrying_at}, failure {failures} in a row)"


MONITOR_OUTAGE_GRACE = 120  # seconds
MONITOR_REFRESH = 1  # seconds


def elapsed_label(seconds: float) -> str:
    minutes, seconds = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes:02d}m {seconds:02d}s"
    if minutes:
        return f"{minutes}m {seconds:02d}s"
    return f"{seconds}s"


def live_monitor(
    state,
    fetch: Callable,
    build_view: Callable[..., TableView],
    finished: Callable,
    outage_grace: float = MONITOR_OUTAGE_GRACE,
    notes: Sequence[str] = (),
    presentation: Display = Display(),
):
    """Refresh a view until ``finished`` holds for the fetched state; returns the
    final state.

    Watching only reads, so a server that drops away for a moment (a restarted
    apiserver, a dropped port-forward) does not end the watch: the last view
    stays up and polling resumes when the server answers again. Only an outage
    longer than ``outage_grace`` seconds is raised."""
    import requests

    transient = (requests.exceptions.ConnectionError, requests.exceptions.Timeout)
    started = monotonic()
    down_since = None

    def frame(done: bool = False):
        view = build_view(state)
        view.notes += notes
        watched = elapsed_label(monotonic() - started)
        if done:
            view.context.append(("Watched for", watched))
        else:
            view.context += [
                ("Watching for", watched),
                ("Refresh", f"every {MONITOR_REFRESH}s"),
            ]
        return renderable(view, presentation)

    with Live(
        frame(), auto_refresh=False, console=make_console(), transient=False
    ) as live:

        def poll():
            nonlocal down_since
            try:
                fresh = fetch()
            except transient as e:
                now = monotonic()
                if down_since is None:
                    down_since = now
                    live.console.print(
                        f"Lost the connection to the server ({e}); retrying for "
                        f"up to {outage_grace:g}s."
                    )
                if now - down_since > outage_grace:
                    raise
                return None
            if down_since is not None:
                live.console.print("Connection to the server restored.")
                down_since = None
            return fresh

        while True:
            fresh = poll()
            if fresh is not None:
                state = fresh
                if finished(state):
                    live.update(frame(done=True), refresh=True)
                    break
            live.update(frame(), refresh=True)
            sleep(MONITOR_REFRESH)
    click.echo()
    return state


def pick_worker(
    ctx: click.Context, server: TabsdataServer, request_filter: List[str], subject: str
) -> str:
    """Resolve a worker to show logs for: the only one directly, or one the
    user picks from the rendered listing when several ran."""
    workers = server.list_workers(filter=request_filter)
    if not workers:
        raise click.ClickException(f"{subject} has no workers yet — nothing has run.")
    if len(workers) == 1:
        return workers[0].id
    if output_format(ctx) == "table":
        show(workers_view(workers, indexed=True))
    if output_format(ctx) != "table" or ctx.obj["no_prompt"]:
        raise click.ClickException(
            f"{subject} has {len(workers)} workers. Pick one and run "
            "'tdk worker logs --worker <WORKER-ID>'."
        )
    choice = logical_prompt(ctx, "Index or ID of the worker to show logs for")
    choice = (choice or "").strip()
    if is_valid_id(choice):
        return choice
    try:
        index = int(choice)
    except ValueError:
        raise click.ClickException(
            f"'{choice}' is neither a worker ID nor an index in the list."
        )
    if not 1 <= index <= len(workers):
        raise click.ClickException(f"Index {index} is out of range (1-{len(workers)}).")
    return workers[index - 1].id


def emit_worker_logs(
    ctx: click.Context, server: TabsdataServer, worker_id: str, file: str
) -> None:
    generated_logs = (
        f"Worker {worker_id}" + os.linesep + server.get_worker_log(worker_id)
    )
    if file == "-":
        click.echo(generated_logs, nl=False)
        return
    if output_format(ctx) != "table":
        if file:
            with open(file, "w") as f:
                f.write(generated_logs)
        render(ctx, data={"logs": generated_logs})
        return
    if file:
        click.echo(f"Saving logs to file '{file}'")
        with open(file, "w") as f:
            f.write(generated_logs)
        click.echo("Logs saved successfully")
    else:
        click.echo(generated_logs)


def confirm_cancel(ctx: click.Context, confirm: str, subject: str) -> None:
    confirm = confirm or logical_prompt(
        ctx, f"Please type 'cancel' to confirm canceling {subject}"
    )
    if confirm != "cancel":
        raise click.ClickException(
            "Cancellation not confirmed. The confirmation word is 'cancel'."
        )
