#
# Copyright 2026 Tabsdata Inc.
#

"""Shared helpers for the execution-family command groups
(``plan``, ``trx``, ``run``, ``worker``)."""

from __future__ import annotations

import os
from itertools import islice
from time import monotonic, sleep
from typing import TYPE_CHECKING, Callable, List

import rich_click as click
from rich.live import Live
from rich.table import Table

from tabsdatak.cli.cli_utils import (
    beautify_time,
    collection_label,
    is_valid_id,
    logical_prompt,
    make_console,
    output_format,
    render,
    take,
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


def echo_plan_status(server: TabsdataServer, plan: str) -> None:
    execution = server.get_execution(plan)
    click.echo(f"The current status of the plan '{plan}' is '{execution.status}'")


def echo_transaction_status(server: TabsdataServer, trx: str) -> None:
    transaction = server.get_transaction(trx)
    click.echo(
        f"The current status of the transaction '{trx}' is '{transaction.status}'"
    )


def echo_function_run_status(server: TabsdataServer, fnrun: str) -> None:
    function_run = server.get_function_run(fnrun)
    click.echo(
        f"The current status of the function run '{fnrun}' is '{function_run.status}'"
    )


def patch_label(item) -> str:
    """The patch a run/worker executed (a function-version id), or '-' for head
    code. The patch id is surfaced onto the row as `function_version_patch_id`;
    absent/None means unpatched (head)."""
    return item.kwargs.get("function_version_patch_id") or "-"


def _truncate(text: str, width: int = 20) -> str:
    text = text or ""
    return text if len(text) <= width else text[:width] + "..."


def plans_table(executions, title: str = "Plans") -> Table:
    table = Table(title=title)
    table.add_column("ID", style="cyan", no_wrap=True)
    table.add_column("Name")
    table.add_column("Project")
    table.add_column("Trigger coll / fn", no_wrap=True)
    table.add_column("Triggered by")
    table.add_column("Start time", no_wrap=True)
    table.add_column("End time", no_wrap=True)
    table.add_column("Status", no_wrap=True)
    for execution in executions:
        table.add_row(
            execution.id,
            _truncate(execution.name),
            execution.project or "-",
            f"{collection_label(execution)} / {execution.function.name}",
            execution.triggered_by,
            beautify_time(execution.started_on_str),
            beautify_time(execution.ended_on_str),
            execution.status,
        )
    return table


def transactions_table(transactions) -> Table:
    table = Table(title="Transactions")
    table.add_column("ID", style="cyan", no_wrap=True)
    table.add_column("Execution plan ID", no_wrap=True)
    table.add_column("Project")
    table.add_column("Collection")
    table.add_column("Triggered by")
    table.add_column("Start time", no_wrap=True)
    table.add_column("End time", no_wrap=True)
    table.add_column("Status", no_wrap=True)
    for transaction in transactions:
        table.add_row(
            transaction.id,
            transaction.execution.id,
            transaction.project or "-",
            collection_label(transaction),
            transaction.triggered_by,
            beautify_time(transaction.started_on_str),
            beautify_time(transaction.ended_on_str),
            transaction.status,
        )
    return table


def function_runs_table(function_runs, title: str = "Function runs") -> Table:
    table = Table(title=title)
    table.add_column("Function run ID", style="cyan", no_wrap=True)
    table.add_column("Function")
    table.add_column("Execution plan ID", no_wrap=True)
    table.add_column("Transaction ID", no_wrap=True)
    table.add_column("Project")
    table.add_column("Collection")
    table.add_column("Start time", no_wrap=True)
    table.add_column("End time", no_wrap=True)
    table.add_column("Patch")
    table.add_column("Status")
    for fn_run in function_runs:
        table.add_row(
            fn_run.id,
            fn_run.function.name,
            fn_run.execution.id,
            fn_run.transaction.id,
            fn_run.project or "-",
            collection_label(fn_run),
            beautify_time(fn_run.started_on_str),
            beautify_time(fn_run.ended_on_str),
            patch_label(fn_run),
            fn_run.status,
        )
    return table


def workers_table(workers, title: str = "Workers", indexed: bool = False) -> Table:
    table = Table(title=title)
    if indexed:
        table.add_column("Index")
    table.add_column("Worker ID", style="cyan", no_wrap=True)
    table.add_column("Function run ID", no_wrap=True)
    table.add_column("Function")
    table.add_column("Execution plan ID", no_wrap=True)
    table.add_column("Transaction ID", no_wrap=True)
    table.add_column("Project")
    table.add_column("Collection")
    table.add_column("Start time", no_wrap=True)
    table.add_column("End time", no_wrap=True)
    table.add_column("Patch")
    table.add_column("Status")
    for position, worker in enumerate(workers, start=1):
        row = [str(position)] if indexed else []
        row.extend([
            worker.id,
            worker.kwargs.get("logical_function_run_id") or "-",
            worker.function.name,
            worker.kwargs.get("execution_id") or "-",
            worker.kwargs.get("transaction_id") or "-",
            worker.project or "-",
            collection_label(worker),
            beautify_time(worker.started_on_str),
            beautify_time(worker.ended_on_str),
            patch_label(worker),
            worker_status_label(worker),
        ])
        table.add_row(*row)
    return table


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


def live_monitor(
    items,
    fetch: Callable,
    build_table: Callable,
    is_final: Callable,
    outage_grace: float = MONITOR_OUTAGE_GRACE,
):
    """Refresh a listing until every item reaches a final status; returns the
    final listing.

    Watching only reads, so a server that drops away for a moment (a restarted
    apiserver, a dropped port-forward) does not end the watch: the last listing
    stays up and polling resumes when the server answers again. Only an outage
    longer than ``outage_grace`` seconds is raised."""
    import requests

    transient = (requests.exceptions.ConnectionError, requests.exceptions.Timeout)
    refresh_rate = 1  # seconds
    down_since = None
    with Live(
        build_table(items), refresh_per_second=refresh_rate, console=make_console()
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
                items = fresh
                live.update(build_table(items))
                if all(is_final(item) for item in items):
                    break
            sleep(1 / refresh_rate)
    return items


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
        click.echo()
        make_console().print(workers_table(workers, indexed=True))
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
        render(ctx, data={"logs": generated_logs}, table=lambda: None)
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
