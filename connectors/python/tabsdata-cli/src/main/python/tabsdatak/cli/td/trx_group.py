#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import TYPE_CHECKING, List

import rich_click as click

from tabsdatak.cli._capability import capability
from tabsdatak.cli._command import TdkGroup
from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    collection_label,
    display,
    format_option,
    hint_common_solutions,
    list_options,
    max_result_option,
    output_format,
    present,
    progress,
    render,
    render_list,
    render_status_done,
    show_hint,
    status_changed,
    view_options,
)
from tabsdatak.cli.td._wait import (
    render_waited_transaction,
    require_wait,
    timeout_option,
    wait_listing,
    wait_option,
    wait_transaction,
)
from tabsdatak.cli.td.execution_utils import (
    PLAN_ID,
    TRX_ID,
    all_workers_option,
    confirm_cancel,
    emit_scope_logs,
    failed_only_option,
    function_runs_view,
    obtain_list_trx_filters,
    obtain_transaction_list,
    span_projects,
    status_codes,
    status_filter_options,
    tail_option,
    transactions_view,
    workers_view,
)
from tabsdatak.common._tabular import Kind, RecordView, renderable
from tabsdatak.sdk.status_utils.groups import Entity, Health, annotate, health

if TYPE_CHECKING:
    from tabsdatak.sdk.tabsdata_server import TabsdataServer

AT_OPTION_HELP = (
    "If provided, only transactions started before that time will be shown. Must be "
    "either a valid timestamp in the form of a unix timestamp (milliseconds since "
    "epoch without a dot, e.g. '1750074554472') or a valid date-time format. "
    "The valid "
    "formats are 'YYYY-MM-DD', 'YYYY-MM-DDTHH', 'YYYY-MM-DDTHH:MM', "
    "'YYYY-MM-DDTHH:MM:SS', and 'YYYY-MM-DDTHH:MM:SS.sss'. A 'Z' character can be "
    "added at the end to indicate UTC time (e.g., '2023-10-01T12Z' or "
    "'2023-10-01T12:00:00.000Z') or it can be omitted to indicate local "
    "time (e.g., '2023-10-01T12' or '2023-10-01T12:00:00.000')."
)


@cli_command
@click.group(cls=TdkGroup)
def trx():
    """Transaction management commands"""


@capability("operations", "read")
@trx.command(name="list")
@click.option(
    "--proj", "-P", type=str, help="Name of the project to filter transactions by."
)
@status_filter_options(Entity.TRANSACTION)
@click.option(
    "--coll", "-c", type=str, help="Name of the collection to filter transactions by."
)
@click.option("--plan", type=PLAN_ID, help="ID of the plan to filter transactions by.")
@click.option(
    "--plan-name",
    help=(
        "A plan name wildcard to match. "
        "For example, 'my_plan*' will match all plans "
        "with names starting with 'my_plan'."
    ),
)
@click.option(
    "--last",
    is_flag=True,
    help="If set, only the last transaction of the list will be shown.",
)
@click.option("--at", help=AT_OPTION_HELP, type=str)
@click.option(
    "--monitor",
    is_flag=True,
    help=(
        "If set, the command will monitor the listed transactions until they all "
        "reach a final state. Exits 7 when '--timeout' runs out."
        " A lost connection to the server is retried for up to two minutes "
        "before the command fails."
    ),
)
@timeout_option()
@format_option
@view_options()
@max_result_option
@list_options
@click.pass_context
def list_transactions(
    ctx: click.Context,
    proj: str,
    status: List[str],
    healths: tuple[str, ...],
    coll: str,
    plan: str,
    plan_name: str,
    last: bool,
    at: str,
    monitor: bool,
    timeout: float | None,
):
    """List transactions across every project you can view."""
    if timeout is not None and not monitor:
        raise click.UsageError("'--timeout' applies only together with '--monitor'.")
    try:
        request_filter = obtain_list_trx_filters(
            status=status_codes(Entity.TRANSACTION, status, healths),
            exe_name=plan_name,
            execution_id=plan,
            collection=coll,
            at=at,
            project=proj,
        )
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        span_projects(server)
        list_of_transactions, truncated = obtain_transaction_list(
            ctx, server, request_filter, last
        )
        if monitor:
            list_of_transactions = wait_listing(
                ctx,
                server,
                list_of_transactions,
                lambda: obtain_transaction_list(ctx, server, request_filter, last)[0],
                Entity.TRANSACTION,
                lambda transactions: renderable(
                    transactions_view(transactions), display(ctx)
                ),
                "transactions",
                timeout,
            )

        render_list(
            ctx,
            items=lambda: [
                annotate(Entity.TRANSACTION, t._data) for t in list_of_transactions
            ],
            truncated=truncated,
            view=None if monitor else lambda: transactions_view(list_of_transactions),
        )
        if output_format(ctx) != "table":
            return
        if not all([
            health(Entity.TRANSACTION, aux_trx.status) is Health.OK
            for aux_trx in list_of_transactions
        ]):
            show_hint(
                ctx,
                "If you want to explore why a transaction failed, run "
                "'tdk trx logs --trx <TRANSACTION-ID>' to pick a worker and read "
                "its logs, or 'tdk worker list --trx <TRANSACTION-ID>' to see the "
                "workers.",
            )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list transactions: {e}")


@capability("operations", "read")
@trx.command()
@click.option(
    "--trx", type=TRX_ID, required=True, help="ID of the transaction to display."
)
@wait_option("the transaction")
@timeout_option()
@format_option
@view_options()
@click.pass_context
def get(ctx: click.Context, trx: str, wait: bool, timeout: float | None):
    """
    Display a transaction: its details plus its function runs and workers.
    """
    require_wait(wait, timeout)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        span_projects(server)
        transaction = server.get_transaction(trx)
        waited = {}
        if wait:
            document = wait_transaction(ctx, transaction, timeout, live=False)
            waited["waited_ms"] = document["waited_ms"]
        function_runs = server.list_function_runs(filter=f"transaction_id:eq:{trx}")
        workers = server.list_workers(filter=f"transaction_id:eq:{trx}")

        def _views():
            return [
                RecordView(
                    title="Transaction",
                    context=[("ID", transaction.id)],
                    fields=[
                        ("Plan ID", transaction.execution.id),
                        ("Project", present(transaction.project)),
                        ("Collection", collection_label(transaction)),
                        ("Triggered by", present(transaction.triggered_by)),
                        ("Triggered on", present(transaction.triggered_on_str)),
                        ("Started on", present(transaction.started_on_str)),
                        ("Ended on", present(transaction.ended_on_str)),
                        ("Status", present(transaction.status)),
                    ],
                    kinds={"Status": Kind.STATUS},
                ),
                function_runs_view(function_runs),
                workers_view(workers),
            ]

        render(
            ctx,
            data={
                "transaction": annotate(Entity.TRANSACTION, transaction._data),
                "function_runs": [
                    annotate(Entity.FUNCTION_RUN, r._data) for r in function_runs
                ],
                "workers": [annotate(Entity.WORKER, w._data) for w in workers],
                **waited,
            },
            view=_views,
        )

    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to display transaction: {e}")


@capability("operations", "destructive")
@trx.command()
@format_option
@click.option(
    "--trx", type=TRX_ID, required=True, help="ID of the transaction to cancel."
)
@click.option(
    "--confirm",
    help="Write 'cancel' to confirm cancellation. Will be prompted for it if "
    "not provided.",
)
@click.pass_context
def cancel(ctx: click.Context, trx: str, confirm: str):
    """
    Cancel a transaction and its non-committed function runs, and every
    non-committed transaction that read its output, in later plans too.
    Canceled runs cannot be recovered.
    """
    progress(ctx, f"Canceling transaction with ID '{trx}'")
    progress(ctx, "-" * 10)
    confirm_cancel(ctx, confirm, f"transaction '{trx}'")
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        changed = status_changed(server.cancel_transaction(trx))
        render_status_done(
            ctx,
            changed=changed,
            action="cancel",
            kind="transaction",
            id=trx,
            message="Transaction canceled successfully",
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to cancel transaction: {e}")


@capability("operations", "write")
@trx.command()
@format_option
@click.option(
    "--trx", type=TRX_ID, required=True, help="ID of the transaction to recover."
)
@wait_option("the recovered transaction")
@timeout_option()
@click.pass_context
def recover(ctx: click.Context, trx: str, wait: bool, timeout: float | None):
    """
    Recover a transaction: its failed or on-hold function runs are
    re-scheduled, each re-running its pinned function version and that
    version's latest patch.
    """
    require_wait(wait, timeout)
    progress(ctx, f"Recovering transaction with ID '{trx}'")
    progress(ctx, "-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        changed = status_changed(server.recover_transaction(trx))
        if wait and changed:
            render_waited_transaction(ctx, server, trx, timeout)
            return
        render_status_done(
            ctx,
            changed=changed,
            action="recover",
            kind="transaction",
            id=trx,
            message="Transaction recovered successfully",
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to recover transaction: {e}")


@capability("operations", "read")
@trx.command()
@format_option
@click.option(
    "--trx", type=TRX_ID, required=True, help="ID of the transaction to monitor."
)
@timeout_option()
@click.pass_context
def monitor(ctx: click.Context, trx: str, timeout: float | None):
    """
    Monitor the execution of a transaction until it finishes. Exits 7 when
    '--timeout' runs out and 8 when a function run did not succeed.
    """
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        render_waited_transaction(ctx, server, trx, timeout)
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to monitor transaction: {e}")


@capability("operations", "read", paths={"--file": "write"})
@trx.command()
@click.option(
    "--trx",
    type=TRX_ID,
    required=True,
    help="ID of the transaction whose logs to show.",
)
@all_workers_option
@tail_option
@failed_only_option
@click.option(
    "--file",
    help=(
        "Path of the file where the logs will be saved, or '-' to write them to "
        "stdout. Any folders provided before the file must already exist."
    ),
    type=click.Path(allow_dash=True),
)
@format_option
@click.pass_context
def logs(
    ctx: click.Context,
    trx: str,
    all_workers: bool,
    tail: int | None,
    failed_only: bool,
    file: str,
):
    """
    Show the logs of the transaction's workers: every worker's with '--all',
    newest first; else one worker's, picked from the listing when several ran.
    Each worker that did not succeed carries the error signature of its log.
    """
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        emit_scope_logs(
            ctx,
            server,
            f"Transaction '{trx}'",
            all_workers=all_workers,
            tail=tail,
            file=file,
            failed_only=failed_only,
            trx=trx,
        )
    except (click.exceptions.Exit, click.Abort):
        raise
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to show logs: {e}")
