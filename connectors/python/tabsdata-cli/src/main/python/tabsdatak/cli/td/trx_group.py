#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import TYPE_CHECKING, List

import rich_click as click
from rich.table import Table

from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    beautify_time,
    collection_label,
    hint_common_solutions,
    make_console,
    output_format,
    output_option,
    render,
    show_hint,
    verify_login_or_prompt,
)
from tabsdatak.cli.td.execution_utils import (
    confirm_cancel,
    emit_worker_logs,
    function_runs_table,
    live_monitor,
    obtain_list_trx_filters,
    obtain_transaction_list,
    pick_worker,
    span_projects,
    transactions_table,
    workers_table,
)
from tabsdatak.sdk.status_utils.transaction import (
    TRANSACTION_FINAL_STATUSES,
    TRANSACTION_SUCCESSFUL_FINAL_STATUSES,
    TRANSACTION_VALID_USER_PROVIDED_STATUSES,
)

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
@click.group()
def trx():
    """Transaction management commands"""


@trx.command(name="list")
@click.option("--proj", type=str, help="Name of the project to filter transactions by.")
@click.option(
    "--status",
    type=str,
    multiple=True,
    help=(
        "Filter transactions by status. The possible statuses are "
        f"'{TRANSACTION_VALID_USER_PROVIDED_STATUSES}'. This field is "
        "case-insensitive. "
    ),
)
@click.option(
    "--coll", type=str, help="Name of the collection to filter transactions by."
)
@click.option("--plan", help="ID of the plan to filter transactions by.")
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
        "reach a final state."
    ),
)
@output_option
@click.pass_context
def list_transactions(
    ctx: click.Context,
    proj: str,
    status: List[str],
    coll: str,
    plan: str,
    plan_name: str,
    last: bool,
    at: str,
    monitor: bool,
):
    """List transactions across every project you can view."""
    verify_login_or_prompt(ctx)
    try:
        request_filter = obtain_list_trx_filters(
            status=status,
            exe_name=plan_name,
            execution_id=plan,
            collection=coll,
            at=at,
            project=proj,
        )
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        span_projects(server)
        list_of_transactions = obtain_transaction_list(server, request_filter, last)

        if output_format(ctx) != "table":
            render(
                ctx,
                data=[t._data for t in list_of_transactions],
                table=lambda: None,
            )
            return

        if monitor:
            click.echo("Waiting for the transactions to finish...")
            list_of_transactions = live_monitor(
                list_of_transactions,
                lambda: obtain_transaction_list(server, request_filter, last),
                transactions_table,
                lambda t: t.status in TRANSACTION_FINAL_STATUSES,
            )
            click.echo("Transactions finished.")
        else:
            click.echo()
            console = make_console()
            console.print(transactions_table(list_of_transactions))
        click.echo()
        if not all([
            aux_trx.status in TRANSACTION_SUCCESSFUL_FINAL_STATUSES
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


@trx.command()
@click.option("--trx", required=True, help="ID of the transaction to display.")
@output_option
@click.pass_context
def get(ctx: click.Context, trx: str):
    """
    Display a transaction: its details plus its function runs and workers.
    """
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        span_projects(server)
        transaction = server.get_transaction(trx)
        function_runs = server.list_function_runs(filter=f"transaction_id:eq:{trx}")
        workers = server.list_workers(filter=f"transaction_id:eq:{trx}")

        if output_format(ctx) != "table":
            render(
                ctx,
                data={
                    "transaction": transaction._data,
                    "function_runs": [r._data for r in function_runs],
                    "workers": [w._data for w in workers],
                },
                table=lambda: None,
            )
            return

        table = Table(title=f"Transaction '{trx}'")
        table.add_column("Plan ID", no_wrap=True)
        table.add_column("Project")
        table.add_column("Collection")
        table.add_column("Triggered on")
        table.add_column("Triggered by")
        table.add_column("Started on")
        table.add_column("Ended on")
        table.add_column("Status", no_wrap=True)
        table.add_row(
            transaction.execution.id,
            transaction.project or "-",
            collection_label(transaction),
            beautify_time(transaction.triggered_on_str),
            transaction.triggered_by,
            beautify_time(transaction.started_on_str),
            beautify_time(transaction.ended_on_str),
            transaction.status,
        )

        click.echo()
        console = make_console()
        console.print(table)
        console.print(function_runs_table(function_runs))
        console.print(workers_table(workers))
        click.echo()

    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to display transaction: {e}")


@trx.command()
@click.option("--trx", required=True, help="ID of the transaction to cancel.")
@click.option(
    "--confirm",
    help="Write 'cancel' to confirm cancellation. Will be prompted for it if "
    "not provided.",
)
@click.pass_context
def cancel(ctx: click.Context, trx: str, confirm: str):
    """
    Cancel a transaction and its non-committed function runs.
    Canceled runs cannot be recovered.
    """
    verify_login_or_prompt(ctx)
    click.echo(f"Canceling transaction with ID '{trx}'")
    click.echo("-" * 10)
    confirm_cancel(ctx, confirm, f"transaction '{trx}'")
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.cancel_transaction(trx)
        click.echo("Transaction canceled successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to cancel transaction: {e}")


@trx.command()
@click.option("--trx", required=True, help="ID of the transaction to recover.")
@click.pass_context
def recover(ctx: click.Context, trx: str):
    """
    Recover a transaction: its failed or on-hold function runs are
    re-scheduled.
    """
    verify_login_or_prompt(ctx)
    click.echo(f"Recovering transaction with ID '{trx}'")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.recover_transaction(trx)
        click.echo("Transaction recovered successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to recover transaction: {e}")


@trx.command()
@click.option("--trx", required=True, help="ID of the transaction to monitor.")
@click.pass_context
def monitor(ctx: click.Context, trx: str):
    """
    Monitor the execution of a transaction.
    """
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        span_projects(server)
        transaction = server.get_transaction(trx)
        from tabsdatak.cli.td.fn_group import _monitor_execution_or_transaction

        _monitor_execution_or_transaction(ctx, transaction=transaction)
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to monitor transaction: {e}")


@trx.command()
@click.option("--trx", required=True, help="ID of the transaction whose logs to show.")
@click.option(
    "--file",
    help=(
        "Path of the file where the logs will be saved, or '-' to write them to "
        "stdout. Any folders provided before the file must already exist."
    ),
)
@output_option
@click.pass_context
def logs(ctx: click.Context, trx: str, file: str):
    """
    Show logs of one of the transaction's workers; when several ran, pick one
    from the listing.
    """
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        span_projects(server)
        target = pick_worker(
            ctx, server, [f"transaction_id:eq:{trx}"], f"Transaction '{trx}'"
        )
        emit_worker_logs(ctx, server, target, file)
    except click.exceptions.Exit:
        raise
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to show logs: {e}")
