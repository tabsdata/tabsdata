#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import TYPE_CHECKING, List

import rich_click as click

from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    hint_common_solutions,
    make_console,
    output_format,
    output_option,
    render,
    show_hint,
    verify_login_or_prompt,
)
from tabsdatak.cli.td.execution_utils import (
    echo_function_run_status,
    echo_plan_status,
    echo_transaction_status,
    emit_worker_logs,
    obtain_list_worker_filters,
    span_projects,
    workers_table,
)
from tabsdatak.sdk.status_utils.worker import WORKER_VALID_USER_PROVIDED_STATUSES

if TYPE_CHECKING:
    from tabsdatak.sdk.tabsdata_server import TabsdataServer


@cli_command
@click.group()
def worker():
    """Worker management commands"""


@worker.command(name="list")
@click.option("--proj", type=str, help="Name of the project to filter workers by.")
@click.option(
    "--status",
    type=str,
    multiple=True,
    help=(
        "Filter workers by status. The possible statuses are "
        f"'{WORKER_VALID_USER_PROVIDED_STATUSES}'. This field is "
        "case-insensitive. "
    ),
)
@click.option(
    "--coll",
    type=str,
    help="Collection of the function to which the workers belong.",
)
@click.option(
    "--fn", type=str, help="Name of the function to which the workers belong."
)
@click.option("--plan", type=str, help="ID of the plan to filter workers by.")
@click.option(
    "--plan-name",
    help=(
        "A plan name wildcard to match. "
        "For example, 'my_plan*' will match all plans "
        "with names starting with 'my_plan'."
    ),
)
@click.option("--fnrun", type=str, help="ID of the function run to filter workers by.")
@click.option("--trx", type=str, help="ID of the transaction to filter workers by.")
@output_option
@click.pass_context
def list_workers(
    ctx: click.Context,
    proj: str,
    status: List[str],
    coll: str,
    fn: str,
    plan: str,
    plan_name: str,
    fnrun: str,
    trx: str,
):
    """List workers across every project you can view."""
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        span_projects(server)
        request_filter = obtain_list_worker_filters(
            status=status,
            exe_name=plan_name,
            execution_id=plan,
            fn=fn,
            fnrun=fnrun,
            collection=coll,
            trx=trx,
            project=proj,
        )
        if output_format(ctx) == "table":
            if plan:
                echo_plan_status(server, plan)
            if trx:
                echo_transaction_status(server, trx)
            if fnrun:
                echo_function_run_status(server, fnrun)
        list_of_workers = server.list_workers(filter=request_filter)

        if output_format(ctx) != "table":
            render(
                ctx,
                data=[w._data for w in list_of_workers],
                table=lambda: None,
            )
            return

        click.echo()
        console = make_console()
        console.print(workers_table(list_of_workers))
        click.echo(f"Number of workers: {len(list_of_workers)}")
        click.echo()
        show_hint(
            ctx,
            "Read logs with 'tdk worker logs --worker <WORKER-ID>', or "
            "'tdk fnrun logs --fnrun <RUN-ID>' for a function run's workers.",
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list workers: {e}")


@worker.command()
@click.option("--worker", required=True, help="ID of the worker whose logs to show.")
@click.option(
    "--file",
    help=(
        "Path of the file where the logs will be saved, or '-' to write them to "
        "stdout. Any folders provided before the file must already exist."
    ),
)
@output_option
@click.pass_context
def logs(ctx: click.Context, worker: str, file: str):
    """
    Show the logs of a worker.
    """
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        emit_worker_logs(ctx, server, worker, file)
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to show logs: {e}")


@worker.command()
@click.option("--worker", required=True, help="ID of the worker to recover.")
@click.pass_context
def recover(ctx: click.Context, worker: str):
    """
    Recover the transaction a worker belongs to: the transaction's failed or
    on-hold function runs are re-scheduled.
    """
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        span_projects(server)
        target = server.get_worker(worker)
        trx = target.transaction.id
        click.echo(f"Recovering transaction '{trx}' of worker '{worker}'")
        click.echo("-" * 10)
        server.recover_transaction(trx)
        click.echo("Transaction recovered successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to recover worker: {e}")
