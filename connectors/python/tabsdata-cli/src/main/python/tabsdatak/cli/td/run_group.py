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
    logical_prompt,
    make_console,
    output_format,
    output_option,
    render,
    verify_login_or_prompt,
)
from tabsdatak.cli.td.execution_utils import (
    echo_plan_status,
    echo_transaction_status,
    emit_worker_logs,
    function_runs_table,
    obtain_list_fn_run_filters,
    pick_worker,
    span_projects,
    workers_table,
)
from tabsdatak.sdk.status_utils.function_run import (
    FUNCTION_RUN_VALID_USER_PROVIDED_STATUSES,
)

if TYPE_CHECKING:
    from tabsdatak.sdk.tabsdata_server import TabsdataServer


@cli_command
@click.group()
def run():
    """Function run management commands"""


@run.command(name="list")
@click.option(
    "--proj", type=str, help="Name of the project to filter function runs by."
)
@click.option(
    "--status",
    type=str,
    multiple=True,
    help=(
        "Filter function runs by status. The possible statuses are "
        f"'{FUNCTION_RUN_VALID_USER_PROVIDED_STATUSES}'. This field is "
        "case-insensitive. "
    ),
)
@click.option(
    "--coll",
    type=str,
    help="Collection of the function to which the function runs belong.",
)
@click.option(
    "--fn",
    type=str,
    help="Name of the function to which the function runs belong.",
)
@click.option("--plan", type=str, help="ID of the plan to filter function runs by.")
@click.option(
    "--plan-name",
    help=(
        "A plan name wildcard to match. "
        "For example, 'my_plan*' will match all plans "
        "with names starting with 'my_plan'."
    ),
)
@click.option(
    "--trx", type=str, help="ID of the transaction to filter function runs by."
)
@output_option
@click.pass_context
def list_runs(
    ctx: click.Context,
    proj: str,
    status: List[str],
    coll: str,
    fn: str,
    plan: str,
    plan_name: str,
    trx: str,
):
    """List function runs across every project you can view."""
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        span_projects(server)
        request_filter = obtain_list_fn_run_filters(
            status=status,
            exe_name=plan_name,
            execution_id=plan,
            fn=fn,
            collection=coll,
            trx=trx,
            project=proj,
        )
        if output_format(ctx) == "table":
            if plan:
                echo_plan_status(server, plan)
            if trx:
                echo_transaction_status(server, trx)
        list_of_function_runs = server.list_function_runs(filter=request_filter)

        if output_format(ctx) != "table":
            render(
                ctx,
                data=[r._data for r in list_of_function_runs],
                table=lambda: None,
            )
            return

        click.echo()
        console = make_console()
        console.print(function_runs_table(list_of_function_runs))
        click.echo(f"Number of function runs: {len(list_of_function_runs)}")
        click.echo()
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list function runs: {e}")


@run.command()
@click.option("--run", required=True, help="ID of the function run to display.")
@output_option
@click.pass_context
def get(ctx: click.Context, run: str):
    """
    Display a function run: its details plus its workers.
    """
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        span_projects(server)
        function_run = server.get_function_run(run)
        workers = server.list_workers(filter=f"logical_function_run_id:eq:{run}")

        if output_format(ctx) != "table":
            render(
                ctx,
                data={
                    "function_run": function_run._data,
                    "workers": [w._data for w in workers],
                },
                table=lambda: None,
            )
            return

        table = Table(title=f"Function run '{run}'")
        table.add_column("Project")
        table.add_column("Collection")
        table.add_column("Function")
        table.add_column("Plan name")
        table.add_column("Plan ID", no_wrap=True)
        table.add_column("Transaction ID", no_wrap=True)
        table.add_column("Started on")
        table.add_column("Ended on")
        table.add_column("Status", no_wrap=True)
        table.add_row(
            function_run.project or "-",
            collection_label(function_run),
            function_run.function.name,
            function_run.execution.name,
            function_run.execution.id,
            function_run.transaction.id,
            beautify_time(function_run.started_on_str),
            beautify_time(function_run.ended_on_str),
            function_run.status,
        )

        click.echo()
        console = make_console()
        console.print(table)
        console.print(workers_table(workers))
        click.echo()

    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to display function run: {e}")


@run.command()
@click.option("--run", required=True, help="ID of the function run whose logs to show.")
@click.option(
    "--file",
    help=(
        "Path of the file where the logs will be saved, or '-' to write them to "
        "stdout. Any folders provided before the file must already exist."
    ),
)
@output_option
@click.pass_context
def logs(ctx: click.Context, run: str, file: str):
    """
    Show logs of one of the function run's workers; when several ran (retries),
    pick one from the listing.
    """
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        span_projects(server)
        target = pick_worker(
            ctx,
            server,
            [f"logical_function_run_id:eq:{run}"],
            f"Function run '{run}'",
        )
        emit_worker_logs(ctx, server, target, file)
    except click.exceptions.Exit:
        raise
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to show logs: {e}")


@run.command()
@click.option("--run", help="ID of the function run to replay.")
@click.pass_context
def replay(ctx: click.Context, run: str):
    """Re-execute a finished function run.

    Mints a new concrete physical function run against the latest patch (or
    head, if no patch exists). The pointer stays bound to the previous
    attempt's outputs until the new attempt finishes successfully.
    """
    verify_login_or_prompt(ctx)
    run = run or logical_prompt(ctx, "ID of the function run to replay")
    click.echo(f"Replaying function run '{run}'")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        result = server.replay_logical_function_run(run)
        click.echo("Replay scheduled")
        click.echo(f"Run id:               {result.get('logical_function_run_id')}")
        click.echo(f"Previous FR (bound):  {result.get('previous_function_run_id')}")
        click.echo(f"New FR (running):     {result.get('new_function_run_id')}")
        click.echo(f"Function version id:  {result.get('function_version_id')}")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to replay function run: {e}")


@run.command()
@click.option("--run", required=True, help="ID of the function run to recover.")
@click.pass_context
def recover(ctx: click.Context, run: str):
    """
    Recover the transaction a function run belongs to: the transaction's
    failed or on-hold function runs are re-scheduled.
    """
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        span_projects(server)
        function_run = server.get_function_run(run)
        trx = function_run.transaction.id
        click.echo(f"Recovering transaction '{trx}' of function run '{run}'")
        click.echo("-" * 10)
        server.recover_transaction(trx)
        click.echo("Transaction recovered successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to recover function run: {e}")
