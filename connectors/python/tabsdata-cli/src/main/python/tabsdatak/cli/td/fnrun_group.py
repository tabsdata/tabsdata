#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import TYPE_CHECKING, List

import rich_click as click

from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    collection_label,
    format_option,
    hint_common_solutions,
    logical_prompt,
    max_result_option,
    present,
    render,
    take,
    verify_login_or_prompt,
    view_options,
)
from tabsdatak.cli.td.execution_utils import (
    emit_worker_logs,
    function_runs_view,
    obtain_list_fn_run_filters,
    patch_label,
    pick_worker,
    span_projects,
    status_context,
    workers_view,
)
from tabsdatak.common._tabular import Kind, RecordView
from tabsdatak.sdk.status_utils.function_run import (
    FUNCTION_RUN_VALID_USER_PROVIDED_STATUSES,
)

if TYPE_CHECKING:
    from tabsdatak.sdk.tabsdata_server import TabsdataServer


@cli_command
@click.group()
def fnrun():
    """Function run management commands"""


@fnrun.command(name="list")
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
@format_option
@view_options()
@max_result_option
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
        list_of_function_runs, truncated = take(
            ctx, server.list_function_runs_generator(filter=request_filter)
        )

        def _view():
            view = function_runs_view(list_of_function_runs)
            view.context = status_context(server, plan=plan, trx=trx)
            return view

        render(
            ctx,
            data=[r._data for r in list_of_function_runs],
            view=_view,
            truncated=truncated,
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list function runs: {e}")


@fnrun.command()
@click.option("--fnrun", required=True, help="ID of the function run to display.")
@format_option
@view_options()
@click.pass_context
def get(ctx: click.Context, fnrun: str):
    """
    Display a function run: its details plus its workers.
    """
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        span_projects(server)
        function_run = server.get_function_run(fnrun)
        workers = server.list_workers(filter=f"logical_function_run_id:eq:{fnrun}")

        def _views():
            return [
                RecordView(
                    title="Function run",
                    context=[("ID", function_run.id)],
                    fields=[
                        ("Project", present(function_run.project)),
                        ("Collection", collection_label(function_run)),
                        ("Function", function_run.function.name),
                        ("Plan name", present(function_run.execution.name)),
                        ("Plan ID", function_run.execution.id),
                        ("Transaction ID", function_run.transaction.id),
                        ("Started on", present(function_run.started_on_str)),
                        ("Ended on", present(function_run.ended_on_str)),
                        ("Patch", present(patch_label(function_run))),
                        ("Status", present(function_run.status)),
                    ],
                    kinds={"Status": Kind.STATUS},
                ),
                workers_view(workers),
            ]

        render(
            ctx,
            data={
                "function_run": function_run._data,
                "workers": [w._data for w in workers],
            },
            view=_views,
        )

    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to display function run: {e}")


@fnrun.command()
@click.option(
    "--fnrun", required=True, help="ID of the function run whose logs to show."
)
@click.option(
    "--file",
    help=(
        "Path of the file where the logs will be saved, or '-' to write them to "
        "stdout. Any folders provided before the file must already exist."
    ),
)
@format_option
@click.pass_context
def logs(ctx: click.Context, fnrun: str, file: str):
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
            [f"logical_function_run_id:eq:{fnrun}"],
            f"Function run '{fnrun}'",
        )
        emit_worker_logs(ctx, server, target, file)
    except click.exceptions.Exit:
        raise
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to show logs: {e}")


@fnrun.command()
@click.option("--fnrun", help="ID of the function run to replay.")
@click.pass_context
def replay(ctx: click.Context, fnrun: str):
    """Re-execute a finished function run.

    Mints a new concrete physical function run against the latest patch (or
    head, if no patch exists). The pointer stays bound to the previous
    attempt's outputs until the new attempt finishes successfully.
    """
    verify_login_or_prompt(ctx)
    fnrun = fnrun or logical_prompt(ctx, "ID of the function run to replay")
    click.echo(f"Replaying function run '{fnrun}'")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        result = server.replay_logical_function_run(fnrun)
        click.echo("Replay scheduled")
        click.echo(f"Run id:               {result.get('logical_function_run_id')}")
        click.echo(f"Previous FR (bound):  {result.get('previous_function_run_id')}")
        click.echo(f"New FR (running):     {result.get('new_function_run_id')}")
        click.echo(f"Function version id:  {result.get('function_version_id')}")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to replay function run: {e}")


@fnrun.command()
@click.option("--fnrun", required=True, help="ID of the function run to recover.")
@click.pass_context
def recover(ctx: click.Context, fnrun: str):
    """
    Recover the transaction a function run belongs to: the transaction's
    failed or on-hold function runs are re-scheduled, each re-running its
    pinned function version and that version's latest patch.
    """
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        span_projects(server)
        function_run = server.get_function_run(fnrun)
        trx = function_run.transaction.id
        click.echo(f"Recovering transaction '{trx}' of function run '{fnrun}'")
        click.echo("-" * 10)
        server.recover_transaction(trx)
        click.echo("Transaction recovered successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to recover function run: {e}")
