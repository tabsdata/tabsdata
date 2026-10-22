#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import TYPE_CHECKING, List

import rich_click as click

from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    collection_label,
    display,
    format_option,
    hint_common_solutions,
    max_result_option,
    more_results_notes,
    output_format,
    present,
    render,
    show_hint,
    verify_login_or_prompt,
    view_options,
)
from tabsdatak.cli.td.execution_utils import (
    confirm_cancel,
    emit_worker_logs,
    function_runs_view,
    live_monitor,
    obtain_execution_list,
    obtain_list_exec_filters,
    pick_worker,
    plans_view,
    span_projects,
    workers_view,
)
from tabsdatak.common._tabular import Kind, RecordView
from tabsdatak.sdk.status_utils.execution import (
    EXECUTION_FINAL_STATUSES,
    EXECUTION_SUCCESSFUL_FINAL_STATUSES,
    EXECUTION_VALID_USER_PROVIDED_STATUSES,
)

if TYPE_CHECKING:
    from tabsdatak.sdk.tabsdata_server import TabsdataServer

AT_OPTION_HELP = (
    "If provided, only plans started before that time will be shown. Must be "
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
def plan():
    """Plan (execution) management commands"""


@plan.command(name="list")
@click.option("--proj", type=str, help="Name of the project to filter plans by.")
@click.option(
    "--status",
    type=str,
    multiple=True,
    help=(
        "Filter plans by status. The possible statuses are "
        f"'{EXECUTION_VALID_USER_PROVIDED_STATUSES}'. This field is "
        "case-insensitive. "
    ),
)
@click.option("--fn", type=str, help="Name of the function to filter plans by.")
@click.option("--coll", type=str, help="Name of the collection to filter plans by.")
@click.option(
    "--name",
    "-n",
    help=(
        "A plan name wildcard to match for the list. "
        "For example, 'my_plan*' will match all plans "
        "with names starting with 'my_plan'."
    ),
)
@click.option(
    "--last",
    is_flag=True,
    help="If set, only the last plan of the list will be shown.",
)
@click.option("--at", help=AT_OPTION_HELP, type=str)
@click.option(
    "--monitor",
    is_flag=True,
    help=(
        "If set, the command will monitor the listed plans until they all reach "
        "a final state."
        " A lost connection to the server is retried for up to two minutes "
        "before the command fails."
    ),
)
@format_option
@view_options()
@max_result_option
@click.pass_context
def list_plans(
    ctx: click.Context,
    proj: str,
    status: List[str],
    fn: str,
    coll: str,
    name: str,
    last: bool,
    at: str,
    monitor: bool,
):
    """List plans across every project you can view."""
    verify_login_or_prompt(ctx)
    request_filter = obtain_list_exec_filters(
        status=status,
        exe_name=name,
        fn=fn,
        collection=coll,
        at=at,
        project=proj,
    )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        span_projects(server)
        list_of_executions, truncated = obtain_execution_list(
            ctx, server, request_filter, last
        )

        if output_format(ctx) != "table" or not monitor:
            render(
                ctx,
                data=[e._data for e in list_of_executions],
                view=lambda: plans_view(list_of_executions),
                truncated=truncated,
            )
            if output_format(ctx) != "table":
                return
        else:
            list_of_executions = live_monitor(
                list_of_executions,
                lambda: obtain_execution_list(ctx, server, request_filter, last)[0],
                plans_view,
                lambda plans: all(e.status in EXECUTION_FINAL_STATUSES for e in plans),
                notes=more_results_notes(ctx, truncated),
                presentation=display(ctx),
            )
        if not all([
            aux_exe.status in EXECUTION_SUCCESSFUL_FINAL_STATUSES
            for aux_exe in list_of_executions
        ]):
            show_hint(
                ctx,
                "If you want to explore why a plan failed, run "
                "'tdk plan logs --plan <PLAN-ID>' to pick a worker and read its "
                "logs, or 'tdk worker list --plan <PLAN-ID>' to see the workers.",
            )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list plans: {e}")


@plan.command()
@click.option("--plan", required=True, help="ID of the plan to display.")
@format_option
@view_options()
@click.pass_context
def get(ctx: click.Context, plan: str):
    """
    Display a plan: its details plus its function runs and workers.
    """
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        span_projects(server)
        execution = server.get_execution(plan)
        function_runs = server.list_function_runs(filter=f"execution_id:eq:{plan}")
        workers = server.list_workers(filter=f"execution_id:eq:{plan}")

        def _views():
            return [
                RecordView(
                    title="Plan",
                    context=[execution.id],
                    fields=[
                        ("Name", present(execution.name)),
                        ("Project", present(execution.project)),
                        ("Collection", collection_label(execution)),
                        ("Function", execution.function.name),
                        ("Triggered by", present(execution.triggered_by)),
                        ("Triggered on", present(execution.triggered_on_str)),
                        ("Started on", present(execution.started_on_str)),
                        ("Ended on", present(execution.ended_on_str)),
                        ("Status", present(execution.status)),
                    ],
                    kinds={"Status": Kind.STATUS},
                ),
                function_runs_view(function_runs),
                workers_view(workers),
            ]

        render(
            ctx,
            data={
                "plan": execution._data,
                "function_runs": [r._data for r in function_runs],
                "workers": [w._data for w in workers],
            },
            view=_views,
        )

    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to display plan: {e}")


@plan.command()
@click.option("--plan", required=True, help="ID of the plan to cancel.")
@click.option(
    "--confirm",
    help="Write 'cancel' to confirm cancellation. Will be prompted for it if "
    "not provided.",
)
@click.pass_context
def cancel(ctx: click.Context, plan: str, confirm: str):
    """
    Cancel a plan: every non-committed transaction in it is canceled, and so
    is every non-committed transaction that read its output, in later plans
    too. Canceled runs cannot be recovered.
    """
    verify_login_or_prompt(ctx)
    click.echo(f"Canceling plan with ID '{plan}'")
    click.echo("-" * 10)
    confirm_cancel(ctx, confirm, f"plan '{plan}'")
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.cancel_execution(plan)
        click.echo("Plan canceled successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to cancel plan: {e}")


@plan.command()
@click.option("--plan", required=True, help="ID of the plan to recover.")
@click.pass_context
def recover(ctx: click.Context, plan: str):
    """
    Recover a plan: its failed or on-hold function runs are re-scheduled,
    each re-running its pinned function version and that version's latest
    patch.
    """
    verify_login_or_prompt(ctx)
    click.echo(f"Recovering plan with ID '{plan}'")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.recover_execution(plan)
        click.echo("Plan recovered successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to recover plan: {e}")


@plan.command()
@click.option("--plan", required=True, help="ID of the plan to monitor.")
@click.pass_context
def monitor(ctx: click.Context, plan: str):
    """
    Monitor the execution of a plan.
    """
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        span_projects(server)
        execution = server.get_execution(plan)
        from tabsdatak.cli.td.fn_group import _monitor_execution_or_transaction

        _monitor_execution_or_transaction(ctx, execution=execution)
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to monitor plan: {e}")


@plan.command()
@click.option("--plan", required=True, help="ID of the plan whose logs to show.")
@click.option(
    "--file",
    help=(
        "Path of the file where the logs will be saved, or '-' to write them to "
        "stdout. Any folders provided before the file must already exist."
    ),
)
@format_option
@click.pass_context
def logs(ctx: click.Context, plan: str, file: str):
    """
    Show logs of one of the plan's workers; when several ran, pick one from
    the listing.
    """
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        span_projects(server)
        target = pick_worker(ctx, server, [f"execution_id:eq:{plan}"], f"Plan '{plan}'")
        emit_worker_logs(ctx, server, target, file)
    except click.exceptions.Exit:
        raise
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to show logs: {e}")
