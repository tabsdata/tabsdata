#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import TYPE_CHECKING, List

import rich_click as click

from tabsdatak.cli._capability import capability
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
    require_id,
    show_hint,
    status_changed,
    verify_login_or_prompt,
    view_options,
)
from tabsdatak.cli.td._wait import (
    render_waited_execution,
    require_wait,
    timeout_option,
    wait_execution,
    wait_listing,
    wait_option,
)
from tabsdatak.cli.td.execution_utils import (
    PLAN_ID_HINT,
    all_workers_option,
    confirm_cancel,
    diagnose_options,
    emit_scope_logs,
    failed_only_option,
    function_runs_view,
    obtain_execution_list,
    obtain_list_exec_filters,
    plans_view,
    render_diagnosis,
    span_projects,
    status_codes,
    status_filter_options,
    tail_option,
    workers_view,
)
from tabsdatak.common._tabular import Kind, RecordView, renderable
from tabsdatak.sdk.status_utils.groups import Entity, Health, annotate, health

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


@capability("operations", "read")
@plan.command(name="list")
@click.option("--proj", "-P", type=str, help="Name of the project to filter plans by.")
@status_filter_options(Entity.EXECUTION)
@click.option("--fn", type=str, help="Name of the function to filter plans by.")
@click.option(
    "--coll", "-c", type=str, help="Name of the collection to filter plans by."
)
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
        "a final state. Exits 7 when '--timeout' runs out."
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
def list_plans(
    ctx: click.Context,
    proj: str,
    status: List[str],
    healths: tuple[str, ...],
    fn: str,
    coll: str,
    name: str,
    last: bool,
    at: str,
    monitor: bool,
    timeout: float | None,
):
    """List plans across every project you can view."""
    if timeout is not None and not monitor:
        raise click.UsageError("'--timeout' applies only together with '--monitor'.")
    verify_login_or_prompt(ctx)
    request_filter = obtain_list_exec_filters(
        status=status_codes(Entity.EXECUTION, status, healths),
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
        if monitor:
            list_of_executions = wait_listing(
                ctx,
                server,
                list_of_executions,
                lambda: obtain_execution_list(ctx, server, request_filter, last)[0],
                Entity.EXECUTION,
                lambda plans: renderable(plans_view(plans), display(ctx)),
                "plans",
                timeout,
            )

        render_list(
            ctx,
            items=lambda: [
                annotate(Entity.EXECUTION, e._data) for e in list_of_executions
            ],
            truncated=truncated,
            view=None if monitor else lambda: plans_view(list_of_executions),
        )
        if output_format(ctx) != "table":
            return
        if not all([
            health(Entity.EXECUTION, aux_exe.status) is Health.OK
            for aux_exe in list_of_executions
        ]):
            show_hint(
                ctx,
                "If you want to explore why a plan failed, run "
                "'tdk plan diagnose --plan <PLAN-ID>', or read its workers' logs "
                "with 'tdk plan logs --plan <PLAN-ID> --all'.",
            )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list plans: {e}")


@capability("operations", "read")
@plan.command()
@click.option("--plan", required=True, help="ID of the plan to display.")
@wait_option("the plan")
@timeout_option()
@format_option
@view_options()
@click.pass_context
def get(ctx: click.Context, plan: str, wait: bool, timeout: float | None):
    """
    Display a plan: its details plus its function runs and workers.
    """
    require_id("--plan", plan, PLAN_ID_HINT)
    require_wait(wait, timeout)
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        span_projects(server)
        execution = server.get_execution(plan)
        waited = {}
        if wait:
            document = wait_execution(ctx, execution, timeout, live=False)
            waited["waited_ms"] = document["waited_ms"]
        function_runs = server.list_function_runs(filter=f"execution_id:eq:{plan}")
        workers = server.list_workers(filter=f"execution_id:eq:{plan}")

        def _views():
            return [
                RecordView(
                    title="Plan",
                    context=[("ID", execution.id)],
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
                "plan": annotate(Entity.EXECUTION, execution._data),
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
        raise click.ClickException(f"Failed to display plan: {e}")


@capability("operations", "destructive")
@plan.command()
@format_option
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
    require_id("--plan", plan, PLAN_ID_HINT)
    verify_login_or_prompt(ctx)
    progress(ctx, f"Canceling plan with ID '{plan}'")
    progress(ctx, "-" * 10)
    confirm_cancel(ctx, confirm, f"plan '{plan}'")
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        changed = status_changed(server.cancel_execution(plan))
        render_status_done(
            ctx,
            changed=changed,
            action="cancel",
            kind="plan",
            id=plan,
            message="Plan canceled successfully",
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to cancel plan: {e}")


@capability("operations", "write")
@plan.command()
@format_option
@click.option("--plan", required=True, help="ID of the plan to recover.")
@wait_option("the recovered plan")
@timeout_option()
@click.pass_context
def recover(ctx: click.Context, plan: str, wait: bool, timeout: float | None):
    """
    Recover a plan: its failed or on-hold function runs are re-scheduled,
    each re-running its pinned function version and that version's latest
    patch.
    """
    require_id("--plan", plan, PLAN_ID_HINT)
    require_wait(wait, timeout)
    verify_login_or_prompt(ctx)
    progress(ctx, f"Recovering plan with ID '{plan}'")
    progress(ctx, "-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        changed = status_changed(server.recover_execution(plan))
        if wait and changed:
            render_waited_execution(ctx, server, plan, timeout)
            return
        render_status_done(
            ctx,
            changed=changed,
            action="recover",
            kind="plan",
            id=plan,
            message="Plan recovered successfully",
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to recover plan: {e}")


@capability("operations", "read")
@plan.command()
@format_option
@click.option("--plan", required=True, help="ID of the plan to monitor.")
@timeout_option()
@click.pass_context
def monitor(ctx: click.Context, plan: str, timeout: float | None):
    """
    Monitor the execution of a plan until it finishes. Exits 7 when
    '--timeout' runs out and 8 when a function run did not succeed.
    """
    require_id("--plan", plan, PLAN_ID_HINT)
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        render_waited_execution(ctx, server, plan, timeout)
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to monitor plan: {e}")


@capability("operations", "read", paths={"--file": "write"})
@plan.command()
@click.option("--plan", required=True, help="ID of the plan whose logs to show.")
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
    plan: str,
    all_workers: bool,
    tail: int | None,
    failed_only: bool,
    file: str,
):
    """
    Show the logs of the plan's workers: every worker's with '--all', newest
    first; else one worker's, picked from the listing when several ran. Each
    worker that did not succeed carries the error signature of its log.
    """
    require_id("--plan", plan, PLAN_ID_HINT)
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        emit_scope_logs(
            ctx,
            server,
            f"Plan '{plan}'",
            all_workers=all_workers,
            tail=tail,
            file=file,
            failed_only=failed_only,
            plan=plan,
        )
    except (click.exceptions.Exit, click.Abort):
        raise
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to show logs: {e}")


@capability("analytics", "read")
@plan.command()
@click.option("--plan", required=True, help="ID of the plan to diagnose.")
@click.option(
    "--max-runs",
    type=click.IntRange(min=0),
    default=5,
    show_default=True,
    help="The most failed runs to report, first failure first; 0 for all.",
)
@diagnose_options
@format_option
@click.pass_context
def diagnose(
    ctx: click.Context, plan: str, max_runs: int, log_lines: int, no_logs: bool
):
    """
    Diagnose why a plan did not succeed.

    Reports the failed and retrying function runs, first failure first, each
    with its repair handles (the replay, recover and patch argv) and the error
    signature of its latest worker's log; the held runs with the failed runs
    that block them; and the root cause, the earliest failure, with its
    category. Exits 0 whenever the diagnosis is produced; the verdict is the
    plan's 'health'. A plan that is not found exits 5.
    """
    require_id("--plan", plan, PLAN_ID_HINT)
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        span_projects(server)
        render_diagnosis(
            ctx,
            server.diagnose_execution(
                plan, max_runs=max_runs, log_lines=log_lines, logs=not no_logs
            ),
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to diagnose plan: {e}")
