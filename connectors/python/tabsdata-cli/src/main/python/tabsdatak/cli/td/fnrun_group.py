#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import TYPE_CHECKING, List

import rich_click as click

from tabsdatak.cli._capability import capability
from tabsdatak.cli._command import TdkGroup
from tabsdatak.cli._prompted import prompted
from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    IdType,
    collection_label,
    format_option,
    hint_common_solutions,
    list_options,
    list_params,
    max_result_option,
    present,
    progress,
    render,
    render_entity,
    render_list,
    render_status_done,
    status_changed,
    take,
    view_options,
)
from tabsdatak.cli.td._wait import (
    render_waited_transaction,
    require_wait,
    timeout_option,
    wait_function_run,
    wait_option,
)
from tabsdatak.cli.td.execution_utils import (
    FNRUN_ID,
    PLAN_ID,
    TRX_ID,
    all_workers_option,
    diagnose_options,
    emit_scope_logs,
    function_runs_view,
    obtain_list_fn_run_filters,
    patch_label,
    render_diagnosis,
    span_projects,
    status_codes,
    status_context,
    status_filter_options,
    tail_option,
    workers_view,
)
from tabsdatak.common._tabular import Kind, RecordView
from tabsdatak.sdk.status_utils.groups import Entity, annotate

if TYPE_CHECKING:
    from tabsdatak.sdk.tabsdata_server import TabsdataServer


@cli_command
@click.group(cls=TdkGroup)
def fnrun():
    """Function run management commands"""


ATTEMPT_ID = IdType(
    "An attempt is a physical function run id, such as a replay's "
    "'new_function_run_id'."
)


@capability("operations", "read")
@fnrun.command(name="list")
@click.option(
    "--proj", "-P", type=str, help="Name of the project to filter function runs by."
)
@status_filter_options(Entity.FUNCTION_RUN)
@click.option(
    "--coll",
    "-c",
    type=str,
    help="Collection of the function to which the function runs belong.",
)
@click.option(
    "--fn",
    type=str,
    help="Name of the function to which the function runs belong.",
)
@click.option("--plan", type=PLAN_ID, help="ID of the plan to filter function runs by.")
@click.option(
    "--plan-name",
    help=(
        "A plan name wildcard to match. "
        "For example, 'my_plan*' will match all plans "
        "with names starting with 'my_plan'."
    ),
)
@click.option(
    "--trx", type=TRX_ID, help="ID of the transaction to filter function runs by."
)
@format_option
@view_options()
@max_result_option
@list_options
@click.pass_context
def list_runs(
    ctx: click.Context,
    proj: str,
    status: List[str],
    healths: tuple[str, ...],
    coll: str,
    fn: str,
    plan: str,
    plan_name: str,
    trx: str,
):
    """List function runs across every project you can view."""
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        span_projects(server)
        request_filter = obtain_list_fn_run_filters(
            status=status_codes(Entity.FUNCTION_RUN, status, healths),
            exe_name=plan_name,
            execution_id=plan,
            fn=fn,
            collection=coll,
            trx=trx,
            project=proj,
        )
        list_of_function_runs, truncated = take(
            ctx, server.list_function_runs_generator(**list_params(ctx, request_filter))
        )

        def _view():
            view = function_runs_view(list_of_function_runs)
            view.context = status_context(server, plan=plan, trx=trx)
            return view

        render_list(
            ctx,
            items=lambda: [
                annotate(Entity.FUNCTION_RUN, r._data) for r in list_of_function_runs
            ],
            truncated=truncated,
            view=_view,
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list function runs: {e}")


@capability("operations", "read")
@fnrun.command()
@click.option(
    "--fnrun", type=FNRUN_ID, required=True, help="ID of the function run to display."
)
@wait_option("the function run")
@click.option(
    "--attempt",
    type=ATTEMPT_ID,
    help=(
        "With '--wait', wait for this attempt (a physical function run id, such "
        "as the 'new_function_run_id' of a replay) to end instead."
    ),
)
@timeout_option()
@format_option
@view_options()
@click.pass_context
def get(
    ctx: click.Context,
    fnrun: str,
    wait: bool,
    attempt: str | None,
    timeout: float | None,
):
    """
    Display a function run: its details plus its workers.
    """
    require_wait(wait, timeout, attempt)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        span_projects(server)
        function_run = server.get_function_run(fnrun)
        waited = {}
        if wait:
            document = wait_function_run(ctx, function_run, timeout, attempt)
            waited["waited_ms"] = document["waited_ms"]
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
                "function_run": annotate(Entity.FUNCTION_RUN, function_run._data),
                "workers": [annotate(Entity.WORKER, w._data) for w in workers],
                **waited,
            },
            view=_views,
        )

    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to display function run: {e}")


@capability("operations", "read", paths={"--file": "write"})
@fnrun.command()
@click.option(
    "--fnrun",
    type=FNRUN_ID,
    required=True,
    help="ID of the function run whose logs to show.",
)
@all_workers_option
@tail_option
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
    ctx: click.Context, fnrun: str, all_workers: bool, tail: int | None, file: str
):
    """
    Show the logs of the function run's workers (one per attempt): every
    worker's with '--all', newest first; else one worker's, picked from the
    listing when several ran. Each worker that did not succeed carries the
    error signature of its log.
    """
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        emit_scope_logs(
            ctx,
            server,
            f"Function run '{fnrun}'",
            all_workers=all_workers,
            tail=tail,
            file=file,
            fnrun=fnrun,
        )
    except (click.exceptions.Exit, click.Abort):
        raise
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to show logs: {e}")


@capability("operations", "write")
@fnrun.command()
@format_option
@prompted(
    "--fnrun",
    type=FNRUN_ID,
    prompt="ID of the function run to replay",
    help="ID of the function run to replay.",
)
@wait_option("the new attempt")
@timeout_option()
@click.pass_context
def replay(ctx: click.Context, fnrun: str, wait: bool, timeout: float | None):
    """Re-execute a finished function run.

    Mints a new concrete physical function run against the latest patch (or
    head, if no patch exists). The pointer stays bound to the previous
    attempt's outputs until the new attempt finishes successfully. '--wait'
    waits for the new attempt to end.
    """
    require_wait(wait, timeout)
    progress(ctx, f"Replaying function run '{fnrun}'")
    progress(ctx, "-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        result = server.replay_logical_function_run(fnrun)
        if wait:
            span_projects(server)
            function_run = server.get_function_run(fnrun)
            document = wait_function_run(
                ctx, function_run, timeout, attempt=result.get("new_function_run_id")
            )
            render(ctx, data={**document, "replay": result})
            return
        render_entity(
            ctx,
            data=result,
            message=(
                "Replay scheduled\n"
                f"Run id:               {result.get('logical_function_run_id')}\n"
                f"Previous FR (bound):  {result.get('previous_function_run_id')}\n"
                f"New FR (running):     {result.get('new_function_run_id')}\n"
                f"Function version id:  {result.get('function_version_id')}"
            ),
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to replay function run: {e}")


@capability("operations", "write")
@fnrun.command()
@format_option
@click.option(
    "--fnrun", type=FNRUN_ID, required=True, help="ID of the function run to recover."
)
@wait_option("the recovered transaction")
@timeout_option()
@click.pass_context
def recover(ctx: click.Context, fnrun: str, wait: bool, timeout: float | None):
    """
    Recover the transaction a function run belongs to: the transaction's
    failed or on-hold function runs are re-scheduled, each re-running its
    pinned function version and that version's latest patch.
    """
    require_wait(wait, timeout)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        span_projects(server)
        function_run = server.get_function_run(fnrun)
        trx = function_run.transaction.id
        progress(ctx, f"Recovering transaction '{trx}' of function run '{fnrun}'")
        progress(ctx, "-" * 10)
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
            details={"function_run": fnrun},
            message="Transaction recovered successfully",
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to recover function run: {e}")


@capability("analytics", "read")
@fnrun.command()
@click.option(
    "--fnrun", type=FNRUN_ID, required=True, help="ID of the function run to diagnose."
)
@diagnose_options
@format_option
@click.pass_context
def diagnose(ctx: click.Context, fnrun: str, log_lines: int, no_logs: bool):
    """
    Diagnose why a function run did not succeed: its repair handles, the
    error signature of its latest worker's log and, when it is on hold, the
    failed runs that block it. Exits 0 whenever the diagnosis is produced; a
    function run that is not found exits 5.
    """
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        span_projects(server)
        render_diagnosis(
            ctx,
            server.diagnose_function_run(fnrun, log_lines=log_lines, logs=not no_logs),
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to diagnose function run: {e}")
