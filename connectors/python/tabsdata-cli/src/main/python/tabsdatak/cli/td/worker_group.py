#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import TYPE_CHECKING, List

import rich_click as click

from tabsdatak.cli._capability import capability
from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    format_option,
    hint_common_solutions,
    list_options,
    list_params,
    max_result_option,
    output_format,
    progress,
    render_list,
    render_status_done,
    require_id,
    show_hint,
    status_changed,
    take,
    verify_login_or_prompt,
    view_options,
)
from tabsdatak.cli.td._wait import (
    render_waited_transaction,
    require_wait,
    timeout_option,
    wait_option,
)
from tabsdatak.cli.td.execution_utils import (
    FNRUN_ID_HINT,
    PLAN_ID_HINT,
    TRX_ID_HINT,
    WORKER_ID_HINT,
    emit_worker_logs,
    obtain_list_worker_filters,
    span_projects,
    status_codes,
    status_context,
    status_filter_options,
    tail_option,
    workers_view,
)
from tabsdatak.sdk.status_utils.groups import Entity, annotate

if TYPE_CHECKING:
    from tabsdatak.sdk.tabsdata_server import TabsdataServer


@cli_command
@click.group()
def worker():
    """Worker management commands"""


@capability("operations", "read")
@worker.command(name="list")
@click.option(
    "--proj", "-P", type=str, help="Name of the project to filter workers by."
)
@status_filter_options(Entity.WORKER)
@click.option(
    "--coll",
    "-c",
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
@format_option
@view_options()
@max_result_option
@list_options
@click.pass_context
def list_workers(
    ctx: click.Context,
    proj: str,
    status: List[str],
    healths: tuple[str, ...],
    coll: str,
    fn: str,
    plan: str,
    plan_name: str,
    fnrun: str,
    trx: str,
):
    """List workers across every project you can view."""
    require_id("--plan", plan, PLAN_ID_HINT)
    require_id("--fnrun", fnrun, FNRUN_ID_HINT)
    require_id("--trx", trx, TRX_ID_HINT)
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        span_projects(server)
        request_filter = obtain_list_worker_filters(
            status=status_codes(Entity.WORKER, status, healths),
            exe_name=plan_name,
            execution_id=plan,
            fn=fn,
            fnrun=fnrun,
            collection=coll,
            trx=trx,
            project=proj,
        )
        list_of_workers, truncated = take(
            ctx, server.list_workers_generator(**list_params(ctx, request_filter))
        )

        def _view():
            view = workers_view(list_of_workers)
            view.context = status_context(server, plan=plan, trx=trx, fnrun=fnrun)
            return view

        render_list(
            ctx,
            items=lambda: [annotate(Entity.WORKER, w._data) for w in list_of_workers],
            truncated=truncated,
            view=_view,
        )
        if output_format(ctx) != "table":
            return
        show_hint(
            ctx,
            "Read logs with 'tdk worker logs --worker <WORKER-ID>', or "
            "'tdk fnrun logs --fnrun <RUN-ID>' for a function run's workers.",
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list workers: {e}")


@capability("operations", "read", paths={"--file": "write"})
@worker.command()
@click.option("--worker", required=True, help="ID of the worker whose logs to show.")
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
def logs(ctx: click.Context, worker: str, tail: int | None, file: str):
    """
    Show the logs of a worker, with the error signature of its log when it
    did not succeed.
    """
    require_id("--worker", worker, WORKER_ID_HINT)
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        emit_worker_logs(ctx, server, worker, tail, file)
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to show logs: {e}")


@capability("operations", "write")
@worker.command()
@format_option
@click.option("--worker", required=True, help="ID of the worker to recover.")
@wait_option("the recovered transaction")
@timeout_option()
@click.pass_context
def recover(ctx: click.Context, worker: str, wait: bool, timeout: float | None):
    """
    Recover the transaction a worker belongs to: the transaction's failed or
    on-hold function runs are re-scheduled.
    """
    require_id("--worker", worker, WORKER_ID_HINT)
    require_wait(wait, timeout)
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        span_projects(server)
        target = server.get_worker(worker)
        trx = target.transaction.id
        progress(ctx, f"Recovering transaction '{trx}' of worker '{worker}'")
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
            details={"worker": worker},
            message="Transaction recovered successfully",
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to recover worker: {e}")
