#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os
from time import sleep
from typing import TYPE_CHECKING, List

import rich_click as click
from rich.live import Live
from rich.table import Table

from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    MutuallyExclusiveOption,
    all_projects_option,
    beautify_time,
    collection_label,
    get_currently_pinned_object,
    hint_common_solutions,
    logical_prompt,
    make_console,
    output_format,
    output_option,
    proj_option,
    render,
    resolve_all_projects,
    show_hint,
    verify_login_or_prompt,
)
from tabsdatak.sdk.status_utils.execution import (
    EXECUTION_FINAL_STATUSES,
    EXECUTION_SUCCESSFUL_FINAL_STATUSES,
    EXECUTION_VALID_USER_PROVIDED_STATUSES,
    user_execution_status_to_api,
)
from tabsdatak.sdk.status_utils.function_run import (
    FUNCTION_RUN_VALID_USER_PROVIDED_STATUSES,
    user_function_run_status_to_api,
)
from tabsdatak.sdk.status_utils.transaction import (
    TRANSACTION_FINAL_STATUSES,
    TRANSACTION_SUCCESSFUL_FINAL_STATUSES,
    TRANSACTION_VALID_USER_PROVIDED_STATUSES,
    user_transaction_status_to_api,
)
from tabsdatak.sdk.status_utils.worker import (
    WORKER_VALID_USER_PROVIDED_STATUSES,
    user_worker_status_to_api,
)

if TYPE_CHECKING:
    from tabsdatak.sdk.tabsdata_server import TabsdataServer


@cli_command
@click.group()
def exe():
    """Execution management commands"""


@exe.command()
@proj_option
@click.option(
    "--plan",
    help="ID of the plan to cancel. Either this or --trx must be provided.",
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["trx"],
)
@click.option(
    "--trx",
    "-t",
    help="ID of the transaction to cancel. Either this or --plan must be provided.",
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["plan"],
)
@click.pass_context
def cancel(ctx: click.Context, plan: str, trx: str):
    """Cancel a plan or transaction."""
    verify_login_or_prompt(ctx)
    if plan:
        click.echo(f"Canceling plan with ID '{plan}'")
        click.echo("-" * 10)
        try:
            server: TabsdataServer = ctx.obj["tabsdataserver"]
            server.cancel_execution(plan)
            click.echo("Plan canceled successfully")
        except Exception as e:
            hint_common_solutions(ctx, e)
            raise click.ClickException(f"Failed to cancel plan: {e}")
    elif trx:
        click.echo(f"Canceling transaction with ID '{trx}'")
        click.echo("-" * 10)
        try:
            server: TabsdataServer = ctx.obj["tabsdataserver"]
            server.cancel_transaction(trx)
            click.echo("Transaction canceled successfully")
        except Exception as e:
            hint_common_solutions(ctx, e)
            raise click.ClickException(f"Failed to cancel transaction: {e}")
    else:
        raise click.ClickException(
            "Either a plan ID with '--plan' or a transaction ID "
            "with '--trx' must be provided."
        )


@exe.command()
@proj_option
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
    "--plan",
    type=str,
    cls=MutuallyExclusiveOption,
    help="ID of the plan to which the function runs belong.",
    mutually_exclusive=["plan-name", "fn", "trx"],
)
@click.option(
    "--plan-name",
    help=(
        "A plan name wildcard to match. "
        "For example, 'my_plan*' will match all plans "
        "with names starting with 'my_plan'."
    ),
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["plan"],
)
@click.option(
    "--fn",
    type=str,
    cls=MutuallyExclusiveOption,
    help=(
        "Name of the function to which the function runs belong. If this is provided, "
        "collection must also be provided."
    ),
    mutually_exclusive=["plan", "trx"],
)
@click.option(
    "--coll",
    type=str,
    help="Collection of the function to which the function runs belong.",
)
@click.option(
    "--trx",
    type=str,
    cls=MutuallyExclusiveOption,
    help="ID of the transaction to which the function runs belong.",
    mutually_exclusive=["plan", "fn"],
)
@all_projects_option
@output_option
@click.pass_context
def list_fn_run(
    ctx: click.Context,
    status: List[str],
    plan: str,
    plan_name: str,
    fn: str,
    coll: str,
    trx: str,
    all_projects: bool,
):
    """
    List all function runs of a specific plan, function or transaction.
    """
    verify_login_or_prompt(ctx)
    coll = coll or get_currently_pinned_object(ctx, "collection")
    if fn and not coll:
        coll = logical_prompt(
            ctx, "Name of the collection to which the function belongs"
        )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        spanning = resolve_all_projects(ctx, all_projects)
        request_filter = obtain_list_fn_run_filters(
            status=status,
            exe_name=plan_name,
            execution_id=plan,
            fn=fn,
            collection=coll,
            trx=trx,
        )
        if plan:
            execution = server.get_execution(plan)
            click.echo(
                f"The current status of the plan '{plan}' is '{execution.status}'"
            )
        if trx:
            transaction = server.get_transaction(trx)
            click.echo(
                f"The current status of the transaction '{trx}' is"
                f" '{transaction.status}'"
            )
        list_of_function_runs = server.list_function_runs(filter=request_filter)

        if output_format(ctx) != "table":
            render(
                ctx,
                data=[r._data for r in list_of_function_runs],
                table=lambda: None,
            )
            return

        table = Table(title="Function Runs")
        table.add_column("Function Run ID", style="cyan", no_wrap=True)
        if spanning:
            table.add_column("Project")
        table.add_column("Collection")
        table.add_column("Function")
        table.add_column("Plan name")
        table.add_column("Plan ID", no_wrap=True)
        table.add_column("Transaction ID", no_wrap=True)
        table.add_column("Started on", no_wrap=True)
        table.add_column("Ended on", no_wrap=True)
        table.add_column("Status")

        for fn_run in list_of_function_runs:
            row = [fn_run.id]
            if spanning:
                row.append(fn_run.project or "-")
            row.extend([
                collection_label(fn_run),
                fn_run.function.name,
                fn_run.execution.name,
                fn_run.execution.id,
                fn_run.transaction.id,
                beautify_time(fn_run.started_on_str),
                beautify_time(fn_run.ended_on_str),
                fn_run.status,
            ])
            table.add_row(*row)

        click.echo()
        console = make_console()
        console.print(table)
        click.echo(f"Number of function runs: {len(list_of_function_runs)}")
        click.echo()
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list function runs: {e}")


def obtain_list_fn_run_filters(
    status: List[str],
    exe_name: str,
    execution_id: str,
    fn: str,
    collection: str,
    trx: str,
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
    return request_filter


@exe.command()
@proj_option
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
@click.option(
    "--at",
    help=(
        "If provided, only plans started before that time will be shown. Must be "
        " either a valid timestamp in the form of a unix timestamp (milliseconds since "
        "epoch without a dot, e.g. '1750074554472') or a valid date-time format. "
        "The valid "
        "formats are 'YYYY-MM-DD', 'YYYY-MM-DDTHH', 'YYYY-MM-DDTHH:MM', "
        "'YYYY-MM-DDTHH:MM:SS', and 'YYYY-MM-DDTHH:MM:SS.sss'. A 'Z' character can be "
        "added at the end to indicate UTC time (e.g., '2023-10-01T12Z' or "
        "'2023-10-01T12:00:00.000Z') or it can be omitted to indicate local "
        "time (e.g., '2023-10-01T12' or '2023-10-01T12:00:00.000')."
    ),
    type=str,
)
@click.option(
    "--monitor",
    is_flag=True,
    help=(
        "If set, the command will monitor the listed plans until they all reach "
        "a final state."
    ),
)
@all_projects_option
@output_option
@click.pass_context
def list_plan(
    ctx: click.Context,
    status: List[str],
    name: str,
    fn: str,
    coll: str,
    last: bool,
    at: str,
    monitor: bool,
    all_projects: bool,
):
    """List all plans"""
    verify_login_or_prompt(ctx)
    coll = coll or get_currently_pinned_object(ctx, "collection")
    request_filter = obtain_list_exec_filters(
        status=status,
        exe_name=name,
        fn=fn,
        collection=coll,
        at=at,
    )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        spanning = resolve_all_projects(ctx, all_projects)
        list_of_executions = obtain_execution_list(server, request_filter, last)

        if output_format(ctx) != "table":
            render(
                ctx,
                data=[e._data for e in list_of_executions],
                table=lambda: None,
            )
            return

        def build_table() -> Table:
            """Generate and show table for transactions"""

            table = Table(title=f"Plans: {len(list_of_executions)}")
            table.add_column("ID", style="cyan", no_wrap=True)
            table.add_column("Name")
            if spanning:
                table.add_column("Project")
            table.add_column("Collection")
            table.add_column("Function")
            table.add_column("Triggered on")
            table.add_column("Trigger", no_wrap=True)
            table.add_column("Status", no_wrap=True)

            for execution in list_of_executions:
                row = [execution.id, execution.name]
                if spanning:
                    row.append(execution.project or "-")
                row.extend([
                    collection_label(execution),
                    execution.function.name,
                    beautify_time(execution.triggered_on_str),
                    execution.trigger_type,
                    execution.status,
                ])
                table.add_row(*row)

            return table

        if monitor:
            click.echo("Waiting for the plans to finish...")

            refresh_rate = 1  # seconds
            with Live(
                build_table(), refresh_per_second=refresh_rate, console=make_console()
            ) as live:
                while True:
                    if all([
                        aux_exe.status in EXECUTION_FINAL_STATUSES
                        for aux_exe in list_of_executions
                    ]):
                        break
                    list_of_executions = obtain_execution_list(
                        server, request_filter, last
                    )
                    live.update(build_table())
                    sleep(1 / refresh_rate)
                list_of_executions = obtain_execution_list(server, request_filter, last)
                live.update(build_table())

            click.echo("Plans finished.")
        else:
            table = build_table()
            click.echo()
            console = make_console()
            console.print(table)
        click.echo()
        if not all([
            aux_exe.status in EXECUTION_SUCCESSFUL_FINAL_STATUSES
            for aux_exe in list_of_executions
        ]):
            show_hint(
                ctx,
                "If you want to explore why a plan failed, run "
                "'tdk exe list-worker --plan <PLAN-ID>' to find the failed "
                "worker(s), then 'tdk exe logs --worker <WORKER-ID>' (or "
                "'--fn-run <RUN-ID>') to read their logs.",
            )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list plans: {e}")


def obtain_execution_list(server: TabsdataServer, request_filter, last: bool):
    list_of_executions = server.list_executions(
        filter=request_filter,
        order_by="triggered_on-",
    )
    if last:
        list_of_executions = list_of_executions[:1]
    return list_of_executions


def obtain_list_exec_filters(
    status: List[str],
    exe_name: str,
    fn: str,
    collection: str,
    at: str,
) -> List[str]:
    """
    Helper function to obtain the filters for listing plans.
    """
    # noinspection PyProtectedMember
    from tabsdatak.sdk.tabsdata_server import _top_and_convert_to_timestamp

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
        try:
            at_timestamp = _top_and_convert_to_timestamp(at)
            request_filter.append(f"triggered_on:le:{at_timestamp}")
        except ValueError as e:
            raise click.ClickException(f"Invalid date-time format for 'at' option: {e}")
    return request_filter


@exe.command()
@proj_option
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
@click.option(
    "--plan-name",
    help=(
        "A plan name wildcard to match. "
        "For example, 'my_plan*' will match all plans "
        "with names starting with 'my_plan'."
    ),
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["plan"],
)
@click.option(
    "--plan",
    help=(
        "A plan ID to filter transactions by. If provided, only transactions "
        "that are part of the specified plan will be shown."
    ),
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["plan-name"],
)
@click.option(
    "--last",
    is_flag=True,
    help="If set, only the last transaction of the list will be shown.",
)
@click.option(
    "--at",
    help=(
        "If provided, only transactions started before that time will be shown. Must "
        "be "
        " either a valid timestamp in the form of a unix timestamp (milliseconds since "
        "epoch without a dot, e.g. '1750074554472') or a valid date-time format. "
        "The valid "
        "formats are 'YYYY-MM-DD', 'YYYY-MM-DDTHH', 'YYYY-MM-DDTHH:MM', "
        "'YYYY-MM-DDTHH:MM:SS', and 'YYYY-MM-DDTHH:MM:SS.sss'. A 'Z' character can be "
        "added at the end to indicate UTC time (e.g., '2023-10-01T12Z' or "
        "'2023-10-01T12:00:00.000Z') or it can be omitted to indicate local "
        "time (e.g., '2023-10-01T12' or '2023-10-01T12:00:00.000')."
    ),
    type=str,
)
@click.option(
    "--monitor",
    is_flag=True,
    help=(
        "If set, the command will monitor the listed transactions until they all reach "
        "a final state."
    ),
)
@all_projects_option
@output_option
@click.pass_context
def list_trx(
    ctx: click.Context,
    status: List[str],
    coll: str,
    plan_name: str,
    plan: str,
    last: bool,
    at: str,
    monitor: bool,
    all_projects: bool,
):
    """List all transactions"""
    verify_login_or_prompt(ctx)
    coll = coll or get_currently_pinned_object(ctx, "collection")
    try:
        request_filter = obtain_list_trx_filters(
            status=status,
            exe_name=plan_name,
            execution_id=plan,
            collection=coll,
            at=at,
        )
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        spanning = resolve_all_projects(ctx, all_projects)
        list_of_transactions = obtain_transaction_list(server, request_filter, last)

        if output_format(ctx) != "table":
            render(
                ctx,
                data=[t._data for t in list_of_transactions],
                table=lambda: None,
            )
            return

        def build_table() -> Table:
            """Generate and show table for transactions"""

            table = Table(title=f"Transactions: {len(list_of_transactions)}")
            table.add_column("ID", style="cyan", no_wrap=True)
            table.add_column("Plan ID")
            if spanning:
                table.add_column("Project")
            table.add_column("Collection")
            table.add_column("Triggered on")
            table.add_column("Triggered by")
            table.add_column("Status", no_wrap=True)

            for transaction in list_of_transactions:
                row = [transaction.id, transaction.execution.id]
                if spanning:
                    row.append(transaction.project or "-")
                row.extend([
                    collection_label(transaction),
                    beautify_time(transaction.triggered_on_str),
                    transaction.triggered_by,
                    transaction.status,
                ])
                table.add_row(*row)

            return table

        if monitor:
            click.echo("Waiting for the transactions to finish...")

            refresh_rate = 1  # seconds
            with Live(
                build_table(), refresh_per_second=refresh_rate, console=make_console()
            ) as live:
                while True:
                    if all([
                        aux_trx.status in TRANSACTION_FINAL_STATUSES
                        for aux_trx in list_of_transactions
                    ]):
                        break
                    list_of_transactions = obtain_transaction_list(
                        server, request_filter, last
                    )
                    live.update(build_table())
                    sleep(1 / refresh_rate)
                list_of_transactions = obtain_transaction_list(
                    server, request_filter, last
                )
                live.update(build_table())

            click.echo("Transactions finished.")
        else:
            table = build_table()
            click.echo()
            console = make_console()
            console.print(table)
        click.echo()
        if not all([
            trx.status in TRANSACTION_SUCCESSFUL_FINAL_STATUSES
            for trx in list_of_transactions
        ]):
            show_hint(
                ctx,
                "If you want to explore why a transaction failed, run "
                "'tdk exe list-worker --trx <TRANSACTION-ID>' to find the failed "
                "worker(s), then 'tdk exe logs --worker <WORKER-ID>' (or "
                "'--fn-run <RUN-ID>') to read their logs.",
            )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list transactions: {e}")


def obtain_transaction_list(server: TabsdataServer, request_filter, last: bool):
    list_of_transactions = server.list_transactions(
        filter=request_filter, order_by="triggered_on-"
    )
    if last:
        list_of_transactions = list_of_transactions[:1]
    return list_of_transactions


def obtain_list_trx_filters(
    status: List[str],
    exe_name: str,
    execution_id: str,
    collection: str,
    at: str,
) -> List[str]:
    """
    Helper function to obtain the filters for listing transactions.
    """
    # noinspection PyProtectedMember
    from tabsdatak.sdk.tabsdata_server import _top_and_convert_to_timestamp

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
        try:
            at_timestamp = _top_and_convert_to_timestamp(at)
            request_filter.append(f"triggered_on:le:{at_timestamp}")
        except ValueError as e:
            raise click.ClickException(f"Invalid date-time format for 'at' option: {e}")
    return request_filter


@exe.command()
@proj_option
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
    "--plan",
    type=str,
    cls=MutuallyExclusiveOption,
    help="ID of the plan to which the workers belong.",
    mutually_exclusive=["plan-name", "fn-run", "fn", "trx"],
)
@click.option(
    "--plan-name",
    help=(
        "A plan name wildcard to match. "
        "For example, 'my_plan*' will match all plans "
        "with names starting with 'my_plan'."
    ),
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["plan"],
)
@click.option(
    "--fn",
    type=str,
    cls=MutuallyExclusiveOption,
    help=(
        "Name of the function to which the workers belong. If this is provided, "
        "collection must also be provided."
    ),
    mutually_exclusive=["fn-run", "plan", "trx"],
)
@click.option(
    "--fn-run",
    type=str,
    cls=MutuallyExclusiveOption,
    help="ID of the function run to which the workers belong.",
    mutually_exclusive=["fn", "plan", "trx"],
)
@click.option(
    "--coll",
    type=str,
    help="Collection of the function to which the workers belong.",
)
@click.option(
    "--trx",
    type=str,
    cls=MutuallyExclusiveOption,
    help="ID of the transaction to which the workers belong.",
    mutually_exclusive=["fn-run", "plan", "fn"],
)
@all_projects_option
@output_option
@click.pass_context
def list_worker(
    ctx: click.Context,
    status: List[str],
    plan: str,
    plan_name: str,
    fn: str,
    fn_run: str,
    coll: str,
    trx: str,
    all_projects: bool,
):
    """
    List all workers of a specific plan, function or transaction.
    """
    verify_login_or_prompt(ctx)
    coll = coll or get_currently_pinned_object(ctx, "collection")
    if fn and not coll:
        coll = logical_prompt(
            ctx, "Name of the collection to which the function belongs"
        )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        spanning = resolve_all_projects(ctx, all_projects)
        request_filter = obtain_list_worker_filters(
            status=status,
            exe_name=plan_name,
            execution_id=plan,
            fn=fn,
            fn_run=fn_run,
            collection=coll,
            trx=trx,
        )
        if plan:
            execution = server.get_execution(plan)
            click.echo(
                f"The current status of the plan '{plan}' is '{execution.status}'"
            )
        if trx:
            transaction = server.get_transaction(trx)
            click.echo(
                f"The current status of the transaction '{trx}' is"
                f" '{transaction.status}'"
            )
        if fn_run:
            function_run = server.get_function_run(fn_run)
            click.echo(
                f"The current status of the function run '{fn_run}' is"
                f" '{function_run.status}'"
            )
        list_of_workers = server.list_workers(filter=request_filter)

        if output_format(ctx) != "table":
            render(
                ctx,
                data=[w._data for w in list_of_workers],
                table=lambda: None,
            )
            return

        table = Table(title="Workers")
        table.add_column("Worker ID", style="cyan", no_wrap=True)
        if spanning:
            table.add_column("Project")
        table.add_column("Collection")
        table.add_column("Function")
        table.add_column("Plan name")
        table.add_column("Plan ID")
        table.add_column("Transaction ID")
        table.add_column("Patch")
        table.add_column("Status")

        for worker in list_of_workers:
            row = [worker.id]
            if spanning:
                row.append(worker.project or "-")
            row.extend([
                collection_label(worker),
                worker.function.name,
                worker.execution.name,
                worker.execution.id,
                worker.transaction.id,
                worker_patch(worker),
                worker.status,
            ])
            table.add_row(*row)

        click.echo()
        console = make_console()
        console.print(table)
        click.echo(f"Number of workers: {len(list_of_workers)}")
        click.echo()
        show_hint(
            ctx,
            "Read logs with 'tdk exe logs --worker <WORKER-ID>', or "
            "'tdk exe logs --fn-run <RUN-ID>' for the run's latest worker.",
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list workers: {e}")


def obtain_list_worker_filters(
    status: list[str] = None,
    exe_name: str = None,
    execution_id: str = None,
    fn: str = None,
    fn_run: str = None,
    collection: str = None,
    trx: str = None,
    worker_type: str = None,
) -> list[str]:
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
    if fn_run:
        request_filter.append(f"logical_function_run_id:eq:{fn_run}")
    if collection:
        request_filter.append(f"collection:eq:{collection}")
    if trx:
        request_filter.append(f"transaction_id:eq:{trx}")
    if worker_type:
        request_filter.append(f"worker_type:eq:{worker_type}")
    return request_filter


@exe.command()
@proj_option
@click.option("--plan", help="ID of the plan to display.")
@output_option
@click.pass_context
def info(ctx: click.Context, plan: str):
    """
    Display a plan by ID.
    """
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        execution = server.get_execution(plan)

        if output_format(ctx) != "table":
            render(ctx, data=execution._data, table=lambda: None)
            return

        table = Table(title=f"Plan '{plan}'")
        table.add_column("Name")
        table.add_column("Collection")
        table.add_column("Function")
        table.add_column("Triggered on")
        table.add_column("Status", no_wrap=True)

        table.add_row(
            execution.name,
            collection_label(execution),
            execution.function.name,
            beautify_time(execution.triggered_on_str),
            execution.status,
        )

        click.echo()
        console = make_console()
        console.print(table)
        click.echo()

    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to display plan: {e}")


@exe.command()
@proj_option
@click.option(
    "--fn-run",
    "fn_run",
    type=str,
    help="ID of the function run whose logs to show (its latest worker).",
)
@click.option(
    "--worker",
    type=str,
    help="ID of a specific worker whose logs to show.",
)
@click.option(
    "--fn",
    type=str,
    help=(
        "Name of a function whose logs to show (its latest worker). Requires a "
        "collection (--coll); narrow to one run with --plan or --trx."
    ),
)
@click.option(
    "--coll",
    "-c",
    type=str,
    help="Collection of the function named with --fn.",
)
@click.option(
    "--plan",
    type=str,
    help="With --fn, narrow to a specific plan (execution) ID.",
)
@click.option(
    "--trx",
    type=str,
    help="With --fn, narrow to a specific transaction ID.",
)
@click.option(
    "--file",
    help=(
        "Path of the file where the logs will be saved, or '-' to write them to "
        "stdout. Any folders provided before the file must already exist."
    ),
)
@output_option
@click.pass_context
def logs(
    ctx: click.Context,
    fn_run: str,
    worker: str,
    fn: str,
    coll: str,
    plan: str,
    trx: str,
    file: str,
):
    """
    Show logs for one worker (--worker), a function run's latest worker
    (--fn-run), or a named function's latest worker (--fn with --coll).
    """
    verify_login_or_prompt(ctx)
    if len([t for t in (worker, fn_run, fn) if t]) != 1:
        raise click.ClickException(
            "Provide exactly one of --worker, --fn-run, or --fn."
        )
    if (plan or trx) and not fn:
        raise click.ClickException("--plan and --trx only apply with --fn.")
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        if worker:
            target = worker
        else:
            if fn_run:
                request_filter = [f"logical_function_run_id:eq:{fn_run}"]
                subject = f"Function run '{fn_run}'"
            else:
                coll = coll or get_currently_pinned_object(ctx, "collection")
                if not coll:
                    coll = logical_prompt(
                        ctx, "Name of the collection to which the function belongs"
                    )
                request_filter = obtain_list_worker_filters(
                    fn=fn, collection=coll, execution_id=plan, trx=trx
                )
                subject = f"Function '{fn}'"
            workers = latest_workers(server, request_filter)
            if not workers:
                raise click.ClickException(
                    f"{subject} has no workers yet — nothing has run."
                )
            target = workers[-1].id

        generated_logs = f"Worker {target}" + os.linesep + server.get_worker_log(target)
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
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to show logs: {e}")


def latest_workers(server: TabsdataServer, request_filter: list) -> list:
    """Workers matching a filter, in server order; the last is the latest."""
    return server.list_workers(filter=request_filter)


def worker_patch(worker) -> str:
    """The patch a worker ran (a function-version id), or '-' for head code.

    The patch id lives on the physical function run, surfaced onto the worker
    row as `function_version_patch_id`; absent/None means unpatched (head).
    """
    return worker.kwargs.get("function_version_patch_id") or "-"


@exe.command()
@proj_option
@click.option(
    "--plan",
    help="ID of the plan to monitor. Either this or --trx must be provided.",
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["trx"],
)
@click.option(
    "--trx",
    "-t",
    help="ID of the transaction to monitor. Either this or --plan must be provided.",
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["plan"],
)
@click.pass_context
def monitor(ctx: click.Context, plan: str, trx: str):
    """
    Monitor the execution of a plan or transaction.
    """
    verify_login_or_prompt(ctx)
    server: TabsdataServer = ctx.obj["tabsdataserver"]
    if not plan and not trx:
        raise click.ClickException(
            "Either a plan ID with '--plan' or a transaction ID "
            "with '--trx' must be provided."
        )
    try:
        execution = None
        transaction = None
        if plan:
            execution = server.get_execution(plan)
        elif trx:
            transaction = server.get_transaction(trx)
        from tabsdatak.cli.td.fn_group import _monitor_execution_or_transaction

        _monitor_execution_or_transaction(
            ctx, execution=execution, transaction=transaction
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        keyword = "plan" if plan else "transaction"
        raise click.ClickException(f"Failed to monitor {keyword}: {e}")


@exe.command()
@proj_option
@click.option(
    "--plan",
    help="ID of the plan to recover. Either this or --trx must be provided.",
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["trx"],
)
@click.option(
    "--trx",
    "-t",
    help="ID of the transaction to recover. Either this or --plan must be provided.",
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["plan"],
)
@click.pass_context
def recover(ctx: click.Context, plan: str, trx: str):
    """
    Recover a plan or transaction.
    """
    verify_login_or_prompt(ctx)
    if plan:
        click.echo(f"Recovering plan with ID '{plan}'")
        click.echo("-" * 10)
        try:
            server: TabsdataServer = ctx.obj["tabsdataserver"]
            server.recover_execution(plan)
            click.echo("Plan recovered successfully")
        except Exception as e:
            hint_common_solutions(ctx, e)
            raise click.ClickException(f"Failed to recover plan: {e}")
    elif trx:
        click.echo(f"Recovering transaction with ID '{trx}'")
        click.echo("-" * 10)
        try:
            server: TabsdataServer = ctx.obj["tabsdataserver"]
            server.recover_transaction(trx)
            click.echo("Transaction recovered successfully")
        except Exception as e:
            hint_common_solutions(ctx, e)
            raise click.ClickException(f"Failed to recover transaction: {e}")
    else:
        raise click.ClickException(
            "Either a plan ID with '--plan' or a transaction ID "
            "with '--trx' must be provided."
        )


@exe.command()
@proj_option
@click.option(
    "--lfr",
    help="ID of the logical function run (LFR) to replay.",
)
@click.pass_context
def replay(ctx: click.Context, lfr: str):
    """Re-execute a finished logical function run.

    Mints a new concrete physical function run against the latest patch (or
    head, if no patch exists). The pointer stays bound to the previous
    attempt's outputs until the new attempt finishes successfully.
    """
    verify_login_or_prompt(ctx)
    lfr = lfr or logical_prompt(ctx, "ID of the logical function run to replay")
    click.echo(f"Replaying logical function run '{lfr}'")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        result = server.replay_logical_function_run(lfr)
        click.echo("Replay scheduled")
        click.echo(f"LFR id:               {result.get('logical_function_run_id')}")
        click.echo(f"Previous FR (bound):  {result.get('previous_function_run_id')}")
        click.echo(f"New FR (running):     {result.get('new_function_run_id')}")
        click.echo(f"Function version id:  {result.get('function_version_id')}")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to replay logical function run: {e}")
