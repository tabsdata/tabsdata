#
# Copyright 2025 Tabs Data Inc.
#

import os
from time import sleep
from typing import List

import rich_click as click
from rich.console import Console
from rich.live import Live
from rich.table import Table

from tabsdata._cli.cli_utils import (
    MutuallyExclusiveOption,
    beautify_time,
    get_currently_pinned_object,
    hint_common_solutions,
    is_valid_id,
    logical_prompt,
    show_hint,
    verify_login_or_prompt,
)
from tabsdata._cli.fn_group import _monitor_execution_or_transaction
from tabsdata.api.status_utils.execution import (
    EXECUTION_FINAL_STATUSES,
    EXECUTION_SUCCESSFUL_FINAL_STATUSES,
    EXECUTION_VALID_USER_PROVIDED_STATUSES,
    user_execution_status_to_api,
)
from tabsdata.api.status_utils.function_run import (
    FUNCTION_RUN_VALID_USER_PROVIDED_STATUSES,
    user_function_run_status_to_api,
)
from tabsdata.api.status_utils.transaction import (
    TRANSACTION_FINAL_STATUSES,
    TRANSACTION_SUCCESSFUL_FINAL_STATUSES,
    TRANSACTION_VALID_USER_PROVIDED_STATUSES,
    user_transaction_status_to_api,
)
from tabsdata.api.status_utils.worker import (
    WORKER_VALID_USER_PROVIDED_STATUSES,
    user_worker_status_to_api,
)
from tabsdata.api.tabsdata_server import (
    TabsdataServer,
    _top_and_convert_to_timestamp,
)


@click.group()
def exe():
    """Execution management commands"""


@exe.command()
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
@click.pass_context
def list_fn_run(
    ctx: click.Context,
    status: List[str],
    plan: str,
    plan_name: str,
    fn: str,
    coll: str,
    trx: str,
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

        table = Table(title="Function Runs")
        table.add_column("Function Run ID", style="cyan", no_wrap=True)
        table.add_column("Collection")
        table.add_column("Function")
        table.add_column("Plan name")
        table.add_column("Plan ID", no_wrap=True)
        table.add_column("Transaction ID", no_wrap=True)
        table.add_column("Started on", no_wrap=True)
        table.add_column("Ended on", no_wrap=True)
        table.add_column("Status")

        for fn_run in list_of_function_runs:
            table.add_row(
                fn_run.id,
                fn_run.collection.name,
                fn_run.function.name,
                fn_run.execution.name,
                fn_run.execution.id,
                fn_run.transaction.id,
                beautify_time(fn_run.started_on_str),
                beautify_time(fn_run.ended_on_str),
                fn_run.status,
            )

        click.echo()
        console = Console()
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
        request_filter.append(
            [f"status:eq:{user_function_run_status_to_api(s)}" for s in status]
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
        list_of_executions = obtain_execution_list(server, request_filter, last)

        def build_table() -> Table:
            """Generate and show table for transactions"""

            table = Table(title=f"Plans: {len(list_of_executions)}")
            table.add_column("ID", style="cyan", no_wrap=True)
            table.add_column("Name")
            table.add_column("Collection")
            table.add_column("Function")
            table.add_column("Triggered on")
            table.add_column("Status", no_wrap=True)

            for execution in list_of_executions:
                table.add_row(
                    execution.id,
                    execution.name,
                    execution.collection.name,
                    execution.function.name,
                    beautify_time(execution.triggered_on_str),
                    execution.status,
                )

            return table

        if monitor:
            click.echo("Waiting for the plans to finish...")

            refresh_rate = 1  # seconds
            with Live(
                build_table(), refresh_per_second=refresh_rate, console=Console()
            ) as live:
                while True:
                    if all(
                        [
                            aux_exe.status in EXECUTION_FINAL_STATUSES
                            for aux_exe in list_of_executions
                        ]
                    ):
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
            console = Console()
            console.print(table)
        click.echo()
        if not all(
            [
                aux_exe.status in EXECUTION_SUCCESSFUL_FINAL_STATUSES
                for aux_exe in list_of_executions
            ]
        ):
            show_hint(
                ctx,
                "If you want to explore why a plan failed, you can "
                "use the 'td exe logs' command with the '--plan "
                "<PLAN-ID>' option to see the logs.",
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
    request_filter = []
    if status:
        request_filter.append(
            [f"status:eq:{user_execution_status_to_api(s)}" for s in status]
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
        list_of_transactions = obtain_transaction_list(server, request_filter, last)

        def build_table() -> Table:
            """Generate and show table for transactions"""

            table = Table(title=f"Transactions: {len(list_of_transactions)}")
            table.add_column("ID", style="cyan", no_wrap=True)
            table.add_column("Plan ID")
            table.add_column("Collection")
            table.add_column("Triggered on")
            table.add_column("Triggered by")
            table.add_column("Status", no_wrap=True)

            for transaction in list_of_transactions:
                table.add_row(
                    transaction.id,
                    transaction.execution.id,
                    transaction.collection.name,
                    beautify_time(transaction.triggered_on_str),
                    transaction.triggered_by,
                    transaction.status,
                )

            return table

        if monitor:
            click.echo("Waiting for the transactions to finish...")

            refresh_rate = 1  # seconds
            with Live(
                build_table(), refresh_per_second=refresh_rate, console=Console()
            ) as live:
                while True:
                    if all(
                        [
                            aux_trx.status in TRANSACTION_FINAL_STATUSES
                            for aux_trx in list_of_transactions
                        ]
                    ):
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
            console = Console()
            console.print(table)
        click.echo()
        if not all(
            [
                trx.status in TRANSACTION_SUCCESSFUL_FINAL_STATUSES
                for trx in list_of_transactions
            ]
        ):
            show_hint(
                ctx,
                "If you want to explore why a transaction failed, you can "
                "use the 'td exe logs' command with the '--trx "
                "<TRANSACTION-ID>' option to see the logs.",
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
    request_filter = []
    if status:
        request_filter.append(
            [f"status:eq:{user_transaction_status_to_api(s)}" for s in status]
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

        table = Table(title="Workers")
        table.add_column("Worker ID", style="cyan", no_wrap=True)
        table.add_column("Collection")
        table.add_column("Function")
        table.add_column("Plan name")
        table.add_column("Plan ID")
        table.add_column("Transaction ID")
        table.add_column("Status")

        for worker in list_of_workers:
            table.add_row(
                worker.id,
                worker.collection.name,
                worker.function.name,
                worker.execution.name,
                worker.execution.id,
                worker.transaction.id,
                worker.status,
            )

        click.echo()
        console = Console()
        console.print(table)
        click.echo(f"Number of workers: {len(list_of_workers)}")
        click.echo()
        show_hint(
            ctx,
            "If you want to explore why a worker failed, you can "
            "use the 'td exe logs' command with the '--worker "
            "<WORKER-ID>' option to see the logs.",
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list workers: {e}")


def obtain_list_worker_filters(
    status: List[str],
    exe_name: str,
    execution_id: str,
    fn: str,
    fn_run: str,
    collection: str,
    trx: str,
) -> List[str]:
    """
    Helper function to obtain the filters for listing workers.
    """

    request_filter = []
    if status:
        request_filter.append(
            [f"status:eq:{user_worker_status_to_api(s)}" for s in status]
        )
    if exe_name:
        request_filter.append(f"execution:lk:{exe_name}")
    if execution_id:
        request_filter.append(f"execution_id:eq:{execution_id}")
    if fn:
        request_filter.append(f"function:eq:{fn}")
    if fn_run:
        request_filter.append(f"function_run_id:eq:{fn_run}")
    if collection:
        request_filter.append(f"collection:eq:{collection}")
    if trx:
        request_filter.append(f"transaction_id:eq:{trx}")
    return request_filter


@exe.command()
@click.option("--plan", help="ID of the plan to display.")
@click.pass_context
def info(ctx: click.Context, plan: str):
    """
    Display a plan by ID.
    """
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        execution = server.get_execution(plan)

        table = Table(title=f"Plan '{plan}'")
        table.add_column("Name")
        table.add_column("Collection")
        table.add_column("Function")
        table.add_column("Triggered on")
        table.add_column("Status", no_wrap=True)

        table.add_row(
            execution.name,
            execution.collection.name,
            execution.function.name,
            beautify_time(execution.triggered_on_str),
            execution.status,
        )

        click.echo()
        console = Console()
        console.print(table)
        click.echo()

    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to display plan: {e}")


@exe.command()
@click.option(
    "--worker",
    help="ID of the worker that generated the logs.",
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["status", "plan", "exec-name", "fn", "coll", "trx"],
)
@click.option(
    "--file",
    help=(
        "Path of the file where the logs will be saved. Any folders provided before "
        "the file must already exist."
    ),
)
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
    mutually_exclusive=["plan-name", "fn", "fn-run", "trx"],
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
@click.pass_context
def logs(
    ctx: click.Context,
    worker: str,
    file: str,
    status: List[str],
    plan: str,
    plan_name: str,
    fn: str,
    fn_run: str,
    coll: str,
    trx: str,
):
    """
    Show the logs generated by a worker.
    """
    verify_login_or_prompt(ctx)
    if not worker:
        coll = coll or get_currently_pinned_object(ctx, "collection")
    if fn and not coll:
        coll = logical_prompt(
            ctx, "Name of the collection to which the function belongs"
        )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        request_filter = obtain_list_worker_filters(
            status=status,
            exe_name=plan_name,
            execution_id=plan,
            fn=fn,
            fn_run=fn_run,
            collection=coll,
            trx=trx,
        )
        if request_filter:
            list_of_workers = obtain_worker_list_from_filters(
                server, request_filter, ctx
            )
        else:
            # If no filters are provided, we can directly use the worker ID provided
            # by the user
            worker = worker or logical_prompt(
                ctx, "ID of the worker that generated the logs"
            )
            list_of_workers = [worker]
        generated_logs = ""
        for w in list_of_workers:
            generated_logs = (
                f"Logs from worker '{w}' underneath:"
                + os.linesep
                + "*" * 20
                + os.linesep
            )
            generated_logs += server.get_worker_log(w)
            generated_logs += os.linesep + "*" * 20 + os.linesep
        if file:
            click.echo(f"Saving logs to file '{file}'")
            with open(file, "w") as f:
                f.write(generated_logs)
            click.echo("Logs saved successfully")
        else:
            click.echo(generated_logs)
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to show worker logs: {e}")


def obtain_worker_list_from_filters(
    server: TabsdataServer, request_filter: List[str], ctx: click.Context
) -> List[str]:
    # If filters are provided, we must show a list of all possible workers for
    # that filter, and then allow the user to select one of them or all of them.

    list_of_workers = server.list_workers(filter=request_filter)

    table = Table(title="Workers")
    table.add_column("Worker ID", style="cyan", no_wrap=True)
    table.add_column("Index", style="cyan", no_wrap=True)
    table.add_column("Collection")
    table.add_column("Function")
    table.add_column("Plan name")
    table.add_column("Plan ID")
    table.add_column("Transaction ID")
    table.add_column("Status")

    for index, worker in enumerate(list_of_workers, 1):
        table.add_row(
            worker.id,
            str(index),
            worker.collection.name,
            worker.function.name,
            worker.execution.name,
            worker.execution.id,
            worker.transaction.id,
            worker.status,
        )

    click.echo()
    console = Console()
    console.print(table)
    click.echo(f"Number of workers: {len(list_of_workers)}")
    click.echo()

    worker_id_or_index = logical_prompt(
        ctx,
        "Index or ID of the worker to recover logs from. If "
        "none is provided (by pressing the return key without writing anything), "
        "all worker logs will be shown.",
        default_value="",
    )
    if not worker_id_or_index:
        if not list_of_workers:
            raise click.ClickException("No workers found for the provided filters.")
        list_of_workers = [w.id for w in list_of_workers]
    else:
        # We must check if the provided value is an index, an ID or neither.
        if is_valid_id(worker_id_or_index):
            # If it's a valid ID, we can use it directly.
            list_of_workers = [worker_id_or_index]
        else:
            try:
                chosen_worker = list_of_workers[int(worker_id_or_index) - 1]
                list_of_workers = [chosen_worker.id]
            except IndexError:
                raise click.ClickException(
                    f"Invalid worker index '{worker_id_or_index}'. Please provide a"
                    " valid index from the list (in this instance, a number between 1"
                    f" and {len(list_of_workers)})."
                )
            except ValueError:
                raise click.ClickException(
                    f"Invalid value provided '{worker_id_or_index}'. It seems it is "
                    "neither a valid worker ID nor a valid index. You can obtain "
                    "one of these values from either the 'Worker ID' or the 'Index' "
                    "column in the table above, or provide none to obtain the logs "
                    "of all workers in the table above."
                )
    return list_of_workers


@exe.command()
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
        _monitor_execution_or_transaction(
            ctx, execution=execution, transaction=transaction
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        keyword = "plan" if plan else "transaction"
        raise click.ClickException(f"Failed to monitor {keyword}: {e}")


@exe.command()
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
