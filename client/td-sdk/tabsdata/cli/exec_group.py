#
# Copyright 2025 Tabs Data Inc.
#

import os
from typing import List

import rich_click as click
from rich.console import Console
from rich.table import Table

from tabsdata.api.tabsdata_server import (
    TabsdataServer,
    top_and_convert_to_timestamp,
)
from tabsdata.cli.cli_utils import (
    MutuallyExclusiveOption,
    convert_user_provided_status_to_api_status,
    get_currently_pinned_object,
    logical_prompt,
    verify_login_or_prompt,
)


@click.group()
def exec():
    """Execution management commands"""


@exec.command()
@click.argument("id")
@click.pass_context
def cancel_exec(ctx: click.Context, id: str):
    """Cancel an execution. This includes all transactions that are part of the
    execution
    """
    verify_login_or_prompt(ctx)
    click.echo(f"Canceling execution with ID '{id}'")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.cancel_execution(id)
        click.echo("Execution canceled successfully")
    except Exception as e:
        raise click.ClickException(f"Failed to cancel execution: {e}")


@exec.command()
@click.argument("id")
@click.pass_context
def cancel_trx(ctx: click.Context, id: str):
    """Cancel a transaction. This includes all functions that are part of the
    transaction
    """
    verify_login_or_prompt(ctx)
    click.echo(f"Canceling transaction with ID '{id}'")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.cancel_transaction(id)
        click.echo("Transaction canceled successfully")
    except Exception as e:
        raise click.ClickException(f"Failed to cancel transaction: {e}")


@exec.command()
@click.option(
    "--status",
    type=str,
    multiple=True,
    help=(
        "Filter executions by status. The status can be provided in long form, like"
        " 'Published', or in short form, like 'P'. It is case-insensitive. "
    ),
)
@click.option("--fn", type=str, help="Name of the function to filter executions by.")
@click.option(
    "--collection", type=str, help="Name of the collection to filter executions by."
)
@click.option(
    "--name",
    "-n",
    help=(
        "An execution name wildcard to match for the list. "
        "For example, 'my_execution*' will match all executions "
        "with names starting with 'my_execution'."
    ),
)
@click.option(
    "--last",
    is_flag=True,
    help="If set, only the last execution of the list will be shown.",
)
@click.option(
    "--at",
    help=(
        "If provided, only executions started before that time will be shown. Must be "
        " either a valid timestamp in the form of a unix timestamp (milliseconds since "
        "epoch) or a valid date-time format. The valid "
        "formats are 'YYYY-MM-DD', 'YYYY-MM-DDTHH', 'YYYY-MM-DDTHH:MM', "
        "'YYYY-MM-DDTHH:MM:SS', and 'YYYY-MM-DDTHH:MM:SS.sss'. A 'Z' character can be "
        "added at the end to indicate UTC time (e.g., '2023-10-01T12Z' or "
        "'2023-10-01T12:00:00.000Z') or it can be omitted to indicate local "
        "time (e.g., '2023-10-01T12' or '2023-10-01T12:00:00.000')."
    ),
    type=str,
)
@click.pass_context
def list_exec(
    ctx: click.Context,
    status: List[str],
    name: str,
    fn: str,
    collection: str,
    last: bool,
    at: str,
):
    """List all executions"""
    verify_login_or_prompt(ctx)
    request_filter = obtain_list_exec_filters(
        status=status,
        exec_name=name,
        fn=fn,
        collection=collection,
        at=at,
    )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        list_of_executions = server.list_executions(
            filter=request_filter,
            order_by="triggered_on-",
        )
        if last:
            list_of_executions = list_of_executions[:1]

        table = Table(title="Executions")
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
                execution.triggered_on_str,
                execution.status,
            )

        click.echo()
        console = Console()
        console.print(table)
        click.echo(f"Number of executions: {len(list_of_executions)}")
        click.echo()
    except Exception as e:
        raise click.ClickException(f"Failed to list executions: {e}")


def obtain_list_exec_filters(
    status: List[str],
    exec_name: str,
    fn: str,
    collection: str,
    at: str,
) -> List[str]:
    """
    Helper function to obtain the filters for listing executions.
    """
    request_filter = []
    if status:
        request_filter.append(
            [
                f"status:eq:{convert_user_provided_status_to_api_status(s)}"
                for s in status
            ]
        )
    if exec_name:
        request_filter.append(f"name:lk:{exec_name}")
    if fn:
        request_filter.append(f"function:eq:{fn}")
    if collection:
        request_filter.append(f"collection:eq:{collection}")
    if at:
        try:
            at_timestamp = top_and_convert_to_timestamp(at)
            request_filter.append(f"triggered_on:le:{at_timestamp}")
        except ValueError as e:
            raise click.ClickException(f"Invalid date-time format for 'at' option: {e}")
    return request_filter


@exec.command()
@click.option(
    "--status",
    type=str,
    multiple=True,
    help=(
        "Filter transactions by status. The status can be provided in long form, like"
        " 'Published', or in short form, like 'P'. It is case-insensitive. "
    ),
)
@click.option(
    "--collection", type=str, help="Name of the collection to filter transactions by."
)
@click.option(
    "--exec-name",
    "-n",
    help=(
        "An execution name wildcard to match. "
        "For example, 'my_execution*' will match all executions "
        "with names starting with 'my_execution'."
    ),
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["exec"],
)
@click.option(
    "--exec",
    help=(
        "An execution ID to filter transactions by. If provided, only transactions "
        "that are part of the specified execution will be shown."
    ),
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["exec-name"],
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
        "epoch) or a valid date-time format. The valid "
        "formats are 'YYYY-MM-DD', 'YYYY-MM-DDTHH', 'YYYY-MM-DDTHH:MM', "
        "'YYYY-MM-DDTHH:MM:SS', and 'YYYY-MM-DDTHH:MM:SS.sss'. A 'Z' character can be "
        "added at the end to indicate UTC time (e.g., '2023-10-01T12Z' or "
        "'2023-10-01T12:00:00.000Z') or it can be omitted to indicate local "
        "time (e.g., '2023-10-01T12' or '2023-10-01T12:00:00.000')."
    ),
    type=str,
)
@click.pass_context
def list_trx(
    ctx: click.Context,
    status: List[str],
    collection: str,
    exec_name: str,
    exec: str,
    last: bool,
    at: str,
):
    """List all transactions"""
    verify_login_or_prompt(ctx)
    try:
        request_filter = obtain_list_trx_filters(
            status=status,
            exec_name=exec_name,
            execution_id=exec,
            collection=collection,
            at=at,
        )
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        list_of_transactions = server.list_transactions(
            filter=request_filter, order_by="triggered_on-"
        )
        if last:
            list_of_transactions = list_of_transactions[:1]

        table = Table(title="Transactions")
        table.add_column("ID", style="cyan", no_wrap=True)
        table.add_column("Execution ID")
        table.add_column("Collection")
        table.add_column("Triggered on")
        table.add_column("Triggered by")
        table.add_column("Status", no_wrap=True)

        for transaction in list_of_transactions:
            table.add_row(
                transaction.id,
                transaction.execution.id,
                transaction.collection.name,
                transaction.triggered_on_str,
                transaction.triggered_by,
                transaction.status,
            )

        click.echo()
        console = Console()
        console.print(table)
        click.echo(f"Number of transactions: {len(list_of_transactions)}")
        click.echo()
    except Exception as e:
        raise click.ClickException(f"Failed to list transactions: {e}")


def obtain_list_trx_filters(
    status: List[str],
    exec_name: str,
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
            [
                f"status:eq:{convert_user_provided_status_to_api_status(s)}"
                for s in status
            ]
        )
    if exec_name:
        request_filter.append(f"execution:lk:{exec_name}")
    if execution_id:
        request_filter.append(f"execution_id:eq:{execution_id}")
    if collection:
        request_filter.append(f"collection:eq:{collection}")
    if at:
        try:
            at_timestamp = top_and_convert_to_timestamp(at)
            request_filter.append(f"triggered_on:le:{at_timestamp}")
        except ValueError as e:
            raise click.ClickException(f"Invalid date-time format for 'at' option: {e}")
    return request_filter


@exec.command()
@click.option(
    "--status",
    type=str,
    multiple=True,
    help=(
        "Filter workers by status. The status can be provided in long form, like"
        " 'Published', or in short form, like 'P'. It is case-insensitive. "
    ),
)
@click.option(
    "--exec",
    type=str,
    cls=MutuallyExclusiveOption,
    help="ID of the execution to which the workers belong.",
    mutually_exclusive=["exec-name", "fn", "trx"],
)
@click.option(
    "--exec-name",
    "-n",
    help=(
        "An execution name wildcard to match. "
        "For example, 'my_execution*' will match all executions "
        "with names starting with 'my_execution'."
    ),
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["exec"],
)
@click.option(
    "--fn",
    type=str,
    cls=MutuallyExclusiveOption,
    help=(
        "Name of the function to which the workers belong. If this is provided, "
        "collection must also be provided."
    ),
    mutually_exclusive=["exec", "trx"],
)
@click.option(
    "--collection",
    type=str,
    cls=MutuallyExclusiveOption,
    help="Collection of the function to which the workers belong.",
    mutually_exclusive=["exec", "trx"],
)
@click.option(
    "--trx",
    type=str,
    cls=MutuallyExclusiveOption,
    help="ID of the transaction to which the workers belong.",
    mutually_exclusive=["exec", "fn"],
)
@click.pass_context
def list_worker(
    ctx: click.Context,
    status: List[str],
    exec: str,
    exec_name: str,
    fn: str,
    collection: str,
    trx: str,
):
    """
    List all workers of a specific execution, function or
        transaction.
    """
    verify_login_or_prompt(ctx)
    if fn:
        collection = (
            collection
            or get_currently_pinned_object(ctx, "collection")
            or logical_prompt(
                ctx, "Name of the collection to which the function belongs"
            )
        )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        request_filter = obtain_list_worker_filters(
            status=status,
            exec_name=exec_name,
            execution_id=exec,
            fn=fn,
            collection=collection,
            trx=trx,
        )
        list_of_workers = server.list_workers(filter=request_filter)

        table = Table(title="Workers")
        table.add_column("Worker ID", style="cyan", no_wrap=True)
        table.add_column("Collection")
        table.add_column("Function")
        table.add_column("Execution")
        table.add_column("Execution ID")
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
    except Exception as e:
        raise click.ClickException(f"Failed to list workers: {e}")


def obtain_list_worker_filters(
    status: List[str],
    exec_name: str,
    execution_id: str,
    fn: str,
    collection: str,
    trx: str,
) -> List[str]:
    """
    Helper function to obtain the filters for listing workers.
    """

    request_filter = []
    if status:
        request_filter.append(
            [
                f"status:eq:{convert_user_provided_status_to_api_status(s)}"
                for s in status
            ]
        )
    if exec_name:
        request_filter.append(f"execution:lk:{exec_name}")
    if execution_id:
        request_filter.append(f"execution_id:eq:{execution_id}")
    if fn:
        request_filter.append(f"function:eq:{fn}")
    if collection:
        request_filter.append(f"collection:eq:{collection}")
    if trx:
        request_filter.append(f"transaction_id:eq:{trx}")
    return request_filter


@exec.command()
@click.argument("id")
@click.pass_context
def info(ctx: click.Context, id: str):
    """
    Display an execution by ID.
    """
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        execution = server.get_execution(id)

        table = Table(title=f"Execution '{id}'")
        table.add_column("Name")
        table.add_column("Collection")
        table.add_column("Function")
        table.add_column("Triggered on")
        table.add_column("Status", no_wrap=True)

        table.add_row(
            execution.name,
            execution.collection.name,
            execution.function.name,
            execution.triggered_on_str,
            execution.status,
        )

        click.echo()
        console = Console()
        console.print(table)
        click.echo()

    except Exception as e:
        raise click.ClickException(f"Failed to display execution: {e}")


@exec.command()
@click.option(
    "--worker",
    help="ID of the worker that generated the logs.",
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["status", "exec", "exec-name", "fn", "collection", "trx"],
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
        "Filter workers by status. The status can be provided in long form, like"
        " 'Published', or in short form, like 'P'. It is case-insensitive. "
    ),
)
@click.option(
    "--exec",
    type=str,
    cls=MutuallyExclusiveOption,
    help="ID of the execution to which the workers belong.",
    mutually_exclusive=["exec-name", "fn", "trx"],
)
@click.option(
    "--exec-name",
    "-n",
    help=(
        "An execution name wildcard to match. "
        "For example, 'my_execution*' will match all executions "
        "with names starting with 'my_execution'."
    ),
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["exec"],
)
@click.option(
    "--fn",
    type=str,
    cls=MutuallyExclusiveOption,
    help=(
        "Name of the function to which the workers belong. If this is provided, "
        "collection must also be provided."
    ),
    mutually_exclusive=["exec", "trx"],
)
@click.option(
    "--collection",
    type=str,
    cls=MutuallyExclusiveOption,
    help="Collection of the function to which the workers belong.",
    mutually_exclusive=["exec", "trx"],
)
@click.option(
    "--trx",
    type=str,
    cls=MutuallyExclusiveOption,
    help="ID of the transaction to which the workers belong.",
    mutually_exclusive=["exec", "fn"],
)
@click.pass_context
def worker_logs(
    ctx: click.Context,
    worker: str,
    file: str,
    status: List[str],
    exec: str,
    exec_name: str,
    fn: str,
    collection: str,
    trx: str,
):
    """
    Show the logs generated by a worker.
    """
    verify_login_or_prompt(ctx)
    if fn:
        collection = (
            collection
            or get_currently_pinned_object(ctx, "collection")
            or logical_prompt(
                ctx, "Name of the collection to which the function belongs"
            )
        )
    server: TabsdataServer = ctx.obj["tabsdataserver"]
    request_filter = obtain_list_worker_filters(
        status=status,
        exec_name=exec_name,
        execution_id=exec,
        fn=fn,
        collection=collection,
        trx=trx,
    )
    if request_filter:
        list_of_workers = obtain_worker_list_from_filters(server, request_filter, ctx)
    else:
        # If no filters are provided, we can directly use the worker ID provided
        # by the user
        worker = worker or logical_prompt(
            ctx, "ID of the worker that generated the logs"
        )
        list_of_workers = [worker]
    try:
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
        raise click.ClickException(f"Failed to show worker logs: {e}")


def obtain_worker_list_from_filters(
    server: TabsdataServer, request_filter: List[str], ctx: click.Context
) -> List[str]:
    # If filters are provided, we must show a list of all possible workers for
    # that filter, and then allow the user to select one of them or all of them.

    list_of_workers = server.list_workers(filter=request_filter)

    table = Table(title="Workers")
    table.add_column("Worker ID", style="cyan", no_wrap=True)
    table.add_column("Collection")
    table.add_column("Function")
    table.add_column("Execution")
    table.add_column("Execution ID")
    table.add_column("Transaction ID")
    table.add_column("Status")
    table.add_column("Counter", no_wrap=True)

    for counter, worker in enumerate(list_of_workers, 1):
        table.add_row(
            worker.id,
            worker.collection.name,
            worker.function.name,
            worker.execution.name,
            worker.execution.id,
            worker.transaction.id,
            worker.status,
            str(counter),
        )

    click.echo()
    console = Console()
    console.print(table)
    click.echo(f"Number of workers: {len(list_of_workers)}")
    click.echo()

    worker_counter = logical_prompt(
        ctx,
        "Counter of the worker to recover logs from. If "
        "none is provided (by pressing the return key without writing anything), "
        "all worker logs will be shown.",
        default_value="",
    )
    if not worker_counter:
        if not list_of_workers:
            raise click.ClickException("No workers found for the provided filters.")
        list_of_workers = [w.id for w in list_of_workers]
    else:
        try:
            chosen_worker = list_of_workers[int(worker_counter) - 1]
        except IndexError:
            raise click.ClickException(
                f"Invalid worker counter '{worker_counter}'. Please provide a valid"
                " counter from the list (in this instance, a number between 1 and"
                f" {len(list_of_workers)})."
            )
        list_of_workers = [chosen_worker.id]
    return list_of_workers


@exec.command()
@click.argument("id")
@click.pass_context
def recover_exec(ctx: click.Context, id: str):
    """
    Recover an execution. This includes all transactions that are part of the
    execution
    """
    verify_login_or_prompt(ctx)
    click.echo(f"Recovering execution with ID '{id}'")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.recover_execution(id)
        click.echo("Execution recovered successfully")
    except Exception as e:
        raise click.ClickException(f"Failed to recover execution: {e}")


@exec.command()
@click.argument("id")
@click.pass_context
def recover_trx(ctx: click.Context, id: str):
    """
    Recover a transaction. This includes all functions that are part of the
    transaction
    """
    verify_login_or_prompt(ctx)
    click.echo(f"Recovering transaction with ID '{id}'")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.recover_transaction(id)
        click.echo("Transaction recovered successfully")
    except Exception as e:
        raise click.ClickException(f"Failed to recover transaction: {e}")
