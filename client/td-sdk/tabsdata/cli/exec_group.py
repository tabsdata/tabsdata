#
# Copyright 2025 Tabs Data Inc.
#

import rich_click as click
from rich.console import Console
from rich.table import Table

from tabsdata.api.tabsdata_server import TabsdataServer
from tabsdata.cli.cli_utils import (
    MutuallyExclusiveOption,
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
def cancel(ctx: click.Context, id: str):
    """Cancel an execution. This includes all transactions that are part of the
    execution
    """
    verify_login_or_prompt(ctx)
    id = id or logical_prompt(ctx, "ID of the execution that will be canceled")
    click.echo(f"Canceling execution with ID '{id}'")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.cancel_execution(id)
        click.echo("Execution canceled successfully")
    except Exception as e:
        raise click.ClickException(f"Failed to cancel execution: {e}")


@exec.command()
@click.option(
    "--trx",
    help="ID of the transaction that will be cancelled.",
)
@click.pass_context
def cancel_trx(ctx: click.Context, trx: str):
    """Cancel a transaction. This includes all functions that are part of the
    transaction
    """
    verify_login_or_prompt(ctx)
    trx = trx or logical_prompt(ctx, "ID of the transaction that will be canceled")
    click.echo(f"Canceling transaction with ID '{trx}'")
    click.echo("-" * 10)
    try:
        ctx.obj["tabsdataserver"].cancel_transaction(trx)
        click.echo("Transaction canceled successfully")
    except Exception as e:
        raise click.ClickException(f"Failed to cancel transaction: {e}")


@exec.command()
@click.pass_context
def list(ctx: click.Context):
    """List all executions"""
    verify_login_or_prompt(ctx)
    try:
        list_of_executions = ctx.obj["tabsdataserver"].executions

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


@exec.command()
@click.option(
    "--published",
    is_flag=True,
    help="Show only the transactions with a 'Published' status.",
)
@click.pass_context
def list_trxs(ctx: click.Context, published: bool):
    """List all transactions"""
    verify_login_or_prompt(ctx)
    try:
        if published:
            click.echo("Listing only the transactions with a 'Published' status")
            request_filter = ["status:eq:P"]
        else:
            request_filter = None
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        list_of_transactions = server.list_transactions(filter=request_filter)

        table = Table(title="Transactions")
        table.add_column("ID", style="cyan", no_wrap=True)
        table.add_column("Execution ID")
        table.add_column("Triggered on")
        table.add_column("Status", no_wrap=True)

        for transaction in list_of_transactions:
            table.add_row(
                transaction.id,
                transaction.execution.id,
                transaction.triggered_on_str,
                transaction.status,
            )

        click.echo()
        console = Console()
        console.print(table)
        click.echo(f"Number of transactions: {len(list_of_transactions)}")
        click.echo()
    except Exception as e:
        raise click.ClickException(f"Failed to list transactions: {e}")


@exec.command()
@click.option(
    "--execution",
    type=str,
    cls=MutuallyExclusiveOption,
    help="ID of the execution to which the workers belong.",
    mutually_exclusive=["fn", "trx"],
)
@click.option(
    "--fn",
    type=str,
    cls=MutuallyExclusiveOption,
    help=(
        "Name of the function to which the workers belong. If this is provided, "
        "collection must also be provided."
    ),
    mutually_exclusive=["execution", "trx"],
)
@click.option(
    "--collection",
    type=str,
    cls=MutuallyExclusiveOption,
    help=(
        "Collection of the function to which the workers belong. If this is provided, "
        "fn must also be provided."
    ),
    mutually_exclusive=["execution", "trx"],
)
@click.option(
    "--trx",
    type=str,
    cls=MutuallyExclusiveOption,
    help="ID of the transaction to which the workers belong.",
    mutually_exclusive=["execution", "fn"],
)
@click.pass_context
def list_workers(
    ctx: click.Context,
    execution: str,
    fn: str,
    collection: str,
    trx: str,
):
    """
    List all workers of a specific execution, function or
        transaction.
    """
    verify_login_or_prompt(ctx)
    if not (execution or fn or trx):
        raise click.ClickException(
            "Exactly one of execution ID, "
            "function name or transaction ID must be provided, "
            "but none were provided."
        )
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
        request_filter = []
        if execution:
            request_filter = [f"execution_id:eq:{execution}"]
        elif fn:
            request_filter = [f"function:eq:{fn}", f"collection:eq:{collection}"]
        elif trx:
            request_filter = [f"transaction_id:eq:{trx}"]
        list_of_workers = server.list_workers(filter=request_filter)

        table = Table(title="Workers")
        table.add_column("Worker ID", style="cyan", no_wrap=True)
        table.add_column("Collection")
        table.add_column("Function")
        table.add_column("Function ID")
        table.add_column("Execution")
        table.add_column("Execution ID")
        table.add_column("Transaction ID")
        table.add_column("Status")

        for worker in list_of_workers:
            table.add_row(
                worker.id,
                worker.collection.name,
                worker.function.name,
                worker.function.id,
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


@exec.command()
@click.option(
    "--worker",
    help="ID of the worker that generated the logs.",
)
@click.option(
    "--file",
    help=(
        "Path of the file where the logs will be saved. Any folders provided before "
        "the file must already exist."
    ),
)
@click.pass_context
def logs(ctx: click.Context, worker: str, file: str):
    """
    Show the logs generated by a worker.
    """
    verify_login_or_prompt(ctx)
    worker = worker or logical_prompt(ctx, "ID of the worker that generated the logs")
    try:
        generated_logs = ctx.obj["tabsdataserver"].get_worker_log(worker)
        if file:
            click.echo(f"Saving logs from worker '{worker}' to file '{file}'")
            with open(file, "w") as f:
                f.write(generated_logs)
            click.echo("Logs saved successfully")
        else:
            click.echo(f"Logs from worker '{worker}':")
            click.echo("*" * 20)
            click.echo(generated_logs)
    except Exception as e:
        raise click.ClickException(f"Failed to recover transaction: {e}")


@exec.command()
@click.argument("id")
@click.pass_context
def recover(ctx: click.Context, trx: str):
    """
    Recover an execution. This includes all transactions that are part of the
    execution
    """
    verify_login_or_prompt(ctx)
    trx = trx or logical_prompt(ctx, "ID of the execution that will be recovered")
    click.echo(f"Recovering execution with ID '{trx}'")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.recover_execution(trx)
        click.echo("Execution recovered successfully")
    except Exception as e:
        raise click.ClickException(f"Failed to recover execution: {e}")


@exec.command()
@click.option(
    "--trx",
    help="ID of the transaction that will be cancelled.",
)
@click.pass_context
def recover_trx(ctx: click.Context, trx: str):
    """
    Recover a transaction. This includes all functions that are part of the
    transaction
    """
    verify_login_or_prompt(ctx)
    trx = trx or logical_prompt(ctx, "ID of the transaction that will be recovered")
    click.echo(f"Recovering transaction with ID '{trx}'")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.recover_transaction(trx)
        click.echo("Transaction recovered successfully")
    except Exception as e:
        raise click.ClickException(f"Failed to recover transaction: {e}")
