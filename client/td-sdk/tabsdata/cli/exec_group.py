#
# Copyright 2025 Tabs Data Inc.
#


import datetime
import os

import rich_click as click
from rich.console import Console
from rich.table import Table

from tabsdata.cli.cli_utils import (
    DOT_FOLDER,
    MutuallyExclusiveOption,
    cleanup_dot_files,
    logical_prompt,
    show_dot_file,
    verify_login_or_prompt,
)


@click.group()
@click.pass_context
def exec(ctx: click.Context):
    """Execution plan management commands"""
    verify_login_or_prompt(ctx)


@exec.command()
@click.option(
    "--trx",
    help="ID of the transaction that will be cancelled.",
)
@click.pass_context
def cancel(ctx: click.Context, trx: str):
    """Cancel a transaction. This includes all functions that are part of the
    transaction and all its dependants
    """
    trx = trx or logical_prompt(ctx, "ID of the transaction that will be canceled")
    click.echo(f"Canceling transaction with ID '{trx}'")
    click.echo("-" * 10)
    try:
        ctx.obj["tabsdataserver"].transaction_cancel(trx)
        click.echo("Execution plan canceled successfully")
    except Exception as e:
        raise click.ClickException(f"Failed to cancel transaction: {e}")


@exec.command()
@click.pass_context
def list_commits(ctx: click.Context):
    """List all commits"""
    try:
        list_of_commits = ctx.obj["tabsdataserver"].commits

        table = Table(title="Commits")
        table.add_column("ID", style="cyan", no_wrap=True)
        table.add_column("Execution Plan ID")
        table.add_column("Triggered on")
        table.add_column("Ended on")

        for commit in list_of_commits:
            table.add_row(
                commit.id,
                commit.execution_plan_id,
                commit.triggered_on_str,
                commit.ended_on_str,
            )

        click.echo()
        console = Console()
        console.print(table)
        click.echo(f"Number of commits: {len(list_of_commits)}")
        click.echo()
    except Exception as e:
        raise click.ClickException(f"Failed to list commits: {e}")


@exec.command()
@click.pass_context
def list_plans(ctx: click.Context):
    """List all execution plans"""
    try:
        list_of_plans = ctx.obj["tabsdataserver"].execution_plans

        table = Table(title="Execution plans")
        table.add_column("ID", style="cyan", no_wrap=True)
        table.add_column("Name")
        table.add_column("Collection")
        table.add_column("Function")
        table.add_column("Triggered on")
        table.add_column("Status", no_wrap=True)

        for plan in list_of_plans:
            table.add_row(
                plan.id,
                plan.name,
                plan.collection,
                plan.function,
                plan.triggered_on_str,
                plan.status,
            )

        click.echo()
        console = Console()
        console.print(table)
        click.echo(f"Number of execution plans: {len(list_of_plans)}")
        click.echo()
    except Exception as e:
        raise click.ClickException(f"Failed to list execution plans: {e}")


@exec.command()
@click.pass_context
def list_trxs(ctx: click.Context):
    """List all transactions"""
    try:
        list_of_transactions = ctx.obj["tabsdataserver"].transactions

        table = Table(title="Transactions")
        table.add_column("ID", style="cyan", no_wrap=True)
        table.add_column("Execution Plan ID")
        table.add_column("Triggered on")
        table.add_column("Status", no_wrap=True)

        for plan in list_of_transactions:
            table.add_row(
                plan.id,
                plan.execution_plan_id,
                plan.triggered_on_str,
                plan.status,
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
    "--data-ver",
    type=str,
    cls=MutuallyExclusiveOption,
    help="ID of the data version to which the workers belong.",
    mutually_exclusive=["plan", "fn", "trx"],
)
@click.option(
    "--plan",
    type=str,
    cls=MutuallyExclusiveOption,
    help="ID of the execution plan to which the workers belong.",
    mutually_exclusive=["data-ver", "fn", "trx"],
)
@click.option(
    "--fn",
    type=str,
    cls=MutuallyExclusiveOption,
    help="ID of the function to which the workers belong.",
    mutually_exclusive=["data-ver", "plan", "trx"],
)
@click.option(
    "--trx",
    type=str,
    cls=MutuallyExclusiveOption,
    help="ID of the transaction to which the workers belong.",
    mutually_exclusive=["data-ver", "plan", "fn"],
)
@click.pass_context
def list_workers(
    ctx: click.Context,
    data_ver: str,
    plan: str,
    fn: str,
    trx: str,
):
    """
    List all workers of a specific data version, execution plan, function or
    transaction.
    """
    if not (data_ver or plan or fn or trx):
        raise click.ClickException(
            "Exactly one of data version, execution plan, "
            "function or transaction ID must be provided, "
            "but none were provided."
        )
    try:
        list_of_workers = ctx.obj["tabsdataserver"].worker_list(
            by_execution_plan_id=plan,
            by_function_id=fn,
            by_data_version_id=data_ver,
            by_transaction_id=trx,
        )

        table = Table(title="Workers")
        table.add_column("Worker ID", style="cyan", no_wrap=True)
        table.add_column("Collection")
        table.add_column("Function")
        table.add_column("Function ID")
        table.add_column("Execution Plan")
        table.add_column("Execution Plan ID")
        table.add_column("Transaction ID")
        table.add_column("Started on")
        table.add_column("Status")

        for worker in list_of_workers:
            table.add_row(
                worker.id,
                worker.collection,
                worker.function,
                worker.function_id,
                worker.execution_plan,
                worker.execution_plan_id,
                worker.transaction_id,
                worker.started_on_str,
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
    worker = worker or logical_prompt(ctx, "ID of the worker that generated the logs")
    try:
        generated_logs = ctx.obj["tabsdataserver"].worker_log(worker)
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
@click.option(
    "--plan",
    help="ID of the execution plan that will be shown.",
)
@click.pass_context
def show_plan(ctx: click.Context, plan: str):
    """
    Show an execution plan. If the appropriate binary is installed, the dot file will
    be opened and show.
    """
    plan = plan or logical_prompt(ctx, "ID of the execution plan that will be shown")
    click.echo(f"Reading execution plan with ID '{plan}'")
    click.echo("-" * 10)
    try:
        dot = ctx.obj["tabsdataserver"].execution_plan_read(plan)
        click.echo("Execution plan shown successfully")
        if dot:
            os.makedirs(DOT_FOLDER, exist_ok=True)
            current_timestamp = int(
                datetime.datetime.now().replace(microsecond=0).timestamp()
            )
            file_name = f"{plan}-{current_timestamp}.dot"
            full_path = os.path.join(DOT_FOLDER, file_name)
            with open(full_path, "w") as f:
                f.write(dot)
            click.echo(f"Plan DOT at path: {full_path}")
            show_dot_file(full_path)
        else:
            click.echo("No DOT returned")
        cleanup_dot_files()
    except Exception as e:
        raise click.ClickException(f"Failed to show execution plan: {e}")


@exec.command()
@click.option(
    "--trx",
    help="ID of the transaction that will be cancelled.",
)
@click.pass_context
def recover(ctx: click.Context, trx: str):
    """
    Recover a transaction. This includes all functions that are part of the
    transaction and all its dependants
    """
    trx = trx or logical_prompt(ctx, "ID of the transaction that will be recovered")
    click.echo(f"Recovering transaction with ID '{trx}'")
    click.echo("-" * 10)
    try:
        ctx.obj["tabsdataserver"].transaction_recover(trx)
        click.echo("Execution plan recovered successfully")
    except Exception as e:
        raise click.ClickException(f"Failed to recover transaction: {e}")
