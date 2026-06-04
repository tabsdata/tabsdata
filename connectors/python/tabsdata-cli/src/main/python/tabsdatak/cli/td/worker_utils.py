#
#  Copyright 2026. Tabs Data Inc.
#
import rich_click as click
from rich.console import Console
from rich.table import Table

from tabsdatak.cli.cli_utils import (
    beautify_time,
    get_currently_pinned_object,
    hint_common_solutions,
    logical_prompt,
    verify_login_or_prompt,
    verify_trigger_type,
)
from tabsdatak.cli.td.exe_group import obtain_list_worker_filters
from tabsdatak.sdk.status_utils.worker import WorkerStatus
from tabsdatak.sdk.tabsdata_server import TabsdataServer

WORKER_NAME = {
    "CT": "cron trigger",
    "ST": "stage trigger",
}


def start(worker_type: str, ctx: click.Context, name: str, coll: str):
    """Start a worker"""
    worker_name = WORKER_NAME[worker_type]

    verify_login_or_prompt(ctx)
    name = name or logical_prompt(
        ctx, f"Name of the function to which the {worker_name} belongs"
    )
    coll = (
        coll
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the function belongs")
    )
    click.echo(f"Starting {worker_name} for function '{name}' in collection '{coll}'")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        function = server.get_function(collection_name=coll, function_name=name)
        if not verify_trigger_type(function, worker_type):
            raise click.ClickException(
                f"Function '{name}' in collection '{coll}' "
                f"does not have a {worker_name}."
            )
        if function.enabled:
            click.echo(
                f"The {worker_name} is already started, so no operation will be"
                " performed."
            )
        else:
            server.enable_function(
                collection_name=coll,
                function_name=name,
                enabled=True,
            )
            click.echo(f"{worker_name.capitalize()} started successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to start {worker_name}: {e}")


def stop(worker_type: str, ctx: click.Context, name: str, coll: str):
    """Stop a worker"""
    worker_name = WORKER_NAME[worker_type]

    verify_login_or_prompt(ctx)
    name = name or logical_prompt(
        ctx, f"Name of the function to which the {worker_name} belongs"
    )
    coll = (
        coll
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the function belongs")
    )
    click.echo(f"Stopping {worker_name} for function '{name}' in collection '{coll}'")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        function = server.get_function(collection_name=coll, function_name=name)
        if not verify_trigger_type(function, worker_type):
            raise click.ClickException(
                f"Function '{name}' in collection '{coll}' "
                f"does not have a {worker_name}."
            )
        if not function.enabled:
            click.echo(
                f"The {worker_name} is already stopped, so no operation will be"
                " performed."
            )
        else:
            server.enable_function(
                collection_name=coll,
                function_name=name,
                enabled=False,
            )
            click.echo(f"{worker_name.capitalize()} stopped successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to stop {worker_name}: {e}")


DISABLED_STATUSES = {
    WorkerStatus.DONE.value,
    WorkerStatus.FAILED.value,
    WorkerStatus.STOPPED.value,
    WorkerStatus.CANCELED.value,
}

ENABLED_STATUSES = {c.value for c in WorkerStatus} - DISABLED_STATUSES


def list(
    worker_type: str,
    ctx: click.Context,
    status: list[str],
    coll: str,
    state: str,
):
    """
    List all workers.
    """
    worker_name = WORKER_NAME[worker_type]

    verify_login_or_prompt(ctx)
    coll = coll or get_currently_pinned_object(ctx, "collection")
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]

        if state == "enabled":
            status = ENABLED_STATUSES
        elif state == "disabled":
            status = DISABLED_STATUSES

        request_filter = obtain_list_worker_filters(
            status=status,
            collection=coll,
            worker_type=worker_type,
        )
        list_of_workers = server.list_workers(filter=request_filter)

        table = Table(title=f"{worker_name.capitalize()} workers")
        table.add_column("Collection", style="cyan", no_wrap=True)
        table.add_column("Function", style="cyan", no_wrap=True)
        table.add_column("Function Version ID")
        table.add_column("Status")
        table.add_column("Started on")
        table.add_column("Ended on")

        for worker in list_of_workers:
            table.add_row(
                worker.collection.name,
                worker.function.name,
                worker.function_version_id,
                worker.status,
                beautify_time(worker.started_on_str),
                beautify_time(worker.ended_on_str),
            )

        click.echo()
        console = Console()
        console.print(table)
        click.echo(f"Number of {worker_name} workers: {len(list_of_workers)}")
        click.echo()
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list {worker_name} workers: {e}")
