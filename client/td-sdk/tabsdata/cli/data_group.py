#
# Copyright 2025 Tabs Data Inc.
#


import rich_click as click
from rich.console import Console
from rich.table import Table

from tabsdata.api.tabsdata_server import TabsdataServer
from tabsdata.cli.cli_utils import (
    get_currently_pinned_object,
    logical_prompt,
    verify_login_or_prompt,
)


@click.group()
def data():
    """Data management commands"""


@data.command()
@click.option(
    "--collection",
    "-c",
    help="Name of the collection to which the data belongs.",
)
@click.option(
    "--table",
    help="The name of the table to which the data belongs.",
)
@click.pass_context
def versions(ctx: click.Context, collection: str, table: str):
    """
    List all versions of the data of a table.
    """
    verify_login_or_prompt(ctx)
    collection = (
        collection
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the data belongs")
    )
    table = table or logical_prompt(
        ctx, "The name of the table to which the data belongs"
    )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        data_version_list = server.list_dataversions(collection, table)

        table = Table(title=f"Data versions for table '{table}'")
        table.add_column("ID", style="cyan", no_wrap=True)
        table.add_column("Function ID")
        table.add_column("Execution ID")
        table.add_column("Triggered on")
        table.add_column("Status")

        for data_version in data_version_list:
            table.add_row(
                data_version.id,
                data_version.function.id,
                data_version.execution.id,
                data_version.triggered_on_str,
                data_version.status,
            )

        click.echo()
        console = Console()
        console.print(table)
        click.echo(f"Number of data versions: {len(data_version_list)}")

    except Exception as e:
        raise click.ClickException(f"Failed to list data versions: {e}")
