#
# Copyright 2024 Tabs Data Inc.
#

import rich_click as click
from rich.console import Console
from rich.table import Table

from tabsdata.api.tabsdata_server import (
    TabsdataServer,
)
from tabsdata.cli.cli_utils import (
    get_currently_pinned_object,
    logical_prompt,
    verify_login_or_prompt,
)


@click.group()
def table():
    """Table management commands"""


@table.command()
@click.option(
    "--name",
    help="Name of the table.",
)
@click.option(
    "--collection",
    "-c",
    help="Name of the collection to which the table belongs.",
)
@click.option(
    "--file",
    help=(
        "File in which the table will be stored. Can be an absolute or a relative "
        "path. Any directory that appears must already exist (it will not be created "
        "as part of the download)."
    ),
)
@click.option(
    "--at",
    help=(
        "If provided, the table values at the given time will be downloaded. Must be "
        "a valid timestamp in the form of a unix timestamp (milliseconds since epoch)."
    ),
    type=int,
)
@click.pass_context
def download(
    ctx: click.Context,
    collection: str,
    name: str,
    file: str,
    at: int,
):
    """Download the table as a parquet file"""
    verify_login_or_prompt(ctx)
    file = file or logical_prompt(ctx, "File in which the table will be stored")
    name = name or logical_prompt(ctx, "Name of the table")
    collection = (
        collection
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the table belongs")
    )
    click.echo(f"Downloading table to file '{file}'")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.download_table(
            collection,
            name,
            file,
            at=at,
        )

        click.echo("Table downloaded successfully")

    except Exception as e:
        raise click.ClickException(f"Failed to download table: {e}")


@table.command()
@click.option(
    "--collection",
    "-c",
    help="Name of the collection to which the tables belong.",
)
@click.pass_context
def list(ctx: click.Context, collection: str):
    """List all tables in a collection"""
    verify_login_or_prompt(ctx)
    collection = (
        collection
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the tables belong")
    )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        list_of_tables = server.list_tables(collection)

        cli_table = Table(title=f"Tables in collection '{collection}'")
        cli_table.add_column("Name", style="cyan", no_wrap=True)
        cli_table.add_column("Function")

        for table in list_of_tables:
            cli_table.add_row(
                table.name,
                table.function.name,
            )

        click.echo()
        console = Console()
        console.print(cli_table)
        click.echo(
            f"Number of tables in collection '{collection}': {len(list_of_tables)}"
        )
        click.echo()

    except Exception as e:
        raise click.ClickException(f"Failed to list tables: {e}")


@table.command()
@click.option(
    "--name",
    help="Name of the table.",
)
@click.option(
    "--collection",
    "-c",
    help="Name of the collection to which the table belongs.",
)
@click.option(
    "--len",
    type=int,
    help="Amount of rows to be sampled. If not provided, all rows will be shown.",
)
@click.option(
    "--offset",
    type=int,
    help=(
        "How many rows to skip before starting to sample. If not provided, "
        "sampling wil start at the first row."
    ),
)
@click.option(
    "--at",
    help=(
        "If provided, the table values at the given time will be shown. Must be "
        "a valid timestamp in the form of a unix timestamp (milliseconds since epoch)."
    ),
    type=int,
)
@click.option(
    "--file",
    help=(
        "File in which the table will be stored as a NDJSON. Can be an absolute or a "
        "relative path. Any directory that appears must already exist (it will not be "
        "created as part of the download)."
    ),
)
@click.pass_context
def sample(
    ctx: click.Context,
    collection: str,
    name: str,
    len: int,
    offset: int,
    file: str,
    at: int,
):
    """Sample rows from the table"""
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the table")
    collection = (
        collection
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the table belongs")
    )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        sampled_table = server.sample_table(
            collection,
            name,
            at=at,
            len=len,
            offset=offset,
        )

        click.echo()
        click.echo(f"Sample of table '{name}'")
        click.echo(sampled_table)
        click.echo()
        if file:
            click.echo(f"Saving sample to file '{file}'")
            sampled_table.write_ndjson(file)
            click.echo("Sample saved successfully")

    except Exception as e:
        raise click.ClickException(f"Failed to sample table: {e}")


@table.command()
@click.option(
    "--name",
    help="Name of the table.",
)
@click.option(
    "--collection",
    "-c",
    help="Name of the collection to which the table belongs.",
)
@click.option(
    "--at",
    help=(
        "If provided, the table schema at the given time will be shown. Must be "
        "a valid timestamp in the form of a unix timestamp (milliseconds since epoch)."
    ),
    type=int,
)
@click.pass_context
def schema(
    ctx: click.Context,
    collection: str,
    name: str,
    at: int,
):
    """Show table schema"""
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the table")
    collection = (
        collection
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the table belongs")
    )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        table_schema = server.get_table_schema(
            collection,
            name,
            at=at,
        )

        table = Table(title=f"Schema of table '{name}'")
        table.add_column("Column", style="cyan", no_wrap=True)
        table.add_column("Type")
        for column in table_schema:
            table.add_row(column["name"], column["type"])

        click.echo()
        console = Console()
        console.print(table)
        click.echo()

    except Exception as e:
        raise click.ClickException(f"Failed to obtain table schema: {e}")
