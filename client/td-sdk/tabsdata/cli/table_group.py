#
# Copyright 2024 Tabs Data Inc.
#


import polars as pl
import rich_click as click
from rich.console import Console
from rich.table import Table

from tabsdata.cli.cli_utils import (
    MutuallyExclusiveOption,
    complete_datetime,
    logical_prompt,
    verify_login_or_prompt,
)


@click.group()
@click.pass_context
def table(ctx: click.Context):
    """Table management commands"""
    verify_login_or_prompt(ctx)


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
    "--version",
    type=str,
    cls=MutuallyExclusiveOption,
    help=(
        "Version of the table. Can be a fixed version or a relative one (HEAD, "
        "HEAD^, and HEAD~## syntax)."
    ),
    mutually_exclusive=["commit", "time"],
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
    "--commit",
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["time", "version"],
    help="The commit ID of the table.",
)
@click.option(
    "--time",
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["commit", "version"],
    help=(
        "If provided, the table values at the given time will be downloaded. The valid "
        "formats are 'YYYY-MM-DD', 'YYYY-MM-DDTHHZ', 'YYYY-MM-DDTHH:MMZ', "
        "'YYYY-MM-DDTHH:MM:SSZ', and 'YYYY-MM-DDTHH:MM:SS.sssZ'."
    ),
)
@click.pass_context
def download(
    ctx: click.Context,
    collection: str,
    name: str,
    commit: str,
    time: str,
    version: str,
    file: str,
):
    """Download the table as a parquet file"""
    file = file or logical_prompt(ctx, "File in which the table will be stored")
    name = name or logical_prompt(ctx, "Name of the table")
    collection = collection or logical_prompt(
        ctx, "Name of the collection to which the table belongs"
    )
    click.echo(f"Downloading table to file '{file}'")
    click.echo("-" * 10)
    try:
        ctx.obj["tabsdataserver"].table_download(
            collection,
            name,
            file,
            commit=commit,
            time=complete_datetime(time),
            version=version,
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
    collection = collection or logical_prompt(
        ctx, "Name of the collection to which the tables belong"
    )
    try:
        list_of_tables = ctx.obj["tabsdataserver"].table_list(collection)

        cli_table = Table(title=f"Tables in collection '{collection}'")
        cli_table.add_column("Name", style="cyan", no_wrap=True)
        cli_table.add_column("Function")

        for table in list_of_tables:
            cli_table.add_row(
                table.name,
                table.function,
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
    "--version",
    type=str,
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["time", "commit"],
    help=(
        "Version of the table. Can be a fixed version or a relative one (HEAD, "
        "HEAD^, and HEAD~## syntax)."
    ),
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
    "--commit",
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["time", "version"],
    help="The commit ID of the table.",
)
@click.option(
    "--time",
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["commit", "version"],
    help=(
        "If provided, the table values at the given time will be shown. The valid "
        "formats are 'YYYY-MM-DD', 'YYYY-MM-DDTHHZ', 'YYYY-MM-DDTHH:MMZ', "
        "'YYYY-MM-DDTHH:MM:SSZ', and 'YYYY-MM-DDTHH:MM:SS.sssZ'."
    ),
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
    commit: str,
    time: str,
    version: str,
    len: int,
    offset=int,
    file=str,
):
    """Sample rows from the table"""
    name = name or logical_prompt(ctx, "Name of the table")
    collection = collection or logical_prompt(
        ctx, "Name of the collection to which the table belongs"
    )
    try:
        sampled_table: pl.DataFrame = ctx.obj["tabsdataserver"].table_sample(
            collection,
            name,
            commit=commit,
            time=complete_datetime(time),
            version=version,
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
    "--version",
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["time", "commit"],
    help=(
        "Version of the table. Can be a fixed version or a relative one (HEAD, "
        "HEAD^, and HEAD~## syntax)."
    ),
)
@click.option(
    "--commit",
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["time", "version"],
    help="The commit ID of the table.",
)
@click.option(
    "--time",
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["commit", "version"],
    help=(
        "If provided, the table schema at the given time will be shown. The valid "
        "formats are 'YYYY-MM-DD', 'YYYY-MM-DDTHHZ', 'YYYY-MM-DDTHH:MMZ', "
        "'YYYY-MM-DDTHH:MM:SSZ', and 'YYYY-MM-DDTHH:MM:SS.sssZ'."
    ),
)
@click.pass_context
def schema(
    ctx: click.Context, collection: str, name: str, commit: str, time: str, version: str
):
    """Show table schema"""
    name = name or logical_prompt(ctx, "Name of the table")
    collection = collection or logical_prompt(
        ctx, "Name of the collection to which the table belongs"
    )
    try:
        table_schema = ctx.obj["tabsdataserver"].table_get_schema(
            collection,
            name,
            commit=commit,
            time=complete_datetime(time),
            version=version,
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
