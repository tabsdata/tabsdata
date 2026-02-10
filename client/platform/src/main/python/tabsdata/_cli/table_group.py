#
# Copyright 2024 Tabs Data Inc.
#

import rich_click as click
from rich.console import Console
from rich.table import Table

from tabsdata._cli.cli_utils import (
    MutuallyExclusiveOption,
    get_currently_pinned_object,
    hint_common_solutions,
    logical_prompt,
    show_hint,
    verify_login_or_prompt,
)
from tabsdata.api.tabsdata_server import (
    TabsdataServer,
)


@click.group()
def table():
    """Table management commands"""


@table.command()
@click.option(
    "--name",
    "-n",
    help="Name of the table to be deleted.",
)
@click.option(
    "--coll",
    "-c",
    help="Name of the collection to which the table belongs.",
)
@click.option(
    "--confirm",
    help="Write 'delete' to confirm deletion. Will be prompted for it if not provided.",
)
@click.pass_context
def delete(ctx: click.Context, name: str, coll: str, confirm: str):
    """Delete a table"""
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the table to be deleted")
    coll = (
        coll
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the table belongs")
    )
    click.echo(f"Deleting table '{name}' in collection '{coll}'")
    click.echo("-" * 10)
    confirm = confirm or logical_prompt(ctx, "Please type 'delete' to confirm deletion")
    if confirm != "delete":
        raise click.ClickException(
            "Deletion not confirmed. The confirmation word is 'delete'."
        )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.delete_table(coll, name)
        click.echo("Table deleted successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to delete table: {e}")


@table.command()
@click.option(
    "--name",
    help="Name of the table.",
)
@click.option(
    "--coll",
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
        "If provided, the table values at the given time will be shown. Must be "
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
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["at-trx", "version"],
)
@click.option(
    "--at-trx",
    help=(
        "ID of a transaction. If provided, the table values at the end of the given "
        "transaction will be shown."
    ),
    type=str,
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["at", "version"],
)
@click.option(
    "--version",
    help=(
        "ID of a dataversion of the table. If provided, the table values at that "
        "dataversion will be shown."
    ),
    type=str,
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["at", "at-trx"],
)
@click.pass_context
def download(
    ctx: click.Context,
    coll: str,
    name: str,
    file: str,
    at: str,
    at_trx: str,
    version: str,
):
    """Download the table as a parquet file"""
    verify_login_or_prompt(ctx)
    file = file or logical_prompt(ctx, "File in which the table will be stored")
    name = name or logical_prompt(ctx, "Name of the table")
    coll = (
        coll
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the table belongs")
    )
    click.echo(f"Downloading table to file '{file}'")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.download_table(
            coll,
            name,
            file,
            at=at,
            at_trx=at_trx,
            version=version,
        )

        click.echo("Table downloaded successfully")

    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to download table: {e}")


@table.command()
@click.option(
    "--coll",
    "-c",
    help="Name of the collection to which the tables belong.",
)
@click.option(
    "--name",
    "-n",
    help=(
        "A name wildcard to match for the list. "
        "For example, 'my_table*' will match all tables "
        "starting with 'my_table'."
    ),
)
@click.option(
    "--at",
    help=(
        "If provided, the table values at the given time will be shown. Must be  either"
        " a valid timestamp in the form of a unix timestamp (milliseconds since epoch"
        " without a dot, e.g. '1750074554472') or a valid date-time format. The valid"
        " formats are 'YYYY-MM-DD', 'YYYY-MM-DDTHH', 'YYYY-MM-DDTHH:MM',"
        " 'YYYY-MM-DDTHH:MM:SS', and 'YYYY-MM-DDTHH:MM:SS.sss'. A 'Z' character can be"
        " added at the end to indicate UTC time (e.g., '2023-10-01T12Z' or"
        " '2023-10-01T12:00:00.000Z') or it can be omitted to indicate local time"
        " (e.g., '2023-10-01T12' or '2023-10-01T12:00:00.000')."
    ),
    type=str,
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["at-trx"],
)
@click.option(
    "--at-trx",
    help=(
        "ID of a transaction. If provided, the table values at the end of the given "
        "transaction will be shown."
    ),
    type=str,
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["at"],
)
@click.pass_context
def list(ctx: click.Context, coll: str, name: str, at: str, at_trx: str):
    """List all tables in a collection"""
    verify_login_or_prompt(ctx)
    coll = (
        coll
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the tables belong")
    )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        request_filter = []
        if name:
            request_filter.append(f"name:lk:{name}")
        list_of_tables = server.list_tables(
            coll, filter=request_filter, at=at, at_trx=at_trx
        )

        cli_table = Table(title=f"Tables in collection '{coll}'")
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
        click.echo(f"Number of tables in collection '{coll}': {len(list_of_tables)}")
        click.echo()

    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list tables: {e}")


@table.command()
@click.option(
    "--name",
    help="Name of the table.",
)
@click.option(
    "--coll",
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
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["at-trx", "version"],
)
@click.option(
    "--at-trx",
    help=(
        "ID of a transaction. If provided, the table values at the end of the given "
        "transaction will be shown."
    ),
    type=str,
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["at", "version"],
)
@click.option(
    "--version",
    help=(
        "ID of a dataversion of the table. If provided, the table values at that "
        "dataversion will be shown."
    ),
    type=str,
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["at", "at-trx"],
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
    coll: str,
    name: str,
    len: int,
    offset: int,
    file: str,
    at: str,
    at_trx: str,
    version: str,
):
    """Sample rows from the table"""
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the table")
    coll = (
        coll
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the table belongs")
    )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        sampled_table = server.sample_table(
            coll,
            name,
            at=at,
            at_trx=at_trx,
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
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to sample table: {e}")


@table.command()
@click.option(
    "--name",
    help="Name of the table.",
)
@click.option(
    "--coll",
    "-c",
    help="Name of the collection to which the table belongs.",
)
@click.option(
    "--at",
    help=(
        "If provided, the table values at the given time will be shown. Must be "
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
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["at-trx", "version"],
)
@click.option(
    "--at-trx",
    help=(
        "ID of a transaction. If provided, the table values at the end of the given "
        "transaction will be shown."
    ),
    type=str,
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["at", "version"],
)
@click.option(
    "--version",
    help=(
        "ID of a dataversion of the table. If provided, the table values at that "
        "dataversion will be shown."
    ),
    type=str,
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["at", "at-trx"],
)
@click.pass_context
def schema(
    ctx: click.Context,
    coll: str,
    name: str,
    at: str,
    at_trx: str,
    version: str,
):
    """Show table schema"""
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the table")
    coll = (
        coll
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the table belongs")
    )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        table_schema = server.get_table_schema(
            coll,
            name,
            at=at,
            at_trx=at_trx,
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
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to obtain table schema: {e}")


@table.command()
@click.option(
    "--coll",
    "-c",
    help="Name of the collection to which the data belongs.",
)
@click.option(
    "--name",
    help="The name of the table to which the data belongs.",
)
@click.option(
    "--details",
    is_flag=True,
    help=(
        "If provided, the command will show detailed information about each data "
        "version, including the number of rows, number of columns (ignoring system "
        "columns) and hash of the schema."
    ),
)
@click.pass_context
def versions(ctx: click.Context, coll: str, name: str, details: bool):
    """
    List all versions of the data of a table.
    """
    verify_login_or_prompt(ctx)
    coll = (
        coll
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the data belongs")
    )
    name = name or logical_prompt(
        ctx, "The name of the table to which the data belongs"
    )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        data_version_list = server.list_dataversions(coll, name)

        table = Table(title=f"Data versions for table '{name}'")
        table.add_column("ID", style="cyan", no_wrap=True)
        table.add_column("Function ID")
        table.add_column("Plan ID")
        table.add_column("Created at")
        table.add_column("Status")
        if details:
            table.add_column("Rows")
            table.add_column("Columns")
            table.add_column("Schema Hash")

        for data_version in data_version_list:
            row_content = [
                data_version.id,
                data_version.function.id,
                data_version.execution.id,
                data_version.created_at_str,
                data_version.status,
            ]
            if details:
                row_content.extend(
                    [
                        (
                            str(data_version.row_count)
                            if data_version.row_count is not None
                            else "-"
                        ),
                        (
                            str(data_version.column_count)
                            if data_version.column_count is not None
                            else "-"
                        ),
                        data_version.schema_hash,
                    ]
                )
            table.add_row(*row_content)

        click.echo()
        console = Console()
        console.print(table)
        click.echo(f"Number of data versions: {len(data_version_list)}")
        if not details:
            show_hint(
                ctx,
                "Use the --details option to see more information about each data "
                "version, including the number of rows, the number of columns and the "
                "hash of the schema.",
            )

    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list data versions: {e}")
