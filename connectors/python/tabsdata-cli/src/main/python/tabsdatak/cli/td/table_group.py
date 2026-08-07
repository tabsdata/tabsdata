#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import TYPE_CHECKING

import rich_click as click
from rich.table import Table

from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    MutuallyExclusiveOption,
    beautify_time,
    echo_more_results,
    format_option,
    get_currently_pinned_object,
    hint_common_solutions,
    logical_prompt,
    make_console,
    max_result_option,
    proj_option,
    render,
    take,
    verify_login_or_prompt,
)

if TYPE_CHECKING:
    from tabsdatak.sdk.tabsdata_server import TabsdataServer


_TABLE_STATUS_LABELS = {"A": "Active", "F": "Frozen", "D": "Deleted"}


def _size_mb(data_bytes) -> str:
    if data_bytes is None:
        return "-"
    return f"{int(data_bytes) / (1024 * 1024):.2f}"


@cli_command
@click.group()
def table():
    """Table management commands"""


@table.command()
@proj_option
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
@proj_option
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
@proj_option
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
@format_option
@max_result_option
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
        list_of_tables, truncated = take(
            ctx,
            server.list_tables_generator(
                coll, filter=request_filter, at=at, at_trx=at_trx
            ),
        )

        def _table():
            from tabsdatak.sdk.tabsdata_server import _convert_timestamp_to_string

            cli_table = Table(title="Tables")
            cli_table.add_column("Name", style="cyan", no_wrap=True)
            cli_table.add_column("Function")
            cli_table.add_column("Status")
            cli_table.add_column("Private")
            cli_table.add_column("Created by")
            cli_table.add_column("Created on")
            for table in list_of_tables:
                data = table.kwargs
                defined_on = data.get("defined_on")
                cli_table.add_row(
                    table.name,
                    table.function.name,
                    _TABLE_STATUS_LABELS.get(
                        data.get("status"), data.get("status") or "-"
                    ),
                    "Yes" if data.get("private") else "No",
                    data.get("defined_by") or "-",
                    _convert_timestamp_to_string(defined_on) if defined_on else "-",
                )
            click.echo()
            make_console().print(cli_table)
            click.echo(
                f"Number of tables in collection '{coll}': {len(list_of_tables)}"
            )
            click.echo()

        render(
            ctx,
            data=lambda: [t._data for t in list_of_tables],
            table=_table,
        )
        echo_more_results(ctx, truncated)

    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list tables: {e}")


@table.command()
@proj_option
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
    help=(
        "Maximum amount of rows to be shown. If not provided, the server default "
        "of 100 rows applies. Cannot be greater than 1000."
    ),
)
@click.option(
    "--offset",
    type=int,
    help=(
        "How many rows to skip before starting to read. If not provided, "
        "reading will start at the first row."
    ),
)
@click.option(
    "--sql",
    help=(
        "A SQL query to run on the table server-side, before the rows are returned. "
        "The table is the only queryable relation and is referenced by its own name "
        "(for example, 'SELECT a, b FROM my_table WHERE a > 10'); referencing any "
        "other table or a file-reading function is rejected. '--offset' is applied "
        "before the query and '--len' caps its result."
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
@format_option
@click.pass_context
def data(
    ctx: click.Context,
    coll: str,
    name: str,
    len: int,
    offset: int,
    sql: str,
    file: str,
    at: str,
    at_trx: str,
    version: str,
):
    """Show rows of data from the table"""
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the table")
    coll = (
        coll
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the table belongs")
    )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        table_data = server.sample_table(
            coll,
            name,
            at=at,
            at_trx=at_trx,
            version=version,
            len=len,
            offset=offset,
            sql=sql,
        )

        def _table():
            click.echo()
            click.echo(f"Data of table '{name}'")
            click.echo(table_data)
            click.echo()

        # table_data is a polars DataFrame -> its rows are the full info.
        render(ctx, data=lambda: table_data.to_dicts(), table=_table)

        if file:
            # Diagnostics to stderr so json/yaml stdout stays parseable.
            click.echo(f"Saving data to file '{file}'", err=True)
            table_data.write_ndjson(file)
            click.echo("Data saved successfully", err=True)

    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to obtain table data: {e}")


@table.command()
@proj_option
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
@format_option
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

        def _table():
            table = Table(title="Schema")
            table.add_column("Column", style="cyan", no_wrap=True)
            table.add_column("Type")
            for column in table_schema:
                table.add_row(column["name"], column["type"])
            click.echo()
            make_console().print(table)
            click.echo()

        # table_schema is already the raw list of column dicts -> full info.
        render(ctx, data=lambda: table_schema, table=_table)

    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to obtain table schema: {e}")


@table.command()
@proj_option
@click.option(
    "--coll",
    "-c",
    help="Name of the collection to which the data belongs.",
)
@click.option(
    "--name",
    help="The name of the table to which the data belongs.",
)
@format_option
@max_result_option
@click.pass_context
def versions(ctx: click.Context, coll: str, name: str):
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
        # /data-versions returns all statuses; the CLI lists committed only.
        data_version_list, truncated = take(
            ctx,
            server.list_dataversions_generator(coll, name, filter="status:eq:C"),
        )

        def _table():
            table = Table(title="Data versions")
            table.add_column("Version", style="cyan", no_wrap=True)
            table.add_column("Function")
            table.add_column("Function version", no_wrap=True)
            table.add_column("Execution plan ID", no_wrap=True)
            table.add_column("Transaction ID", no_wrap=True)
            table.add_column("Created on", no_wrap=True)
            table.add_column("Schema change")
            table.add_column("Data changed")
            table.add_column("Rows")
            table.add_column("Size (MB)")
            table.add_column("Status")
            for data_version in data_version_list:
                data = data_version.kwargs
                row_count = data.get("row_count")
                table.add_row(
                    data_version.id,
                    data.get("function") or "-",
                    data.get("function_version_id") or "-",
                    data.get("execution_id") or "-",
                    data.get("transaction_id") or "-",
                    beautify_time(data_version.created_at_str),
                    "Yes" if data.get("schema_data_changed") else "No",
                    "Yes" if data.get("data_changed") else "No",
                    str(row_count) if row_count is not None else "-",
                    _size_mb(data.get("data_bytes")),
                    data_version.status,
                )
            click.echo()
            make_console().print(table)
            click.echo(f"Number of data versions: {len(data_version_list)}")

        render(
            ctx,
            data=lambda: [dv._data for dv in data_version_list],
            table=_table,
        )
        echo_more_results(ctx, truncated)

    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list data versions: {e}")
