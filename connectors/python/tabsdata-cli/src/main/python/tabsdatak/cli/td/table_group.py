#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import TYPE_CHECKING

import rich_click as click

from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    MutuallyExclusiveOption,
    ancestry,
    format_option,
    get_currently_pinned_object,
    hint_common_solutions,
    logical_prompt,
    max_result_option,
    output_format,
    present,
    proj_option,
    render,
    take,
    verify_login_or_prompt,
    view_options,
)
from tabsdatak.cli.td.tag_commands import tag_group
from tabsdatak.common._tabular import Column, Kind, TableView

if TYPE_CHECKING:
    import polars as pl

    from tabsdatak.common._tabular import TableView
    from tabsdatak.sdk.tabsdata_server import TabsdataServer


_TABLE_STATUS_LABELS = {"A": "Active", "F": "Frozen", "D": "Deleted"}

TABLE_DATA_MAX_WIDTH = 128
TABLE_DATA_PLACEHOLDER = "null"


def _size_mb(data_bytes) -> str | None:
    if data_bytes is None:
        return None
    return f"{int(data_bytes) / (1024 * 1024):.2f}"


NO_DATA_AT_VERSION = "The table holds no data at that version"


def render_no_data(ctx: click.Context) -> None:
    if output_format(ctx) == "table":
        click.echo(NO_DATA_AT_VERSION)
    else:
        render(ctx, data=None)


UNCOMMITTED_VERSION_STATES = {
    "Scheduled": "is still in progress",
    "Run Requested": "is still in progress",
    "Rescheduled": "is still in progress",
    "Running": "is still in progress",
    "Error": "is still in progress",
    "Done": "is done and awaiting commit",
    "Failed": "failed",
    "On Hold": "is on hold",
    "Canceled": "was canceled",
    "Yanked": "was yanked",
}


def warn_uncommitted_version(
    server: TabsdataServer, coll: str, name: str, version: str | None, has_data: bool
) -> None:
    if not version:
        return
    found = server.list_dataversions(coll, name, filter=f"id:eq:{version}")
    state = UNCOMMITTED_VERSION_STATES.get(found[0].status) if found else None
    if state is None:
        return
    served = (
        "showing data from an earlier committed version"
        if has_data
        else "no earlier committed version holds data"
    )
    click.echo(f"Warning: version {version} {state}; {served}", err=True)


def table_data_view(
    name: str,
    frame: pl.DataFrame,
    collection: str | None = None,
    project: str | None = None,
) -> TableView:
    from tabsdatak.common._tabular import Column, TableView

    context = [("Project", project)] if project else []
    if collection:
        context.append(("Collection", collection))
    context.append(("Table", name))
    rows = "row" if frame.height == 1 else "rows"
    columns = "column" if frame.width == 1 else "columns"
    context.append(("Size", f"{frame.height} {rows} × {frame.width} {columns}"))
    return TableView(
        title="Data",
        context=context,
        columns=[
            Column(column, detail=str(dtype)) for column, dtype in frame.schema.items()
        ],
        rows=[list(values) for values in frame.iter_rows()],
        rules=True,
        placeholder=TABLE_DATA_PLACEHOLDER,
        max_width=TABLE_DATA_MAX_WIDTH,
    )


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
        downloaded = server.download_table(
            coll,
            name,
            file,
            at=at,
            at_trx=at_trx,
            version=version,
        )
        warn_uncommitted_version(server, coll, name, version, downloaded is not None)

        if downloaded is None:
            click.echo(f"{NO_DATA_AT_VERSION}; no file was written")
        else:
            click.echo("Table downloaded successfully")

    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to download table: {e}")


@table.command(name="list")
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
@view_options()
@max_result_option
@click.pass_context
def list_tables(ctx: click.Context, coll: str, name: str, at: str, at_trx: str):
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

        def _view():
            from tabsdatak.sdk.tabsdata_server import _convert_timestamp_to_string

            rows = []
            for table in list_of_tables:
                data = table.kwargs
                defined_on = data.get("defined_on")
                status = data.get("status")
                rows.append([
                    table.name,
                    table.function.name,
                    present(_TABLE_STATUS_LABELS.get(status, status)),
                    "Yes" if data.get("private") else "No",
                    present(data.get("defined_by")),
                    _convert_timestamp_to_string(defined_on) if defined_on else None,
                ])
            return TableView(
                title="Tables",
                context=ancestry(ctx, collection=coll),
                columns=[
                    Column("Name", Kind.ID),
                    Column("Function"),
                    Column("Status", Kind.STATUS),
                    Column("Private", Kind.FLAG),
                    Column("Defined by"),
                    Column("Defined on", Kind.TIME),
                ],
                rows=rows,
                count="tables",
            )

        render(
            ctx,
            data=lambda: [t._data for t in list_of_tables],
            view=_view,
            truncated=truncated,
        )

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
@click.option(
    "--extra-cols",
    type=click.Choice(["none", "meta", "sys", "all"]),
    help=(
        "Which non-user columns to show on top of the user ones: 'meta' adds the "
        "'@td.' connector metadata, 'sys' adds the '$td.' system columns, 'all' adds "
        "both. Defaults to 'none'."
    ),
)
@view_options(browse=True)
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
    extra_cols: str,
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
            extra_cols=extra_cols,
        )
        warn_uncommitted_version(server, coll, name, version, table_data is not None)

        if table_data is None:
            render_no_data(ctx)
            return

        # table_data is a polars DataFrame -> its rows are the full info.
        render(
            ctx,
            data=lambda: table_data.to_dicts(),
            view=lambda: table_data_view(
                name, table_data, coll, server.connection.project
            ),
        )

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
@click.option(
    "--extra-cols",
    type=click.Choice(["none", "meta", "sys", "all"]),
    help=(
        "Which non-user columns to show on top of the user ones: 'meta' adds the "
        "'@td.' connector metadata, 'sys' adds the '$td.' system columns, 'all' adds "
        "both. Defaults to 'none'."
    ),
)
@format_option
@view_options()
@click.pass_context
def schema(
    ctx: click.Context,
    coll: str,
    name: str,
    at: str,
    at_trx: str,
    version: str,
    extra_cols: str,
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
            extra_cols=extra_cols,
        )
        warn_uncommitted_version(server, coll, name, version, table_schema is not None)

        if table_schema is None:
            render_no_data(ctx)
            return

        def _view():
            return TableView(
                title="Schema",
                context=ancestry(ctx, collection=coll, table=name),
                columns=[Column("Column", Kind.ID), Column("Type")],
                rows=[[column["name"], column["type"]] for column in table_schema],
                count="columns",
            )

        # table_schema is already the raw list of column dicts -> full info.
        render(ctx, data=lambda: table_schema, view=_view)

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
@view_options()
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

        def _view():
            rows = []
            for data_version in data_version_list:
                data = data_version.kwargs
                rows.append([
                    data_version.id,
                    present(data.get("function")),
                    present(data.get("function_version_id")),
                    present(data.get("execution_id")),
                    present(data.get("transaction_id")),
                    present(data_version.created_at_str),
                    "Yes" if data.get("schema_data_changed") else "No",
                    "Yes" if data.get("data_changed") else "No",
                    data.get("row_count"),
                    _size_mb(data.get("data_bytes")),
                    data_version.status,
                ])
            return TableView(
                title="Data versions",
                context=ancestry(ctx, collection=coll, table=name),
                columns=[
                    Column("Version", Kind.ID),
                    Column("Function"),
                    Column("Function version", Kind.ID),
                    Column("Execution plan ID", Kind.ID),
                    Column("Transaction ID", Kind.ID),
                    Column("Created on", Kind.TIME),
                    Column("Schema change", Kind.FLAG),
                    Column("Data changed", Kind.FLAG),
                    Column("Rows", Kind.NUMBER),
                    Column("Size", Kind.NUMBER, detail="MB"),
                    Column("Status", Kind.STATUS),
                ],
                rows=rows,
                count="data versions",
            )

        render(
            ctx,
            data=lambda: [dv._data for dv in data_version_list],
            view=_view,
            truncated=truncated,
        )

    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list data versions: {e}")


table.add_command(tag_group("table"))
