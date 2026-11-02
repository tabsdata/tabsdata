#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os
import re
from dataclasses import replace
from typing import TYPE_CHECKING

import rich_click as click

from tabsdatak.cli._capability import capability
from tabsdatak.cli._errors import ClassifiedException, TdkException, classify
from tabsdatak.cli._registry import cli_command
from tabsdatak.cli._space import OUT_OF_SPACE, room
from tabsdatak.cli.cli_utils import (
    DEFAULT_SAMPLE_LEN,
    MutuallyExclusiveOption,
    ancestry,
    command_failure,
    format_option,
    get_currently_pinned_object,
    hint_common_solutions,
    list_options,
    list_params,
    logical_prompt,
    max_result_option,
    output_format,
    present,
    progress,
    proj_option,
    render,
    render_deleted,
    render_done,
    render_list,
    require_id,
    split_csv,
    tabular,
    take,
    verify_login_or_prompt,
    view_options,
)
from tabsdatak.cli.td.execution_utils import TRX_ID_HINT
from tabsdatak.cli.td.project_group import TOPOLOGY_VERSION_HINT, topology_options
from tabsdatak.cli.td.tag_commands import tag_group
from tabsdatak.common._tabular import Column, Kind, RecordView, TableView
from tabsdatak.sdk.status_utils.groups import Entity, annotate

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


_UNKNOWN_RELATION = re.compile(r"relation '[^']*' was not found")


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


@capability("development", "destructive")
@table.command()
@proj_option
@format_option
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
    coll, name = _resolve_table(ctx, coll, name)
    progress(ctx, f"Deleting table '{name}' in collection '{coll}'")
    progress(ctx, "-" * 10)
    confirm = confirm or logical_prompt(
        ctx, "Please type 'delete' to confirm deletion", option="--confirm delete"
    )
    if confirm != "delete":
        raise click.ClickException(
            "Deletion not confirmed. The confirmation word is 'delete'."
        )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.delete_table(coll, name)
        render_deleted(
            ctx,
            kind="table",
            name=name,
            collection=coll,
            project=server.connection.project,
            message="Table deleted successfully",
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to delete table: {e}")


@capability("analytics", "read", "write", paths={"--file": "write"})
@table.command()
@proj_option
@click.option(
    "--name",
    "-n",
    help="Name of the table.",
)
@click.option(
    "--coll",
    "-c",
    help="Name of the collection to which the table belongs.",
)
@format_option
@click.option(
    "--file",
    help=(
        "File in which the table will be stored. Can be an absolute or a relative "
        "path. Any directory that appears must already exist (it will not be created "
        "as part of the download)."
    ),
    type=click.Path(),
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
    require_id("--at-trx", at_trx, TRX_ID_HINT)
    verify_login_or_prompt(ctx)
    file = file or logical_prompt(
        ctx, "File in which the table will be stored", option="--file"
    )
    coll, name = _resolve_table(ctx, coll, name)
    progress(ctx, f"Downloading table to file '{file}'")
    progress(ctx, "-" * 10)
    try:
        from tabsdatak.sdk.tabsdata_server import DownloadTooLarge

        server: TabsdataServer = ctx.obj["tabsdataserver"]
        space = room(os.path.dirname(os.path.abspath(file)))
        try:
            downloaded = server.download_table(
                coll,
                name,
                file,
                at=at,
                at_trx=at_trx,
                version=version,
                max_bytes=space.limit,
            )
        except (DownloadTooLarge, OSError) as error:
            if isinstance(error, OSError) and error.errno not in OUT_OF_SPACE:
                raise
            raise TdkException(
                "invalid_request",
                f"Table '{name}' does not fit in '{space.directory}': it is larger "
                f"than the {space.limit} bytes a file may take there.",
                hint="Free some space, or download to another directory.",
            ) from None
        warn_uncommitted_version(server, coll, name, version, downloaded is not None)
        render_done(
            ctx,
            action="download",
            kind="table",
            id=name,
            details={
                "collection": coll,
                "file": file if downloaded is not None else None,
            },
            message=(
                "Table downloaded successfully"
                if downloaded is not None
                else f"{NO_DATA_AT_VERSION}; no file was written"
            ),
        )
    except Exception as e:
        raise command_failure(ctx, "download table", e)


@capability("analytics", "read")
@table.command(name="list")
@proj_option
@click.option(
    "--coll",
    "-c",
    help=(
        "Name of the collection to which the tables belong. Default: the pinned "
        "collection, else every collection of the project."
    ),
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
        "If provided, the tables as they were at the given time will be listed. "
        "Must be either a valid timestamp in the form of a unix timestamp "
        "(milliseconds since epoch without a dot, e.g. '1750074554472') or a valid "
        "date-time format. The valid formats are 'YYYY-MM-DD', 'YYYY-MM-DDTHH', "
        "'YYYY-MM-DDTHH:MM', 'YYYY-MM-DDTHH:MM:SS', and 'YYYY-MM-DDTHH:MM:SS.sss'. "
        "A 'Z' character can be added at the end to indicate UTC time (e.g., "
        "'2023-10-01T12Z' or '2023-10-01T12:00:00.000Z') or it can be omitted to "
        "indicate local time (e.g., '2023-10-01T12' or '2023-10-01T12:00:00.000')."
    ),
    type=str,
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["at_trx"],
)
@click.option(
    "--at-trx",
    help=(
        "ID of a transaction. If provided, the tables as they were at the end of "
        "the given transaction will be listed."
    ),
    type=str,
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["at"],
)
@format_option
@view_options()
@max_result_option
@list_options
@click.pass_context
def list_tables(
    ctx: click.Context,
    coll: str,
    name: str,
    at: str,
    at_trx: str,
):
    """List the tables of a collection, or of the whole project"""
    require_id("--at-trx", at_trx, TRX_ID_HINT)
    verify_login_or_prompt(ctx)
    coll = coll or get_currently_pinned_object(ctx, "collection")
    if coll is None and (at or at_trx):
        raise click.UsageError(
            f"'{'--at' if at else '--at-trx'}' lists one collection; pass '--coll'."
        )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        request_filter = []
        if name:
            request_filter.append(f"name:lk:{name}")
        params = list_params(ctx, request_filter)
        if coll is None:
            listing = server.list_project_tables_generator(**params)
        else:
            listing = server.list_tables_generator(coll, at=at, at_trx=at_trx, **params)
        list_of_tables, truncated = take(ctx, listing)

        def _view():
            from tabsdatak.sdk.tabsdata_server import _convert_timestamp_to_string

            columns = [Column("Collection", Kind.ID)] if coll is None else []
            columns += [
                Column("Name", Kind.ID),
                Column("Function"),
                Column("Status", Kind.STATUS),
                Column("Private", Kind.FLAG),
                Column("Defined by"),
                Column("Defined on", Kind.TIME),
            ]
            rows = []
            for table in list_of_tables:
                data = table.kwargs
                defined_on = data.get("defined_on")
                status = data.get("status")
                row = [present(data.get("collection"))] if coll is None else []
                row += [
                    table.name,
                    table.function.name,
                    present(_TABLE_STATUS_LABELS.get(status, status)),
                    "Yes" if data.get("private") else "No",
                    present(data.get("defined_by")),
                    _convert_timestamp_to_string(defined_on) if defined_on else None,
                ]
                rows.append(row)
            return TableView(
                title="Tables",
                context=ancestry(ctx, collection=coll),
                columns=columns,
                rows=rows,
                count="tables",
            )

        render_list(
            ctx,
            items=lambda: [t._data for t in list_of_tables],
            truncated=truncated,
            view=_view,
        )

    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list tables: {e}")


@capability("analytics", "read", paths={"--file": "write"})
@table.command()
@proj_option
@click.option(
    "--name",
    "-n",
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
        "other table, a join or a file-reading function is rejected. '--offset' "
        "and '--len' page the query's result."
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
    type=click.Path(),
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
    require_id("--at-trx", at_trx, TRX_ID_HINT)
    verify_login_or_prompt(ctx)
    coll, name = _resolve_table(ctx, coll, name)
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

        if file:
            # Diagnostics to stderr so json/yaml stdout stays parseable.
            click.echo(f"Saving data to file '{file}'", err=True)
            table_data.write_ndjson(file)
            click.echo("Data saved successfully", err=True)

        render(
            ctx,
            data=lambda: tabular(table_data, len or DEFAULT_SAMPLE_LEN),
            view=lambda: table_data_view(
                name, table_data, coll, server.connection.project
            ),
        )

    except Exception as e:
        hint_common_solutions(ctx, e)
        message = f"Failed to obtain table data: {e}"
        if sql and _UNKNOWN_RELATION.search(str(e)):
            raise ClassifiedException(
                replace(
                    classify(e),
                    message=message,
                    hint=(
                        "In '--sql' the table is the only relation, named after "
                        f"itself: SELECT ... FROM {name}"
                    ),
                )
            ) from e
        raise click.ClickException(message)


@capability("analytics", "read")
@table.command()
@proj_option
@click.option(
    "--name",
    "-n",
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
    require_id("--at-trx", at_trx, TRX_ID_HINT)
    verify_login_or_prompt(ctx)
    coll, name = _resolve_table(ctx, coll, name)
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


@capability("analytics", "read")
@table.command()
@proj_option
@click.option(
    "--coll",
    "-c",
    help="Name of the collection to which the data belongs.",
)
@click.option(
    "--name",
    "-n",
    help="The name of the table to which the data belongs.",
)
@format_option
@view_options()
@max_result_option
@list_options
@click.pass_context
def versions(ctx: click.Context, coll: str, name: str):
    """
    List all versions of the data of a table.
    """
    verify_login_or_prompt(ctx)
    coll, name = _resolve_table(ctx, coll, name)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        # /data-versions returns all statuses; the CLI lists committed only.
        data_version_list, truncated = take(
            ctx,
            server.list_dataversions_generator(
                coll, name, **list_params(ctx, "status:eq:C")
            ),
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

        render_list(
            ctx,
            items=lambda: [
                annotate(Entity.DATA_VERSION, dv._data) for dv in data_version_list
            ],
            truncated=truncated,
            view=_view,
        )

    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list data versions: {e}")


_AT_HELP = (
    "Read the table as it was at this time: a unix timestamp in milliseconds "
    "(e.g. '1750074554472') or 'YYYY-MM-DD', 'YYYY-MM-DDTHH', 'YYYY-MM-DDTHH:MM', "
    "'YYYY-MM-DDTHH:MM:SS' or 'YYYY-MM-DDTHH:MM:SS.sss', in UTC with a trailing 'Z' "
    "and in local time without it."
)
_AT_TRX_HELP = "Read the table as it was at the end of this transaction."
_VERSION_HELP = (
    "Read this data version of the table: its ID, or HEAD, HEAD^ or HEAD~N counted "
    "back over the committed versions."
)


def _pin_options(fn):
    for option in (
        click.option(
            "--version",
            help=_VERSION_HELP,
            cls=MutuallyExclusiveOption,
            mutually_exclusive=["at", "at_trx"],
        ),
        click.option(
            "--at-trx",
            help=_AT_TRX_HELP,
            cls=MutuallyExclusiveOption,
            mutually_exclusive=["at", "version"],
        ),
        click.option(
            "--at",
            help=_AT_HELP,
            cls=MutuallyExclusiveOption,
            mutually_exclusive=["at_trx", "version"],
        ),
    ):
        fn = option(fn)
    return fn


def _table_options(fn):
    for option in (
        click.option(
            "--coll",
            "-c",
            help="Name of the collection to which the table belongs.",
        ),
        click.option("--name", "-n", help="Name of the table."),
    ):
        fn = option(fn)
    return fn


def _resolve_table(ctx: click.Context, coll: str | None, name: str | None):
    name = name or logical_prompt(ctx, "Name of the table", option="--name")
    coll = (
        coll
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(
            ctx,
            "Name of the collection to which the table belongs",
            option="--coll",
            hint=(
                "Pass --coll. 'tdk table list --filter name:eq:<name>' shows the "
                "table's collection."
            ),
        )
    )
    return coll, name


def _sdk_table(ctx: click.Context, coll: str, name: str):
    from tabsdatak.sdk.tabsdata_server import Table as SdkTable

    server: TabsdataServer = ctx.obj["tabsdataserver"]
    return SdkTable(server.connection, coll, name)


def _timestamp(ms) -> str:
    from tabsdatak.sdk.tabsdata_server import _convert_timestamp_to_string

    return _convert_timestamp_to_string(ms) if ms else "-"


def _size_cell(value) -> str:
    return "-" if value is None else str(value)


@capability("analytics", "read")
@table.command()
@proj_option
@_table_options
@_pin_options
@format_option
@view_options()
@click.pass_context
def stats(
    ctx: click.Context,
    coll: str,
    name: str,
    at: str,
    at_trx: str,
    version: str,
):
    """Show the size and schema of a table"""
    require_id("--at-trx", at_trx, TRX_ID_HINT)
    verify_login_or_prompt(ctx)
    coll, name = _resolve_table(ctx, coll, name)
    try:
        document = _sdk_table(ctx, coll, name).stats(
            version=version, at=at, at_trx=at_trx
        )

        def _view():
            pinned = document["version"] or {}
            context = ancestry(ctx, collection=coll, table=name)
            return [
                RecordView(
                    title="Table",
                    context=context,
                    fields=[
                        ("Version", present(pinned.get("id"))),
                        ("Committed on", _timestamp(pinned.get("ended_on"))),
                        ("Function", present(pinned.get("function"))),
                        ("Rows", document["row_count"]),
                        ("Columns", document["column_count"]),
                        ("Size (MB)", _size_mb(document["data_bytes"])),
                    ],
                ),
                TableView(
                    title="Schema",
                    context=context,
                    columns=[Column("Column", Kind.ID), Column("Type")],
                    rows=[
                        [column["name"], column["type"]]
                        for column in document["schema"]
                    ],
                    count="columns",
                ),
            ]

        render(ctx, data=document, view=_view)
    except Exception as e:
        raise command_failure(ctx, "obtain table stats", e)


@capability("analytics", "read")
@table.command()
@proj_option
@_table_options
@_pin_options
@click.option(
    "--columns",
    help="Comma-separated columns to profile. Defaults to every column.",
)
@click.option(
    "--approx",
    is_flag=True,
    default=False,
    help=(
        "Profile random windows of the table instead of scanning all of it; the "
        "result is approximate unless the sample holds every row."
    ),
)
@click.option(
    "--sample-rows",
    type=click.IntRange(min=1, max=10000),
    default=1000,
    show_default=True,
    help="Rows '--approx' samples in all, in windows of at most 1000.",
)
@format_option
@view_options()
@click.pass_context
def profile(
    ctx: click.Context,
    coll: str,
    name: str,
    at: str,
    at_trx: str,
    version: str,
    columns: str,
    approx: bool,
    sample_rows: int,
):
    """Show per-column null rates, distinct counts and ranges of a table.

    Exact by default: the server aggregates the whole data version. When the
    version is too large or the query times out, or with '--approx', the stats
    come from random windows and 'approximate' says so"""
    require_id("--at-trx", at_trx, TRX_ID_HINT)
    verify_login_or_prompt(ctx)
    coll, name = _resolve_table(ctx, coll, name)
    selected = split_csv(columns) if columns else None
    from tabsdatak.sdk._tables import UnknownColumn

    try:
        try:
            document = _sdk_table(ctx, coll, name).profile(
                columns=selected,
                version=version,
                at=at,
                at_trx=at_trx,
                approx=approx,
                sample_rows=sample_rows,
            )
        except UnknownColumn as error:
            raise TdkException("usage", str(error)) from None

        def _view():
            notes = [f"Rows: {_size_cell(document['row_count'])}"]
            if document["approximate"]:
                reason = document["reason"]
                notes.append(
                    f"Approximate: from a sample of {document['sample_size']} rows"
                    + (f" ({reason})" if reason else "")
                )
            rows = []
            for column in document["columns"]:
                rate = column.get("null_rate")
                rows.append([
                    column["name"],
                    column["type"],
                    column.get("non_null"),
                    None if rate is None else f"{rate:.2%}",
                    *(column.get(stat) for stat in ("distinct", "min", "max", "mean")),
                ])
            return TableView(
                title="Profile",
                context=ancestry(ctx, collection=coll, table=name),
                columns=[
                    Column("Column", Kind.ID),
                    Column("Type"),
                    Column("Non-null", Kind.NUMBER),
                    Column("Null rate", Kind.NUMBER),
                    Column("Distinct", Kind.NUMBER),
                    Column("Min"),
                    Column("Max"),
                    Column("Mean"),
                ],
                rows=rows,
                notes=notes,
            )

        render(ctx, data=document, view=_view)
    except Exception as e:
        raise command_failure(ctx, "profile table", e)


@capability("analytics", "read")
@table.command()
@proj_option
@_table_options
@topology_options
@click.option(
    "--direction",
    type=click.Choice(["up", "down", "both"]),
    default="both",
    show_default=True,
    help="Follow what feeds the table (up), what it feeds (down) or both.",
)
@click.option(
    "--depth",
    type=click.IntRange(min=1),
    help=(
        "Go at most this many edges away (a table two steps away, through the "
        "function between them, is at depth 2). Unlimited by default."
    ),
)
@click.option(
    "--include-system",
    is_flag=True,
    default=False,
    help="Keep the system tables in the graph.",
)
@format_option
@view_options()
@click.pass_context
def flow(
    ctx: click.Context,
    coll: str,
    name: str,
    topology_version: str,
    at: str,
    direction: str,
    depth: int,
    include_system: bool,
):
    """Show the functions and tables upstream and downstream of a table"""
    require_id("--topology-version", topology_version, TOPOLOGY_VERSION_HINT)
    verify_login_or_prompt(ctx)
    coll, name = _resolve_table(ctx, coll, name)
    try:
        document = _sdk_table(ctx, coll, name).flow(
            direction=direction,
            depth=depth,
            at=at,
            topology_version=topology_version,
            include_system=include_system,
        )

        def _view():
            nodes = {node["id"]: node for node in document["nodes"]}

            def label(node_id: str) -> str:
                node = nodes[node_id]
                return f"{node['kind']} {node['collection']}/{node['name']}"

            rows = []
            for edge in document["edges"]:
                kind = edge["kind"]
                if edge.get("trigger") and kind == "dependency":
                    kind = "dependency, trigger"
                rows.append([label(edge["from"]), label(edge["to"]), kind])
            notes = []
            for heading in ("upstream", "downstream"):
                found = ", ".join(
                    f"{t['collection']}/{t['table']} ({t['distance']})"
                    for t in document[heading]
                )
                notes.append(f"{heading.capitalize()} tables: {found or '-'}")
            return TableView(
                title="Flow",
                context=ancestry(ctx, collection=coll, table=name),
                columns=[Column("From"), Column("To"), Column("Kind")],
                rows=rows,
                notes=notes,
            )

        render(ctx, data=document, view=_view)
    except Exception as e:
        raise command_failure(ctx, "obtain table flow", e)


def _columns_cell(columns) -> str:
    return ", ".join(f"{c['name']} ({c['type']})" for c in columns) or "-"


def _retyped_cell(columns) -> str:
    return ", ".join(f"{c['name']} ({c['from']} -> {c['to']})" for c in columns) or "-"


@capability("analytics", "read")
@table.command()
@proj_option
@_table_options
@click.option(
    "--from",
    "from_version",
    help=(
        "Compare the schema at this data version (ID, HEAD, HEAD^ or HEAD~N) "
        "with the one at '--to' instead of listing the schema history."
    ),
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["against"],
)
@click.option(
    "--to",
    "to_version",
    help="The data version '--from' compares with. Defaults to the newest.",
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["against"],
)
@click.option(
    "--against",
    help=(
        "Compare the table's schema with that of another table, as "
        "COLLECTION/TABLE or, in the same collection, TABLE."
    ),
)
@click.option(
    "--version",
    help=(
        "With '--against', the data version of this table to compare. Defaults "
        "to the newest."
    ),
)
@click.option(
    "--max-events",
    type=click.IntRange(min=1),
    default=10,
    show_default=True,
    help="Most schema changes the history lists, newest first.",
)
@format_option
@view_options()
@click.pass_context
def diff(
    ctx: click.Context,
    coll: str,
    name: str,
    from_version: str,
    to_version: str,
    against: str,
    version: str,
    max_events: int,
):
    """Show how a table's schema changed, or how it differs from another's.

    With no '--from' or '--against', lists the schema changes over the
    committed data versions, newest first"""
    verify_login_or_prompt(ctx)
    if to_version and not from_version:
        raise click.UsageError("'--to' needs '--from'.")
    if version and not against:
        raise click.UsageError("'--version' needs '--against'.")
    coll, name = _resolve_table(ctx, coll, name)
    try:
        subject = _sdk_table(ctx, coll, name)
        if against:
            other_coll, _, other_name = against.rpartition("/")
            other = _sdk_table(ctx, other_coll or coll, other_name)
            document = subject.schema_diff(other, from_version=version)
        elif from_version:
            document = subject.schema_diff(
                from_version=from_version, to_version=to_version
            )
        else:
            document = subject.schema_history(max_events=max_events)

        def _view():
            context = ancestry(ctx, collection=coll, table=name)
            if "events" in document:
                notes = []
                if document["capped"]:
                    notes.append(
                        f"Showing the newest {max_events} changes; raise "
                        "'--max-events' for older ones."
                    )
                return TableView(
                    title="Schema changes",
                    context=context,
                    columns=[
                        Column("Version", Kind.ID),
                        Column("Committed on", Kind.TIME),
                        Column("Function"),
                        Column("Added"),
                        Column("Removed"),
                        Column("Retyped"),
                    ],
                    rows=[
                        [
                            event["version_id"],
                            _timestamp(event["ended_on"]),
                            present(event["function"]),
                            _columns_cell(event["added"]),
                            _columns_cell(event["removed"]),
                            _retyped_cell(event["retyped"]),
                        ]
                        for event in document["events"]
                    ],
                    notes=notes,
                )
            old, new = document["from"], document["to"]
            return RecordView(
                title="Schema diff",
                context=context,
                fields=[
                    (
                        "From",
                        f"{old['collection']}/{old['table']}@{old['version_id']}",
                    ),
                    ("To", f"{new['collection']}/{new['table']}@{new['version_id']}"),
                    ("Added", _columns_cell(document["added"])),
                    ("Removed", _columns_cell(document["removed"])),
                    ("Retyped", _retyped_cell(document["retyped"])),
                ],
            )

        render(ctx, data=document, view=_view)
    except Exception as e:
        raise command_failure(ctx, "diff table schema", e)


table.add_command(tag_group("table"))
