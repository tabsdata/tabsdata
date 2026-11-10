#
# Copyright 2026 Tabsdata Inc.
#

"""``tdk connection`` -- connectors, connection documents and the connection
catalog.

``types``, ``template``, ``validate`` and ``check --conn-file`` are offline:
they introspect the connectors registered through the SPI registry
(``avail_srcs`` / ``avail_stream_srcs`` / ``avail_dests``) and speak the
connectors' own envelope dialect (``{kind, apiVersion, type, spec}``). The
other commands read the server's catalog of a source/destination collection's
connection and run probes (``check``, ``metadata``, ``sample``) against it as
server-side jobs.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import rich_click as click

from tabsdatak.cli._capability import capability
from tabsdatak.cli._command import TdkGroup
from tabsdatak.cli._errors import TdkException
from tabsdatak.cli._prompted import pinned, prompted
from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    IdType,
    all_projects_option,
    ancestry,
    command_failure,
    format_option,
    get_currently_pinned_object,
    list_options,
    list_params,
    logical_prompt,
    max_result_option,
    output_format,
    paged_options,
    present,
    progress,
    proj_option,
    record_fields_option,
    render,
    render_list,
    resolve_all_projects,
    serialize,
    take,
    verify_login_or_prompt,
    view_options,
)
from tabsdatak.cli.td._wait import (
    require_wait,
    timeout_option,
    wait_connection_probe,
    wait_option,
)
from tabsdatak.common._tabular import Column, Kind, RecordView, TableView

if TYPE_CHECKING:
    from tabsdatak.sdk.tabsdata_server import TabsdataServer


@cli_command
@click.group(cls=TdkGroup)
def connection():
    """Connection commands"""


_TYPE_MODES = {"in": "Source", "out": "Destination", "cdc": "Stream Source"}


@capability("knowledge", "read")
@connection.command(login=False)
@click.option(
    "--system",
    help=(
        "Only the types of systems whose name contains this text "
        "(case-insensitive), e.g. 'mysql' or 'storage'."
    ),
)
@click.option(
    "--mode",
    type=click.Choice(list(_TYPE_MODES), case_sensitive=False),
    help="Only sources ('in'), destinations ('out') or stream sources ('cdc').",
)
@format_option
@view_options()
@max_result_option
@paged_options
@click.pass_context
def types(ctx, system: str | None, mode: str | None):
    """List the connection types available from installed connectors"""
    # noinspection PyProtectedMember
    from tabsdatak.cli import template

    wanted = None if mode is None else _TYPE_MODES[mode.lower()]
    rows = [
        row
        for row in template.list_connectors()
        if (wanted is None or row.mode == wanted)
        and (system is None or system.lower() in row.system.lower())
    ]
    rows, truncated = take(ctx, rows)

    def _view():
        return TableView(
            title="Available connectors",
            columns=[Column("Type", Kind.ID), Column("Mode"), Column("System")],
            rows=[[row.type, row.mode, row.system] for row in rows],
            count="connectors",
        )

    render_list(
        ctx,
        items=[{"type": r.type, "mode": r.mode, "system": r.system} for r in rows],
        truncated=truncated,
        view=_view,
    )


def _type_option(fn):
    return prompted(
        "--type",
        "-t",
        "type_",
        prompt="Connector type (see 'tdk connection types')",
        help="Connector type, e.g. 'mysql-sql-in', 's3-file-out'. "
        "See 'tdk connection types'.",
    )(fn)


def _resolve(ctx, type_):
    # noinspection PyProtectedMember
    from tabsdatak.cli import template

    try:
        # Types are globally unique, so the direction is always inferred.
        return template.resolve_conn(type_, None)
    except template.TemplateError as e:
        raise click.ClickException(str(e))


def _emit(ctx, type_, document, file):
    # noinspection PyProtectedMember
    from tabsdatak.cli import template

    text = template.to_yaml(document)
    written = file if file and file != "-" else None
    if written:
        Path(written).write_text(text)
    if output_format(ctx) == "table":
        if written:
            click.echo(f"Wrote connection document to {written}")
        else:
            click.echo(text, nl=False)
    render(ctx, data={"type": type_, "file": written, "template": text})


# Named "template" explicitly: the function can't be called ``template`` without
# shadowing the imported engine module.
@capability("knowledge", "read", paths={"--file": "write"})
@connection.command("template", login=False)
@_type_option
# JSON can't carry the template's "# accepts:" comments: ``-o json`` wraps the
# YAML text as ``{"type", "file", "template"}``.
@format_option
@click.option(
    "--file",
    "-f",
    help="Write the template to this file. '-' (or omitting it) writes to stdout.",
    type=click.Path(allow_dash=True),
)
@click.pass_context
def template_(ctx, type_, file):
    """Print a fill-in connection template for a connector type"""
    # noinspection PyProtectedMember
    from tabsdatak.cli import template

    info = _resolve(ctx, type_)
    # Show every config: required fields as fill-ins, optional ones with their
    # default (default-None fields commented out so the document stays valid).
    _emit(ctx, info.type, template.build_template(info.conn_cls, all_fields=True), file)


@capability("development", "read", "read", paths={"--file": "read"})
@connection.command(login=False)
@format_option
@click.option(
    "--file",
    "-f",
    "file",
    required=True,
    help="Connection YAML file to validate, or '-' to read from stdin.",
    type=click.Path(allow_dash=True),
)
@click.pass_context
def validate(ctx, file):
    """Validate a connection YAML document against its connector schema"""
    # noinspection PyProtectedMember
    from tabsdatak.cli import template
    from tabsdatak.cli.td.collection_group import _read_connection_document

    document, env_vars = _read_connection_document(ctx, file)
    try:
        template.validate(document)
    except template.TemplateError as e:
        raise click.ClickException(str(e)) from None
    if output_format(ctx) == "table":
        click.echo("Connection document is valid")
    render(
        ctx,
        data={
            "valid": True,
            "type": document.get("type") if isinstance(document, dict) else None,
            "secret_env_vars": env_vars,
        },
    )


_MODES = {"src": "S", "dest": "D"}
_MODE_OF = {code: mode for mode, code in _MODES.items()}
_CHECK_STATUSES = ("ok", "error", "unknown", "timeout", "unsupported")
PROBE_TIMEOUT_SECONDS = 60.0
MAX_PROBE_TIMEOUT_SECONDS = 300.0
DEFAULT_SAMPLE_ROWS = 20
MAX_SAMPLE_ROWS = 100
LOCAL_ENTITIES_SHOWN = 20


_COLLECTION_HELP = (
    "Name of the source/destination collection whose connection to use. "
    "Default: the pinned collection."
)


def _collection_option(fn):
    return prompted(
        "--name",
        "-n",
        prompt="Name of the source/destination collection",
        default=pinned("collection"),
        help=_COLLECTION_HELP,
    )(fn)


def _name_option(fn):
    return click.option(
        "--name",
        "-n",
        help=_COLLECTION_HELP,
    )(fn)


def type_filter_option(fn):
    return click.option(
        "--type",
        "-t",
        "type_",
        help="Only connections of this connector type (see 'tdk connection types').",
    )(fn)


def mode_option(fn):
    return click.option(
        "--mode",
        type=click.Choice(tuple(_MODES)),
        help="Only source (src) or destination (dest) connections.",
    )(fn)


def _wait_options(default: bool):
    def decorate(fn):
        fn = timeout_option(PROBE_TIMEOUT_SECONDS, MAX_PROBE_TIMEOUT_SECONDS)(fn)
        return wait_option("the probe", "the probe", default)(fn)

    return decorate


_PROBE_ID_HINT = (
    "'tdk connection check|refresh|sample --name NAME' starts a probe and prints "
    "its id; the collection NAME goes to those commands, not to '--probe'."
)
_PROBE_ID = IdType(_PROBE_ID_HINT)


def _collection_name(ctx: click.Context, name: str | None) -> str:
    return (
        name
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(
            ctx, "Name of the source/destination collection", option="--name"
        )
    )


def connector_kind(type_: str) -> str:
    """The Conn class (``module:qualname``) a connector type binds to: the
    server keys connections by it. A class given as such passes through."""
    # noinspection PyProtectedMember
    from tabsdatak.cli import template

    if ":" in type_:
        return type_
    try:
        conn_cls = template.resolve_conn(type_, None).conn_cls
    except template.TemplateError as e:
        raise click.BadParameter(str(e), param_hint="'--type'")
    return f"{conn_cls.__module__}:{conn_cls.__qualname__}"


def connector_types(kind: str) -> str:
    """The connector types installed here that bind to the Conn class ``kind``
    (``module:qualname``), as ``tdk connection list`` shows them; the class
    itself when none does."""
    # noinspection PyProtectedMember
    from tabsdatak.cli import template

    types = [
        info.type
        for info in template.list_connectors()
        if f"{info.conn_cls.__module__}:{info.conn_cls.__qualname__}" == kind
    ]
    return ", ".join(dict.fromkeys(types)) or kind


def _types(row: dict) -> str:
    return ", ".join((row.get("compatible") or {}).get("types") or []) or str(
        row.get("kind") or "-"
    )


def _fields_view(title: str, rows: list[tuple[str, object]]) -> RecordView:
    fields = []
    for label, value in rows:
        if isinstance(value, (dict, list)):
            value = serialize(value, "yaml").rstrip("\n")
        fields.append((label, present(value)))
    return RecordView(title=title, fields=fields)


def _probe_rows(probe: dict) -> list[tuple[str, object]]:
    return [
        ("Probe ID", probe.get("probe_id")),
        ("Collection", probe.get("collection")),
        ("Kind", probe.get("kind")),
        ("Status", probe.get("status")),
        ("Requested on", probe.get("requested_on")),
        ("Ended on", probe.get("ended_on")),
        ("Result", probe.get("result")),
        ("Error", (probe.get("error") or {}).get("message")),
    ]


def _render_probe(ctx: click.Context, probe: dict) -> None:
    render(
        ctx,
        data=probe,
        view=lambda: _fields_view("Connection probe", _probe_rows(probe)),
    )


def _run_probe(
    ctx: click.Context,
    kind: str,
    name: str | None,
    wait: bool,
    timeout: float | None,
    **params,
) -> dict:
    server: TabsdataServer = ctx.obj["tabsdataserver"]
    probe = server.submit_connection_probe(name, kind, **params)
    progress(ctx, f"The {kind} probe of '{name}' is {probe.get('status')}.")
    if not wait:
        return probe
    return wait_connection_probe(
        ctx, server, probe, PROBE_TIMEOUT_SECONDS if timeout is None else timeout
    )


@capability("analytics", "read")
@connection.command("list")
@proj_option
@all_projects_option
@type_filter_option
@mode_option
@click.option(
    "--status",
    type=click.Choice(_CHECK_STATUSES),
    metavar="STATUS",
    help=f"Only connections whose last check ended so: {', '.join(_CHECK_STATUSES)}.",
)
@format_option
@view_options()
@max_result_option
@list_options
@click.pass_context
def list_(ctx: click.Context, all_projects: bool, type_, mode, status):
    """List the connections of source and destination collections"""
    filters = []
    if type_:
        filters.append(f"kind:eq:{connector_kind(type_)}")
    if mode:
        filters.append(f"collection_type:eq:{_MODES[mode]}")
    if status:
        filters.append(f"check_status:eq:{status}")
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        spanning = resolve_all_projects(ctx, all_projects)
        rows, truncated = take(
            ctx, server.list_connections_generator(**list_params(ctx, filters))
        )

        def _view():
            columns = [Column("Collection", Kind.ID)]
            if spanning:
                columns.append(Column("Project"))
            columns += [
                Column("Type"),
                Column("Mode"),
                Column("Check", Kind.STATUS),
                Column("Metadata", Kind.STATUS),
                Column("Entities", Kind.NUMBER),
            ]
            cells = []
            for row in rows:
                line = [present(row.get("collection"))]
                if spanning:
                    line.append(present(row.get("project")))
                line += [
                    _types(row),
                    present(_MODE_OF.get(row.get("collection_type"))),
                    present(row.get("check_status")),
                    present(row.get("metadata_status")),
                    row.get("entity_count", 0),
                ]
                cells.append(line)
            return TableView(
                title="Connections",
                context=ancestry(ctx),
                columns=columns,
                rows=cells,
                count="connections",
            )

        render_list(ctx, items=rows, truncated=truncated, view=_view)
    except Exception as e:
        raise command_failure(ctx, "list connections", e)


@capability("analytics", "read", escalate={"--definition": {"credential": "session"}})
@connection.command()
@proj_option
@_collection_option
@click.option(
    "--definition",
    is_flag=True,
    help=(
        "Add the connection document, dehydrated (no secret values or names). "
        "Needs a user session with manage_project."
    ),
)
@format_option
@view_options()
@record_fields_option
@click.pass_context
def get(ctx: click.Context, name: str, definition: bool):
    """Display the catalog of a collection's connection: probe status, schema
    fingerprint and the compatible local connectors"""
    from tabsdatak.cli.td.collection_group import _public_connection

    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        catalog = server.get_connection_catalog(name)
        if definition:
            catalog["definition"] = _public_connection(
                server.get_collection_connection(name)
            )

        def _view():
            compatible = catalog.get("compatible") or {}
            rows = [
                ("Collection", catalog.get("collection")),
                ("Project", catalog.get("project")),
                ("Kind", catalog.get("kind")),
                ("Types", ", ".join(compatible.get("types") or [])),
                ("Publishers", ", ".join(compatible.get("publishers") or [])),
                ("Subscribers", ", ".join(compatible.get("subscribers") or [])),
                ("Mode", _MODE_OF.get(catalog.get("collection_type"))),
                ("Check", catalog.get("check_status")),
                ("Check error", catalog.get("check_error")),
                ("Checked on", catalog.get("checked_on")),
                ("Metadata", catalog.get("metadata_status")),
                ("Metadata error", catalog.get("metadata_error")),
                ("Metadata on", catalog.get("metadata_on")),
                ("Entities", catalog.get("entity_count")),
                ("Truncated", catalog.get("truncated")),
                ("Schema hash", catalog.get("schema_hash")),
            ]
            if definition:
                rows.append(("Definition", catalog["definition"]))
            return _fields_view("Connection", rows)

        render(ctx, data=catalog, view=_view)
    except Exception as e:
        raise command_failure(ctx, "display the connection", e)


def _check_local(
    ctx: click.Context, conn_file: str, metadata: bool, sample: str | None, limit
) -> dict:
    """Run the connector's explorer here, against a connection document, with
    no server: ``check_connect``, then ``metadata`` and ``data`` when asked."""
    # noinspection PyProtectedMember
    from tabsdatak import _connectors
    from tabsdatak.cli import template
    from tabsdatak.cli.td.collection_group import _read_connection_document

    document, _ = _read_connection_document(ctx, conn_file)
    type_ = document.get("type") if isinstance(document, dict) else None
    try:
        template.resolve_class(str(type_))
    except ValueError as e:
        raise TdkException(
            "usage",
            f"The connector of connection type '{type_}' is not installed: {e}",
            hint="'tdk connection types' lists the installed connectors.",
        )
    try:
        template.validate(document)
    except template.TemplateError as e:
        raise click.ClickException(str(e))
    conn = template.resolve_class(type_).from_dict(document)
    explorer_cls = _connectors.explorer_for_conn(conn)
    if explorer_cls is None:
        return {"supported": False}
    explorer = explorer_cls()
    try:
        explorer.check_connect(conn)
    except Exception as e:  # noqa: BLE001 -- the verdict is the result
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}
    result: dict = {"ok": True}
    if not (metadata or sample):
        return result
    entities = sorted(explorer.metadata(conn) or [], key=lambda e: e.fqn())
    if metadata:
        result["entity_count"] = len(entities)
        result["entities"] = [e.fqn() for e in entities[:LOCAL_ENTITIES_SHOWN]]
    if sample:
        entity = next((e for e in entities if e.fqn() == sample), None)
        if entity is None:
            raise TdkException(
                "not_found", f"'{sample}' is not an entity of this connection."
            )
        limit = limit or DEFAULT_SAMPLE_ROWS
        records = explorer.data(conn, entity, limit)
        if records is None:
            result["sample"] = {"entity": sample, "supported": False}
        else:
            columns: list[str] = []
            for record in records:
                columns.extend(k for k in record if k not in columns)
            rows = [[record.get(c) for c in columns] for record in records[:limit]]
            result["sample"] = _sample_rows(
                sample, columns, rows, truncated=len(records) > limit
            )
    return result


def _sample_rows(entity, columns: list[str], rows: list, truncated: bool) -> dict:
    return {
        "entity": entity,
        "columns": [{"name": column} for column in columns],
        "rows": rows,
        "row_count": len(rows),
        "truncated": truncated,
    }


@capability(
    "operations",
    "read",
    paths={"--conn-file": "read"},
    escalate={
        "--metadata": {"fs": "read"},
        "--sample": {"fs": "read"},
        "--len": {"fs": "read"},
    },
)
@connection.command(login=False)
@proj_option
@_name_option
@_wait_options(default=True)
@click.option(
    "--conn-file",
    help=(
        "Check this connection document ('-' for stdin) here instead, with the "
        "locally installed connector and no server."
    ),
    type=click.Path(allow_dash=True),
)
@click.option(
    "--metadata",
    is_flag=True,
    help="With '--conn-file': also list the entities.",
)
@click.option(
    "--sample", metavar="FQN", help="With '--conn-file': also read rows of FQN."
)
@click.option(
    "--len",
    "limit",
    type=click.IntRange(1, MAX_SAMPLE_ROWS),
    help=f"With '--sample': rows to read. Default: {DEFAULT_SAMPLE_ROWS}.",
)
@format_option
@view_options()
@click.pass_context
def check(ctx: click.Context, name, wait, timeout, conn_file, metadata, sample, limit):
    """Check that a collection's connection connects, as a server-side probe.
    A check that ran and could not connect exits 0 with 'ok: false' in its
    result"""
    require_wait(wait, timeout)
    if conn_file is None:
        if metadata or sample or limit is not None:
            raise click.UsageError(
                "'--metadata', '--sample' and '--len' apply only with '--conn-file'."
            )
    elif name is not None:
        raise click.UsageError("Pass either '--name' or '--conn-file'.")
    elif limit is not None and not sample:
        raise click.UsageError("'--len' applies only with '--sample'.")
    try:
        if conn_file is not None:
            result = _check_local(ctx, conn_file, metadata, sample, limit)
            render(
                ctx,
                data=result,
                view=lambda: _fields_view("Connection check", list(result.items())),
            )
            return
        verify_login_or_prompt(ctx)
        name = _collection_name(ctx, name)
        _render_probe(ctx, _run_probe(ctx, "check", name, wait, timeout))
    except Exception as e:
        raise command_failure(ctx, "check the connection", e)


@capability("operations", "write")
@connection.command()
@proj_option
@_collection_option
@_wait_options(default=True)
@format_option
@view_options()
@click.pass_context
def refresh(ctx: click.Context, name, wait, timeout):
    """Explore a collection's connection again, as a server-side probe, and
    replace its catalog of entities"""
    require_wait(wait, timeout)
    try:
        _render_probe(ctx, _run_probe(ctx, "metadata", name, wait, timeout))
    except Exception as e:
        raise command_failure(ctx, "refresh the connection", e)


def _sample_document(probe: dict) -> dict:
    result = probe.get("result") or {}
    base = {
        "probe_id": probe.get("probe_id"),
        "expires_on": probe.get("expires_on"),
        "waited_ms": probe.get("waited_ms"),
    }
    if result.get("supported") is False:
        return {**base, "supported": False}
    return {
        **base,
        **_sample_rows(
            result.get("entity"),
            result.get("columns") or [],
            result.get("rows") or [],
            truncated=bool(result.get("truncated")),
        ),
    }


@capability("development", "read")
@connection.command()
@proj_option
@click.option(
    "--entity",
    required=True,
    metavar="FQN",
    help="Catalog entity to read rows of (see 'tdk connection entities').",
)
@_collection_option
@click.option(
    "--len",
    "limit",
    type=click.IntRange(1, MAX_SAMPLE_ROWS),
    default=DEFAULT_SAMPLE_ROWS,
    show_default=True,
    help="Rows to read.",
)
@_wait_options(default=True)
@format_option
@view_options()
@click.pass_context
def sample(ctx: click.Context, name, entity, limit, wait, timeout):
    """Read sample rows of a catalog entity, as a server-side probe. The rows
    are kept on the server for ten minutes and the read is audit-logged"""
    require_wait(wait, timeout)
    try:
        probe = _run_probe(
            ctx, "sample", name, wait, timeout, entity=entity, limit=limit
        )
        if not wait:
            _render_probe(ctx, probe)
            return
        document = _sample_document(probe)

        if document.get("supported") is False:
            if output_format(ctx) == "table":
                click.echo("This connector cannot read sample rows.")
            render(ctx, data=document)
            return

        def _view():
            return TableView(
                title=document.get("entity"),
                columns=[Column(column["name"]) for column in document["columns"]],
                rows=document["rows"],
                count="rows",
            )

        render(ctx, data=document, view=_view)
    except Exception as e:
        raise command_failure(ctx, "sample the connection", e)


@capability("analytics", "read")
@connection.command()
@proj_option
@_collection_option
@click.option("--schema", is_flag=True, help="Add each entity's fields.")
@format_option
@view_options()
@max_result_option
@list_options
@click.pass_context
def entities(ctx: click.Context, name, schema):
    """List the entities the last refresh of a collection's connection found.
    Match names with '--filter fqn:lk:GLOB', where '*' matches any text"""
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        rows, truncated = take(
            ctx,
            server.list_connection_entities_generator(
                name,
                fields=schema,
                **list_params(ctx),
            ),
        )

        def _view():
            return TableView(
                title="Entities",
                context=ancestry(ctx, collection=name),
                columns=[
                    Column("FQN", Kind.ID),
                    Column("Kind"),
                    Column("Fields", Kind.NUMBER),
                ],
                rows=[
                    [
                        present(row.get("fqn")),
                        present(row.get("entity_kind")),
                        row.get("field_count", 0),
                    ]
                    for row in rows
                ],
                count="entities",
            )

        render_list(ctx, items=rows, truncated=truncated, view=_view)
    except Exception as e:
        raise command_failure(ctx, "list the connection entities", e)


@capability("operations", "read")
@connection.command()
@proj_option
@click.option(
    "--probe",
    "probe_id",
    type=_PROBE_ID,
    required=True,
    help=(
        "ID of the probe to display, as 'tdk connection check|refresh|sample' print it."
    ),
)
@_wait_options(default=False)
@format_option
@view_options()
@click.pass_context
def probe(ctx: click.Context, probe_id, wait, timeout):
    """Display a connection probe"""
    require_wait(wait, timeout)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        found = server.get_connection_probe(probe_id)
        if wait:
            found = wait_connection_probe(
                ctx,
                server,
                found,
                PROBE_TIMEOUT_SECONDS if timeout is None else timeout,
            )
        _render_probe(ctx, found)
    except Exception as e:
        raise command_failure(ctx, "display the probe", e)
