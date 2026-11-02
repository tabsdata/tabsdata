#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING

import rich_click as click

from tabsdatak.cli._capability import capability
from tabsdatak.cli._machine import machine
from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    all_projects_option,
    ancestry,
    format_option,
    get_currently_pinned_object,
    hint_common_solutions,
    list_options,
    list_params,
    logical_prompt,
    max_result_option,
    present,
    progress,
    proj_option,
    render,
    render_deleted,
    render_entity,
    render_list,
    resolve_all_projects,
    serialize,
    take,
    verify_login_or_prompt,
    view_options,
    warn_deprecated,
)
from tabsdatak.cli.td.tag_commands import tag_group, tags_of, tags_text
from tabsdatak.common._tabular import Column, Kind, RecordView, TableView

if TYPE_CHECKING:
    from tabsdatak.sdk.tabsdata_server import TabsdataServer


@cli_command
@click.group()
def collection():
    """Collection management commands"""


@capability(
    "development",
    "write",
    paths={"--conn-file": "read"},
    escalate={"--conn-file": {"credential": "session"}},
)
@collection.command()
@proj_option
@format_option
@click.option("--name", "-n", help="Name of the collection to create.")
@click.option("--description", help="Description of the collection.")
@click.option(
    "--group",
    help=(
        "Name of the group to assign. Defaults to 'default' when omitted. "
        "Use 'tdk group list' to see available groups."
    ),
)
@click.option(
    "--storage-path",
    "base_path",
    help=(
        "Logical storage prefix for the collection's artifacts. Defaults to '/'. "
        "The physical backend is selected by the server's configured mounts."
    ),
)
@click.option(
    "--conn-file",
    help=(
        "Connection YAML document ('-' to read from stdin). Required for a "
        "source/destination collection — it is created WITH its connection. "
        "The connection is validated and dehydrated client-side; secret "
        "values are stripped into the secret payload."
    ),
    type=click.Path(allow_dash=True),
)
@click.pass_context
def create(
    ctx: click.Context,
    name: str,
    description: str,
    group: str,
    base_path: str,
    conn_file: str,
):
    """Create a new collection"""
    verify_login_or_prompt(ctx)
    name = (
        name
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to create", option="--name")
    )
    description = description or ""
    connection = None
    secrets = None
    if conn_file:
        connection, group, secrets = _load_connection_document(ctx, conn_file, group)
    elif group in ("sources", "destinations"):
        raise click.ClickException(
            f"a {group} collection is created WITH its connection; pass --conn-file"
        )
    progress(ctx, "Creating a new collection")
    progress(ctx, "-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        created = server.create_collection(
            name,
            description,
            group=group,
            base_path=base_path,
            connection=connection,
            secrets=secrets,
        )
        render_entity(
            ctx, data=lambda: created._data, message="Collection created successfully"
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to create collection: {e}")


_DIRECTION_TO_GROUP = {
    "src": "sources",
    "stream-src": "sources",
    "dest": "destinations",
}


def _group_for_conn(conn_cls) -> str:
    """The collection group a connection belongs to, resolved from the SPI
    registry: a connection bound to a (stream) source -> ``sources``, one
    bound to a destination -> ``destinations``.

    Every connection now shares the uniform ``connectionDef`` kind, so the
    direction is taken from the registered binding rather than parsed out of
    the kind string."""
    # noinspection PyProtectedMember
    from tabsdatak.cli import template

    for r in template.list_connectors():
        if r.conn_cls is conn_cls:
            return _DIRECTION_TO_GROUP[r.direction]
    raise click.ClickException(
        f"connection type {conn_cls.__qualname__!r} is not a registered "
        "source or destination connection"
    )


_COLLECTION_TYPE_LABELS = {"F": "Data Factory", "S": "Source", "D": "Destination"}


def _connection_system(doc: dict) -> str:
    """The target-system name (e.g. 'PostgreSQL') of a connection document,
    resolved from its ``type`` through the connector registry; falls back to the
    raw type for an unregistered connector."""
    # noinspection PyProtectedMember
    from tabsdatak.cli import template

    raw = str(doc.get("type", ""))
    try:
        conn_cls = template.resolve_class(doc.get("type"))
    except Exception:  # noqa: BLE001 -- unknown / unregistered type
        return raw
    for r in template.list_connectors():
        if r.conn_cls is conn_cls:
            return r.system
    return raw


def _public_connection(conn_read: dict) -> dict:
    """The connection view safe to display: just the dehydrated document, never
    the secret names."""
    return {k: v for k, v in conn_read.items() if k != "secret_names"}


def _read_connection_document(ctx, conn_file) -> tuple[dict, list[str]]:
    """Read a connection YAML document, with its ``$secret:<ENV_VAR>`` values
    resolved from the environment (on the client); returns it and the env vars
    read, which are also echoed to the user."""
    # noinspection PyProtectedMember
    from tabsdatak.cli import template
    from tabsdatak.common._error import CommonException
    from tabsdatak.common._json_serde import (
        env_secret_ref_vars,
        resolve_env_secret_refs,
    )

    try:
        raw = (
            sys.stdin.read()
            if conn_file == "-"
            else Path(conn_file).read_text(encoding="utf-8")
        )
    except OSError as e:
        raise click.ClickException(f"cannot read connection file: {e}") from None
    try:
        document = template.from_yaml(raw)
    except Exception as e:  # noqa: BLE001 -- any YAML parse failure
        raise click.ClickException(f"invalid connection YAML: {e}") from None
    env_vars = env_secret_ref_vars(document)
    try:
        document = resolve_env_secret_refs(document)
    except CommonException as e:
        raise click.ClickException(str(e))
    if env_vars:
        progress(
            ctx,
            "Connection secrets resolved from environment variable(s): "
            + ", ".join(env_vars),
        )
    return document, env_vars


def _load_connection_document(ctx, conn_file, group):
    """Read the connection YAML, validate it as a `Conn`, and dehydrate it.

    Returns ``(connection_yaml, group, secrets)``: the dehydrated connection
    re-serialized as YAML (secret leaves reduced to ``secret:``), the group its
    kind implies, and the harvested secret values. No secret value is sent
    inside the connection document — they ride in the secret payload.
    """
    # noinspection PyProtectedMember
    from tabsdatak.cli import template

    document, _ = _read_connection_document(ctx, conn_file)
    try:
        dehydrated, secrets = template.dehydrate_document(document)
    except template.TemplateError as e:
        raise click.ClickException(str(e))
    implied = _group_for_conn(template.resolve_class(document["type"]))
    if group is not None and group != implied:
        raise click.ClickException(
            f"connection type {document['type']!r} makes this a {implied} "
            f"collection, but --group says '{group}'"
        )
    return template.to_yaml(dehydrated), implied, secrets


@capability("development", "destructive")
@collection.command()
@proj_option
@format_option
@click.option("--name", "-n", help="Name of the collection to delete.")
@click.option(
    "--confirm",
    help="Write 'delete' to confirm deletion. Will be prompted for it if not provided.",
)
@click.pass_context
def delete(ctx: click.Context, name: str, confirm: str):
    """Delete a collection by name"""
    verify_login_or_prompt(ctx)
    name = (
        name
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to delete", option="--name")
    )
    progress(ctx, f"Deleting collection: {name}")
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
        server.delete_collection(name)
        render_deleted(
            ctx,
            kind="collection",
            name=name,
            project=server.connection.project,
            message="Collection deleted successfully",
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to delete collection: {e}")


@capability("analytics", "read")
@collection.command()
@proj_option
@format_option
@view_options()
@click.option("--name", "-n", help="Name of the collection to display.")
@click.pass_context
def get(ctx: click.Context, name: str):
    """Display a collection by name"""
    verify_login_or_prompt(ctx)
    name = (
        name
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to display", option="--name")
    )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        collection = server.get_collection(name)
        tags = tags_of(server, collection=name)
        data = {**(collection._data or {}), "tags": tags}
        collection_type = data.get("collection_type")
        conn_read = None
        if collection_type in ("S", "D"):
            conn_read = _public_connection(server.get_collection_connection(name))

        def _view():
            fields = [
                ("Name", collection.name),
                ("Description", present(collection.description)),
                ("Project", present(data.get("project"))),
                ("Group", present(data.get("group"))),
                (
                    "Collection type",
                    present(
                        _COLLECTION_TYPE_LABELS.get(collection_type, collection_type)
                    ),
                ),
                ("Defined by", present(collection.defined_by)),
                ("Defined on", present(collection.defined_on_str)),
                ("Tags", present(tags_text(tags))),
            ]
            if conn_read is not None:
                doc = conn_read.get("connection") or {}
                fields.append(("Connection type", _connection_system(doc)))
                fields.append((
                    "Connection info",
                    serialize(conn_read, "yaml").rstrip("\n"),
                ))
            return RecordView(title="Collection", fields=fields)

        render(ctx, data=lambda: data, view=_view)
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to display collection: {e}")


@capability("analytics", "read", credential="session")
@collection.command(name="connection", hidden=True)
@proj_option
@click.option(
    "-o",
    "--format",
    "fmt",
    type=click.Choice(("json", "yaml")),
    default="json",
    show_default=True,
    help="Output format.",
)
@click.option(
    "--name",
    "-n",
    help="Name of the source/destination collection whose connection to display.",
)
@click.pass_context
def connection_document(ctx: click.Context, fmt: str, name: str):
    """Display the connection of a source/destination collection, dehydrated:
    secret and var names are never shown. Deprecated: 'tdk connection get
    --definition' carries the same document."""
    warn_deprecated("tdk collection connection", "'tdk connection get --definition'")
    verify_login_or_prompt(ctx)
    name = (
        name
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(
            ctx, "Name of the collection whose connection to display", option="--name"
        )
    )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        conn_read = _public_connection(server.get_collection_connection(name))
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to display connection: {e}")
    run = machine()
    if run is not None:
        run.emit(conn_read)
        return
    click.echo(serialize(conn_read, fmt), nl=(fmt == "json"))


@capability("analytics", "read")
@collection.command(name="list")
@proj_option
@all_projects_option
@format_option
@view_options()
@max_result_option
@list_options
@click.pass_context
def list_collections(ctx: click.Context, all_projects: bool):
    """List all collections"""
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        spanning = resolve_all_projects(ctx, all_projects)
        list_of_collections, truncated = take(
            ctx, server.list_collections_generator(**list_params(ctx))
        )

        def _view():
            columns = [Column("Name", Kind.ID), Column("Description")]
            if spanning:
                columns.append(Column("Project"))
            columns += [Column("Defined by"), Column("Defined on", Kind.TIME)]
            rows = []
            for collection in list_of_collections:
                row = [collection.name, present(collection.description)]
                if spanning:
                    row.append(present(collection.project))
                row += [
                    present(collection.defined_by),
                    present(collection.defined_on_str),
                ]
                rows.append(row)
            return TableView(
                title="Collections",
                context=ancestry(ctx),
                columns=columns,
                rows=rows,
                count="collections",
            )

        render_list(
            ctx,
            items=lambda: [c._data for c in list_of_collections],
            truncated=truncated,
            view=_view,
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list collections: {e}")


@capability(
    "development",
    "write",
    paths={"--conn-file": "read"},
    escalate={"--conn-file": {"credential": "session"}},
)
@collection.command()
@proj_option
@format_option
@click.option("--name", "-n", help="Name of the collection to update.")
@click.option("--new-name", help="New name for the collection.")
@click.option("--description", help="New description for the collection.")
@click.option(
    "--storage-path",
    "base_path",
    help=(
        "New logical storage prefix for the collection's artifacts. Existing data "
        "versions keep their stamped path; only new artifacts use the new prefix."
    ),
)
@click.option(
    "--conn-file",
    help=(
        "Connection YAML document ('-' to read from stdin) to create or replace "
        "the source/destination collection's connection. Validated and dehydrated "
        "client-side; secret values are stripped into the secret payload."
    ),
    type=click.Path(allow_dash=True),
)
@click.pass_context
def update(
    ctx: click.Context,
    name: str,
    new_name: str,
    description: str,
    base_path: str,
    conn_file: str,
):
    """Update a collection by name"""
    verify_login_or_prompt(ctx)
    name = (
        name
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to update", option="--name")
    )
    progress(ctx, f"Updating collection: {name}")
    progress(ctx, "-" * 10)
    connection = None
    secrets = None
    if conn_file:
        connection, _group, secrets = _load_connection_document(ctx, conn_file, None)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        # Scalar fields go through the collection-update endpoint; the connection
        # (if any) through its dedicated upsert endpoint. When only --conn-file is
        # given, skip the no-op scalar update.
        scalar_change = new_name or description is not None or base_path is not None
        if scalar_change or not conn_file:
            server.update_collection(
                name,
                new_name=new_name,
                new_description=description,
                new_base_path=base_path,
            )
        if connection is not None:
            # A rename lands first, so the connection upsert targets the new name.
            server.update_collection_connection(
                new_name or name, connection=connection, secrets=secrets
            )
        render_entity(
            ctx,
            data=lambda: server.get_collection(new_name or name)._data,
            message="Collection updated successfully",
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to update collection: {e}")


collection.add_command(tag_group("collection"))
