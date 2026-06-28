#
# Copyright 2024 Tabs Data Inc.
#


import json
import sys
from pathlib import Path

import rich_click as click
from rich.console import Console
from rich.table import Table

from tabsdata._cli.cli_utils import (
    get_currently_pinned_object,
    hint_common_solutions,
    logical_prompt,
    verify_login_or_prompt,
)
from tabsdata._io.connections.named_connection.kind_registry import split_kind
from tabsdata._io.connections.named_connection.named_connection import (
    secrets_from_env,
)
from tabsdata.api.tabsdata_server import TabsdataServer


@click.group()
def collection():
    """Collection management commands"""


@collection.command()
@click.option("--name", "-n", help="Name of the collection to create.")
@click.option("--description", help="Description of the collection.")
@click.option(
    "--group",
    help=(
        "Name of the group to assign. Defaults to 'default' when omitted. "
        "Use 'td group list' to see available groups."
    ),
)
@click.option(
    "--base-path",
    help=(
        "Logical storage prefix for the collection's artifacts. Defaults to '/'. "
        "The physical backend is selected by the server's configured mounts."
    ),
)
@click.option(
    "--conn-file",
    help=(
        "Connection JSON document ('-' to read from stdin). Required for a "
        "source/destination collection — it is created WITH its connection. "
        "'env:' references are resolved from this shell's environment."
    ),
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
        or logical_prompt(ctx, "Name of the collection to create")
    )
    description = description or ""
    document = None
    secrets = None
    if conn_file:
        document, group, secrets = _load_connection_document(conn_file, name, group)
    elif group in ("source", "destination"):
        raise click.ClickException(
            f"a {group} collection is created WITH its connection; pass --conn-file"
        )
    click.echo("Creating a new collection")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.create_collection(
            name,
            description,
            group=group,
            base_path=base_path,
            connection=document,
            secrets=secrets,
        )
        click.echo("Collection created successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to create collection: {e}")


def _load_connection_document(
    conn_file: str, name: str, group: str | None
) -> tuple[dict, str, list]:
    """The dehydrated document, the group its kind implies, and the secrets
    payload resolved from this shell's environment."""
    try:
        raw = (
            sys.stdin.read()
            if conn_file == "-"
            else Path(conn_file).read_text(encoding="utf-8")
        )
    except OSError as e:
        raise click.ClickException(f"cannot read connection file: {e}")
    try:
        document = json.loads(raw)
    except json.JSONDecodeError as e:
        raise click.ClickException(f"invalid connection JSON: {e}")
    doc_name = document.get("name")
    if doc_name is None:
        document["name"] = name
    elif doc_name != name:
        raise click.ClickException(
            f"the connection document is named '{doc_name}' but the collection "
            f"is '{name}' — a connection is named by its collection (1:1)"
        )
    try:
        _, mode = split_kind(document.get("kind", ""))
    except ValueError as e:
        raise click.ClickException(str(e))
    implied = "source" if mode == "src" else "destination"
    if group is not None and group != implied:
        raise click.ClickException(
            f"kind '{document['kind']}' makes this a {implied} collection, "
            f"but --group says '{group}'"
        )
    try:
        secrets = secrets_from_env(document)
    except ValueError as e:
        raise click.ClickException(str(e))
    return document, implied, secrets


@collection.command()
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
        or logical_prompt(ctx, "Name of the collection to delete")
    )
    click.echo(f"Deleting collection: {name}")
    click.echo("-" * 10)
    confirm = confirm or logical_prompt(ctx, "Please type 'delete' to confirm deletion")
    if confirm != "delete":
        raise click.ClickException(
            "Deletion not confirmed. The confirmation word is 'delete'."
        )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.delete_collection(name)
        click.echo("Collection deleted successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to delete collection: {e}")


@collection.command()
@click.option("--name", "-n", help="Name of the collection to display.")
@click.pass_context
def info(ctx: click.Context, name: str):
    """Display a collection by name"""
    verify_login_or_prompt(ctx)
    name = (
        name
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to display")
    )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        collection = server.get_collection(name)

        table = Table(title=f"Collection '{name}'")
        table.add_column("Name", style="cyan", no_wrap=True)
        table.add_column("Description")
        table.add_column("Base path")
        table.add_column("Created on")
        table.add_column("Created by")

        table.add_row(
            collection.name,
            collection.description,
            collection.base_path or "(inherits project)",
            collection.created_on_str,
            collection.created_by,
        )

        click.echo()
        console = Console()
        console.print(table)
        click.echo()
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to display collection: {e}")


@collection.command()
@click.pass_context
def list(ctx: click.Context):
    """List all collections"""
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        list_of_collections = server.collections

        table = Table(title="Collections")
        table.add_column("Name", style="cyan", no_wrap=True)
        table.add_column("Description")
        table.add_column("Base path")
        table.add_column("Created on")
        table.add_column("Created by")

        for collection in list_of_collections:
            table.add_row(
                collection.name,
                collection.description,
                collection.base_path or "(inherits project)",
                collection.created_on_str,
                collection.created_by,
            )

        click.echo()
        console = Console()
        console.print(table)
        click.echo(f"Number of collections: {len(list_of_collections)}")
        click.echo()
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list collections: {e}")


@collection.command()
@click.option("--name", "-n", help="Name of the collection to update.")
@click.option("--new-name", help="New name for the collection.")
@click.option("--description", help="New description for the collection.")
@click.option(
    "--base-path",
    help=(
        "New logical storage prefix for the collection's artifacts. Existing data "
        "versions keep their stamped path; only new artifacts use the new prefix."
    ),
)
@click.option(
    "--inherit-base-path",
    is_flag=True,
    help="Clear the collection's base_path so it inherits the project's.",
)
@click.pass_context
def update(
    ctx: click.Context,
    name: str,
    new_name: str,
    description: str,
    base_path: str,
    inherit_base_path: bool,
):
    """Update a collection by name"""
    verify_login_or_prompt(ctx)
    name = (
        name
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to update")
    )
    click.echo(f"Updating collection: {name}")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.update_collection(
            name,
            new_name=new_name,
            new_description=description,
            new_base_path=base_path,
            inherit_base_path=inherit_base_path,
        )
        click.echo("Collection updated successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to update collection: {e}")
