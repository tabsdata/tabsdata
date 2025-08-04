#
# Copyright 2024 Tabs Data Inc.
#


import rich_click as click
from rich.console import Console
from rich.table import Table

from tabsdata._cli.cli_utils import (
    get_currently_pinned_object,
    hint_common_solutions,
    logical_prompt,
    store_pinned_objects,
    verify_login_or_prompt,
)
from tabsdata.api.tabsdata_server import TabsdataServer


@click.group()
def collection():
    """Collection management commands"""


@collection.command()
@click.option("--name", "-n", help="Name of the collection to create.")
@click.option("--description", help="Description of the collection.")
@click.pass_context
def create(
    ctx: click.Context,
    name: str,
    description: str,
):
    """Create a new collection"""
    verify_login_or_prompt(ctx)
    name = (
        name
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to create")
    )
    description = description or ""
    click.echo("Creating a new collection")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.create_collection(name, description)
        click.echo("Collection created successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to create collection: {e}")


@collection.command()
@click.option("--name", "-n", help="Name of the collection to delete.")
@click.option(
    "--confirm",
    help="Write 'delete' to confirm deletion. Will be prompted for it if not provided.",
)
@click.pass_context
def delete(ctx: click.Context, name: str, confirm: str):
    """Delete a collection by name. Currently not supported."""
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
        table.add_column("Created on")
        table.add_column("Created by")

        table.add_row(
            collection.name,
            collection.description,
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
        table.add_column("Created on")
        table.add_column("Created by")

        for collection in list_of_collections:
            table.add_row(
                collection.name,
                collection.description,
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
@click.option("--name", "-n", help="Name of the collection to pin.")
@click.pass_context
def pin(ctx: click.Context, name: str):
    """Pin a collection by name"""
    click.echo(f"Pinning collection: {name}")
    click.echo("-" * 10)
    try:
        previously_pinned = ctx.obj["pinned_objects"].get("collection")
        if previously_pinned:
            click.echo(f"Unpinning previously pinned collection: {previously_pinned}")
        ctx.obj["pinned_objects"]["collection"] = name
        store_pinned_objects(ctx)
        click.echo("Collection pinned successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to pin collection: {e}")


@collection.command()
@click.pass_context
def unpin(ctx: click.Context):
    """Pin a collection by name"""
    click.echo("Unpinning collection")
    click.echo("-" * 10)
    try:
        previously_pinned = ctx.obj["pinned_objects"].get("collection")
        if not previously_pinned:
            click.echo("No previously pinned collection to unpin.")
        else:
            ctx.obj["pinned_objects"]["collection"] = None
            store_pinned_objects(ctx)
            click.echo("Collection unpinned successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to unpin collection: {e}")


@collection.command()
@click.option("--name", "-n", help="Name of the collection to update.")
@click.option("--new-name", "-n", help="New name for the collection.")
@click.option("--description", help="New description for the collection.")
@click.pass_context
def update(
    ctx: click.Context,
    name: str,
    new_name: str,
    description: str,
):
    """Update a collection by name"""
    verify_login_or_prompt(ctx)
    name = (
        name
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to update")
    )
    description = description or ""
    click.echo(f"Updating collection: {name}")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.update_collection(name, new_name=new_name, new_description=description)
        click.echo("Collection updated successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to update collection: {e}")
