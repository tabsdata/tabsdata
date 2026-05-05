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
    store_pinned_objects,
    verify_login_or_prompt,
)
from tabsdata.api.tabsdata_server import TabsdataServer


@click.group()
def collection():
    """Collection management commands"""


@collection.command()
@click.option(
    "--name",
    "-n",
    help="Name of the collection to which the permissions will be added.",
)
@click.option(
    "--to-coll",
    "-t",
    help="Name of the collection from which the permission will be obtained.",
)
@click.pass_context
def add_perm(ctx: click.Context, name: str, to_coll: str):
    """Add a permission to another collection"""
    verify_login_or_prompt(ctx)
    name = (
        name
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to add the permission to")
    )
    to_coll = to_coll or logical_prompt(
        ctx, "Name of the collection from which the permission will be obtained"
    )
    try:
        click.echo(
            f"Adding permission to collection '{name}' to read collection '{to_coll}'"
        )
        click.echo("-" * 10)
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        collection = server.get_collection(name)
        permission = collection.create_permission(to_coll)

        click.echo("Permission added successfully")
        click.echo(f"Permission ID: {permission.id}")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to add permission: {e}")


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
@click.option(
    "--name",
    "-n",
    help="Name of the collection from which the permissions will be deleted.",
)
@click.option(
    "--to-coll",
    "-t",
    help="Name of the collection from which permission was obtained.",
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["id"],
)
@click.option(
    "--id",
    help="ID of the permission to delete.",
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["to-coll"],
)
@click.pass_context
def delete_perm(ctx: click.Context, name: str, to_coll: str, id: str):
    """Delete a permission to another collection"""
    verify_login_or_prompt(ctx)
    name = (
        name
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(
            ctx, "Name of the collection from which the permission will be deleted"
        )
    )
    if not id:
        to_coll = to_coll or logical_prompt(
            ctx, "Name of the collection from which the permission was obtained"
        )
    try:
        if id:
            click.echo(f"Deleting permission with ID '{id}' from collection '{name}'")
        else:
            click.echo(
                f"Deleting permission from collection '{name}' to read collection "
                f"'{to_coll}'"
            )
        click.echo("-" * 10)
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        collection = server.get_collection(name)
        if to_coll:
            server.get_collection(to_coll)  # Ensure the target collection exists
        filter = f"id:eq:{id}" if id else f"to_collection:eq:{to_coll}"
        permissions = collection.list_permissions(filter=filter)
        try:
            permission = permissions[0]
        except IndexError:
            if id:
                raise click.ClickException(
                    f"No permission found with ID '{id}' in collection '{name}'. The "
                    f"existing permissions are: {', '.join(p.id for p in permissions)}"
                )
            else:
                raise click.ClickException(
                    f"No permission found from collection '{name}' to read collection "
                    f"'{to_coll}'. The collections to which permissions "
                    "exist are: "
                    f"{', '.join(p.to_collection.name for p in permissions)}"
                )
        collection.delete_permission(permission)

        click.echo("Permission deleted successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to delete permission: {e}")


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
@click.option(
    "--name", "-n", help="Name of the collection to which the permissions belong."
)
@click.pass_context
def list_perm(ctx: click.Context, name: str):
    """List all permissions of a collection"""
    verify_login_or_prompt(ctx)
    name = (
        name
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, "Name of the collection to which the permissions belong")
    )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        collection = server.get_collection(name)
        list_of_permissions = collection.permissions

        table = Table(title=f"Permissions of collection '{name}'")
        table.add_column("ID", style="cyan", no_wrap=True)
        table.add_column("To collection")
        table.add_column("Granted on")
        table.add_column("Granted by")

        for permission in list_of_permissions:
            table.add_row(
                permission.id,
                permission.to_collection.name,
                permission.granted_on_str,
                permission.granted_by,
            )

        click.echo()
        console = Console()
        console.print(table)
        click.echo(f"Number of permissions: {len(list_of_permissions)}")
        click.echo()
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list permissions: {e}")


@collection.command()
@click.option("--name", "-n", help="Name of the collection to pin.")
@click.pass_context
def pin(ctx: click.Context, name: str):
    """Pin a collection by name"""
    name = name or logical_prompt(ctx, "Name of the collection to be pinned")
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
    """Unpin the currently pinned collection"""
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
@click.option("--new-name", help="New name for the collection.")
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
