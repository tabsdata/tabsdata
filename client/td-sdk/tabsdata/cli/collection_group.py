#
# Copyright 2024 Tabs Data Inc.
#


import rich_click as click
from rich.console import Console
from rich.table import Table

from tabsdata.cli.cli_utils import logical_prompt, verify_login_or_prompt


@click.group()
@click.pass_context
def collection(ctx: click.Context):
    """Collection management commands"""
    verify_login_or_prompt(ctx)


@collection.command()
@click.argument("name")
@click.option("--description", help="Description of the collection.")
@click.pass_context
def create(
    ctx: click.Context,
    name: str,
    description: str,
):
    """Create a new collection"""
    description = description or ""
    click.echo("Creating a new collection")
    click.echo("-" * 10)
    try:
        ctx.obj["tabsdataserver"].collection_create(name, description)
        click.echo("Collection created successfully")
    except Exception as e:
        raise click.ClickException(f"Failed to create collection: {e}")


@collection.command()
@click.argument("name")
@click.option(
    "--confirm",
    help="Write 'delete' to confirm deletion. Will be prompted for it if not provided.",
)
@click.pass_context
def delete(ctx: click.Context, name: str, confirm: str):
    """Delete a collection by name. Currently not supported."""
    raise click.ClickException("Deleting a collection is currently not supported.")
    click.echo(f"Deleting collection: {name}")
    click.echo("-" * 10)
    confirm = confirm or logical_prompt(ctx, "Please type 'delete' to confirm deletion")
    if confirm != "delete":
        raise click.ClickException(
            "Deletion not confirmed. The confirmation word is 'delete'."
        )
    try:
        ctx.obj["tabsdataserver"].collection_delete(name)
        click.echo("Collection deleted successfully")
    except Exception as e:
        raise click.ClickException(f"Failed to delete collection: {e}")


@collection.command()
@click.argument("name")
@click.pass_context
def info(ctx: click.Context, name: str):
    """Display a collection by name"""
    try:
        collection = ctx.obj["tabsdataserver"].collection_get(name)
        click.echo(collection)

        table = Table(title=f"Collection '{name}'")
        table.add_column("Name", style="cyan", no_wrap=True)
        table.add_column("Description")
        table.add_column("Created on")
        table.add_column("Created by")

        table.add_row(
            collection.name,
            collection.description,
            collection.created_on_string,
            collection.created_by,
        )

        click.echo()
        console = Console()
        console.print(table)
        click.echo()
    except Exception as e:
        raise click.ClickException(f"Failed to display collection: {e}")


@collection.command()
@click.pass_context
def list(ctx: click.Context):
    """List all collections"""
    try:
        list_of_collections = ctx.obj["tabsdataserver"].collections

        table = Table(title="Collections")
        table.add_column("Name", style="cyan", no_wrap=True)
        table.add_column("Description")
        table.add_column("Created on")
        table.add_column("Created by")

        for collection in list_of_collections:
            table.add_row(
                collection.name,
                collection.description,
                collection.created_on_string,
                collection.created_by,
            )

        click.echo()
        console = Console()
        console.print(table)
        click.echo(f"Number of collections: {len(list_of_collections)}")
        click.echo()
    except Exception as e:
        raise click.ClickException(f"Failed to list collections: {e}")


@collection.command()
@click.argument("name")
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
    description = description or ""
    click.echo(f"Updating collection: {name}")
    click.echo("-" * 10)
    try:
        ctx.obj["tabsdataserver"].collection_update(
            name, new_name=new_name, new_description=description
        )
        click.echo("Collection updated successfully")
    except Exception as e:
        raise click.ClickException(f"Failed to update collection: {e}")
