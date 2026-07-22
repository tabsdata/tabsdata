#
# Copyright 2026 Tabsdata Inc.
#


import rich_click as click
from rich.table import Table

from tabsdata._cli.cli_utils import (
    hint_common_solutions,
    logical_prompt,
    make_console,
    verify_login_or_prompt,
)
from tabsdata.api.tabsdata_server import TabsdataServer


@click.group()
def group():
    """Group management commands"""


@group.command()
@click.option("--name", "-n", help="Name of the group to create.")
@click.option("--description", help="Description of the group.")
@click.pass_context
def create(ctx: click.Context, name: str, description: str):
    """Create a new group"""
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the group to create")
    description = description or ""
    click.echo("Creating a new group")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.create_group(name, description)
        click.echo("Group created successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to create group: {e}")


@group.command()
@click.option("--name", "-n", help="Name of the group to delete.")
@click.option(
    "--confirm",
    help="Write 'delete' to confirm deletion. Will be prompted for it if not provided.",
)
@click.pass_context
def delete(ctx: click.Context, name: str, confirm: str):
    """Delete a group by name. System groups cannot be deleted."""
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the group to delete")
    click.echo(f"Deleting group: {name}")
    click.echo("-" * 10)
    confirm = confirm or logical_prompt(ctx, "Please type 'delete' to confirm deletion")
    if confirm != "delete":
        raise click.ClickException(
            "Deletion not confirmed. The confirmation word is 'delete'."
        )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.delete_group(name)
        click.echo("Group deleted successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to delete group: {e}")


@group.command()
@click.option("--name", "-n", help="Name of the group to display.")
@click.pass_context
def info(ctx: click.Context, name: str):
    """Display a group by name"""
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the group to display")
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        grp = server.get_group(name)

        table = Table(title=f"Group '{name}'")
        table.add_column("Name", style="cyan", no_wrap=True)
        table.add_column("ID", no_wrap=True)
        table.add_column("System", no_wrap=True)
        table.add_column("Description")
        table.add_column("Created on")
        table.add_column("Created by")

        table.add_row(
            grp.name,
            grp.id,
            str(bool(grp.system)),
            grp.description,
            grp.created_on_str,
            grp.created_by,
        )

        click.echo()
        make_console().print(table)
        click.echo()
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to display group: {e}")


@group.command()
@click.pass_context
def list(ctx: click.Context):
    """List all groups"""
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        list_of_groups = server.groups

        table = Table(title="Groups")
        table.add_column("Name", style="cyan", no_wrap=True)
        table.add_column("ID", no_wrap=True)
        table.add_column("Description")
        table.add_column("Created on")
        table.add_column("Created by")

        for grp in list_of_groups:
            table.add_row(
                grp.name,
                grp.id,
                grp.description,
                grp.created_on_str,
                grp.created_by,
            )

        click.echo()
        make_console().print(table)
        click.echo(f"Number of groups: {len(list_of_groups)}")
        click.echo()
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list groups: {e}")


@group.command()
@click.option("--name", "-n", help="Name of the group to update.")
@click.option("--new-name", help="New name for the group.")
@click.option("--description", help="New description for the group.")
@click.pass_context
def update(
    ctx: click.Context,
    name: str,
    new_name: str,
    description: str,
):
    """Update a group by name. System groups cannot be updated."""
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the group to update")
    click.echo(f"Updating group: {name}")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.update_group(name, new_name=new_name, new_description=description)
        click.echo("Group updated successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to update group: {e}")
