#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import TYPE_CHECKING

import rich_click as click
from rich.table import Table

from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    beautify_list,
    hint_common_solutions,
    logical_prompt,
    make_console,
    output_option,
    proj_option,
    render,
    verify_login_or_prompt,
)

if TYPE_CHECKING:
    from tabsdatak.sdk.tabsdata_server import TabsdataServer


@cli_command
@click.group()
def group():
    """Group management commands"""


@group.command()
@proj_option
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
@proj_option
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
@proj_option
@output_option
@click.option("--name", "-n", help="Name of the group to display.")
@click.pass_context
def get(ctx: click.Context, name: str):
    """Display a group by name"""
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the group to display")
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        grp = server.get_group(name)
        collection_names = sorted(
            c.name
            for c in server.list_collections()
            if (c._data or {}).get("group") == name
        )

        def _table():
            table = Table(title="Group", show_header=False, show_lines=True)
            table.add_column("Field", style="cyan", no_wrap=True)
            table.add_column("Value")
            table.add_row("Name:", grp.name)
            table.add_row("Description:", grp.description or "")
            table.add_row("System:", "Yes" if grp.system else "No")
            table.add_row("Created by:", grp.created_by or "-")
            table.add_row("Created on:", grp.created_on_str or "-")
            table.add_row("Collections:", beautify_list(collection_names))
            click.echo()
            make_console(emoji=False, markup=False).print(table)
            click.echo()

        render(ctx, data=lambda: grp._data, table=_table)
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to display group: {e}")


@group.command()
@proj_option
@output_option
@click.pass_context
def list(ctx: click.Context):
    """List all groups"""
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        list_of_groups = server.groups

        def _table():
            table = Table(title="Groups")
            table.add_column("Name", style="cyan", no_wrap=True)
            table.add_column("Description")
            table.add_column("Created by")
            table.add_column("Created on")
            for grp in list_of_groups:
                table.add_row(
                    grp.name,
                    grp.description,
                    grp.created_by,
                    grp.created_on_str,
                )
            click.echo()
            make_console().print(table)
            click.echo(f"Number of groups: {len(list_of_groups)}")
            click.echo()

        render(ctx, data=lambda: [g._data for g in list_of_groups], table=_table)
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list groups: {e}")


@group.command()
@proj_option
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
