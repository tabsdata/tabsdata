#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import TYPE_CHECKING

import rich_click as click

from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    ancestry,
    format_option,
    hint_common_solutions,
    logical_prompt,
    max_result_option,
    present,
    proj_option,
    render,
    take,
    verify_login_or_prompt,
    view_options,
)
from tabsdatak.cli.td.tag_commands import tag_group
from tabsdatak.common._tabular import Column, Kind, RecordView, TableView

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
@format_option
@view_options()
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

        def _view():
            return RecordView(
                title="Group",
                fields=[
                    ("Name", grp.name),
                    ("Description", present(grp.description)),
                    ("System", "Yes" if grp.system else "No"),
                    ("Defined by", present(grp.defined_by)),
                    ("Defined on", present(grp.defined_on_str)),
                    ("Collections", present(", ".join(collection_names))),
                ],
            )

        render(ctx, data=lambda: grp._data, view=_view)
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to display group: {e}")


@group.command(name="list")
@proj_option
@format_option
@view_options()
@max_result_option
@click.pass_context
def list_groups(ctx: click.Context):
    """List all groups"""
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        list_of_groups, truncated = take(ctx, server.list_groups_generator())

        def _view():
            return TableView(
                title="Groups",
                context=ancestry(ctx),
                columns=[
                    Column("Name", Kind.ID),
                    Column("Description"),
                    Column("Defined by"),
                    Column("Defined on", Kind.TIME),
                ],
                rows=[
                    [
                        grp.name,
                        present(grp.description),
                        present(grp.defined_by),
                        present(grp.defined_on_str),
                    ]
                    for grp in list_of_groups
                ],
                count="groups",
            )

        render(
            ctx,
            data=lambda: [g._data for g in list_of_groups],
            view=_view,
            truncated=truncated,
        )
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


group.add_command(tag_group("group"))
