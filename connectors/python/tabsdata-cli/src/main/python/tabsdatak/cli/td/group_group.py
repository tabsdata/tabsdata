#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import TYPE_CHECKING

import rich_click as click

from tabsdatak.cli._capability import capability
from tabsdatak.cli._command import TdkGroup
from tabsdatak.cli._prompted import prompted
from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    ancestry,
    format_option,
    hint_common_solutions,
    list_options,
    list_params,
    max_result_option,
    present,
    progress,
    proj_option,
    render,
    render_deleted,
    render_entity,
    render_list,
    require_word,
    take,
    view_options,
)
from tabsdatak.cli.td.tag_commands import tag_group, tags_of, tags_text
from tabsdatak.common._tabular import Column, Kind, RecordView, TableView

if TYPE_CHECKING:
    from tabsdatak.sdk.tabsdata_server import TabsdataServer


@cli_command
@click.group(cls=TdkGroup)
def group():
    """Group management commands"""


@capability("development", "write", credential="session")
@group.command()
@proj_option
@format_option
@prompted(
    "--name",
    "-n",
    prompt="Name of the group to create",
    help="Name of the group to create.",
)
@click.option("--description", help="Description of the group.")
@click.pass_context
def create(ctx: click.Context, name: str, description: str):
    """Create a new group"""
    description = description or ""
    progress(ctx, "Creating a new group")
    progress(ctx, "-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        created = server.create_group(name, description)
        render_entity(
            ctx, data=lambda: created._data, message="Group created successfully"
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to create group: {e}")


@capability("development", "destructive", credential="session")
@group.command()
@proj_option
@format_option
@prompted(
    "--name",
    "-n",
    prompt="Name of the group to delete",
    help="Name of the group to delete.",
)
@click.option(
    "--confirm",
    help="Write 'delete' to confirm deletion. Will be prompted for it if not provided.",
)
@click.pass_context
def delete(ctx: click.Context, name: str, confirm: str):
    """Delete a group by name. System groups cannot be deleted."""
    progress(ctx, f"Deleting group: {name}")
    progress(ctx, "-" * 10)
    require_word(ctx, confirm, "delete", "Please type 'delete' to confirm deletion")
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.delete_group(name)
        render_deleted(
            ctx,
            kind="group",
            name=name,
            project=server.connection.project,
            message="Group deleted successfully",
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to delete group: {e}")


@capability("analytics", "read")
@group.command()
@proj_option
@format_option
@view_options()
@prompted(
    "--name",
    "-n",
    prompt="Name of the group to display",
    help="Name of the group to display.",
)
@click.pass_context
def get(ctx: click.Context, name: str):
    """Display a group by name"""
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        grp = server.get_group(name)
        tags = tags_of(server, group=name)
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
                    ("Tags", present(tags_text(tags))),
                ],
            )

        render(ctx, data=lambda: {**(grp._data or {}), "tags": tags}, view=_view)
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to display group: {e}")


@capability("analytics", "read")
@group.command(name="list")
@proj_option
@format_option
@view_options()
@max_result_option
@list_options
@click.pass_context
def list_groups(ctx: click.Context):
    """List all groups"""
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        list_of_groups, truncated = take(
            ctx, server.list_groups_generator(**list_params(ctx))
        )

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

        render_list(
            ctx,
            items=lambda: [g._data for g in list_of_groups],
            truncated=truncated,
            view=_view,
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list groups: {e}")


@capability("development", "write", credential="session")
@group.command()
@proj_option
@format_option
@prompted(
    "--name",
    "-n",
    prompt="Name of the group to update",
    help="Name of the group to update.",
)
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
    progress(ctx, f"Updating group: {name}")
    progress(ctx, "-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        updated = server.update_group(
            name, new_name=new_name, new_description=description
        )
        render_entity(
            ctx, data=lambda: updated._data, message="Group updated successfully"
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to update group: {e}")


group.add_command(tag_group("group"))
