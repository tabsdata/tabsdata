#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import TYPE_CHECKING, Callable

import rich_click as click

from tabsdatak.cli.cli_utils import (
    ancestry,
    format_option,
    get_currently_pinned_object,
    hint_common_solutions,
    logical_prompt,
    present,
    proj_option,
    render,
    verify_login_or_prompt,
    view_options,
)
from tabsdatak.common._tabular import Column, Kind, TableView

if TYPE_CHECKING:
    from tabsdatak.sdk.tabsdata_server import TabsdataServer

_KIND_LABELS = {"T": "Topology", "S": "System", "U": "User"}

# Per entity: its label and whether it lives in a collection.
_ENTITIES = {
    "project": ("project", False),
    "group": ("group", False),
    "collection": ("collection", False),
    "function": ("function", True),
    "table": ("table", True),
}


def _target_options(entity: str) -> Callable:
    label, in_collection = _ENTITIES[entity]

    def decorate(command):
        if in_collection:
            command = click.option(
                "--coll",
                "-c",
                help=f"Name of the collection to which the {label} belongs.",
            )(command)
        if entity != "project":
            command = click.option(
                "--name", "-n", help=f"Name of the {label} whose tags to use."
            )(command)
        return proj_option(command)

    return decorate


def _target(ctx: click.Context, entity: str, name: str, coll: str) -> dict:
    label, in_collection = _ENTITIES[entity]
    if entity == "project":
        return {}
    name = name or logical_prompt(ctx, f"Name of the {label}")
    if entity == "group":
        return {"group": name}
    if entity == "collection":
        return {"collection": name}
    coll = (
        coll
        or get_currently_pinned_object(ctx, "collection")
        or logical_prompt(ctx, f"Name of the collection to which the {label} belongs")
    )
    return {"collection": coll, entity: name}


def _context(ctx: click.Context, entity: str, target: dict) -> list:
    context = ancestry(
        ctx,
        collection=target.get("collection"),
        function=target.get("function"),
        table=target.get("table"),
    )
    if entity == "group":
        context.append(("Group", target["group"]))
    return context


def tag_group(entity: str) -> click.Group:
    """The ``tag`` subcommands of an entity's command group."""
    label, _ = _ENTITIES[entity]

    @click.group()
    def tag():
        pass

    tag.help = (
        "Manage the tags of the project; `list` covers every entity in it"
        if entity == "project"
        else f"Manage the tags of a {label}"
    )

    @tag.command("list")
    @_target_options(entity)
    @format_option
    @view_options()
    @click.pass_context
    def list_tags(ctx: click.Context, name: str = None, coll: str = None):
        """List the tags, system and user"""
        verify_login_or_prompt(ctx)
        target = _target(ctx, entity, name, coll)
        try:
            server: TabsdataServer = ctx.obj["tabsdataserver"]
            tags = sorted(
                server.list_tags(**target),
                key=lambda t: (
                    t.get("entity_type", ""),
                    t.get("entity_id", ""),
                    t["key"],
                ),
            )

            def _view():
                columns = [
                    Column("Key", Kind.ID),
                    Column("Value"),
                    Column("Kind"),
                ]
                if entity == "project":
                    columns = [Column("Entity"), Column("Id", Kind.ID), *columns]
                rows = []
                for t in tags:
                    row = [
                        t["key"],
                        present(t["value"]),
                        _KIND_LABELS.get(t["kind"], t["kind"]),
                    ]
                    if entity == "project":
                        row = [t["entity_type"], t["entity_id"], *row]
                    rows.append(row)
                return TableView(
                    title="Tags",
                    context=_context(ctx, entity, target),
                    columns=columns,
                    rows=rows,
                    count="tags",
                )

            render(ctx, data=tags, view=_view)
        except Exception as e:
            hint_common_solutions(ctx, e)
            raise click.ClickException(f"Failed to list tags: {e}")

    @tag.command("set")
    @_target_options(entity)
    @click.option("--key", "-k", help="Key of the tag.")
    @click.option("--value", "-v", default="", help="Value of the tag.")
    @click.pass_context
    def set_tag(
        ctx: click.Context, key: str, value: str, name: str = None, coll: str = None
    ):
        """Set a user tag, replacing its value if the key is set"""
        verify_login_or_prompt(ctx)
        target = _target(ctx, entity, name, coll)
        key = key or logical_prompt(ctx, "Key of the tag")
        try:
            server: TabsdataServer = ctx.obj["tabsdataserver"]
            server.set_tag(key, value, **target)
            click.echo(f"Tag '{key}' set")
        except Exception as e:
            hint_common_solutions(ctx, e)
            raise click.ClickException(f"Failed to set tag: {e}")

    @tag.command("delete")
    @_target_options(entity)
    @click.option("--key", "-k", help="Key of the tag.")
    @click.pass_context
    def delete_tag(ctx: click.Context, key: str, name: str = None, coll: str = None):
        """Remove a user tag"""
        verify_login_or_prompt(ctx)
        target = _target(ctx, entity, name, coll)
        key = key or logical_prompt(ctx, "Key of the tag")
        try:
            server: TabsdataServer = ctx.obj["tabsdataserver"]
            server.delete_tag(key, **target)
            click.echo(f"Tag '{key}' removed")
        except Exception as e:
            hint_common_solutions(ctx, e)
            raise click.ClickException(f"Failed to remove tag: {e}")

    return tag
