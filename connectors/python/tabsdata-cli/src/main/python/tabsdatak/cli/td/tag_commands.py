#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import TYPE_CHECKING, Callable

import rich_click as click

from tabsdatak.cli._capability import capability
from tabsdatak.cli._command import TdkGroup
from tabsdatak.cli._prompted import pinned, prompted
from tabsdatak.cli.cli_utils import (
    ancestry,
    format_option,
    hint_common_solutions,
    present,
    proj_option,
    render_done,
    render_entity,
    render_list,
    view_options,
)
from tabsdatak.common._tabular import Column, Kind, TableView

if TYPE_CHECKING:
    from tabsdatak.sdk.tabsdata_server import TabsdataServer

_KIND_LABELS = {"T": "Topology", "S": "System", "U": "User"}
PARENT_TAG = "_parent"
_FACET_LEN = 1000

# Per entity: its label and whether it lives in a collection.
_ENTITIES = {
    "project": ("project", False),
    "group": ("group", False),
    "collection": ("collection", False),
    "function": ("function", True),
    "table": ("table", True),
}


def tags_of(server: TabsdataServer, **target: str) -> dict[str, str]:
    """An entity's tags as ``{key: value}``, the platform's ``_`` keys
    included; ``target`` as :meth:`TabsdataServer.list_tags` takes it."""
    return {tag["key"]: tag["value"] for tag in server.list_tags(**target)}


def tags_text(tags: dict[str, str]) -> str | None:
    return ", ".join(f"{key}={value}" for key, value in sorted(tags.items())) or None


def _target_options(entity: str) -> Callable:
    label, in_collection = _ENTITIES[entity]

    def decorate(command):
        if in_collection:
            command = prompted(
                "--coll",
                "-c",
                prompt=f"Name of the collection to which the {label} belongs",
                default=pinned("collection"),
                help=f"Name of the collection to which the {label} belongs.",
            )(command)
        if entity != "project":
            command = prompted(
                "--name",
                "-n",
                prompt=f"Name of the {label}",
                help=f"Name of the {label} whose tags to use.",
            )(command)
        return proj_option(command)

    return decorate


def _target(ctx: click.Context, entity: str, name: str, coll: str) -> dict:
    if entity == "project":
        return {}
    if entity == "group":
        return {"group": name}
    if entity == "collection":
        return {"collection": name}
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

    @click.group(cls=TdkGroup)
    def tag():
        pass

    writes = (
        capability("admin", "write", credential="session")
        if entity in ("group", "project")
        else capability("development", "write")
    )

    tag.help = (
        "Manage the tags of the project; `list` covers every entity in it"
        if entity == "project"
        else f"Manage the tags of a {label}"
    )

    @capability("analytics", "read")
    @tag.command("list")
    @_target_options(entity)
    @format_option
    @view_options()
    @click.pass_context
    def list_tags(ctx: click.Context, name: str = None, coll: str = None):
        """List the tags, system and user"""
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

            render_list(ctx, items=tags, truncated=False, view=_view)
        except Exception as e:
            hint_common_solutions(ctx, e)
            raise click.ClickException(f"Failed to list tags: {e}")

    @writes
    @tag.command("set")
    @_target_options(entity)
    @format_option
    @prompted("--key", "-k", prompt="Key of the tag", help="Key of the tag.")
    @click.option("--value", "-v", default="", help="Value of the tag.")
    @click.pass_context
    def set_tag(
        ctx: click.Context, key: str, value: str, name: str = None, coll: str = None
    ):
        """Set a user tag, replacing its value if the key is set"""
        target = _target(ctx, entity, name, coll)
        try:
            server: TabsdataServer = ctx.obj["tabsdataserver"]
            render_entity(
                ctx,
                data=server.set_tag(key, value, **target),
                message=f"Tag '{key}' set",
            )
        except Exception as e:
            hint_common_solutions(ctx, e)
            raise click.ClickException(f"Failed to set tag: {e}")

    @writes
    @tag.command("delete")
    @_target_options(entity)
    @format_option
    @prompted("--key", "-k", prompt="Key of the tag", help="Key of the tag.")
    @click.pass_context
    def delete_tag(ctx: click.Context, key: str, name: str = None, coll: str = None):
        """Remove a user tag"""
        target = _target(ctx, entity, name, coll)
        try:
            server: TabsdataServer = ctx.obj["tabsdataserver"]
            server.delete_tag(key, **target)
            render_done(
                ctx,
                action="delete",
                kind="tag",
                id=key,
                details=target,
                message=f"Tag '{key}' removed",
            )
        except Exception as e:
            hint_common_solutions(ctx, e)
            raise click.ClickException(f"Failed to remove tag: {e}")

    if entity == "project":
        _add_facets(tag)
    return tag


def _facet_view(ctx: click.Context, title: str, column: str, items, context=()):
    def _view():
        return TableView(
            title=title,
            context=[*ancestry(ctx), *context],
            columns=[Column(column, Kind.ID)],
            rows=[[item] for item in items],
            count=title.lower(),
        )

    return _view


def _add_facets(tag: click.Group) -> None:
    @capability("analytics", "read")
    @tag.command("keys")
    @proj_option
    @format_option
    @view_options()
    @click.pass_context
    def tag_keys(ctx: click.Context):
        """List the tag keys open on the project's entities"""
        try:
            server: TabsdataServer = ctx.obj["tabsdataserver"]
            keys = server.list_tag_keys(len=_FACET_LEN)
            render_list(
                ctx,
                items=keys,
                truncated=False,
                view=_facet_view(ctx, "Tag keys", "Key", keys),
            )
        except Exception as e:
            hint_common_solutions(ctx, e)
            raise click.ClickException(f"Failed to list tag keys: {e}")

    @capability("analytics", "read")
    @tag.command("values")
    @proj_option
    @format_option
    @view_options()
    @prompted(
        "--key",
        "-k",
        prompt="Key of the tag",
        help="Key of the tag whose values to list.",
    )
    @click.pass_context
    def tag_values(ctx: click.Context, key: str):
        """List the values a tag key is open with on the project's entities"""
        try:
            server: TabsdataServer = ctx.obj["tabsdataserver"]
            values = server.list_tag_values(key, len=_FACET_LEN)
            render_list(
                ctx,
                items=values,
                truncated=False,
                view=_facet_view(ctx, "Tag values", "Value", values, [("Key", key)]),
            )
        except Exception as e:
            hint_common_solutions(ctx, e)
            raise click.ClickException(f"Failed to list tag values: {e}")
