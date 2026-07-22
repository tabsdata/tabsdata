#
# Copyright 2026 Tabsdata Inc.
#


import rich_click as click

from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    hint_common_solutions,
    output_option,
    render,
    store_pinned_objects,
)


@cli_command
@click.group()
def pin():
    """Manage the pinned project and collection.

    Pins are persisted across invocations and act as defaults for
    project-scoped commands. A collection is pinned *within* a project, so a
    collection pin requires a pinned project; unpinning or switching the
    project clears the collection.
    """


@pin.command()
@click.option("--proj", "-P", "project", help="Project to pin.")
@click.option(
    "--coll",
    "-c",
    "collection",
    help="Collection to pin (requires a pinned project).",
)
@click.pass_context
def set(ctx: click.Context, project: str, collection: str):
    """Pin a project and/or a collection.

    Examples:
        tdk pin set --proj sales
        tdk pin set --coll orders            # requires a pinned project
        tdk pin set --proj sales --coll orders
    """
    if not (project or collection):
        raise click.ClickException("Nothing to pin. Specify --proj and/or --coll.")
    pinned = ctx.obj["pinned_objects"]
    # A collection only resolves relative to a pinned project -- one being set
    # in this same command, or a pre-existing pin. Fail fast otherwise.
    if collection and not (project or pinned.get("project")):
        raise click.ClickException(
            "No project pinned. Pin a project first with 'tdk pin set "
            "--proj <name>' before pinning a collection."
        )
    try:
        if project:
            previously_pinned = pinned.get("project")
            if previously_pinned and previously_pinned != project:
                click.echo(f"Unpinning previously pinned project: {previously_pinned}")
            pinned["project"] = project
            click.echo(f"Pinning project: {project}")
            # Switching to a different project orphans a collection pinned under
            # the old one -- clear it, unless this command also sets a new
            # collection (handled below). Re-pinning the same project keeps it.
            if (
                previously_pinned
                and previously_pinned != project
                and not collection
                and pinned.get("collection")
            ):
                orphan = pinned.get("collection")
                pinned["collection"] = None
                click.echo(
                    "Also unpinning collection (it belonged to the previously "
                    f"pinned project): {orphan}"
                )
        if collection:
            previously_pinned = pinned.get("collection")
            if previously_pinned and previously_pinned != collection:
                click.echo(
                    f"Unpinning previously pinned collection: {previously_pinned}"
                )
            pinned["collection"] = collection
            click.echo(f"Pinning collection: {collection}")
        store_pinned_objects(ctx)
        click.echo("Pins updated successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to set pins: {e}")


@pin.command()
@click.option(
    "--proj",
    "project",
    is_flag=True,
    help="Unpin the project (also clears the collection).",
)
@click.option("--coll", "collection", is_flag=True, help="Unpin the collection.")
@click.pass_context
def unset(ctx: click.Context, project: bool, collection: bool):
    """Unpin the project and/or the collection.

    With no flags, unpins both. ``--proj`` also clears the collection (a
    collection pin only resolves under a project); ``--coll`` clears just the
    collection.

    Examples:
        tdk pin unset            # unpin both
        tdk pin unset --proj     # unpin both (project + its collection)
        tdk pin unset --coll     # unpin the collection only
    """
    try:
        pinned = ctx.obj["pinned_objects"]
        # No flags means "unpin everything". Clearing the project always
        # cascades to the collection, since a collection only resolves under a
        # project.
        unpin_all = not project and not collection
        clear_project = project or unpin_all
        clear_collection = collection or clear_project

        cleared = []
        if clear_project and pinned.get("project"):
            cleared.append(f"project ({pinned['project']})")
        if clear_collection and pinned.get("collection"):
            cleared.append(f"collection ({pinned['collection']})")

        if clear_project:
            pinned["project"] = None
        if clear_collection:
            pinned["collection"] = None
        store_pinned_objects(ctx)

        if cleared:
            click.echo("Unpinned: " + ", ".join(cleared))
        else:
            click.echo("Nothing was pinned.")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to unset pins: {e}")


@pin.command()
@output_option
@click.pass_context
def show(ctx: click.Context):
    """Show the currently pinned project and collection."""
    pinned = ctx.obj["pinned_objects"]
    project = pinned.get("project")
    collection = pinned.get("collection")

    def _table():
        click.echo(f"Project:    {project if project else '(none)'}")
        click.echo(f"Collection: {collection if collection else '(none)'}")

    render(
        ctx,
        data={"project": project, "collection": collection},
        table=_table,
    )
