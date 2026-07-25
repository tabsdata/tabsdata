#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import TYPE_CHECKING

import rich_click as click
from rich.table import Table

from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    hint_common_solutions,
    logical_prompt,
    make_console,
    output_option,
    render,
    verify_login_or_prompt,
)

if TYPE_CHECKING:
    from tabsdatak.sdk.tabsdata_server import TabsdataServer


@cli_command
@click.group()
def project():
    """Project management commands"""


@project.command()
@click.option("--name", "-n", help="Name of the project to create.")
@click.option("--description", help="Description of the project.")
@click.pass_context
def create(ctx: click.Context, name: str, description: str):
    """Create a new project"""
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the project to create")
    description = description or ""
    click.echo("Creating a new project")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.create_project(name, description)
        click.echo("Project created successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to create project: {e}")


@project.command()
@click.option("--name", "-n", help="Name of the project to delete.")
@click.option(
    "--confirm",
    help="Write 'delete' to confirm deletion. Will be prompted for it if not provided.",
)
@click.pass_context
def delete(ctx: click.Context, name: str, confirm: str):
    """Delete a project by name."""
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the project to delete")
    click.echo(f"Deleting project: {name}")
    click.echo("-" * 10)
    confirm = confirm or logical_prompt(ctx, "Please type 'delete' to confirm deletion")
    if confirm != "delete":
        raise click.ClickException(
            "Deletion not confirmed. The confirmation word is 'delete'."
        )
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.delete_project(name)
        click.echo("Project deleted successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to delete project: {e}")


@project.command()
@output_option
@click.option("--name", "-n", help="Name of the project to display.")
@click.pass_context
def get(ctx: click.Context, name: str):
    """Display a project by name"""
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the project to display")
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        prj = server.get_project(name)

        def _table():
            table = Table(title="Project", show_header=False, show_lines=True)
            table.add_column("Field", style="cyan", no_wrap=True)
            table.add_column("Value")
            table.add_row("Name:", prj.name)
            table.add_row("Description:", prj.description or "")
            table.add_row("Created by:", prj.created_by or "-")
            table.add_row("Created on:", prj.created_on_str or "-")
            click.echo()
            make_console(emoji=False, markup=False).print(table)
            click.echo()

        render(ctx, data=lambda: prj._data, table=_table)
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to display project: {e}")


@project.command()
@output_option
@click.pass_context
def list(ctx: click.Context):
    """List all projects"""
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        list_of_projects = server.projects

        def _table():
            table = Table(title="Projects")
            table.add_column("Name", style="cyan", no_wrap=True)
            table.add_column("Description")
            table.add_column("Created by")
            table.add_column("Created on")
            for prj in list_of_projects:
                table.add_row(
                    prj.name,
                    prj.description,
                    prj.created_by,
                    prj.created_on_str,
                )
            click.echo()
            make_console().print(table)
            click.echo(f"Number of projects: {len(list_of_projects)}")
            click.echo()

        render(ctx, data=lambda: [p._data for p in list_of_projects], table=_table)
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list projects: {e}")


@project.command()
@click.option("--name", "-n", help="Name of the project to update.")
@click.option("--new-name", help="New name for the project.")
@click.option("--description", help="New description for the project.")
@click.pass_context
def update(
    ctx: click.Context,
    name: str,
    new_name: str,
    description: str,
):
    """Update a project by name"""
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the project to update")
    click.echo(f"Updating project: {name}")
    click.echo("-" * 10)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        server.update_project(name, new_name=new_name, new_description=description)
        click.echo("Project updated successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to update project: {e}")
