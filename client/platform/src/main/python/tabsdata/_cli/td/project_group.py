#
# Copyright 2026 Tabs Data Inc.
#


import rich_click as click
from rich.console import Console
from rich.table import Table

from tabsdata._cli.cli_utils import (
    hint_common_solutions,
    logical_prompt,
    verify_login_or_prompt,
)
from tabsdata.api.tabsdata_server import TabsdataServer


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
    """Delete a project by name. The sandbox project cannot be deleted on
    single-project backends."""
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
@click.option("--name", "-n", help="Name of the project to display.")
@click.pass_context
def info(ctx: click.Context, name: str):
    """Display a project by name"""
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the project to display")
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        prj = server.get_project(name)

        table = Table(title=f"Project '{name}'")
        table.add_column("Name", style="cyan", no_wrap=True)
        table.add_column("ID", no_wrap=True)
        table.add_column("Description")
        table.add_column("Created on")
        table.add_column("Created by")

        table.add_row(
            prj.name,
            prj.id,
            prj.description,
            prj.created_on_str,
            prj.created_by,
        )

        click.echo()
        Console().print(table)
        click.echo()
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to display project: {e}")


@project.command()
@click.pass_context
def list(ctx: click.Context):
    """List all projects"""
    verify_login_or_prompt(ctx)
    try:
        server: TabsdataServer = ctx.obj["tabsdataserver"]
        list_of_projects = server.projects

        table = Table(title="Projects")
        table.add_column("Name", style="cyan", no_wrap=True)
        table.add_column("ID", no_wrap=True)
        table.add_column("Description")
        table.add_column("Created on")
        table.add_column("Created by")

        for prj in list_of_projects:
            table.add_row(
                prj.name,
                prj.id,
                prj.description,
                prj.created_on_str,
                prj.created_by,
            )

        click.echo()
        Console().print(table)
        click.echo(f"Number of projects: {len(list_of_projects)}")
        click.echo()
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
