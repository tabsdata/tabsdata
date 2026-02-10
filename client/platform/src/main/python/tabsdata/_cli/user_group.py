#
# Copyright 2024 Tabs Data Inc.
#


import rich_click as click
from rich.console import Console
from rich.table import Table

from tabsdata._cli.cli_utils import (
    hint_common_solutions,
    logical_prompt,
    verify_login_or_prompt,
)


@click.group()
def user():
    """User management commands"""


@user.command()
@click.option("--name", "-n", help="Name of the user to create.")
@click.option("--full-name", "-f", help="Full name of the user.")
@click.option("--email", "-e", help="Email of the user.")
@click.option(
    "--password",
    "-p",
    help="Password for the user. Will be prompted for it if not provided.",
)
@click.option("--disabled", is_flag=True, default=False, help="Disable the user.")
@click.pass_context
def create(
    ctx: click.Context,
    name: str,
    full_name: str,
    email: str,
    password: str,
    disabled: bool,
):
    """Create a new user"""
    verify_login_or_prompt(ctx)
    click.echo("Creating a new user")
    click.echo("-" * 10)
    name = name or logical_prompt(ctx, "Name of the user")
    if password is None:
        password = logical_prompt(ctx, "Password for the user", hide_input=True)
        second_password = click.prompt("Please re-enter the password", hide_input=True)
        if password != second_password:
            raise click.ClickException("Passwords do not match.")
    full_name = full_name or logical_prompt(
        ctx, "Full name of the user", default_value=name
    )
    email = email or logical_prompt(ctx, "Email of the user", default_value="")
    try:
        ctx.obj["tabsdataserver"].create_user(
            name,
            password,
            full_name=full_name,
            email=email if email else None,
            enabled=not disabled,
        )
        click.echo("User created successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to create user: {e}")


@user.command()
@click.option("--name", "-n", help="Name of the user to delete.")
@click.option(
    "--confirm",
    help="Write 'delete' to confirm deletion. Will be prompted for it if not provided.",
)
@click.pass_context
def delete(ctx: click.Context, name: str, confirm: str):
    """Delete a user by name"""
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the user to delete")
    click.echo(f"Deleting user: {name}")
    click.echo("-" * 10)
    confirm = confirm or logical_prompt(ctx, "Please type 'delete' to confirm deletion")
    if confirm != "delete":
        raise click.ClickException(
            "Deletion not confirmed. The confirmation word is 'delete'."
        )
    try:
        ctx.obj["tabsdataserver"].delete_user(name)
        click.echo("User deleted successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to delete user: {e}")


@user.command()
@click.option("--name", "-n", help="Name of the user to display.")
@click.pass_context
def info(ctx: click.Context, name: str):
    """Display a user by name"""
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the user to display")
    try:
        user = ctx.obj["tabsdataserver"].get_user(name)

        table = Table(title=f"User '{name}'")
        table.add_column("Name", style="cyan", no_wrap=True)
        table.add_column("Full name")
        table.add_column("Email")
        table.add_column("Enabled")

        table.add_row(user.name, user.full_name, user.email, str(user.enabled))

        click.echo()
        console = Console()
        console.print(table)
        click.echo()
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to display user: {e}")


@user.command()
@click.pass_context
def list(ctx: click.Context):
    """List all users"""
    verify_login_or_prompt(ctx)
    try:
        list_of_users = ctx.obj["tabsdataserver"].users

        table = Table(title="Users")
        table.add_column("Name", style="cyan", no_wrap=True)
        table.add_column("Full name")
        table.add_column("Email")
        table.add_column("Enabled")

        for user in list_of_users:
            table.add_row(user.name, user.full_name, user.email, str(user.enabled))

        click.echo()
        console = Console()
        console.print(table)
        click.echo(f"Number of users: {len(list_of_users)}")
        click.echo()
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list users: {e}")


@user.command()
@click.option("--name", "-n", help="Name of the user to update.")
@click.option("--full-name", "-f", help="New full name of the user.")
@click.option("--email", "-e", help="New email of the user.")
@click.option("--password", "-p", help="New password for the user.")
@click.option(
    "--enabled",
    type=bool,
    default=None,
    help="Indicate if the user is enabled or disabled",
)
@click.pass_context
def update(
    ctx: click.Context,
    name: str,
    full_name: str,
    email: str,
    password: str,
    enabled: bool,
):
    """Update a user by name"""
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the user to update")
    click.echo(f"Updating user: {name}")
    click.echo("-" * 10)
    try:
        ctx.obj["tabsdataserver"].update_user(
            name,
            full_name=full_name,
            email=email,
            password=password,
            enabled=enabled,
        )
        click.echo("User updated successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to update user: {e}")
