#
# Copyright 2024 Tabs Data Inc.
#


import rich_click as click
from rich.console import Console
from rich.table import Table

from tabsdata.cli.cli_utils import logical_prompt, verify_login_or_prompt


@click.group()
@click.pass_context
def user(ctx: click.Context):
    """User management commands"""
    verify_login_or_prompt(ctx)


@user.command()
@click.argument("name")
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
    click.echo("Creating a new user")
    click.echo("-" * 10)
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
        ctx.obj["tabsdataserver"].user_create(
            name,
            password,
            full_name=full_name,
            email=email if email else None,
            enabled=not disabled,
        )
        click.echo("User created successfully")
    except Exception as e:
        raise click.ClickException(f"Failed to create user: {e}")


@user.command()
@click.argument("name")
@click.option(
    "--confirm",
    help="Write 'delete' to confirm deletion. Will be prompted for it if not provided.",
)
@click.pass_context
def delete(ctx: click.Context, name: str, confirm: str):
    """Delete a user by name"""
    click.echo(f"Deleting user: {name}")
    click.echo("-" * 10)
    confirm = confirm or logical_prompt(ctx, "Please type 'delete' to confirm deletion")
    if confirm != "delete":
        raise click.ClickException(
            "Deletion not confirmed. The confirmation word is 'delete'."
        )
    try:
        ctx.obj["tabsdataserver"].user_delete(name)
        click.echo("User deleted successfully")
    except Exception as e:
        raise click.ClickException(f"Failed to delete user: {e}")


@user.command()
@click.argument("name")
@click.pass_context
def info(ctx: click.Context, name: str):
    """Display a user by name"""
    try:
        user = ctx.obj["tabsdataserver"].user_get(name)

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
        raise click.ClickException(f"Failed to display user: {e}")


@user.command()
@click.pass_context
def list(ctx: click.Context):
    """List all users"""
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
        raise click.ClickException(f"Failed to list users: {e}")


@user.command()
@click.argument("name")
@click.option("--full-name", "-f", help="Full name of the user.")
@click.option("--email", "-e", help="Email of the user.")
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
    enabled: bool,
):
    """Update a user by name"""
    # TODO: Implement change password logic, for now only full name, email
    #  and disabled are updated
    click.echo(f"Updating user: {name}")
    click.echo("-" * 10)
    try:
        ctx.obj["tabsdataserver"].user_update(
            name,
            full_name=full_name,
            email=email,
            enabled=enabled,
        )
        click.echo("User updated successfully")
    except Exception as e:
        raise click.ClickException(f"Failed to update user: {e}")
