#
# Copyright 2024 Tabs Data Inc.
#

import rich_click as click
from rich.console import Console
from rich.table import Table

from tabsdata.api.tabsdata_server import TabsdataServer
from tabsdata.cli.cli_utils import (
    beautify_list,
    logical_prompt,
    verify_login_or_prompt,
)


@click.group()
def auth():
    """User session management commands"""


@auth.command()
@click.pass_context
def info(ctx: click.Context):
    """Display information of the currently authenticated user"""
    verify_login_or_prompt(ctx)
    try:
        info = ctx.obj["tabsdataserver"].auth_info()

        table = Table(title="Current authenticated user")
        table.add_column("Name", style="cyan", no_wrap=True)
        table.add_column("Email")
        table.add_column("Current role")
        table.add_column("All roles")

        table.add_row(
            info["name"],
            info["email"],
            info["current_role"],
            beautify_list(info["roles"]),
        )

        click.echo()
        console = Console()
        console.print(table)
        click.echo()
    except Exception as e:
        raise click.ClickException(f"Failed to display current user information: {e}")


@auth.command()
@click.argument("role", type=str)
@click.pass_context
def role_change(
    ctx: click.Context,
    role: str,
):
    """Change the role of the currently authenticated user"""
    verify_login_or_prompt(ctx)
    click.echo("Changing role")
    click.echo("-" * 10)
    try:
        ctx.obj["tabsdataserver"].role_change(role)
        click.echo("Role changed successfully")
    except Exception as e:
        raise click.ClickException(f"Failed to change role: {e}")


@auth.command()
@click.argument("server-url")
@click.option(
    "--user",
    "-u",
    help="Username for the Tabsdata Server. Will be prompted for it if not provided.",
)
@click.option(
    "--old-password",
    "-p",
    help=(
        "Old password for the Tabsdata Server. It is discouraged to send the password "
        "as a "
        "plain argument, it should be either sent as the value of an environment "
        "variable or written through the prompt. Will be prompted for it if not "
        "provided."
    ),
)
@click.option(
    "--new-password",
    "-n",
    help=(
        "New password for the Tabsdata Server. It is discouraged to send the password "
        "as a "
        "plain argument, it should be either sent as the value of an environment "
        "variable or written through the prompt. Will be prompted for it if not "
        "provided."
    ),
)
@click.pass_context
def password_change(
    ctx: click.Context, server_url: str, user: str, old_password: str, new_password: str
):
    """Change the password of a user in a Tabsdata Server"""
    user = user or logical_prompt(ctx, "Username for the Tabsdata Server")
    password = old_password or logical_prompt(
        ctx,
        "Old password",
        hide_input=True,
    )
    if new_password:
        # If the new password is provided, we don't need to ask for it again
        pass
    else:
        new_password = logical_prompt(
            ctx,
            "New password",
            hide_input=True,
        )
        new_pass_verification = logical_prompt(
            ctx, "(Repeat) New password", hide_input=True
        )
        if new_password != new_pass_verification:
            raise click.ClickException(
                "Two different new passwords were entered. "
                "Please provide the same password twice."
            )
    try:
        # We create a new TabsdataServer instance to avoid using the current session
        # credentials, which might result in a total deadlock if the old password is
        # no longer valid, so the user must log in to change the password, but can't
        # log in until the password is changed.
        server = TabsdataServer(server_url)
        server.password_change(user, password, new_password)
        click.echo("Password changed successfully.")
    except Exception as e:
        raise click.ClickException(f"Failed to change password: {e}")
