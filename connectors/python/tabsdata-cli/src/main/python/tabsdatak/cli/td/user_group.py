#
# Copyright 2026 Tabsdata Inc.
#


import rich_click as click

from tabsdatak.cli._capability import capability
from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    format_option,
    hint_common_solutions,
    list_options,
    list_params,
    logical_prompt,
    max_result_option,
    present,
    progress,
    render,
    render_deleted,
    render_entity,
    render_list,
    take,
    verify_login_or_prompt,
    view_options,
)
from tabsdatak.common._tabular import Column, Kind, RecordView, TableView


@cli_command
@click.group()
def user():
    """User management commands"""


@capability("admin", "write", credential="session")
@user.command()
@format_option
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
    progress(ctx, "Creating a new user")
    progress(ctx, "-" * 10)
    name = name or logical_prompt(ctx, "Name of the user", option="--name")
    if password is None:
        password = logical_prompt(
            ctx, "Password for the user", hide_input=True, option="--password"
        )
        second_password = logical_prompt(
            ctx, "Please re-enter the password", hide_input=True, option="--password"
        )
        if password != second_password:
            raise click.ClickException("Passwords do not match.")
    full_name = full_name or logical_prompt(
        ctx, "Full name of the user", default_value=name, option="--full-name"
    )
    email = email or logical_prompt(
        ctx, "Email of the user", default_value="", option="--email"
    )
    try:
        user = ctx.obj["tabsdataserver"].create_user(
            name,
            password,
            full_name=full_name,
            email=email if email else None,
            enabled=not disabled,
        )
        render_entity(ctx, data=lambda: user._data, message="User created successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to create user: {e}")


@capability("admin", "destructive", credential="session")
@user.command()
@format_option
@click.option("--name", "-n", help="Name of the user to delete.")
@click.option(
    "--confirm",
    help="Write 'delete' to confirm deletion. Will be prompted for it if not provided.",
)
@click.pass_context
def delete(ctx: click.Context, name: str, confirm: str):
    """Delete a user by name"""
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the user to delete", option="--name")
    progress(ctx, f"Deleting user: {name}")
    progress(ctx, "-" * 10)
    confirm = confirm or logical_prompt(
        ctx, "Please type 'delete' to confirm deletion", option="--confirm delete"
    )
    if confirm != "delete":
        raise click.ClickException(
            "Deletion not confirmed. The confirmation word is 'delete'."
        )
    try:
        ctx.obj["tabsdataserver"].delete_user(name)
        render_deleted(ctx, kind="user", name=name, message="User deleted successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to delete user: {e}")


@capability("admin", "read", credential="session")
@user.command()
@format_option
@view_options()
@click.option("--name", "-n", help="Name of the user to display.")
@click.pass_context
def get(ctx: click.Context, name: str):
    """Display a user by name"""
    verify_login_or_prompt(ctx)
    name = name or logical_prompt(ctx, "Name of the user to display", option="--name")
    try:
        user = ctx.obj["tabsdataserver"].get_user(name)

        def _view():
            return RecordView(
                title="User",
                fields=[
                    ("Name", user.name),
                    ("Full name", present(user.full_name)),
                    ("Email", present(user.email)),
                    ("Enabled", user.enabled),
                ],
            )

        render(ctx, data=lambda: user._data, view=_view)
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to display user: {e}")


@capability("admin", "read", credential="session")
@user.command(name="list")
@format_option
@view_options()
@max_result_option
@list_options
@click.pass_context
def list_users(ctx: click.Context):
    """List all users"""
    verify_login_or_prompt(ctx)
    try:
        list_of_users, truncated = take(
            ctx, ctx.obj["tabsdataserver"].list_users_generator(**list_params(ctx))
        )

        def _view():
            return TableView(
                title="Users",
                columns=[
                    Column("Name", Kind.ID),
                    Column("Full name"),
                    Column("Email"),
                    Column("Enabled", Kind.FLAG),
                ],
                rows=[
                    [
                        user.name,
                        present(user.full_name),
                        present(user.email),
                        user.enabled,
                    ]
                    for user in list_of_users
                ],
                count="users",
            )

        render_list(
            ctx,
            items=lambda: [u._data for u in list_of_users],
            truncated=truncated,
            view=_view,
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to list users: {e}")


@capability("admin", "write", credential="session")
@user.command()
@format_option
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
    name = name or logical_prompt(ctx, "Name of the user to update", option="--name")
    progress(ctx, f"Updating user: {name}")
    progress(ctx, "-" * 10)
    try:
        user = ctx.obj["tabsdataserver"].update_user(
            name,
            full_name=full_name,
            email=email,
            password=password,
            enabled=enabled,
        )
        render_entity(ctx, data=lambda: user._data, message="User updated successfully")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to update user: {e}")
