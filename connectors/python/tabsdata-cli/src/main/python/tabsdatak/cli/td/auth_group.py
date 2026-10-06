#
# Copyright 2026 Tabsdata Inc.
#

import os
import shutil
import sys
from pathlib import Path

import rich_click as click
from rich.table import Table

from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    beautify_list,
    format_option,
    logical_prompt,
    make_console,
    render,
    verify_login_or_prompt,
)
from tabsdatak.common._datetime import _convert_timestamp_to_string


@cli_command
@click.group()
def auth():
    """User session management commands"""


@auth.command()
@click.option("--server", "-s", help="Tabsdata Server URL")
@click.option(
    "--pem",
    help="Path to the certificate PEM file, or '-' to read it from stdin.",
)
@click.pass_context
def add_cert(ctx: click.Context, server: str, pem: str):
    """
    Add a certificate for a Tabsdata Server.
    Only needed for self-signed certificates.
    """
    # noinspection PyProtectedMember
    from tabsdatak.sdk._apiserver import (
        DEFAULT_TABSDATA_CERTIFICATE_FOLDER,
        HTTP_PROTOCOL,
        HTTPS_PROTOCOL,
        _obtain_certificate_file_path,
    )

    server = server or logical_prompt(ctx, "Tabsdata Server URL")
    pem = pem or logical_prompt(ctx, "Path to the certificate PEM file")
    click.echo("Adding certificate")
    click.echo("-" * 10)
    try:
        if not server.startswith(HTTP_PROTOCOL) and not server.startswith(
            HTTPS_PROTOCOL
        ):
            server = HTTPS_PROTOCOL + server
        certificate_path = _obtain_certificate_file_path(server)
        os.makedirs(DEFAULT_TABSDATA_CERTIFICATE_FOLDER, exist_ok=True)
        if pem == "-":
            Path(certificate_path).write_text(sys.stdin.read())
        else:
            shutil.copy(pem, certificate_path)
        click.echo(f"Certificate added successfully for server '{server}'")
        click.echo(f"Certificate stored at '{certificate_path}'")
    except Exception as e:
        raise click.ClickException(f"Failed to add certificate for server: {e}")


@auth.command()
@click.option("--server", "-s", help="Tabsdata Server URL")
@click.pass_context
def delete_cert(ctx: click.Context, server: str):
    """Delete a certificate for a Tabsdata Server"""
    # noinspection PyProtectedMember
    from tabsdatak.sdk._apiserver import (
        HTTP_PROTOCOL,
        HTTPS_PROTOCOL,
        _obtain_certificate_file_path,
    )

    server = server or logical_prompt(ctx, "Tabsdata Server URL")
    click.echo("Deleting certificate")
    click.echo("-" * 10)
    try:
        if not server.startswith(HTTP_PROTOCOL) and not server.startswith(
            HTTPS_PROTOCOL
        ):
            server = HTTPS_PROTOCOL + server
        certificate_path = _obtain_certificate_file_path(server)
        if os.path.exists(certificate_path):
            os.remove(certificate_path)
            click.echo(f"Certificate deleted successfully for server '{server}'")
        else:
            click.echo(
                "No certificate found for the specified server. It should be "
                f"stored at '{certificate_path}'."
            )
    except Exception as e:
        raise click.ClickException(f"Failed to delete certificate for server: {e}")


@auth.command()
@format_option
@click.pass_context
def info(ctx: click.Context):
    """Display information of the currently authenticated user"""
    verify_login_or_prompt(ctx)
    try:
        info = ctx.obj["tabsdataserver"].auth_info()

        def _table():
            table = Table(title="Current authenticated user")
            table.add_column("Name", style="cyan", no_wrap=True)
            table.add_column("Email")
            table.add_column("Roles")
            table.add_row(
                info["name"],
                info["email"],
                beautify_list(info["roles"]),
            )
            click.echo()
            make_console().print(table)
            click.echo()

        render(ctx, data=info, table=_table)
    except Exception as e:
        raise click.ClickException(f"Failed to display current user information: {e}")


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
    if not new_password:
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
    from tabsdatak.sdk.tabsdata_server import TabsdataServer

    try:
        # We create a new TabsdataServer instance to avoid using the current session
        # credentials, which might result in a total deadlock if the old password is
        # no longer valid, so the user must log in to change the password, but can't
        # log in until the password is changed.
        server = TabsdataServer(server_url)
        server.change_password(user, password, new_password)
        click.echo("Password changed successfully.")
    except Exception as e:
        raise click.ClickException(f"Failed to change password: {e}")


def _moment(timestamp: int | None, missing: str) -> str:
    return _convert_timestamp_to_string(timestamp) if timestamp else missing


def _whole_number(ctx: click.Context, message: str) -> int:
    answer = logical_prompt(ctx, message)
    try:
        return int(str(answer).strip())
    except ValueError:
        raise click.ClickException(f"{message}: '{answer}' is not a whole number.")


@auth.group(name="service-token")
def service_token():
    """Service token management commands"""


@service_token.command(name="create")
@click.option("--slot", type=click.IntRange(1, 2), help="Which slot to fill, 1 or 2.")
@click.option(
    "--expires-in-days",
    type=int,
    help=(
        "Days until the token expires, or -1 for a token that never expires "
        "(only where the deployment permits it). There is no default."
    ),
)
@click.option("--comment", help="A label, so the two slots tell themselves apart.")
@format_option
@click.pass_context
def service_token_create(
    ctx: click.Context, slot: int, expires_in_days: int, comment: str
):
    """Create a service token in one of your two slots.

    The token is shown once, here, and cannot be recovered afterwards: the
    server keeps only a SHA-256 digest of it.
    """
    verify_login_or_prompt(ctx)
    slot = slot or _whole_number(ctx, "Slot to fill (1 or 2)")
    if expires_in_days is None:
        expires_in_days = _whole_number(ctx, "Days until it expires (-1 for never)")
    try:
        created = ctx.obj["tabsdataserver"].create_service_token(
            slot, expires_in_days, comment=comment
        )

        def _table():
            table = Table(title="Service token created")
            table.add_column("Slot", style="cyan", no_wrap=True)
            table.add_column("Token")
            table.add_column("Expires on")
            table.add_row(
                str(created["slot"]),
                created["token"],
                _moment(created.get("expires_on"), "never"),
            )
            click.echo()
            make_console().print(table)
            click.echo()
            click.echo(
                "Store it now: this is the only time the token is shown. Pass "
                "it to 'tdk login --token', or export it as TDK_TOKEN and run "
                "'tdk login --token' with no value."
            )
            click.echo()

        render(ctx, data=created, table=_table)
    except Exception as e:
        raise click.ClickException(f"Failed to create the token: {e}")


@service_token.command(name="list")
@format_option
@click.pass_context
def service_token_list(ctx: click.Context):
    """List your service tokens.

    `Last used` has a resolution of ten minutes, which is what the rotation
    runbook needs to tell a retired token from one still in production.
    """
    verify_login_or_prompt(ctx)
    try:
        tokens = ctx.obj["tabsdataserver"].list_service_tokens()

        def _table():
            table = Table(title="Service tokens")
            table.add_column("Slot", style="cyan", no_wrap=True)
            table.add_column("Hint")
            table.add_column("Comment")
            table.add_column("Created on")
            table.add_column("Expires on")
            table.add_column("Last used")
            for entry in tokens:
                table.add_row(
                    str(entry["slot"]),
                    entry["hint"],
                    entry.get("comment") or "",
                    _moment(entry["created_on"], "-"),
                    _moment(entry.get("expires_on"), "never"),
                    _moment(entry.get("last_used_on"), "never"),
                )
            click.echo()
            make_console().print(table)
            click.echo()

        render(ctx, data=tokens, table=_table)
    except Exception as e:
        raise click.ClickException(f"Failed to list the tokens: {e}")


@service_token.command(name="revoke")
@click.option("--slot", type=click.IntRange(1, 2), help="Which slot to free, 1 or 2.")
@click.pass_context
def service_token_revoke(ctx: click.Context, slot: int):
    """Revoke the service token in one of your slots.

    The slot becomes free for the next rotation, and the token stops working
    immediately.
    """
    verify_login_or_prompt(ctx)
    slot = slot or _whole_number(ctx, "Slot to revoke (1 or 2)")
    try:
        ctx.obj["tabsdataserver"].revoke_service_token(slot)
        click.echo(f"Token in slot {slot} revoked successfully")
    except Exception as e:
        raise click.ClickException(f"Failed to revoke the token: {e}")
