#
# Copyright 2026 Tabsdata Inc.
#

import os
import shutil
import sys
from pathlib import Path

import rich_click as click

from tabsdatak.cli._capability import capability
from tabsdatak.cli._credentials import (
    SERVICE_TOKEN,
    credential_kind,
    ensure_stateful,
)
from tabsdatak.cli._registry import cli_command
from tabsdatak.cli.cli_utils import (
    deprecated_option,
    format_option,
    logical_prompt,
    present,
    progress,
    render,
    render_done,
    render_list,
    verify_login_or_prompt,
    view_options,
)
from tabsdatak.common._datetime import _convert_timestamp_to_string
from tabsdatak.common._tabular import Column, Kind, RecordView, TableView


@cli_command
@click.group()
def auth():
    """User session management commands"""


@capability("session", "write", "read", paths={"--pem": "read"})
@auth.command()
@format_option
@click.option("--server", "-s", help="Tabsdata Server URL")
@click.option(
    "--pem",
    help="Path to the certificate PEM file, or '-' to read it from stdin.",
    type=click.Path(allow_dash=True),
)
@click.pass_context
def add_cert(ctx: click.Context, server: str, pem: str):
    """
    Add a certificate for a Tabsdata Server.
    Only needed for self-signed certificates.
    """
    ensure_stateful(ctx, "tdk auth add-cert")
    # noinspection PyProtectedMember
    from tabsdatak.sdk._apiserver import (
        DEFAULT_TABSDATA_CERTIFICATE_FOLDER,
        HTTP_PROTOCOL,
        HTTPS_PROTOCOL,
        _obtain_certificate_file_path,
    )

    server = server or logical_prompt(ctx, "Tabsdata Server URL", option="--server")
    pem = pem or logical_prompt(ctx, "Path to the certificate PEM file", option="--pem")
    progress(ctx, "Adding certificate")
    progress(ctx, "-" * 10)
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
        render_done(
            ctx,
            action="add",
            kind="certificate",
            id=server,
            details={"path": certificate_path},
            message=(
                f"Certificate added successfully for server '{server}'\n"
                f"Certificate stored at '{certificate_path}'"
            ),
        )
    except Exception as e:
        raise click.ClickException(f"Failed to add certificate for server: {e}")


@capability("session", "write")
@auth.command()
@format_option
@click.option("--server", "-s", help="Tabsdata Server URL")
@click.pass_context
def delete_cert(ctx: click.Context, server: str):
    """Delete a certificate for a Tabsdata Server"""
    ensure_stateful(ctx, "tdk auth delete-cert")
    # noinspection PyProtectedMember
    from tabsdatak.sdk._apiserver import (
        HTTP_PROTOCOL,
        HTTPS_PROTOCOL,
        _obtain_certificate_file_path,
    )

    server = server or logical_prompt(ctx, "Tabsdata Server URL", option="--server")
    progress(ctx, "Deleting certificate")
    progress(ctx, "-" * 10)
    try:
        if not server.startswith(HTTP_PROTOCOL) and not server.startswith(
            HTTPS_PROTOCOL
        ):
            server = HTTPS_PROTOCOL + server
        certificate_path = _obtain_certificate_file_path(server)
        found = os.path.exists(certificate_path)
        if found:
            os.remove(certificate_path)
            message = f"Certificate deleted successfully for server '{server}'"
        else:
            message = (
                "No certificate found for the specified server. It should be "
                f"stored at '{certificate_path}'."
            )
        render_done(
            ctx,
            action="delete",
            kind="certificate",
            id=server,
            details={"path": certificate_path, "changed": found},
            message=message,
        )
    except Exception as e:
        raise click.ClickException(f"Failed to delete certificate for server: {e}")


@capability("session", "read")
@auth.command()
@format_option
@view_options()
@click.pass_context
def info(ctx: click.Context):
    """Display information of the currently authenticated user"""
    verify_login_or_prompt(ctx)
    try:
        server = ctx.obj["tabsdataserver"]
        info = {
            **server.auth_info(),
            "credential": credential_kind(server.connection.bearer_token or ""),
        }
        if info["credential"] == SERVICE_TOKEN:
            token = server.auth_service_token()
            info.update({key: token.get(key) for key in ("slot", "hint", "expires_on")})

        def _view():
            return RecordView(
                title="Current authenticated user",
                fields=[
                    ("Name", info["name"]),
                    ("Email", present(info["email"])),
                    ("Roles", present(", ".join(info["roles"] or []))),
                    ("Credential", present(info["credential"])),
                ],
            )

        render(ctx, data=info, view=_view)
    except Exception as e:
        raise click.ClickException(f"Failed to display current user information: {e}")


@capability("session", "write", credential="session", interactive=True)
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
    help=(
        "New password for the Tabsdata Server. It is discouraged to send the password "
        "as a "
        "plain argument, it should be either sent as the value of an environment "
        "variable or written through the prompt. Will be prompted for it if not "
        "provided."
    ),
)
@click.option(
    "-n",
    "new_password_short",
    hidden=True,
    callback=deprecated_option("-n", "'--new-password'"),
)
@click.pass_context
def password_change(
    ctx: click.Context,
    server_url: str,
    user: str,
    old_password: str,
    new_password: str,
    new_password_short: str,
):
    """Change the password of a user in a Tabsdata Server"""
    new_password = new_password or new_password_short
    user = user or logical_prompt(
        ctx, "Username for the Tabsdata Server", option="--user"
    )
    password = old_password or logical_prompt(
        ctx,
        "Old password",
        hide_input=True,
        option="--old-password",
    )
    if not new_password:
        new_password = logical_prompt(
            ctx,
            "New password",
            hide_input=True,
            option="--new-password",
        )
        new_pass_verification = logical_prompt(
            ctx, "(Repeat) New password", hide_input=True, option="--new-password"
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


def _whole_number(ctx: click.Context, message: str, option: str) -> int:
    answer = logical_prompt(ctx, message, option=option)
    try:
        return int(str(answer).strip())
    except ValueError:
        raise click.ClickException(f"{message}: '{answer}' is not a whole number.")


@auth.group(name="service-token")
def service_token():
    """Service token management commands"""


@capability("admin", "write", credential="session")
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
@view_options()
@click.pass_context
def service_token_create(
    ctx: click.Context, slot: int, expires_in_days: int, comment: str
):
    """Create a service token in one of your two slots.

    The token is shown once, here, and cannot be recovered afterwards: the
    server keeps only a SHA-256 digest of it.
    """
    verify_login_or_prompt(ctx)
    slot = slot or _whole_number(ctx, "Slot to fill (1 or 2)", "--slot")
    if expires_in_days is None:
        expires_in_days = _whole_number(
            ctx, "Days until it expires (-1 for never)", "--expires-in-days"
        )
    try:
        created = ctx.obj["tabsdataserver"].create_service_token(
            slot, expires_in_days, comment=comment
        )

        def _view():
            return RecordView(
                title="Service token created",
                fields=[
                    ("Slot", created["slot"]),
                    ("Token", created["token"]),
                    ("Expires on", _moment(created.get("expires_on"), "never")),
                ],
                widths={"Token": 0},
                notes=[
                    "Store it now: this is the only time the token is shown. Pass "
                    "it to 'tdk login --token', or export it as TDK_TOKEN and run "
                    "'tdk login --token' with no value. With TDK_SERVER exported "
                    "too, tdk uses it directly, without logging in."
                ],
            )

        render(ctx, data=created, view=_view)
    except Exception as e:
        raise click.ClickException(f"Failed to create the token: {e}")


@capability("admin", "read", credential="session")
@service_token.command(name="list")
@format_option
@view_options()
@click.pass_context
def service_token_list(ctx: click.Context):
    """List your service tokens.

    `Last used` has a resolution of ten minutes, which is what the rotation
    runbook needs to tell a retired token from one still in production.
    """
    verify_login_or_prompt(ctx)
    try:
        tokens = ctx.obj["tabsdataserver"].list_service_tokens()

        def _view():
            return TableView(
                title="Service tokens",
                columns=[
                    Column("Slot", Kind.ID),
                    Column("Hint"),
                    Column("Comment"),
                    Column("Created on", Kind.TIME),
                    Column("Expires on", Kind.TIME),
                    Column("Last used", Kind.TIME),
                ],
                rows=[
                    [
                        entry["slot"],
                        entry["hint"],
                        present(entry.get("comment")),
                        present(_moment(entry["created_on"], "")),
                        _moment(entry.get("expires_on"), "never"),
                        _moment(entry.get("last_used_on"), "never"),
                    ]
                    for entry in tokens
                ],
                count="service tokens",
            )

        render_list(ctx, items=tokens, truncated=False, view=_view)
    except Exception as e:
        raise click.ClickException(f"Failed to list the tokens: {e}")


@capability("admin", "destructive", credential="session")
@service_token.command(name="revoke")
@format_option
@click.option("--slot", type=click.IntRange(1, 2), help="Which slot to free, 1 or 2.")
@click.pass_context
def service_token_revoke(ctx: click.Context, slot: int):
    """Revoke the service token in one of your slots.

    The slot becomes free for the next rotation, and the token stops working
    immediately.
    """
    verify_login_or_prompt(ctx)
    slot = slot or _whole_number(ctx, "Slot to revoke (1 or 2)", "--slot")
    try:
        ctx.obj["tabsdataserver"].revoke_service_token(slot)
        render_done(
            ctx,
            action="revoke",
            kind="service_token",
            id=str(slot),
            message=f"Token in slot {slot} revoked successfully",
        )
    except Exception as e:
        raise click.ClickException(f"Failed to revoke the token: {e}")
