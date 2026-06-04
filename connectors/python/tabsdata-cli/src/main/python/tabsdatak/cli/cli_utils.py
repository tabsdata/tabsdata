#
# Copyright 2026 Tabs Data Inc.
#

"""Shared helpers for the ``td`` CLI.

Also hosts two **temporary** stand-ins -- :func:`tdabout` and
:func:`td_resource` -- that mirror the legacy
``tabsdata._utils.internal._about.tdabout`` and
``tabsdata._utils.internal._resources.td_resource`` surface so the
``td`` migration can land without dragging in the explicitly-excluded
``_utils/internal/`` subtree. Both are placeholders pending proper
v2 ports in a future ticket (see TODO blocks below)."""

from __future__ import annotations

import json
import os
import platform
import re
import sys
from importlib.metadata import PackageNotFoundError, version
from importlib.resources import as_file, files
from pathlib import Path

import requests
import rich_click as click
from rich_click import Option, UsageError

# noinspection PyProtectedMember
from tabsdatak.sdk._apiserver import (
    BASE_API_URL,
    DEFAULT_TABSDATA_DIRECTORY,
    APIServer,
    APIServerError,
    obtain_connection,
)
from tabsdatak.sdk.tabsdata_server import Function, TabsdataServer

CONNECTION_FILE = "connection.json"


# ---------------------------------------------------------------------------
# Temporary stand-ins for legacy ``_utils.internal._about.tdabout`` and
# ``_utils.internal._resources.td_resource``.
#
# TODO(tabsdatak-migration): these are placeholders so the ``td`` CLI
# migration can land without porting the explicitly-excluded
# ``tabsdata/_utils/internal/`` subtree. Replace with proper v2
# implementations in a follow-up ticket once the v2 packaging /
# resource-layout story is settled. Until then:
#   * ``tdabout`` prints a one-line banner via importlib.metadata.
#   * ``td_resource`` resolves a packaged resource path via
#     importlib.resources rooted at the ``tabsdatak.cli`` package.
# ---------------------------------------------------------------------------


_DEFAULT_ABOUT_PACKAGE = "tabsdata-cli"
_DEFAULT_RESOURCE_PACKAGE = "tabsdatak.cli"


def tdabout(package_name: str = _DEFAULT_ABOUT_PACKAGE) -> None:
    """Print a one-line banner naming the package and its version.

    TODO(tabsdatak-migration): replace with the proper v2 ``tdabout``
    once the legacy ``_utils/internal/_about.py`` is ported. The
    current placeholder is intentionally minimal -- it does not walk
    every related package or read build-time manifests.
    """
    try:
        pkg_version = version(package_name)
    except PackageNotFoundError:
        pkg_version = "unknown"
    print(f"{package_name} {pkg_version}", file=sys.stdout)


def td_resource(
    resource: str,
    *,
    package: str = _DEFAULT_RESOURCE_PACKAGE,
) -> Path:
    """Resolve a packaged resource path using ``importlib.resources``.

    TODO(tabsdatak-migration): replace with the proper v2
    ``td_resource`` once the legacy
    ``_utils/internal/_resources.py`` is ported. The current
    placeholder does NOT implement the editable-package overlay
    behaviour of the legacy helper.
    """
    parts = Path(resource.replace("\\", "/")).parts
    traversable = files(package).joinpath(*parts)
    with as_file(traversable) as path:
        if not path.exists():
            raise FileNotFoundError(
                f"Resource '{resource}' not found in package '{package}'"
            )
        return path


def is_valid_id(possible_id: str) -> bool:
    """
    Check if the provided string is a valid ID for an APIServer entity,
    for example a worker.
    A valid ID is a 26 characters long, all capital letters and digits.
    For now only needed in the CLI, if it ever is needed in more places it should be
    moved to general utils.
    """
    id_pattern = re.compile(r"^([A-Z0-9]{26})$")
    return bool(id_pattern.match(possible_id))


def beautify_list(list_to_show) -> str:
    if not list_to_show:
        return "<None>"
    if isinstance(list_to_show, list):
        return "\n".join(list_to_show)
    return str(list_to_show)


def beautify_time(time: str) -> str:
    if time == "None":
        return "-"
    return time


def prompt_with_escape(message, default=None, hide_input=False):
    """Prompt using prompt_toolkit so that both Ctrl+C and ESC abort."""
    from prompt_toolkit import PromptSession
    from prompt_toolkit.key_binding import KeyBindings

    kb = KeyBindings()

    @kb.add("escape")
    def _(event):
        event.app.exit(exception=KeyboardInterrupt)

    suffix = ": "
    if default is not None:
        prompt_text = f"{message} [{default}]{suffix}"
    else:
        prompt_text = f"{message}{suffix}"

    session = PromptSession()
    session.app.ttimeoutlen = 0.1
    session.app.timeoutlen = 0.1

    result = session.prompt(
        prompt_text,
        key_bindings=kb,
        is_password=hide_input,
    )

    return result if result else default


def logical_prompt(
    ctx: click.Context, message: str, default_value=None, hide_input: bool = False
):
    """
    Prompt the user for a value if prompt is enabled. Otherwise, either return the
        default value, or raise an error.
    """

    if ctx.obj["no_prompt"]:
        if default_value is None:
            raise click.ClickException(
                "Prompting is disabled and some required "
                "values are missing. Please provide the "
                "required values or avoid using '--no-prompt'."
            )
        return default_value
    try:
        import sys

        if sys.stdin.isatty():
            return prompt_with_escape(
                message, default=default_value, hide_input=hide_input
            )
        else:
            return click.prompt(message, default=default_value, hide_input=hide_input)
    except (KeyboardInterrupt, EOFError):
        click.echo()
        raise click.exceptions.Exit(0)


def utils_login(
    ctx: click.Context, server_url: str, username: str, password: str, role: str = None
):
    try:
        obtain_connection(
            server_url,
            username,
            password,
            role=role,
            credentials_file=get_credentials_file_path(ctx),
        )
    except Exception as e:
        if isinstance(e, requests.exceptions.ConnectionError):
            show_hint(
                ctx,
                "It seems like there is no Tabsdata server at the "
                f"provided URL: {server_url}.\n "
                "Please ensure that the URL is correct and the server is "
                "running and reachable.\n If started locally, the status can be "
                "checked by "
                "executing 'tdserver status'.",
            )
        elif isinstance(e, APIServerError):
            show_hint(
                ctx,
                "It seems like the Tabsdata server is running but refusing "
                "to login. Please ensure that the credentials are correct and "
                "the server is healthy.",
            )
        raise click.ClickException(f"Failed to login: {e}")
    click.echo("Login successful.")


def request_login_information(ctx: click.Context):
    server_url = logical_prompt(ctx, "Server URL")
    username = logical_prompt(ctx, "Username")
    password = logical_prompt(ctx, "Password", hide_input=True)
    role = logical_prompt(ctx, "Role", default_value="user")
    return server_url, username, password, role


def initialise_tabsdata_server_connection(ctx: click.Context):
    try:
        with open(os.path.join(DEFAULT_TABSDATA_DIRECTORY, CONNECTION_FILE)) as f:
            credentials = json.load(f)
        connection = APIServer(
            credentials.get("url"),
            credentials_file=get_credentials_file_path(ctx),
        )
        connection.refresh_token = credentials.get("refresh_token")
        connection.bearer_token = credentials.get("bearer_token")
        connection.token_type = credentials.get("token_type")
        connection.expires_in = credentials.get("expires_in")
        connection.expiration_time = credentials.get("expiration_time")
        tabsdata_server = TabsdataServer.__new__(TabsdataServer)
        tabsdata_server.connection = connection
    except FileNotFoundError:
        tabsdata_server = None
    ctx.obj["tabsdataserver"] = tabsdata_server


def get_pinned_objects_file_path(ctx: click.Context) -> str:
    """
    Get the path to the pinned objects file.
    """
    return os.path.join(ctx.obj["tabsdata_directory"], "pinned.json")


def get_currently_pinned_object(ctx: click.Context, object: str) -> str | None:
    """
    Get the currently pinned object from the context.
    """
    currently_pinned = ctx.obj["pinned_objects"].get(object, None)
    if currently_pinned:
        click.echo(f"Using currently pinned {object}: {currently_pinned}")
    return currently_pinned


def load_pinned_objects(ctx: click.Context):
    try:
        with open(get_pinned_objects_file_path(ctx)) as f:
            pinned_objects = json.load(f)
        ctx.obj["pinned_objects"] = pinned_objects
    except FileNotFoundError:
        ctx.obj["pinned_objects"] = {}


def store_pinned_objects(ctx: click.Context):
    try:
        with open(get_pinned_objects_file_path(ctx), "w") as f:
            json.dump(ctx.obj["pinned_objects"], f)
    except Exception as e:
        click.echo(f"Failed to store pinned objects: {e}")
        click.echo("This will not affect the rest of the command execution.")


def verify_login_or_prompt(ctx: click.Context):
    if not ctx.obj["tabsdataserver"]:
        click.echo("No credentials found. Please login first.")
        server_url, username, password, role = request_login_information(ctx)
        utils_login(ctx, server_url, username, password, role)
        initialise_tabsdata_server_connection(ctx)


class MutuallyExclusiveOption(Option):
    def __init__(self, *args, **kwargs):
        self.mutually_exclusive = set(kwargs.pop("mutually_exclusive", []))
        help = kwargs.get("help", "")
        if self.mutually_exclusive:
            ex_str = ", ".join(self.mutually_exclusive)
            kwargs["help"] = help + (
                " NOTE: This argument is mutually exclusive with  arguments: ["
                + ex_str
                + "]."
            )
        super(MutuallyExclusiveOption, self).__init__(*args, **kwargs)

    def handle_parse_result(self, ctx, opts, args):
        if self.mutually_exclusive.intersection(opts) and self.name in opts:
            raise UsageError(
                "Illegal usage: `{}` is mutually exclusive with arguments `{}`.".format(
                    self.name, ", ".join(self.mutually_exclusive)
                )
            )
        return super(MutuallyExclusiveOption, self).handle_parse_result(ctx, opts, args)


class CurrentPlatform:
    """Just a class to get the current platform information in a simple way."""

    def __init__(self):
        self.platform = platform.system()

    def is_windows(self):
        return self.platform == "Windows"

    def is_linux(self):
        return self.platform == "Linux"

    def is_mac(self):
        return self.platform == "Darwin"

    def is_unix(self):
        return self.is_linux() or self.is_mac()


CURRENT_CLI_PLATFORM = CurrentPlatform()


def get_credentials_file_path(ctx: click.Context) -> str:
    """
    Get the path to the credentials file.
    """
    return os.path.join(ctx.obj["tabsdata_directory"], CONNECTION_FILE)


def load_cli_options(ctx: click.Context):
    try:
        with open(get_cli_options_file_path(ctx)) as f:
            cli_options = json.load(f)
        ctx.obj["cli_options"] = cli_options
    except FileNotFoundError:
        ctx.obj["cli_options"] = {}


def get_cli_options_file_path(ctx: click.Context) -> str:
    """
    Get the path to the cli options file.
    """
    return os.path.join(ctx.obj["tabsdata_directory"], "options.json")


def store_cli_options(ctx: click.Context):
    try:
        with open(get_cli_options_file_path(ctx), "w") as f:
            json.dump(ctx.obj["cli_options"], f)
    except Exception as e:
        click.echo(f"Failed to store CLI options: {e}")
        click.echo("This will not affect the rest of the command execution.")


def get_current_cli_option(ctx: click.Context, option: str, default=None) -> str | None:
    """
    Get the current CLI option from the context.
    """
    cli_option = ctx.obj["cli_options"].get(option, default)
    return cli_option


def set_current_cli_option(ctx: click.Context, option: str, value: str):
    """
    Set the current CLI option in the context.
    """
    ctx.obj["cli_options"][option] = value


def show_hint(ctx: click.Context, hint: str, final_empty_line: bool = False):
    """
    Show a hint in the CLI.
    """
    if get_current_cli_option(ctx, "hints", default="enabled") == "enabled":
        click.echo(click.style("Hint: ", fg="green", bold=True), nl=False)
        click.echo(hint)
        click.echo("Use 'td hints off' to stop seeing these hints.")
        if final_empty_line:
            click.echo()


def hint_common_solutions(ctx: click.Context, e: Exception):
    """
    Show common solutions for CLI issues.
    """
    if isinstance(e, requests.exceptions.ConnectionError):
        server: TabsdataServer = ctx.obj.get("tabsdataserver")
        server_url = server.connection.url[: -len(BASE_API_URL)] if server else None
        if server_url:
            show_hint(
                ctx,
                "It seems like there is no Tabsdata server at the "
                f"provided URL: {server_url}.\n "
                "Please ensure that the URL is correct and the server is "
                "running and reachable.\n If started locally, the status can be "
                "checked by "
                "executing 'tdserver status'.",
            )
    elif isinstance(e, AttributeError):
        show_hint(
            ctx,
            "It seems like you might have an expired session. You can "
            "log in again with 'td login'.",
        )


def verify_trigger_type(function: Function, type: str):
    # TODO: Awaiting the information needed to be added to the function information
    #  endpoint.
    return True
