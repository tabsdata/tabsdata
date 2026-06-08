#
# Copyright 2026 Tabs Data Inc.
#

"""Shared helpers for the ``td`` CLI.

Also hosts a temporary stand-in for :func:`tdabout` that mirrors the
legacy ``tabsdata._utils.internal._about.tdabout`` surface so the
``td`` migration can land without dragging in the explicitly-excluded
``_utils/internal/`` subtree. Placeholder pending a proper v2 port
(see TODO block below).

:func:`td_resource` is the proper v2 resolver re-exported from
``tabsdatak.server._tdruntime.python.resource`` — it honours the
editable-install overlay that lets resources live under the repo's
``variant/`` tree.
"""

from __future__ import annotations

import hashlib
import importlib.machinery
import importlib.util
import json
import os
import platform
import re
import sys
from importlib.metadata import PackageNotFoundError, version
from typing import Any

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
from tabsdatak.server._tdruntime.python.resource import td_resource  # noqa: F401

CONNECTION_FILE = "connection.json"


# ---------------------------------------------------------------------------
# Temporary stand-in for legacy ``_utils.internal._about.tdabout``.
#
# TODO(tabsdatak-migration): this is a placeholder so the ``td`` CLI
# migration can land without porting the explicitly-excluded
# ``tabsdata/_utils/internal/`` subtree. Replace with a proper v2
# implementation in a follow-up ticket once the v2 packaging story
# is settled. Until then, ``tdabout`` prints a one-line banner via
# importlib.metadata.
# ---------------------------------------------------------------------------


_DEFAULT_ABOUT_PACKAGE = "tabsdata-cli"


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


# ---------------------------------------------------------------------------
# Dynamic-import of a user function from a "path/to/file.py::name" string.
#
# Used by ``td fn register`` to materialise the user's decorated
# function (a tabsdatak ``*Info`` descriptor) without touching
# ``sys.path``. The legacy version lived in the SDK; v2 keeps it
# CLI-side because the SDK takes a ready-made ``info`` object directly.
# ---------------------------------------------------------------------------


def _split_python_function_path(path: str) -> tuple[str, str]:
    if len(path.split("::")) != 2:
        raise ValueError(
            f"Failed to split path '{path}' in two parts. Please make sure that "
            f"the path to the registered function is of the form "
            f"'/path/to/file.py::function_name'"
        )
    file_path, function_name = path.split("::")
    return file_path, function_name


def _dynamic_import_function_from_path(path: str) -> Any:  # noqa: C901
    """Import the symbol named by ``path`` (``"/path/to/file.py::name"``).

    Loads the user's file as a submodule of a synthetic package whose
    ``__path__`` points at the file's directory. The synthetic package
    name is derived from a sha1 of the directory, so:

    * a user file named after an installed package
      (``databricks.py``, ``snowflake.py``, ...) does NOT shadow the
      installed package -- ``sys.modules["databricks"]`` is left alone.
    * two user files with the same basename loaded from different
      directories don't collide.
    * relative imports (``from . import helpers``) work because the
      synthetic package's ``__path__`` lists the file's directory.

    Absolute imports of siblings (``from helpers import X`` in a user
    file) do NOT work, because we deliberately do not mutate
    ``sys.path``. Users must use relative imports for sibling files.
    """
    file_path, function_name = _split_python_function_path(path)
    file_path = os.path.abspath(file_path)
    if not os.path.exists(file_path):
        if not file_path.endswith(".py"):
            raise FileNotFoundError(
                f"File not found: {file_path}. The .py extension may be missing."
            )
        raise FileNotFoundError(f"File not found: {file_path}")

    folder_path = os.path.dirname(file_path)
    folder_name = os.path.splitext(os.path.basename(file_path))[0]

    folder_digest = hashlib.sha1(folder_path.encode("utf-8")).hexdigest()[:12]
    package_name = f"td_usr_pkg_{folder_digest}"
    if package_name not in sys.modules:
        package_spec = importlib.machinery.ModuleSpec(
            package_name, loader=None, is_package=True
        )
        package_spec.submodule_search_locations = [folder_path]
        sys.modules[package_name] = importlib.util.module_from_spec(package_spec)

    module_name = f"{package_name}.{folder_name}"
    module_spec = importlib.util.spec_from_file_location(
        module_name,
        file_path,
        submodule_search_locations=[folder_path],
    )
    if module_spec is None or module_spec.loader is None:
        raise ImportError(
            f"Failed to load module spec from {file_path}. "
            "The file may not be a valid Python module."
        )
    module = importlib.util.module_from_spec(module_spec)
    if module is None:
        raise ImportError(
            f"Failed to create module from spec for {file_path}. "
            "The file may not be a valid Python module."
        )
    sys.modules[module_name] = module
    try:
        module_spec.loader.exec_module(module)
    except BaseException as e:
        # Don't leave a half-initialized module behind to poison later imports.
        sys.modules.pop(module_name, None)
        raise ImportError(f"Failed to execute module {file_path}: {str(e)}")
    try:
        symbol = getattr(module, function_name)
    except AttributeError:
        raise AttributeError(
            f"Function '{function_name}' not found in module {file_path}."
        )
    return symbol


def load_info_and_project_root(
    path: str,
    dir_to_bundle: str | None = None,
) -> tuple[Any, str]:
    """Resolve ``--path`` and ``--dir-to-bundle`` flags into the
    ``(info, project_root)`` pair the v2 SDK ``register_function`` /
    ``update_function`` / ``patch_function`` methods expect.

    ``project_root`` defaults to the directory containing the user's
    file when ``dir_to_bundle`` is unset -- mirrors the legacy
    behaviour where the bundle's project root was implicit.
    """
    file_path = path.split("::", 1)[0]
    project_root = dir_to_bundle or os.path.dirname(os.path.abspath(file_path))
    info = _dynamic_import_function_from_path(path)
    return info, project_root
