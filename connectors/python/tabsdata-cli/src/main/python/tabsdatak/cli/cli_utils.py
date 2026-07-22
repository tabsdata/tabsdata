#
# Copyright 2026 Tabsdata Inc.
#

"""Shared helpers for the ``tdk`` CLI.

Also hosts a temporary stand-in for :func:`tdabout` that mirrors the
legacy ``tabsdatak.common._information.about.dabout`` surface so the
``tdk`` migration can land without dragging in the explicitly-excluded
``_utils/internal/`` subtree. Placeholder pending a proper v2 port
(see TODO block below).
"""

from __future__ import annotations

import hashlib
import importlib.abc
import importlib.machinery
import importlib.util
import io
import json
import os
import platform
import re
import sys
from contextlib import contextmanager
from importlib.metadata import PackageNotFoundError, version
from types import ModuleType
from typing import TYPE_CHECKING, Any, Iterator, Sequence

import rich_click as click
from rich_click import Option, UsageError

# Heavy imports (the server SDK, requests, ruamel) are deferred into the
# functions that use them so importing this shared helper -- which every command
# module does -- stays cheap. Type-only names live under TYPE_CHECKING.
if TYPE_CHECKING:
    from rich.console import Console

RENDER_WIDTH = 1024


def make_console(**kwargs) -> "Console":
    from rich.console import Console

    width = None if sys.stdout.isatty() else RENDER_WIDTH
    return Console(width=width, **kwargs)


if TYPE_CHECKING:
    from tabsdatak.sdk.tabsdata_server import Function

CONNECTION_FILE = "connection.json"

# Mirrors tabsdatak.sdk._apiserver.DEFAULT_TABSDATA_DIRECTORY. Defined locally so
# the common CLI path (which needs only this constant) never imports the
# requests-heavy _apiserver module.
DEFAULT_TABSDATA_DIRECTORY = os.path.join(os.path.expanduser("~"), ".tabsdata")


# ---------------------------------------------------------------------------
# Temporary stand-in for legacy ``_utils.internal._about.tdabout``.
#
# TODO(tabsdatak-migration): this is a placeholder so the ``tdk`` CLI
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


def utils_login(ctx: click.Context, server_url: str, username: str, password: str):
    import requests

    # noinspection PyProtectedMember
    from tabsdatak.sdk._apiserver import APIServerError, obtain_connection

    try:
        obtain_connection(
            server_url,
            username,
            password,
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
    return server_url, username, password


class _LazyTabsdataServer:
    """Stand-in for the connected ``TabsdataServer`` that builds the real,
    SDK-backed object (and pays its heavy imports) only on first attribute
    access. Commands that never touch the server -- ``examples``,
    ``connection``, ``--help`` -- never trigger it. It is truthy so the usual
    ``if not ctx.obj["tabsdataserver"]`` login checks keep working."""

    def __init__(self, ctx: click.Context, credentials: dict):
        self._ctx = ctx
        self._credentials = credentials
        self._server = None

    def _build(self):
        if self._server is None:
            # noinspection PyProtectedMember
            from tabsdatak.sdk._apiserver import APIServer
            from tabsdatak.sdk.tabsdata_server import TabsdataServer

            credentials = self._credentials
            connection = APIServer(
                credentials.get("url"),
                credentials_file=get_credentials_file_path(self._ctx),
            )
            connection.refresh_token = credentials.get("refresh_token")
            connection.bearer_token = credentials.get("bearer_token")
            connection.token_type = credentials.get("token_type")
            connection.expires_in = credentials.get("expires_in")
            connection.expiration_time = credentials.get("expiration_time")
            # The active project rides on the connection: it defaults to the
            # pinned project, and a per-command `--proj` overrides it.
            connection.project = self._ctx.obj.get("pinned_objects", {}).get("project")
            server = TabsdataServer.__new__(TabsdataServer)
            server.connection = connection
            self._server = server
        return self._server

    def __getattr__(self, name: str):
        return getattr(self._build(), name)

    def __bool__(self) -> bool:
        return True


def initialise_tabsdata_server_connection(ctx: click.Context):
    try:
        with open(os.path.join(DEFAULT_TABSDATA_DIRECTORY, CONNECTION_FILE)) as f:
            credentials = json.load(f)
    except FileNotFoundError:
        ctx.obj["tabsdataserver"] = None
        return
    ctx.obj["tabsdataserver"] = _LazyTabsdataServer(ctx, credentials)


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


def _apply_proj(ctx: click.Context, param, value):
    """``--proj`` option callback: target a project for this command.

    The active project rides on the server connection. The root group seeds it
    from the pinned project; a per-command ``--proj`` overrides it for this
    invocation. The connection injects the active project into every
    tenant-scoped request, so commands never thread it any further."""
    if value:
        server = ctx.obj.get("tabsdataserver")
        if server is not None:
            server.connection.project = value
    return value


def proj_option(fn):
    """Decorator adding a per-command ``--proj`` that targets a project,
    overriding the pinned project for that invocation."""
    return click.option(
        "--proj",
        "-P",
        "proj",
        expose_value=False,
        callback=_apply_proj,
        help="Project to target; overrides the pinned project.",
    )(fn)


def all_projects_option(fn):
    """Decorator adding ``--all-projects`` to a cross-project list command."""
    return click.option(
        "--all-projects",
        "all_projects",
        is_flag=True,
        default=False,
        help=(
            "List across every project you can view, ignoring any pinned or "
            "--proj project."
        ),
    )(fn)


def resolve_all_projects(ctx: click.Context, all_projects: bool) -> bool:
    """Apply ``--all-projects`` for this command and report whether the listing
    spans projects.

    ``--all-projects`` points the connection at the wildcard, overriding any
    pinned or ``--proj`` project. Even without the flag a listing still spans
    projects when no project is selected at all (the implicit fallback)."""
    from tabsdatak.sdk._apiserver import WILDCARD_PROJECT_MARKER

    server = ctx.obj.get("tabsdataserver")
    if server is None:
        return False
    if all_projects:
        server.connection.project = WILDCARD_PROJECT_MARKER
    return server.connection.project in (None, WILDCARD_PROJECT_MARKER)


# ---------------------------------------------------------------------------
# Output rendering: -o/--output {table,json,yaml}.
# ---------------------------------------------------------------------------

OUTPUT_FORMATS = ("table", "json", "yaml")


def _apply_output(ctx: click.Context, param, value):
    """``-o/--output`` callback: stash the chosen format on the context so the
    command's renderer can dispatch on it."""
    ctx.ensure_object(dict)
    ctx.obj["output_format"] = value or "table"
    return value


def output_option(fn):
    """Decorator adding ``-o/--output {table,json,yaml}`` (default ``table``)."""
    return click.option(
        "-o",
        "--output",
        "output",
        type=click.Choice(OUTPUT_FORMATS),
        default="table",
        show_default=True,
        expose_value=False,
        callback=_apply_output,
        help="Output format.",
    )(fn)


def output_format(ctx: click.Context) -> str:
    """The active output format for this command (defaults to ``table``)."""
    return (ctx.obj or {}).get("output_format", "table")


def render(ctx: click.Context, *, data, table) -> None:
    """Render a command's result in the active output format.

    ``table`` is a zero-arg callable that prints the human-facing view (the
    per-command rich table); it is invoked only in ``table`` mode. ``data`` is
    -- or returns -- the full DTO(s) to serialize for ``json``/``yaml``; pass a
    callable to defer fetching the full payload when the table view is shown.
    JSON is pretty-printed and both machine formats carry all available fields;
    data goes to stdout so it stays parseable."""
    fmt = output_format(ctx)
    if fmt == "table":
        table()
        return
    payload = data() if callable(data) else data
    if fmt == "json":
        click.echo(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
    else:  # yaml
        from ruamel.yaml import YAML

        # Normalize through JSON (default=str) so any value is YAML-safe, then
        # dump with ruamel (a guaranteed dependency via tabsdata-api; PyYAML is
        # not a declared tabsdata-cli dependency). Key order is preserved.
        normalized = json.loads(json.dumps(payload, default=str))
        yaml = YAML()
        yaml.default_flow_style = False
        yaml.allow_unicode = True
        buf = io.StringIO()
        yaml.dump(normalized, buf)
        click.echo(buf.getvalue(), nl=False)


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
        server_url, username, password = request_login_information(ctx)
        utils_login(ctx, server_url, username, password)
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
        click.echo("Use 'tdk hints off' to stop seeing these hints.")
        if final_empty_line:
            click.echo()


def hint_common_solutions(ctx: click.Context, e: Exception):
    """
    Show common solutions for CLI issues.
    """
    import requests

    # noinspection PyProtectedMember
    from tabsdatak.sdk._apiserver import BASE_API_URL

    if isinstance(e, requests.exceptions.ConnectionError):
        server = ctx.obj.get("tabsdataserver")
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
            "log in again with 'tdk login'.",
        )


def verify_trigger_type(function: Function, type: str):
    # TODO: Awaiting the information needed to be added to the function information
    #  endpoint.
    return True


# ---------------------------------------------------------------------------
# Dynamic-import of a user function from a "path/to/file.py::name" string.
#
# Used by ``tdk fn register`` to materialise the user's decorated
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


class _SiblingFinder(importlib.abc.MetaPathFinder):
    """Resolves a bare top-level name to a module or package in one of
    ``folders`` (a ``<name>.py`` file or a ``<name>/`` package), so an
    absolute ``import <name>`` inside a bundled user file finds either a
    flat sibling (the entry file's own folder) or a top-level bundle
    package (the bundle root).

    Appended to the *end* of ``sys.meta_path``, so every real finder
    (builtin, frozen, ``PathFinder`` over ``sys.path``) gets first
    refusal: installed packages always win, and a user file named after
    an installed package cannot shadow it. A bundle name is reachable by
    absolute import only when it does not collide with an installed
    package; a colliding name stays importable via the relative form
    (``from .<name> import ...``).

    Resolution is delegated to ``PathFinder`` restricted to ``folders``
    so ``.py`` files, package directories, and parent/submodule binding
    all behave exactly as a normal on-``sys.path`` import would --
    matching what the runtime runner does when it puts the extracted
    bundle folder on ``sys.path``.
    """

    def __init__(self, folders: Sequence[str]) -> None:
        self.folders = list(dict.fromkeys(folders))
        self.claimed: set[str] = set()

    def find_spec(
        self,
        fullname: str,
        path: Sequence[str] | None = None,
        target: ModuleType | None = None,
    ) -> importlib.machinery.ModuleSpec | None:
        if "." in fullname:
            return None
        spec = importlib.machinery.PathFinder.find_spec(
            fullname, list(self.folders), target
        )
        if spec is None:
            return None
        self.claimed.add(fullname)
        return spec


@contextmanager
def _sibling_imports(folders: Sequence[str]) -> Iterator[None]:
    """Activate sibling-fallback resolution while a user file executes,
    then restore ``sys.meta_path`` and drop every name the finder
    resolved from ``sys.modules`` so nothing outlives the register call
    (no cross-function leakage into a shared CLI process, where a later
    function may have a same-named sibling with different contents)."""
    finder = _SiblingFinder(folders)
    prefixes = tuple(os.path.abspath(folder) + os.sep for folder in finder.folders)
    sys.meta_path.append(finder)
    try:
        yield
    finally:
        try:
            sys.meta_path.remove(finder)
        except ValueError:
            pass
        for bare in finder.claimed:
            module = sys.modules.get(bare)
            spec = getattr(module, "__spec__", None)
            locations = [
                os.path.abspath(location)
                for location in (
                    ([spec.origin] if spec and spec.origin else [])
                    + list(getattr(spec, "submodule_search_locations", None) or [])
                )
            ]
            if locations and all(
                location.startswith(prefixes) for location in locations
            ):
                sys.modules.pop(bare, None)


def _bundle_relative_dotted_name(file_path: str, bundle_root: str) -> str:
    """Dotted module path of ``file_path`` relative to ``bundle_root``.

    ``<root>/a/b/c/d.py`` under ``<root>`` -> ``"a.b.c.d"``; a file
    directly at the root -> its bare leaf. This is the name the module
    will be importable under at runtime, where the runner puts the
    bundle's ``user_code/`` (a verbatim copy of the tree rooted at
    ``bundle_root``) on ``sys.path`` -- so the recorded ``func`` /
    class ref must match the on-disk nesting. Falls back to the bare
    leaf if the file is not under ``bundle_root``.
    """
    leaf = os.path.splitext(os.path.basename(file_path))[0]
    try:
        relative = os.path.relpath(file_path, bundle_root)
    except ValueError:
        return leaf
    if relative.startswith(os.pardir + os.sep) or relative == os.pardir:
        return leaf
    parts = os.path.splitext(relative)[0].split(os.sep)
    return ".".join(parts)


def _dynamic_import_function_from_path(
    path: str, bundle_root: str | None = None
) -> Any:  # noqa: C901
    """Import the symbol named by ``path`` (``"/path/to/file.py::name"``).

    Loads the user's file as a submodule of a synthetic package whose
    ``__path__`` chain mirrors the file's nesting under ``bundle_root``
    (the folder that becomes the bundle's ``user_code/``). The synthetic
    root package name is derived from a sha1 of ``bundle_root``, so:

    * a user file named after an installed package
      (``databricks.py``, ``snowflake.py``, ...) does NOT shadow the
      installed package -- ``sys.modules["databricks"]`` is left alone.
    * two user files with the same relative path loaded from different
      roots don't collide.
    * relative imports (``from . import helpers``, ``from ..cousin
      import X``) work at every nesting level because the synthetic
      package chain reproduces the on-disk package structure -- exactly
      what the runtime runner reconstructs from ``user_code/`` (PEP 420
      namespace packages, no ``__init__.py`` required).
    * absolute imports of siblings (``import helpers`` /
      ``from helpers import X``) work when the sibling name does not
      collide with an installed package: a ``_SiblingFinder`` appended to
      ``sys.meta_path`` resolves the bare name to the sibling file INSIDE
      the synthetic package (installed-first, so no shadowing). A sibling
      whose name collides with an installed package stays reachable via
      the relative form only.

    The stamped ``__module__`` (used to serialise ``func`` / user
    classes into ``function_def.json``) is the file's dotted path
    relative to ``bundle_root`` (e.g. ``a.b.c.d``), so the runner can
    re-import it from ``user_code/a/b/c/d.py``. ``sys.path`` is never
    mutated; sibling resolution runs entirely through the synthetic
    package.

    Not reentrant: it mutates process-global ``sys.modules`` /
    ``sys.meta_path`` for the duration of the import and tears the
    entries down before returning. Sequential calls in one process are
    safe (each fully cleans up first); concurrent calls in the same
    interpreter would race and must be serialised by the caller.
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
    if bundle_root is None:
        bundle_root = folder_path
    bundle_root = os.path.abspath(bundle_root)
    user_module_name = _bundle_relative_dotted_name(file_path, bundle_root)

    root_digest = hashlib.sha1(bundle_root.encode("utf-8")).hexdigest()[:12]
    root_package = f"td_usr_pkg_{root_digest}"
    module_name = f"{root_package}.{user_module_name}"

    # Materialise the synthetic package chain root -> ... -> parent of the
    # entry module, each ``__path__`` pointing at the matching on-disk
    # folder, so relative imports at every level resolve on-disk. Every
    # entry we add is removed in the ``finally`` so back-to-back
    # registrations in one CLI process don't see each other's stale tree.
    synthetic_names: list[str] = []
    relative_parts = user_module_name.split(".")
    accumulated = root_package
    current_folder = bundle_root
    for part in relative_parts[:-1]:
        current_folder = os.path.join(current_folder, part)
        accumulated = f"{accumulated}.{part}"
        if accumulated not in sys.modules:
            pkg_spec = importlib.machinery.ModuleSpec(
                accumulated, loader=None, is_package=True
            )
            pkg_spec.submodule_search_locations = [current_folder]
            sys.modules[accumulated] = importlib.util.module_from_spec(pkg_spec)
            synthetic_names.append(accumulated)
    if root_package not in sys.modules:
        root_spec = importlib.machinery.ModuleSpec(
            root_package, loader=None, is_package=True
        )
        root_spec.submodule_search_locations = [bundle_root]
        sys.modules[root_package] = importlib.util.module_from_spec(root_spec)
        synthetic_names.append(root_package)

    module_spec = importlib.util.spec_from_file_location(module_name, file_path)
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
    synthetic_names.append(module_name)
    try:
        try:
            with _sibling_imports([folder_path, bundle_root]):
                module_spec.loader.exec_module(module)
        except BaseException as e:
            raise ImportError(f"Failed to execute module {file_path}: {str(e)}")

        try:
            symbol = getattr(module, function_name)
        except AttributeError:
            raise AttributeError(
                f"Function '{function_name}' not found in module {file_path}."
            )

        # Strip the synthetic ``td_usr_pkg_<sha>.`` prefix off the stamped
        # ``__module__`` so ``info.to_json`` serialises ``func`` (via
        # ``callable_to_str``) and user-defined classes (via
        # ``qualified_class``) under the bundle-relative name -- importable
        # from ``user_code/`` at runtime, not the CLI-process-local
        # synthetic package. This covers the entry module itself and any
        # sibling module a serialised ``func`` / ``Conn`` / ``Src`` was
        # imported from, at any nesting depth.
        #
        # Only objects carrying the synthetic prefix are rewritten: those
        # are the bundle's own modules. Imported builtins / installed types
        # (e.g. ``datetime`` pulled in with ``from datetime import
        # datetime``) never carry the prefix, so they are skipped -- which
        # also avoids ``__module__`` assignment on immutable C types.
        prefix = f"{root_package}."

        def strip_synthetic_module(obj: Any) -> None:
            name = getattr(obj, "__module__", None)
            if isinstance(name, str) and name.startswith(prefix):
                obj.__module__ = name[len(prefix) :]

        underlying = getattr(symbol, "func", None)
        strip_synthetic_module(underlying if underlying is not None else symbol)

        for value in vars(module).values():
            if isinstance(value, type):
                strip_synthetic_module(value)
        return symbol
    finally:
        for name in synthetic_names:
            sys.modules.pop(name, None)


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
    info = _dynamic_import_function_from_path(path, bundle_root=project_root)
    return info, project_root
