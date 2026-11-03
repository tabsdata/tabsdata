#
# Copyright 2026 Tabsdata Inc.
#

"""Shared helpers for the ``tdk`` CLI.

``tdk`` migration can land without dragging in the explicitly-excluded
``_utils/internal/`` subtree. Placeholder pending a proper v2 port
(see TODO block below).
"""

from __future__ import annotations

import base64
import binascii
import datetime
import decimal
import hashlib
import importlib.machinery
import importlib.util
import io
import json
import logging
import os
import re
import sys
import weakref
from contextlib import contextmanager
from http import HTTPStatus
from itertools import islice
from types import ModuleType
from typing import TYPE_CHECKING, Any, Iterable, Iterator, Sequence

import rich_click as click
from rich_click import Option, UsageError

from tabsdatak.cli._credentials import (
    credential_kind,
    ensure_unexpired,
    env_project,
    env_server,
)
from tabsdatak.cli._errors import TdkException
from tabsdatak.cli._machine import machine
from tabsdatak.cli._unattended import unattended_cause
from tabsdatak.common._constants import tabsdata_directory

# Heavy imports (the server SDK, requests, ruamel) are deferred into the
# functions that use them so importing this shared helper -- which every command
# module does -- stays cheap. Type-only names live under TYPE_CHECKING.
if TYPE_CHECKING:
    from rich.console import Console

logger = logging.getLogger(__name__)

RENDER_WIDTH = 1024
_SERVER_VERSIONS: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()


def make_console(**kwargs) -> "Console":
    from rich.console import Console

    width = None if sys.stdout.isatty() else RENDER_WIDTH
    return Console(width=width, **kwargs)


CONNECTION_FILE = "connection.json"
OPTIONS_FILE = "options.json"
# A profile keeps its own ``<NAME>-connection.json`` and ``<NAME>-options.json``
# (its pins); the bare files are the default profile. Every other setting is
# shared by all profiles in ``tdk-settings.json``, the profile ``tdk set
# --profile`` records among them.
SETTINGS_FILE = "tdk-settings.json"
PROFILE_KEY = "profile"
# 2.2 kept the settings in each profile's options file and the profile in
# ``profile.json``; the first run without ``tdk-settings.json`` carries over
# the ones its commands used.
LEGACY_PROFILE_FILE = "profile.json"

DEFAULT_TABSDATA_DIRECTORY = tabsdata_directory()

# Keys of the persisted settings, all managed by ``tdk set`` / ``tdk unset``.
# On/off settings store ENABLED / DISABLED; ``pins``, the one key of a profile's
# options file, nests the pinned project and collection.
OPTION_HINTS = "hints"
OPTION_PROMPT = "prompt"
OPTION_TRIGGER_WAIT = "trigger_wait"
OPTION_SERVER = "server"
OPTION_PINS = "pins"

ENABLED = "enabled"
DISABLED = "disabled"

PIN_PROJECT = "project"
PIN_COLLECTION = "collection"

_SHARED_SETTINGS = (OPTION_HINTS, OPTION_PROMPT, OPTION_TRIGGER_WAIT, OPTION_SERVER)

# Mirrors what tabsdatak.sdk._apiserver.process_url() fills in when a
# user-typed server URL carries no protocol or no port; defined here so the
# CLI never imports the requests-heavy _apiserver module.
DEFAULT_SERVER_PROTOCOL = "http://"
DEFAULT_APISERVER_PORT = "2457"

SERVER_URL_DEFAULTS_HELP = (
    f"If omitted, the protocol defaults to '{DEFAULT_SERVER_PROTOCOL}' and the "
    f"port to {DEFAULT_APISERVER_PORT}."
)


def is_valid_id(possible_id: str) -> bool:
    """
    Check if the provided string is a valid ID for an APIServer entity,
    for example a worker.
    Matches both id forms 2.0 emits: a 26-character lowercase base32hex id, and a
    logical id (``<key><YYYYMMDD>-<counter>``, e.g. ``wkr20260728-3``). Uppercase
    is accepted too since the id decoder is case-insensitive.
    For now only needed in the CLI, if it ever is needed in more places it should be
    moved to general utils.
    """
    id_pattern = re.compile(r"^([0-9a-vA-V]{26}|[a-zA-Z]{1,3}[0-9]{8}-[0-9]{1,10})$")
    return bool(id_pattern.match(possible_id))


def require_id(option: str, value: str | None, hint: str) -> None:
    """Fail as a usage error, before any request, when ``value`` is not an id;
    ``hint`` says where the id comes from."""
    if value is not None and not is_valid_id(value):
        raise TdkException("usage", f"'{option} {value}' is not an id.", hint=hint)


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


def present(value):
    """``value``, or None when it is missing, so a table view shows its
    placeholder."""
    if value is None or value == "" or value == "None":
        return None
    return value


def collection_label(obj) -> str:
    """Collection name for display, suffixed with ' (deleted)' when the owning
    collection has been soft-deleted. The name itself is the real pre-deletion
    name (resolved server-side); the suffix only flags that it no longer exists.
    Degrades to the plain name when the response carries no `collection_deleted`.
    """
    name = obj.collection.name
    data = getattr(obj, "_data", None) or {}
    return f"{name} (deleted)" if data.get("collection_deleted") else name


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


def _prompting_disabled_message(ctx: click.Context) -> str:
    """Why prompting is off, and how to turn it back on: the persisted
    ``tdk set --prompt false`` outlives the invocation, so it is named ahead of
    an unattended session's variable and of the one-shot ``--no-prompt``
    flag."""
    if not ctx.obj.get("prompt_setting", True):
        return (
            "Prompting is disabled by the 'prompt' setting and some required "
            "values are missing. Please provide the required values or "
            "re-enable prompting with 'tdk set --prompt true'."
        )
    cause = unattended_cause()
    if cause is not None:
        return (
            f"Prompting is off in an unattended session ({cause}) and some "
            "required values are missing. Please provide the required values."
        )
    return (
        "Prompting is disabled and some required "
        "values are missing. Please provide the "
        "required values or avoid using '--no-prompt'."
    )


def require_word(ctx: click.Context, given: str | None, word: str, prompt: str):
    """Asks for the confirmation ``word`` unless ``given``, and fails as a
    usage error, with the flag that supplies it, unless it matches."""
    given = given or logical_prompt(ctx, prompt, option=f"--confirm {word}")
    if given != word:
        raise TdkException(
            "usage",
            f"Not confirmed: the confirmation word is '{word}'.",
            hint=f"Pass --confirm {word}.",
        )


def logical_prompt(
    ctx: click.Context,
    message: str,
    default_value=None,
    hide_input: bool = False,
    option: str | None = None,
    hint: str | None = None,
):
    """
    Prompt the user for a value if prompt is enabled. Otherwise, either return the
        default value, or fail naming ``option``, the flag that supplies it, with
        ``hint`` or by default "Pass ``option``." as the hint.
    """

    if ctx.obj["no_prompt"]:
        if default_value is not None:
            return default_value
        missing = f"Missing required value: {option or message}."
        if machine() is None:
            missing = f"{missing} {_prompting_disabled_message(ctx)}"
        if hint is None and option:
            hint = f"Pass {option}."
        raise TdkException("missing_argument", missing, hint=hint)
    try:
        import sys

        if sys.stdin.isatty():
            return prompt_with_escape(
                message, default=default_value, hide_input=hide_input
            )
        try:
            return click.prompt(message, default=default_value, hide_input=hide_input)
        except click.Abort:
            if option is None:
                raise
            raise TdkException(
                "missing_argument",
                f"Missing required value: {option}. Standard input is not a "
                "terminal and has no answer for it.",
                hint=hint or f"Pass {option}.",
            ) from None
    except (KeyboardInterrupt, EOFError):
        click.echo(err=True)
        raise click.Abort() from None


def utils_login(
    ctx: click.Context,
    server_url: str,
    username: str,
    password: str,
    token: str = None,
):
    import requests

    # noinspection PyProtectedMember
    from tabsdatak.sdk._apiserver import APIServerError, obtain_connection

    try:
        obtain_connection(
            server_url,
            username,
            password,
            credentials_file=get_credentials_file_path(ctx),
            token=token,
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
        elif isinstance(e, APIServerError) and e.status != HTTPStatus.UPGRADE_REQUIRED:
            show_hint(
                ctx,
                (
                    "It seems like the Tabsdata server is running but refusing "
                    "the token. A token stops working once it expires or is "
                    "revoked; check 'tdk auth service-token list' from a password "
                    "session."
                )
                if token
                else (
                    "It seems like the Tabsdata server is running but refusing "
                    "to login. Please ensure that the credentials are correct and "
                    "the server is healthy."
                ),
            )
        raise click.ClickException(f"Failed to login: {e}")


def request_login_information(ctx: click.Context):
    server_url = (
        env_server() or configured_server(ctx) or logical_prompt(ctx, "Server URL")
    )
    username = logical_prompt(ctx, "Username")
    password = logical_prompt(ctx, "Password", hide_input=True)
    return server_url, username, password


class _LazyTabsdataServer:
    """Stand-in for the connected ``TabsdataServer`` that builds the real,
    SDK-backed object (and pays its heavy imports) only on first attribute
    access. Commands that never touch the server -- ``examples``,
    ``connection``, ``--help`` -- never trigger it. It is truthy so the usual
    ``if not ctx.obj["tabsdataserver"]`` login checks keep working."""

    def __init__(
        self, ctx: click.Context, credentials: dict, credentials_file: str | None
    ):
        self._ctx = ctx
        self._credentials = credentials
        self._credentials_file = credentials_file
        self._server = None

    def _build(self):
        if self._server is None:
            # noinspection PyProtectedMember
            from tabsdatak.sdk._apiserver import APIServer
            from tabsdatak.sdk.tabsdata_server import TabsdataServer

            env_credential = self._ctx.obj.get("env_credential")
            if env_credential is not None:
                ensure_unexpired(env_credential)
            credentials = self._credentials
            run = machine()
            connection = APIServer(
                credentials.get("url"),
                credentials_file=self._credentials_file,
                **(
                    {"timeout": run.request_timeout}
                    if run is not None and run.request_timeout
                    else {}
                ),
            )
            connection.refresh_token = credentials.get("refresh_token")
            connection.bearer_token = credentials.get("bearer_token")
            connection.token_type = credentials.get("token_type")
            connection.expires_in = credentials.get("expires_in")
            connection.expiration_time = credentials.get("expiration_time")
            # A per-command `--proj` overrides this default.
            connection.project = env_project() or get_pin(self._ctx, PIN_PROJECT)
            server = TabsdataServer.__new__(TabsdataServer)
            server.connection = connection
            self._server = server
        return self._server

    def __getattr__(self, name: str):
        return getattr(self._build(), name)

    def __bool__(self) -> bool:
        return True


class FailingTabsdataServer:
    """Stand-in for the server when the environment credential is invalid:
    commands that never touch the server (``--help`` included) still run, and
    the first one that does fails with the credential error."""

    def __init__(self, error: Exception):
        self._error = error

    def __getattr__(self, name: str):
        raise self._error

    def __bool__(self) -> bool:
        return True


def initialise_tabsdata_server_connection(ctx: click.Context):
    from tabsdatak.sdk import _credentials

    env_credential = ctx.obj.get("env_credential")
    ctx.obj["credential_kind"] = None
    if env_credential is not None:
        ctx.obj["credential_kind"] = env_credential.kind
        ctx.obj["tabsdataserver"] = _LazyTabsdataServer(
            ctx,
            {"url": env_credential.server, "bearer_token": env_credential.bearer},
            credentials_file=None,
        )
        return
    credentials = _credentials.read(get_credentials_file_path(ctx))
    if credentials is None:
        ctx.obj["tabsdataserver"] = None
        return
    ctx.obj["credential_kind"] = credential_kind(credentials.get("bearer_token") or "")
    ctx.obj["tabsdataserver"] = _LazyTabsdataServer(
        ctx, credentials, credentials_file=get_credentials_file_path(ctx)
    )


def get_currently_pinned_object(ctx: click.Context, object: str) -> str | None:
    """
    Get the currently pinned object from the context.
    """
    currently_pinned = get_pin(ctx, object)
    if currently_pinned:
        click.echo(f"Using currently pinned {object}: {currently_pinned}", err=True)
    return currently_pinned


def _set_active_project(ctx: click.Context, project: str | None):
    server = ctx.obj.get("tabsdataserver")
    if server is not None:
        server.connection.project = project


def _apply_proj(ctx: click.Context, param, value):
    """``--proj`` option callback: target a project for this command.

    The active project rides on the server connection. The root group seeds it
    from the pinned project; a per-command ``--proj`` overrides it for this
    invocation. The connection builds project-scoped paths from it and narrows
    cross-project listings to it, so commands never thread it any further.

    Carrying the option on the context too marks the command as project-scoped
    -- which is what makes :func:`resolve_project_or_prompt` ask for a project
    before anything else -- and lets a command that has to log in first apply
    ``--proj`` to the connection that login builds."""
    ctx.ensure_object(dict)
    ctx.obj["project_scoped"] = True
    if value:
        ctx.obj["proj"] = value
        _set_active_project(ctx, value)
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
        help=(
            "Project to target; overrides the pinned project. Prompted for when "
            "a command needs one and neither is set."
        ),
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


def _spans_projects(ctx: click.Context) -> bool:
    """Whether this command runs fine with no project at all -- true for the
    cross-project listings, which are exactly the ones offering
    ``--all-projects``."""
    return any(param.name == "all_projects" for param in ctx.command.params)


def resolve_project_or_prompt(ctx: click.Context):
    """Settle the active project before the command prompts for anything else.

    Collections, functions and tables are all addressed within a project, so a
    project-scoped command with no project selected asks for one first --
    otherwise it would prompt for a collection only to fail afterwards on the
    missing project. With prompting off the project is left unset and the
    request fails with the SDK's ``NoProjectSelectedError``, which names both
    ways to supply one."""
    if ctx.obj.get("no_prompt") or not ctx.obj.get("project_scoped"):
        return
    if _spans_projects(ctx):
        return
    server = ctx.obj.get("tabsdataserver")
    if server is None or server.connection.project:
        return
    _set_active_project(
        ctx, logical_prompt(ctx, "Name of the project to target", option="--proj")
    )


def resolve_all_projects(ctx: click.Context, all_projects: bool) -> bool:
    """Apply ``--all-projects`` for this command and report whether the listing
    spans projects.

    ``--all-projects`` clears the active project, overriding any pinned or
    ``--proj`` project — an unset project makes listings span every project
    the requester can view. Even without the flag a listing still spans
    projects when no project is selected at all (the implicit fallback)."""
    server = ctx.obj.get("tabsdataserver")
    if server is None:
        return False
    if all_projects:
        server.connection.project = None
    return server.connection.project is None


def ancestry(
    ctx: click.Context,
    collection: str | None = None,
    function: str | None = None,
    table: str | None = None,
) -> list[tuple[str, str]]:
    """The entities a project-scoped listing lives under, outermost first, for
    its title line; a listing that spans projects says so."""
    server = (ctx.obj or {}).get("tabsdataserver")
    project = server.connection.project if server else None
    parts = [("Project", project or "all")]
    if collection:
        parts.append(("Collection", collection))
    if function:
        parts.append(("Function", function))
    if table:
        parts.append(("Table", table))
    return parts


# ---------------------------------------------------------------------------
# Output rendering: -o/--format {table,json,jsonb,yaml}.
# ---------------------------------------------------------------------------

DOCUMENT_FORMAT = "jsonb"
OUTPUT_FORMATS = ("table", "json", DOCUMENT_FORMAT, "yaml")
_AS_DATA = object()


def _apply_format(ctx: click.Context, param, value):
    """``-o/--format`` callback: stash the chosen format on the context so the
    command's renderer can dispatch on it. A machine run speaks ``jsonb``
    only, which it also gives for ``json``."""
    ctx.ensure_object(dict)
    if machine() is not None:
        if value not in (None, "json", DOCUMENT_FORMAT):
            raise TdkException(
                "usage", f"'--format {value}' is not available in a machine run."
            )
        value = DOCUMENT_FORMAT
    ctx.obj["output_format"] = value or "table"
    _check_view_options(ctx)
    return value


def format_option(fn):
    """Decorator adding ``-o/--format {table,json,jsonb,yaml}`` (default
    ``table``, ``jsonb`` in a machine run)."""
    return click.option(
        "-o",
        "--format",
        "fmt",
        type=click.Choice(OUTPUT_FORMATS),
        default=None,
        show_default="table",
        expose_value=False,
        callback=_apply_format,
        help=(
            "Output format. 'json' and 'yaml' give a listing as an array and "
            "table data as one object per row; 'jsonb' gives a listing as "
            "{items, truncated, next} and table data as columns and rows."
        ),
    )(fn)


def output_format(ctx: click.Context) -> str:
    """The active output format for this command (defaults to ``table``)."""
    return (ctx.obj or {}).get("output_format", "table")


def progress(ctx: click.Context, message: str = "") -> None:
    """Echo a progress or diagnostic line: to stdout in the table view, to
    stderr in the machine formats, so stdout holds only the rendered
    document."""
    click.echo(message, err=output_format(ctx) != "table")


def progress_console(ctx: click.Context) -> "Console":
    """The console for live progress views, on the same stream as
    :func:`progress`."""
    return make_console(stderr=output_format(ctx) != "table")


def serialize(payload, fmt: str) -> str:
    """Serialize a payload to a ``json``/``jsonb`` or ``yaml`` string (the
    machine formats of ``--format``). JSON is pretty-printed; both carry every
    field."""
    if fmt in ("json", DOCUMENT_FORMAT):
        return json.dumps(payload, indent=2, ensure_ascii=False, default=str)
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
    return buf.getvalue()


def split_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


def render(
    ctx: click.Context,
    *,
    data,
    view=None,
    truncated: bool = False,
    compat=_AS_DATA,
) -> None:
    """Render a command's result in the active output format.

    ``view`` is a zero-arg callable returning the ``TableView`` or ``RecordView``
    (or a list of them) shown in ``table`` mode, through the options of
    ``view_options``. ``data`` is -- or returns -- the full DTO(s) to serialize
    for ``jsonb`` and a machine run; pass a callable to defer fetching the full
    payload when the table view is shown. ``compat``, when given, is -- or
    returns -- what ``json`` and ``yaml`` give instead: their documents keep
    the shapes of tdk 2.2, which scripts parse. JSON is pretty-printed and the
    machine formats carry all available fields; data goes to stdout so it stays
    parseable. ``truncated`` says the listing was cut by ``--max-result``: the
    view's footer tells, and the machine formats tell on stderr unless a
    machine run carries it in its result. Without view the command has
    already printed its human output."""
    fmt = output_format(ctx)
    if fmt == "table":
        if view is None:
            if truncated:
                click.echo(more_results_message(ctx))
            return
        from tabsdatak.common._tabular import show

        shown = view()
        views = list(shown) if isinstance(shown, (list, tuple)) else [shown]
        views[-1].notes += more_results_notes(ctx, truncated)
        show(views, display(ctx))
        return
    if truncated and machine() is None:
        click.echo(more_results_message(ctx), err=True)
    run = machine()
    if run is not None:
        run.emit(data() if callable(data) else data)
        return
    source = data if compat is _AS_DATA or fmt == DOCUMENT_FORMAT else compat
    payload = source() if callable(source) else source
    text = serialize(payload, fmt)
    # YAML already ends with a newline; JSON does not.
    click.echo(text, nl=(fmt != "yaml"))


# ---------------------------------------------------------------------------
# Table views: --browse/--print, --wrap, --max-width.
# ---------------------------------------------------------------------------

VIEW_OPTIONS = "view_options"
VIEW_OPTIONS_GIVEN = "view_options_given"
BROWSE_OPTION = "--browse/--print"
WRAP_OPTION = "--wrap"
MAX_WIDTH_OPTION = "--max-width"


def _apply_view_option(flag: str):
    def apply(ctx: click.Context, param, value):
        from click.core import ParameterSource

        ctx.ensure_object(dict)
        ctx.obj.setdefault(VIEW_OPTIONS, {})[param.name] = value
        if ctx.get_parameter_source(param.name) is not ParameterSource.DEFAULT:
            given = ctx.obj.setdefault(VIEW_OPTIONS_GIVEN, [])
            given.append(flag if flag != BROWSE_OPTION else _browse_flag(value))
            _check_view_options(ctx)
        return value

    return apply


def _check_view_options(ctx: click.Context) -> None:
    given = (ctx.obj or {}).get(VIEW_OPTIONS_GIVEN, [])
    if given and output_format(ctx) != "table":
        raise UsageError(
            f"The '{given[0]}' option only applies to '-o table'.", ctx=ctx
        )


def _browse_flag(browse: bool) -> str:
    return "--browse" if browse else "--print"


def view_options(browse: bool = False):
    """Decorator adding the table view options: ``--browse/--print`` (``browse``
    is the command's default), ``--wrap`` and ``--max-width``."""

    def decorate(fn):
        fn = click.option(
            MAX_WIDTH_OPTION,
            "max_width",
            type=click.IntRange(min=0),
            default=None,
            expose_value=False,
            callback=_apply_view_option(MAX_WIDTH_OPTION),
            help=(
                "Maximum width of each column, in characters; longer values are "
                "cut and end in '…'. Use 0 for no limit. Defaults to the width "
                "each table sets."
            ),
        )(fn)
        fn = click.option(
            WRAP_OPTION,
            "wrap",
            is_flag=True,
            default=False,
            expose_value=False,
            callback=_apply_view_option(WRAP_OPTION),
            help=(
                "Show every value whole, wrapping long and multi-line values "
                "inside their column instead of cutting them."
            ),
        )(fn)
        return click.option(
            BROWSE_OPTION,
            "browse",
            default=browse,
            show_default=True,
            expose_value=False,
            callback=_apply_view_option(BROWSE_OPTION),
            help=(
                "Open the table in a viewer that scrolls both ways, or print it "
                "and return."
            ),
        )(fn)

    return decorate


def display(ctx: click.Context):
    """The ``Display`` chosen with the table view options of this command."""
    from tabsdatak.common._tabular import Display

    chosen = (ctx.obj or {}).get(VIEW_OPTIONS, {})
    return Display(
        browse=bool(chosen.get("browse", False)),
        wrap=bool(chosen.get("wrap", False)),
        max_width=chosen.get("max_width"),
    )


def render_entity(ctx: click.Context, *, data, message: str) -> None:
    """Render the object a mutation produced: ``message`` for humans, the DTO
    itself in the machine formats."""
    if output_format(ctx) == "table":
        click.echo(message)
        return
    render(ctx, data=data)


def render_deleted(
    ctx: click.Context,
    *,
    kind: str,
    name: str,
    message: str,
    collection: str | None = None,
    project: str | None = None,
) -> None:
    """Render a deletion: ``message`` for humans, else
    ``{"deleted": {"kind", "name", "collection", "project"}}``."""
    render_entity(
        ctx,
        data={
            "deleted": {
                "kind": kind,
                "name": name,
                "collection": collection,
                "project": project,
            }
        },
        message=message,
    )


def render_done(
    ctx: click.Context,
    *,
    action: str,
    kind: str,
    id: str | None,
    message: str,
    details: dict | None = None,
) -> None:
    """Render an action that returns no object: ``message`` for humans, else
    ``{"done": {"action", "kind", "id", **details}}``."""
    render_entity(
        ctx,
        data={"done": {"action": action, "kind": kind, "id": id, **(details or {})}},
        message=message,
    )


NO_STATUS_CHANGE = "UpdateStatusRunError::0004"
_UNCHANGED = {
    "cancel": "Nothing to cancel: every run of {kind} '{id}' had already ended.",
    "recover": "Nothing to recover: no run of {kind} '{id}' is in a recoverable state.",
}


def status_changed(response) -> bool:
    """Whether a cancel or recover changed any run; the server answers a no-op
    with a warning rather than an error."""
    try:
        warnings = (response.json().get("context") or {}).get("warnings") or []
    except (AttributeError, ValueError):
        return True
    return not any(warning.get("code") == NO_STATUS_CHANGE for warning in warnings)


def render_status_done(
    ctx: click.Context,
    *,
    changed: bool,
    action: str,
    kind: str,
    id: str,
    message: str,
    details: dict | None = None,
) -> None:
    """``render_done`` for a cancel or recover, with ``changed`` in the
    details and an honest message when nothing changed, which the details
    also carry as ``message``, so an agent can say why."""
    details = {**(details or {}), "changed": changed}
    if not changed:
        message = details["message"] = _UNCHANGED[action].format(kind=kind, id=id)
    render_done(ctx, action=action, kind=kind, id=id, message=message, details=details)


def render_list(
    ctx: click.Context, *, items, truncated: bool, view=None, compat=_AS_DATA
) -> None:
    """Render a listing: ``view`` for humans, as in :func:`render`; in ``jsonb``
    and a machine run the list envelope ``{"items": [...], "truncated": bool,
    "next": cursor}``, where ``next`` is the ``--cursor`` that continues a
    truncated listing, or null; in ``json`` and ``yaml`` the bare items, or
    ``compat`` when given."""

    def _items():
        return items() if callable(items) else items

    render(
        ctx,
        data=lambda: {
            "items": _items(),
            "truncated": truncated,
            "next": list_next(ctx),
        },
        view=view,
        truncated=truncated,
        compat=_items if compat is _AS_DATA else compat,
    )


def server_version(server) -> str | None:
    """The tabsdata version the server runs, read once per connection;
    ``None`` when it cannot be read."""
    try:
        return _SERVER_VERSIONS[server]
    except (KeyError, TypeError):
        pass
    from tabsdatak.sdk._apiserver import APIServerError

    try:
        found = (server.connection.runtime_info_get().json().get("data") or {}).get(
            "version"
        )
    except APIServerError as error:
        found = error.server_version
    except Exception as error:
        logger.debug("The server version could not be read: %s", error)
        found = None
    found = found if isinstance(found, str) else None
    try:
        _SERVER_VERSIONS[server] = found
    except TypeError:
        pass
    return found


def version_skew(server) -> str | None:
    """The server's version when it differs from this tdk's, else ``None``."""
    from tabsdatak.cli.td.cli import __version__

    found = server_version(server)
    return found if found is not None and found != __version__ else None


DEFAULT_SAMPLE_LEN = 100


def _json_value(value):
    if isinstance(value, (datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, datetime.timedelta):
        return value.total_seconds()
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    return value


def tabular(frame, page_len: int) -> dict:
    """The tabular-data shape: columns with their Polars dtype, then the rows
    as arrays. ``truncated`` is true when the page is full, so more rows may
    follow."""
    return {
        "columns": [
            {"name": name, "dtype": str(dtype)} for name, dtype in frame.schema.items()
        ],
        "rows": [[_json_value(value) for value in row] for row in frame.rows()],
        "row_count": frame.height,
        "truncated": frame.height >= page_len,
    }


# ---------------------------------------------------------------------------
# Listing size: --max-result.
# ---------------------------------------------------------------------------

DEFAULT_MAX_RESULT = 100
FILTER_OPERATORS = ("eq", "ne", "gt", "ge", "lt", "le", "lk", "btw", "is")

_FILTER = re.compile(rf"[^:]+:(?:{'|'.join(FILTER_OPERATORS)}):.*", re.DOTALL)
_ORDER_BY = re.compile(r"[A-Za-z_][A-Za-z0-9_]*[+-]?")


def _apply_max_result(ctx: click.Context, param, value):
    """``--max-result`` callback: stash the cap on the context so the command's
    listing helper can apply it."""
    ctx.ensure_object(dict)
    ctx.obj["max_result"] = DEFAULT_MAX_RESULT if value is None else value
    return value


def max_result_option(fn):
    """Decorator adding ``--max-result`` to a list command."""
    return click.option(
        "--max-result",
        "max_result",
        type=click.IntRange(min=0),
        default=DEFAULT_MAX_RESULT,
        show_default=True,
        expose_value=False,
        callback=_apply_max_result,
        help="Maximum number of rows to list. Use 0 for no limit.",
    )(fn)


def max_result(ctx: click.Context) -> int:
    """The active row cap for this command (0 means no limit), at most the
    machine run's ``max_result``."""
    requested = (ctx.obj or {}).get("max_result", DEFAULT_MAX_RESULT)
    run = machine()
    if run is None or run.max_result is None:
        return requested
    cap = run.max_result
    if cap and (not requested or requested > cap):
        return cap
    return requested


def _apply_filters(ctx: click.Context, param, value):
    for condition in value:
        if not _FILTER.fullmatch(condition):
            raise click.BadParameter(
                f"'{condition}' is not FIELD:OP:VALUE with OP one of "
                f"{', '.join(FILTER_OPERATORS)}.",
                ctx=ctx,
                param=param,
            )
    ctx.ensure_object(dict)
    ctx.obj["list_filters"] = list(value)
    return value


def _apply_order_by(ctx: click.Context, param, value):
    if value is not None and not _ORDER_BY.fullmatch(value):
        raise click.BadParameter(
            f"'{value}' is not FIELD, FIELD+ or FIELD-.", ctx=ctx, param=param
        )
    ctx.ensure_object(dict)
    ctx.obj["list_order_by"] = value
    return value


def encode_cursor(
    pagination_id: str | None, next_step: str | None, skip: int, listing: str
) -> str:
    """A listing's ``next``: where it resumes, and the ``listing`` key of the
    command, its options, filters and order it belongs to."""
    document = json.dumps(
        {"p": pagination_id, "n": next_step, "s": skip, "k": listing},
        separators=(",", ":"),
    )
    return base64.urlsafe_b64encode(document.encode()).decode().rstrip("=")


def _optional_str(value) -> bool:
    return value is None or isinstance(value, str)


def decode_cursor(cursor: str) -> tuple[str | None, str | None, int, str]:
    try:
        raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4))
        document = json.loads(raw)
        position = (document["p"], document["n"], document["s"], document["k"])
    except (ValueError, TypeError, KeyError, binascii.Error):
        position = None
    if (
        position is None
        or not _optional_str(position[0])
        or not _optional_str(position[1])
        or type(position[2]) is not int
        or position[2] < 0
        or not isinstance(position[3], str)
    ):
        raise click.BadParameter(f"'{cursor}' is not a cursor from a listing's 'next'.")
    return position


def _apply_cursor(ctx: click.Context, param, value):
    ctx.ensure_object(dict)
    ctx.obj["list_cursor"] = decode_cursor(value) if value else None
    return value


def list_options(fn):
    """Decorator adding ``--filter``, ``--order-by`` and ``--cursor`` to a list
    command backed by a paged server listing; read them with
    :func:`list_params`."""
    for option in (
        click.option(
            "--cursor",
            expose_value=False,
            callback=_apply_cursor,
            help=(
                "Continue a listing from the 'next' cursor it returned, with the "
                "same filters and order."
            ),
        ),
        click.option(
            "--order-by",
            "order_by",
            metavar="FIELD[+|-]",
            expose_value=False,
            callback=_apply_order_by,
            help="Sort by a field, ascending (+, the default) or descending (-).",
        ),
        click.option(
            "--filter",
            "filters",
            metavar="FIELD:OP:VALUE",
            multiple=True,
            expose_value=False,
            callback=_apply_filters,
            help=(
                "Filter on a field, with OP one of "
                f"{', '.join(FILTER_OPERATORS)}. Repeatable: conditions on "
                "different fields must all hold, conditions on the same field "
                "are alternatives. Combines with the command's own filters."
            ),
        ),
    ):
        fn = option(fn)
    return fn


def list_params(
    ctx: click.Context,
    filter: list[str] | str | None = None,
    order_by: str | None = None,
) -> dict:
    """The ``filter``, ``order_by`` and ``cursor`` arguments for an SDK listing:
    the command's own ``filter`` plus every ``--filter``, ``--order-by`` over
    the command's default ``order_by``, and ``--cursor``."""
    ctx.ensure_object(dict)
    obj = ctx.obj
    filters = [filter] if isinstance(filter, str) else list(filter or [])
    filters += obj.get("list_filters", [])
    order_by = obj.get("list_order_by") or order_by
    obj["list_key"] = _listing_key(ctx, filters, order_by)
    cursor = obj.get("list_cursor")
    if cursor is not None:
        from tabsdatak.sdk.tabsdata_server import ListCursor

        *position, listing = cursor
        if listing != obj["list_key"]:
            raise click.UsageError(
                "'--cursor' continues a different listing; pass it with the same "
                "command, filters and order that returned it."
            )
        cursor = ListCursor(*position)
    return {"filter": filters or None, "order_by": order_by, "cursor": cursor}


def _listing_key(ctx: click.Context, filters: list[str], order_by: str | None) -> str:
    params = sorted(ctx.params.items())
    names = []
    while ctx is not None:
        names.append(ctx.command.name)
        ctx = ctx.parent
    document = json.dumps([names[::-1], params, sorted(filters), order_by], default=str)
    return hashlib.sha256(document.encode()).hexdigest()[:12]


def list_next(ctx: click.Context) -> str | None:
    """The cursor that continues the listing :func:`take` cut short."""
    return (ctx.obj or {}).get("list_next")


def take(ctx: click.Context, rows: Iterable) -> tuple[list, bool]:
    """Drain a listing up to the active ``--max-result``.

    Fetches one row beyond the cap so a full page is distinguishable from a
    truncated one, then discards it. Returns the capped rows and whether the
    server holds more; when it does and ``rows`` is an SDK ``Listing``, the
    cursor of the first row left out becomes :func:`list_next`."""
    limit = max_result(ctx)
    ctx.ensure_object(dict)
    ctx.obj["list_next"] = None
    if not limit:
        return list(rows), False
    capped = list(islice(rows, limit + 1))
    if len(capped) <= limit:
        return capped, False
    position = getattr(rows, "position", None)
    listing = ctx.obj.get("list_key")
    if position is not None and listing is not None:
        ctx.obj["list_next"] = encode_cursor(
            position.pagination_id, position.next_step, position.skip, listing
        )
    return capped[:limit], True


def more_results_notes(ctx: click.Context, truncated: bool) -> list[str]:
    return [more_results_message(ctx)] if truncated else []


def more_results_message(ctx: click.Context) -> str:
    cursor = list_next(ctx)
    return (
        f"More results available: showing the first {max_result(ctx)}. Raise "
        "'--max-result <N>', pass '--max-result 0' for all"
        + (f", or continue with '--cursor {cursor}'." if cursor else ".")
    )


def verify_login_or_prompt(ctx: click.Context):
    """Settle what every command needs before it runs: a logged-in connection
    and, for a project-scoped command, an active project. Commands call it as
    their first statement, so the project is asked for ahead of the collection,
    function or table it scopes."""
    if not ctx.obj["tabsdataserver"]:
        if ctx.obj.get("no_prompt"):
            raise TdkException(
                "auth_required",
                "No credentials found.",
                hint=(
                    "Run 'tdk login', or set TDK_SERVER together with TDK_TOKEN "
                    "or TDK_BEARER."
                ),
            )
        progress(ctx, "No credentials found. Please login first.")
        server_url, username, password = request_login_information(ctx)
        utils_login(ctx, server_url, username, password)
        progress(ctx, "Login successful.")
        initialise_tabsdata_server_connection(ctx)
        # `--proj` was parsed before there was a connection to put it on; the
        # login just built one, so apply it now.
        if ctx.obj.get("proj"):
            _set_active_project(ctx, ctx.obj["proj"])
    resolve_project_or_prompt(ctx)


class MutuallyExclusiveOption(Option):
    def __init__(self, *args, **kwargs):
        self.mutually_exclusive = {
            name.replace("-", "_") for name in kwargs.pop("mutually_exclusive", [])
        }
        help = kwargs.get("help", "")
        if self.mutually_exclusive:
            kwargs["help"] = help + (
                " NOTE: This argument is mutually exclusive with  arguments: ["
                + self._excluded()
                + "]."
            )
        super(MutuallyExclusiveOption, self).__init__(*args, **kwargs)

    def _excluded(self) -> str:
        return ", ".join(
            sorted(name.replace("_", "-") for name in self.mutually_exclusive)
        )

    def handle_parse_result(self, ctx, opts, args):
        if self.mutually_exclusive.intersection(opts) and self.name in opts:
            raise UsageError(
                "Illegal usage: `{}` is mutually exclusive with arguments `{}`.".format(
                    self.name.replace("_", "-"), self._excluded()
                )
            )
        return super(MutuallyExclusiveOption, self).handle_parse_result(ctx, opts, args)


_PROFILE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")


def validate_profile_name(name: str) -> str:
    if not _PROFILE_NAME.fullmatch(name):
        raise click.BadParameter(
            f"'{name}' is not a valid profile name: use letters, digits, '.', "
            "'_' or '-', starting with a letter or digit."
        )
    return name


def profile_file_name(profile: str | None, file_name: str) -> str:
    return file_name if profile is None else f"{profile}-{file_name}"


def default_profile(ctx: click.Context) -> str | None:
    """The profile commands run under when given no ``--profile``, as set by
    ``tdk set --profile``; ``None`` for the default one."""
    return get_current_cli_option(ctx, PROFILE_KEY) or None


def set_profile(ctx: click.Context, profile: str | None) -> None:
    """Point the invocation at ``profile``'s connection and options files, or at
    the bare ones for the default profile. Callers reload what they hold from
    the previously selected files."""
    directory = ctx.obj["tabsdata_directory"]
    ctx.obj["profile"] = profile
    ctx.obj["connection_file"] = os.path.join(
        directory, profile_file_name(profile, CONNECTION_FILE)
    )
    ctx.obj["options_file"] = os.path.join(
        directory, profile_file_name(profile, OPTIONS_FILE)
    )


def active_profile(ctx: click.Context) -> str | None:
    """The profile this invocation runs under; ``None`` for the default one."""
    return ctx.obj.get("profile")


def get_credentials_file_path(ctx: click.Context) -> str:
    """
    Get the path to the credentials file.
    """
    return ctx.obj["connection_file"]


def load_settings(ctx: click.Context):
    """Load the settings every profile shares."""
    directory = ctx.obj["tabsdata_directory"]
    path = os.path.join(directory, SETTINGS_FILE)
    try:
        with open(path) as f:
            settings = json.load(f)
    except FileNotFoundError:
        settings = _legacy_settings(directory)
        if settings:
            _store_json(path, settings, "settings")
    ctx.obj["settings"] = settings if isinstance(settings, dict) else {}


def _legacy_settings(directory: str) -> dict:
    profile = _read_json(os.path.join(directory, LEGACY_PROFILE_FILE)).get(PROFILE_KEY)
    profile = profile if isinstance(profile, str) and profile else None
    options = _read_json(
        os.path.join(directory, profile_file_name(profile, OPTIONS_FILE))
    )
    settings = {key: options[key] for key in _SHARED_SETTINGS if key in options}
    if profile is not None:
        settings[PROFILE_KEY] = profile
    return settings


def _read_json(path: str) -> dict:
    try:
        with open(path) as f:
            content = json.load(f)
    except (OSError, ValueError):
        return {}
    return content if isinstance(content, dict) else {}


def store_settings(ctx: click.Context):
    _store_json(
        os.path.join(ctx.obj["tabsdata_directory"], SETTINGS_FILE),
        _settings(ctx),
        "settings",
    )


def _settings(ctx: click.Context) -> dict:
    return ctx.obj.setdefault("settings", {})


def load_cli_options(ctx: click.Context):
    """Load the pins of the active profile."""
    try:
        with open(get_cli_options_file_path(ctx)) as f:
            cli_options = json.load(f)
    except FileNotFoundError:
        cli_options = {}
    pins = cli_options.get(OPTION_PINS) if isinstance(cli_options, dict) else None
    ctx.obj["cli_options"] = {OPTION_PINS: pins if isinstance(pins, dict) else {}}


def get_cli_options_file_path(ctx: click.Context) -> str:
    """
    Get the path to the cli options file.
    """
    return ctx.obj["options_file"]


def store_cli_options(ctx: click.Context):
    _store_json(get_cli_options_file_path(ctx), ctx.obj["cli_options"], "CLI options")


def _store_json(path: str, content: dict, what: str):
    try:
        with open(path, "w") as f:
            json.dump(content, f)
    except Exception as e:
        click.echo(f"Failed to store {what}: {e}", err=True)
        click.echo("This will not affect the rest of the command execution.", err=True)


def get_current_cli_option(ctx: click.Context, option: str, default=None) -> str | None:
    """A setting shared by every profile (persist with
    :func:`store_settings`)."""
    return _settings(ctx).get(option, default)


def set_current_cli_option(ctx: click.Context, option: str, value: str):
    _settings(ctx)[option] = value


def clear_current_cli_option(ctx: click.Context, option: str):
    """Remove a setting, restoring its default."""
    _settings(ctx).pop(option, None)


def cli_option_enabled(ctx: click.Context, option: str, default: bool = True) -> bool:
    """Whether an on/off option is enabled. Anything other than the stored
    ``disabled`` marker reads as enabled."""
    stored = get_current_cli_option(
        ctx, option, default=ENABLED if default else DISABLED
    )
    return stored != DISABLED


def set_cli_option_enabled(ctx: click.Context, option: str, enabled: bool):
    """Turn an on/off option on or off (persist with
    :func:`store_settings`)."""
    set_current_cli_option(ctx, option, ENABLED if enabled else DISABLED)


def trigger_waits(ctx: click.Context) -> bool:
    """Whether ``fn trigger`` waits for its plan when given neither ``--wait``
    nor ``--no-wait``: the ``tdk set --trigger-wait`` setting, on unless set
    and never read in a machine run."""
    return machine() is None and cli_option_enabled(ctx, OPTION_TRIGGER_WAIT)


def configured_server(ctx: click.Context) -> str | None:
    """The server URL ``tdk login`` falls back to when given no ``--server``,
    as set by ``tdk set --server``."""
    return get_current_cli_option(ctx, OPTION_SERVER)


def login_server(ctx: click.Context) -> str | None:
    """The server URL of the stored ``tdk login``, if any."""
    # noinspection PyProtectedMember
    from tabsdatak.sdk._apiserver import BASE_API_URL

    try:
        with open(get_credentials_file_path(ctx), encoding="utf-8") as stored:
            url = json.load(stored).get("url")
    except (OSError, TypeError, ValueError, AttributeError, KeyError):
        return None
    if not isinstance(url, str) or not url:
        return None
    return url.removesuffix(BASE_API_URL)


def get_pins(ctx: click.Context) -> dict:
    """The pinned objects of the active profile, as stored in its options."""
    return ctx.obj["cli_options"].setdefault(OPTION_PINS, {})


def get_pin(ctx: click.Context, name: str) -> str | None:
    return get_pins(ctx).get(name)


def set_pin(ctx: click.Context, name: str, value: str | None):
    """Pin (or, with ``None``, unpin) an object (persist with
    :func:`store_cli_options`)."""
    get_pins(ctx)[name] = value


def clear_pins(ctx: click.Context):
    """Drop every pin (persist with :func:`store_cli_options`)."""
    get_pins(ctx).clear()


def show_hint(ctx: click.Context, hint: str, final_empty_line: bool = False):
    """
    Show a hint in the CLI, on the :func:`progress` stream.
    """
    if machine() is None and cli_option_enabled(ctx, OPTION_HINTS):
        progress(ctx, click.style("Hint: ", fg="green", bold=True) + hint)
        progress(ctx, "Use 'tdk unset --hints' to stop seeing these hints.")
        if final_empty_line:
            progress(ctx)


def warn_deprecated(used: str, instead: str) -> None:
    """Say on stderr that ``used`` still works but is deprecated, and what
    replaces it."""
    click.echo(
        f"Warning: '{used}' is deprecated and will be removed; use {instead}.",
        err=True,
    )


def deprecated_option(used: str, instead: str):
    """An option callback that warns, with :func:`warn_deprecated`, when the
    option is given."""

    def callback(ctx: click.Context, param, value):
        if value:
            warn_deprecated(used, instead)
        return value

    return callback


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


def command_failure(ctx: click.Context, action: str, e: Exception) -> Exception:
    """The exception a command raises for ``e``: a tdk or usage error as is,
    anything else as ``Failed to <action>: <e>``, after the common hints."""
    if isinstance(e, (TdkException, click.UsageError)):
        return e
    hint_common_solutions(ctx, e)
    return click.ClickException(f"Failed to {action}: {e}")


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


class _SiblingFinder:
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
        return _importable_dotted_name([leaf], file_path)
    if relative.startswith(os.pardir + os.sep) or relative == os.pardir:
        return _importable_dotted_name([leaf], file_path)
    parts = os.path.splitext(relative)[0].split(os.sep)
    return _importable_dotted_name(parts, file_path)


def _importable_dotted_name(parts: Sequence[str], file_path: str) -> str:
    """The dotted name `parts` spell, refused when a part cannot be imported
    under that name at runtime (the runner imports the file from the bundle
    tree, where a dot in a file or folder name reads as a package
    separator)."""
    for part in parts:
        if not part.isidentifier():
            raise ValueError(
                f"'{file_path}' cannot be imported: '{part}' is not a valid "
                f"Python module name. Rename the file or folder so every part "
                f"of its path is a valid identifier (letters, digits and "
                f"underscores, not starting with a digit)."
            )
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


@contextmanager
def bundle_confinement() -> Iterator[str | None]:
    """The ``confine_to`` of a function bundle: the machine run's, or
    ``None``. A bundled file resolving outside it is ``path_not_allowed``."""
    from tabsdatak.common._error import CommonErrorCode, CommonRuntimeException

    run = machine()
    workspace = run.confine_to if run is not None else None
    try:
        yield workspace
    except CommonRuntimeException as error:
        if error.api_error is not CommonErrorCode.COMMON_14:
            raise
        raise TdkException(
            "path_not_allowed",
            f"Not bundled: {str(error).split(': ', 1)[-1]}.",
            hint=f"Keep the bundled files, symlinks followed, under {workspace}.",
        ) from error
