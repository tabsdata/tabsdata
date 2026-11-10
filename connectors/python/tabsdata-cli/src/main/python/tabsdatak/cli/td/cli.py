#
# Copyright 2026 Tabsdata Inc.
#

import functools
import json
import os
import sys
import textwrap

import rich_click as click

from tabsdatak.cli._capability import capability
from tabsdatak.cli._credentials import (
    TOKEN_ENV,
    ensure_stateful,
    env_credential,
    env_server,
    refine_auth_error,
    stored_expiry,
)
from tabsdatak.cli._errors import (
    ClassifiedException,
    TdkException,
    classify,
)
from tabsdatak.cli._machine import machine
from tabsdatak.cli._registry import cli_extensions, collect_cli_commands
from tabsdatak.cli._unattended import unattended_cause
from tabsdatak.cli.cli_utils import (
    DEFAULT_TABSDATA_DIRECTORY,
    OPTION_PINS,
    OPTION_PROMPT,
    SERVER_URL_DEFAULTS_HELP,
    FailingTabsdataServer,
    clear_pins,
    cli_option_enabled,
    configured_server,
    default_profile,
    format_option,
    get_credentials_file_path,
    hint_common_solutions,
    initialise_tabsdata_server_connection,
    load_cli_options,
    load_settings,
    logical_prompt,
    make_console,
    output_format,
    progress,
    render,
    render_done,
    set_profile,
    store_cli_options,
    utils_login,
    validate_profile_name,
    verify_login_or_prompt,
)

# Built-in command groups, imported for their @cli_command side effects: each
# module registers its top-level group, attached below via collect_cli_commands.
# Only registration runs at import time -- every command's execution-time imports
# (the server SDK, connection templates, ...) are deferred into the command body.
from tabsdatak.cli.td import (  # noqa: F401
    auth_group,
    collection_group,
    connection_group,
    docs_group,
    doctor_group,
    examples_group,
    fn_group,
    fnrun_group,
    group_group,
    metrics_group,
    permission_group,
    plan_group,
    project_group,
    settings_group,
    table_group,
    trigger_group,
    trx_group,
    user_group,
    worker_group,
)
from tabsdatak.common._tabular import HELP_STYLES

CYAN = "[cyan]"
NO_CYAN = "[/cyan]"

BOLD = "[bold]"
NO_BOLD = "[/bold]"

UNDERLINE = "[underline]"
NO_UNDERLINE = "[underline]"

HELP_WIDTH = 100
NO_PROMPT_FLAG = "--no-prompt"

_help_width = None if sys.stdout.isatty() else HELP_WIDTH
click.rich_click.WIDTH = _help_width
click.rich_click.MAX_WIDTH = _help_width
for _style, _value in HELP_STYLES.items():
    setattr(click.rich_click, _style, _value)
if _help_width is not None:
    os.environ.setdefault("TERMINAL_WIDTH", str(_help_width))

console = make_console()

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@functools.cache
def _version() -> str:
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version("tabsdata-cli")
    except PackageNotFoundError:
        return "0.0.0"


def __getattr__(name: str):
    # ``__version__`` resolves on first access: reading the package metadata
    # costs every invocation tens of milliseconds.
    if name == "__version__":
        return _version()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def _print_version(ctx: click.Context, param, value: bool):
    if value and not ctx.resilient_parsing:
        click.echo(f"Tabsdata Client {_version()}")
        ctx.exit()


def _full_help_options(command) -> list:
    options = []
    for param in getattr(command, "params", []):
        if isinstance(param, click.Option) and not param.hidden:
            options.append({
                "opts": list(param.opts) + list(param.secondary_opts),
                "help": param.help or "",
                "is_flag": bool(param.is_flag),
                "required": bool(param.required),
                "default": None if param.is_flag else param.to_info_dict()["default"],
                "type": getattr(param.type, "name", None),
            })
    return options


def _full_help_node(name: str, command, path: str, parent=None) -> dict:
    node = {
        "name": name,
        "path": path,
        "help": (command.help or command.short_help or "").strip(),
        "options": _full_help_options(command),
        "arguments": [
            {"name": param.name, "required": bool(param.required)}
            for param in getattr(command, "params", [])
            if isinstance(param, click.Argument)
        ],
    }
    if isinstance(command, click.Group):
        ctx = click.Context(command, info_name=name, parent=parent)
        subcommands = {
            sub: command.get_command(ctx, sub) for sub in command.list_commands(ctx)
        }
        node["commands"] = [
            _full_help_node(sub, subcommand, f"{path} {sub}", ctx)
            for sub, subcommand in sorted(subcommands.items())
            if not subcommand.hidden
        ]
    else:
        words = path.split(" ", 1)[1] if " " in path else ""
        for extension in cli_extensions():
            node.update(extension.describe(command, words))
    return node


def _full_help_text(node: dict, lines: list | None = None) -> str:
    lines = [] if lines is None else lines
    lines.append(node["path"])
    if node["help"]:
        for line in node["help"].splitlines():
            lines.append(f"    {line}".rstrip())
    if node["arguments"]:
        lines.append("    Arguments:")
        for arg in node["arguments"]:
            tag = "required" if arg["required"] else "optional"
            lines.append(f"      {arg['name']} ({tag})")
    if node["options"]:
        lines.append("    Options:")
        for opt in node["options"]:
            flags = ", ".join(opt["opts"])
            tag = "required" if opt["required"] else "optional"
            suffix = f"  {opt['help']}" if opt["help"] else ""
            lines.append(f"      {flags} ({tag}){suffix}".rstrip())
    lines.append("")
    for sub in node.get("commands", []):
        _full_help_text(sub, lines)
    return "\n".join(lines)


def build_full_help(root, output: str = "text", compact: bool = False) -> str:
    """Render the help for ``root`` and every command/subcommand under it.

    Each option and argument is tagged ``(required)`` or ``(optional)`` in the
    text form, and carries a ``required`` boolean in the json form.

    ``output`` is ``"text"`` (a flat, indented listing) or ``"json"`` (a nested
    tree of ``{name, path, help, options, arguments, commands}`` objects, to
    which each CLI extension adds), which is ``compact`` in a machine run."""
    tree = _full_help_node("tdk", root, "tdk")
    if output == "json":
        commands = tree.pop("commands", [])
        for extension in cli_extensions():
            tree.update(extension.describe_root())
        tree = {**tree, "commands": commands}
        if compact:
            return json.dumps(tree, separators=(",", ":"), default=str)
        return json.dumps(tree, indent=2, default=str)
    return _full_help_text(tree)


def _incoming_argv(args, kwargs) -> list:
    if kwargs.get("args") is not None:
        return list(kwargs["args"])
    if args and args[0] is not None:
        return list(args[0])
    return list(sys.argv[1:])


def _hoist_no_prompt(argv: list) -> list:
    """``--no-prompt`` is accepted in any position before ``--``."""
    head = argv[: argv.index("--")] if "--" in argv else argv
    if NO_PROMPT_FLAG not in head:
        return argv
    rest = [arg for arg in head if arg != NO_PROMPT_FLAG]
    return [NO_PROMPT_FLAG, *rest, *argv[len(head) :]]


def _full_output_format(argv: list) -> str:
    fmt = "text"
    for flag in ("--format", "-o"):
        if flag in argv:
            index = argv.index(flag)
            if index + 1 < len(argv):
                fmt = argv[index + 1]
    if fmt not in ("text", "json"):
        click.echo(f"Invalid --format '{fmt}'; expected 'text' or 'json'.", err=True)
        raise SystemExit(2)
    return fmt


class _RootGroup(click.RichGroup):
    """Top-level group. ``main`` runs each invocation through the registered
    CLI extensions (:func:`tabsdatak.cli._registry.cli_extension`), hoists
    ``--no-prompt`` and intercepts ``--help --full`` (before Click's own
    ``--help`` handling) to print the recursive full help for every command
    and subcommand. ``--full`` is only honored together with ``--help``, and
    ``--format`` only together with ``--full``: ``tdk --help [--full [--format
    FORMAT]]``.

    ``invoke`` gives every failure its canonical exit code
    (:mod:`tabsdatak.cli._errors`)."""

    def main(self, *args, **kwargs):
        def run(argv: list, **overrides):
            return self._main(argv, {**kwargs, **overrides})

        for extension in reversed(cli_extensions()):
            run = self._wrapped(extension, run)
        return run(_incoming_argv(args, kwargs))

    def _wrapped(self, extension, inner):
        def run(argv: list, **overrides):
            return extension.main(
                self, argv, lambda argv, **more: inner(argv, **{**overrides, **more})
            )

        return run

    def _main(self, argv: list, kwargs: dict):
        if ("--help" in argv or "-h" in argv) and "--full" in argv:
            output = _full_output_format(argv)
            click.echo(build_full_help(self, output, compact=machine() is not None))
            raise SystemExit(0)
        return super().main(**{**kwargs, "args": _hoist_no_prompt(argv)})

    def invoke(self, ctx: click.Context):
        try:
            return super().invoke(ctx)
        except (click.exceptions.Exit, click.Abort):
            raise
        except click.ClickException as e:
            e.error = self._classify(ctx, e)
            e.exit_code = e.error.exit_code
            if e.error.hint and e.error.hint not in e.message:
                e.message = f"{e.message}\n{e.error.hint}"
            raise
        except Exception as e:
            raise ClassifiedException(self._classify(ctx, e)) from e

    @staticmethod
    def _classify(ctx: click.Context, e: Exception):
        obj = ctx.obj or {}
        listed = bool(obj.get("list_filters") or obj.get("list_order_by"))
        return refine_auth_error(
            classify(e, listed),
            obj.get("env_credential"),
            login_expiry=stored_expiry(obj.get("connection_file")),
        )


def _activate_profile(ctx: click.Context, profile: str | None):
    """Load what every command reads from ``profile``: its connection and its
    pins."""
    set_profile(ctx, profile)
    initialise_tabsdata_server_connection(ctx)
    load_cli_options(ctx)


def _apply_profile(ctx: click.Context, param, value):
    """``--profile`` option callback: run this command under the named profile,
    or the default profile when given no name, instead of the one the root
    group loaded. Eager, so it lands before ``--proj`` and the other options
    that act on the loaded connection."""
    if value is not None:
        credential = ctx.obj.get("env_credential")
        if credential is not None:
            raise TdkException(
                "usage",
                f"'--profile' has no effect: the credential comes from "
                f"{credential.env}.",
            )
        _activate_profile(ctx, validate_profile_name(value) if value else None)
    return value


def _profile_option() -> click.Option:
    return click.Option(
        ["--profile", "profile"],
        metavar="[NAME]",
        is_flag=False,
        flag_value="",
        expose_value=False,
        is_eager=True,
        callback=_apply_profile,
        help=(
            "Profile whose login and pinned project and collection this "
            "command uses; with no NAME, the default profile. Default: the "
            "profile set with 'tdk set --profile', else the default profile."
        ),
    )


def _attach_profile_option(command: click.Command):
    """Give every command under ``command`` the per-command ``--profile``.
    ``tdk set`` and ``tdk unset`` already carry their own, which manages the
    default profile instead, and keep it. A group with no registered
    subcommands may resolve them lazily, so it gets the option on whatever it
    resolves, when it resolves it, without being materialized here."""
    if isinstance(command, click.Group):
        if not command.commands:
            _defer_profile_option(command)
        for sub in command.commands.values():
            _attach_profile_option(sub)
        return
    if any(param.name == "profile" for param in command.params):
        return
    command.params.append(_profile_option())


def _defer_profile_option(group: click.Group):
    if getattr(group, "_profile_deferred", False):
        return
    group._profile_deferred = True
    make_context, get_command = group.make_context, group.get_command

    def _make_context(*args, **kwargs):
        ctx = make_context(*args, **kwargs)
        _attach_profile_option(ctx.command)
        return ctx

    def _get_command(ctx, cmd_name):
        sub = get_command(ctx, cmd_name)
        if sub is not None:
            _attach_profile_option(sub)
        return sub

    group.make_context = _make_context
    group.get_command = _get_command


@click.group(cls=_RootGroup, context_settings=CONTEXT_SETTINGS)
@click.option(
    "--version",
    is_flag=True,
    expose_value=False,
    is_eager=True,
    callback=_print_version,
    help="Show the version and exit.",
)
@click.option(
    "--no-prompt",
    is_flag=True,
    help=(
        "Disable all prompts for this invocation. If a prompt is required for "
        "proper execution, the command will fail. Turn prompting off for good "
        "with 'tdk set --prompt false'."
    ),
)
@click.option(
    "--full",
    is_flag=True,
    expose_value=False,
    help=(
        "With --help, print the full help for every command and subcommand, "
        "then exit. Optionally add '--format text|json' (default: text): "
        "'tdk --help [--full [--format FORMAT]]'."
    ),
)
@click.pass_context
def cli(ctx: click.Context, no_prompt: bool):
    """Main CLI for the Tabsdata system."""
    from tabsdatak.common._compatibility import CompatibilityError, check_python

    try:
        check_python()
    except CompatibilityError as version_error:
        raise click.ClickException("\n".join(version_error.messages)) from None

    no_prompt = no_prompt or machine() is not None or unattended_cause() is not None
    try:
        credential, failure = env_credential(), None
    except TdkException as error:
        credential, failure = None, error
    if credential is not None or failure is not None:
        ctx.obj = {
            "tabsdata_directory": DEFAULT_TABSDATA_DIRECTORY,
            "no_prompt_flag": no_prompt,
            "env_credential": credential,
            "cli_options": {OPTION_PINS: {}},
            "settings": {},
            "prompt_setting": True,
            "no_prompt": no_prompt,
        }
        set_profile(ctx, None)
        if failure is not None:
            ctx.obj["tabsdataserver"] = FailingTabsdataServer(failure)
        else:
            initialise_tabsdata_server_connection(ctx)
        return
    try:
        os.makedirs(DEFAULT_TABSDATA_DIRECTORY, exist_ok=True)
        ctx.obj = {
            "tabsdata_directory": DEFAULT_TABSDATA_DIRECTORY,
            "no_prompt_flag": no_prompt,
        }
        load_settings(ctx)
        # 'tdk set --prompt false' is a standing '--no-prompt'; the flag turns
        # prompting off for a single invocation.
        ctx.obj["prompt_setting"] = cli_option_enabled(ctx, OPTION_PROMPT)
        ctx.obj["no_prompt"] = no_prompt or not ctx.obj["prompt_setting"]
        _activate_profile(ctx, default_profile(ctx))
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to initialize CLI: {e}")


# Attach every @cli_command-annotated group: the built-ins imported above, plus
# any enterprise-tier variant commands that register themselves when their
# package is installed. No manual add_command list to keep
# in sync -- a new command only needs the @cli_command annotation.
for _command in collect_cli_commands():
    cli.add_command(_command)
for _extension in cli_extensions():
    cli.params.extend(_extension.root_params())


@capability("session", "read", interactive=True)
@cli.command()
@click.pass_context
def shell(ctx: click.Context):
    """Start an interactive shell session"""
    from tabsdatak.cli.shell import run_shell

    run_shell(ctx, _version())


@capability("knowledge", "read")
@cli.command()
@format_option
@click.pass_context
def about(ctx: click.Context):
    """Show build metadata and system information"""
    from tabsdatak.common._information.about import tdabout

    if output_format(ctx) == "table":
        tdabout()
        return
    text = tdabout(capture=True)
    if text is None:
        raise click.ClickException("Failed to obtain the build metadata.")
    render(ctx, data={"version": _version(), "text": text})


MANIFEST_COLUMN_WIDTH = 80

MANIFEST_RULE_CHARACTERS = "═─"

MANIFEST_ROW_SEPARATORS = {
    "╒": "╤",
    "╞": "╪",
    "├": "┼",
    "╘": "╧",
}


def _narrow_manifest(content: str, width: int = MANIFEST_COLUMN_WIDTH) -> str:
    narrowed = []
    widths = None
    for line in content.split("\n"):
        if line.startswith("╒"):
            widths = [len(segment) for segment in line[1:-1].split("╤")]
        if widths is None or widths[-1] <= width:
            narrowed.append(line)
            continue
        narrowed.extend(_narrow_manifest_line(line, widths, width))
    return "\n".join(narrowed)


def _narrow_manifest_line(line: str, widths: list[int], width: int) -> list[str]:
    capped = widths[:-1] + [width]
    separator = MANIFEST_ROW_SEPARATORS.get(line[:1])
    if separator:
        rule = line[1] if line[1] in MANIFEST_RULE_CHARACTERS else "─"
        return [line[0] + separator.join(rule * size for size in capped) + line[-1]]
    if not line.startswith("│"):
        return [line]
    cells = line[1:-1].split("│")
    if len(cells) != len(widths):
        return [line]
    wrapped = [
        textwrap.wrap(cells[-1].strip(), width - 2, break_long_words=False) or [""]
    ]
    rows = []
    for index, piece in enumerate(wrapped[0]):
        head = cells[:-1] if index == 0 else [" " * size for size in widths[:-1]]
        rows.append("│" + "│".join(head) + f"│ {piece.ljust(width - 2)} │")
    return rows


def _show_manifest(
    ctx: click.Context,
    resource: str,
    title: str,
    dedent: bool = False,
    wide: bool = False,
):
    from tabsdatak.common._resources.reader import td_resource

    path = td_resource(f"assets/manifest/{resource}")
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    if dedent:
        content = textwrap.dedent(content).strip()
    if output_format(ctx) == "table":
        shown = content if wide else _narrow_manifest(content)
        console.print()
        console.print(f"{UNDERLINE}{title}{NO_UNDERLINE}")
        console.print()
        console.print(f"{CYAN}{shown}{NO_CYAN}")
        console.print()
    render(ctx, data={"title": title, "text": content})


@cli.group(no_args_is_help=True)
def info():
    """
    Show the license, third-party dependencies and release notes of tabsdata.
    """


@capability("knowledge", "read")
@info.command(name="third-party")
@format_option
@click.option(
    "--wide",
    is_flag=True,
    default=False,
    help="Show the license column at its full width, instead of wrapping it.",
)
@click.pass_context
def third_party(ctx: click.Context, wide: bool):
    """Show the third-party dependencies of tabsdata."""
    _show_manifest(ctx, "THIRD-PARTY", "Third Party Dependencies", wide=wide)


@capability("knowledge", "read")
@info.command(name="license")
@format_option
@click.pass_context
def license_(ctx: click.Context):
    """Show the license of tabsdata."""
    _show_manifest(ctx, "LICENSE", "License", dedent=True)


@capability("knowledge", "read")
@info.command(name="release-notes")
@format_option
@click.pass_context
def release_notes(ctx: click.Context):
    """Show the release notes of the current version of tabsdata."""
    _show_manifest(ctx, "RELEASE-NOTES", "Release Notes")


@capability("session", "write")
@cli.command()
@format_option
@click.option(
    "--server",
    "-s",
    help=(
        "URL of the Tabsdata Server. Defaults to the server set with 'tdk set "
        "--server'; will be prompted for it if neither is provided. "
        + SERVER_URL_DEFAULTS_HELP
    ),
)
@click.option(
    "--user",
    "-u",
    help="Username for the Tabsdata Server. Will be prompted for it if not provided.",
)
@click.option(
    "--password",
    "-p",
    help=(
        "Password for the Tabsdata Server. It is discouraged to send the password as a "
        "plain argument, it should be either sent as the value of an environment "
        "variable or written through the prompt. Will be prompted for it if not "
        "provided."
    ),
)
@click.option(
    "--token",
    help=(
        "Log in with a service token instead of a username and password, "
        "for automation. Reads TDK_TOKEN when the option is given no value. A "
        "token session is never renewed: it works until the token expires or "
        "is revoked."
    ),
    is_flag=False,
    flag_value=TOKEN_ENV,
)
@click.pass_context
def login(ctx: click.Context, server: str, user: str, password: str, token: str):
    """Login to the Tabsdata Server"""
    ensure_stateful(ctx, "tdk login")
    server = (
        server
        or env_server()
        or configured_server(ctx)
        or logical_prompt(ctx, "Tabsdata Server URL", option="--server")
    )
    if token:
        if token == TOKEN_ENV:
            token = os.environ.get(TOKEN_ENV)
            if not token:
                raise click.ClickException(
                    f"'--token' was given no value and {TOKEN_ENV} is not set."
                )
        if user or password:
            raise click.ClickException(
                "'--token' authenticates on its own; drop '--user' and '--password'."
            )
        utils_login(ctx, server, None, None, token=token)
    else:
        user = user or logical_prompt(
            ctx, "Username for the Tabsdata Server", option="--user"
        )
        password = password or logical_prompt(
            ctx,
            "Password",
            hide_input=True,
            option="--password",
        )
        utils_login(ctx, server, user, password)
    render_done(
        ctx,
        action="login",
        kind="session",
        id=server,
        details={"user": user},
        message="Login successful.",
    )
    from tabsdatak.cli.td.config_utils import execute_actions_for_command

    execute_actions_for_command("login")


@capability("session", "write")
@cli.command()
@format_option
@click.pass_context
def logout(ctx: click.Context):
    """Logout from the Tabsdata Server: the login stored here always goes; the
    server is asked to end the session, which otherwise expires on its own"""
    ensure_stateful(ctx, "tdk logout")
    try:
        ctx.obj["tabsdataserver"].logout()
        ended = None
    except Exception as e:
        ended = str(e)
    try:
        os.remove(get_credentials_file_path(ctx))
    except Exception:
        pass
    try:
        clear_pins(ctx)
        store_cli_options(ctx)
    except Exception:
        pass
    if ended is None:
        render_done(
            ctx, action="logout", kind="session", id=None, message="Logout successful."
        )
    else:
        render_done(
            ctx,
            action="logout",
            kind="session",
            id=None,
            details={"server_session": "not_ended", "reason": ended},
            message=(
                "Logged out here. The server could not end the session, which "
                f"expires on its own: {ended}"
            ),
        )


@capability("operations", "read")
@cli.command()
@format_option
@click.pass_context
def status(ctx: click.Context):
    """Check the status of the server"""
    verify_login_or_prompt(ctx)
    progress(ctx, "Obtaining server status")
    progress(ctx, "-" * 10)
    try:
        current_status = ctx.obj["tabsdataserver"].status
        if output_format(ctx) == "table":
            click.echo(str(current_status))
        render(
            ctx,
            data=lambda: {
                "status": current_status.status,
                "latency_as_nanos": current_status.latency_as_nanos,
                **current_status.kwargs,
            },
        )
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to get status: {e}")


_attach_profile_option(cli)
