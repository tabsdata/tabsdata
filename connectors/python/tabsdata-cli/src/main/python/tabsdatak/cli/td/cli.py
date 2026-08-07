#
# Copyright 2026 Tabsdata Inc.
#

import json
import os
import sys
import textwrap
from collections.abc import Mapping
from importlib.metadata import PackageNotFoundError, version

import rich_click as click

try:
    __version__ = version("tabsdata-cli")
except PackageNotFoundError:
    __version__ = "0.0.0"

from tabsdatak.cli._registry import collect_cli_commands
from tabsdatak.cli.cli_utils import (
    DEFAULT_TABSDATA_DIRECTORY,
    OPTION_PROMPT,
    RENDER_WIDTH,
    SERVER_URL_DEFAULTS_HELP,
    clear_pins,
    cli_option_enabled,
    configured_server,
    get_credentials_file_path,
    hint_common_solutions,
    initialise_tabsdata_server_connection,
    load_cli_options,
    logical_prompt,
    make_console,
    store_cli_options,
    tdabout,
    utils_login,
    verify_login_or_prompt,
)

# Built-in command groups, imported for their @cli_command side effects: each
# module registers its top-level group, attached below via collect_cli_commands.
# Only registration runs at import time -- every command's execution-time imports
# (the server SDK, connection templates, ...) are deferred into the command body.
# TODO: role and stage management are temporarily removed; reintroduce their
# modules here (they self-register via @cli_command) in a later version.
from tabsdatak.cli.td import (  # noqa: F401
    auth_group,
    collection_group,
    connection_group,
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

CYAN = "[cyan]"
NO_CYAN = "[/cyan]"

BOLD = "[bold]"
NO_BOLD = "[/bold]"

UNDERLINE = "[underline]"
NO_UNDERLINE = "[underline]"

_render_width = None if sys.stdout.isatty() else RENDER_WIDTH
click.rich_click.WIDTH = _render_width
click.rich_click.MAX_WIDTH = _render_width
if _render_width is not None:
    os.environ.setdefault("TERMINAL_WIDTH", str(_render_width))

console = make_console()

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])

# Env vars each supported AI agent always sets when it runs a shell; tdk refuses
# to run inside them (agents must use the Tabsdata MCP server instead).
_AI_AGENT_ENV_VARS = ("GEMINI_CLI", "CLAUDECODE", "CODEX_THREAD_ID")
# Escape hatch: set to 1/true/yes to bypass the guard (warns instead of failing).
_AI_AGENT_BYPASS_ENV_VAR = "TDK_IN_AGENT"
_AI_AGENT_ERROR_MESSAGE = (
    "Tabsdata tdk command does not support running within an AI agent, "
    "use Tabsdata MCP instead"
)


def _env_is_truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes"}


def detect_ai_agent(env: Mapping[str, str] | None = None) -> str | None:
    """Return the name of the first AI-agent marker env var that is set (and
    non-empty), or ``None`` when tdk is not running inside a supported agent."""
    env = os.environ if env is None else env
    for var in _AI_AGENT_ENV_VARS:
        value = env.get(var)
        if value is not None and value.strip() != "":
            return var
    return None


def enforce_not_in_ai_agent(env: Mapping[str, str] | None = None) -> None:
    """Exit if tdk is being run inside a supported AI agent, unless the
    ``_TDK_IN_AGENT`` escape hatch is set truthy (in which case warn and continue)."""
    env = os.environ if env is None else env
    if detect_ai_agent(env) is None:
        return
    if _env_is_truthy(env.get(_AI_AGENT_BYPASS_ENV_VAR)):
        click.echo(
            f"WARNING: {_AI_AGENT_BYPASS_ENV_VAR} is set -- bypassing the AI-agent "
            "guard; tdk is not supported within an AI agent, use Tabsdata MCP instead.",
            err=True,
        )
        return
    click.echo(_AI_AGENT_ERROR_MESSAGE, err=True)
    raise SystemExit(1)


def _full_help_options(command) -> list:
    options = []
    for param in getattr(command, "params", []):
        if isinstance(param, click.Option):
            options.append({
                "opts": list(param.opts) + list(param.secondary_opts),
                "help": param.help or "",
                "is_flag": bool(param.is_flag),
                "required": bool(param.required),
                "default": (
                    param.default
                    if (param.default is not None and not param.is_flag)
                    else None
                ),
                "type": getattr(param.type, "name", None),
            })
    return options


def _full_help_node(name: str, command, path: str) -> dict:
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
        node["commands"] = [
            _full_help_node(sub, command.commands[sub], f"{path} {sub}")
            for sub in sorted(command.commands)
        ]
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


def build_full_help(root, output: str = "text", prog_name: str = "tdk") -> str:
    """Render the help for ``root`` and every command/subcommand under it.

    Each option and argument is tagged ``(required)`` or ``(optional)`` in the
    text form, and carries a ``required`` boolean in the json form.

    ``output`` is ``"text"`` (a flat, indented listing) or ``"json"`` (a nested
    tree of ``{name, path, help, options, arguments, commands}`` objects)."""
    tree = _full_help_node(prog_name, root, prog_name)
    if output == "json":
        # ``default=str`` coerces any non-JSON option default (e.g. a Choice or
        # Path sentinel) to its string form rather than failing the dump.
        return json.dumps(tree, indent=2, default=str)
    return _full_help_text(tree)


def _incoming_argv(args, kwargs) -> list:
    if kwargs.get("args") is not None:
        return list(kwargs["args"])
    if args and args[0] is not None:
        return list(args[0])
    return list(sys.argv[1:])


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


class _AiAgentGuardedGroup(click.RichGroup):
    """Top-level group that refuses to run inside a supported AI agent. The check
    runs in ``main`` -- before any argument parsing -- so even ``--help`` and
    ``--version`` are gated, not just subcommands.

    ``main`` also intercepts ``--help --full`` (before Click's own ``--help``
    handling) to print the recursive full help for every command and
    subcommand. ``--full`` is only honored together with ``--help``, and
    ``--format`` only together with ``--full``:
    ``tdk --help [--full [--format FORMAT]]``."""

    def main(self, *args, **kwargs):
        enforce_not_in_ai_agent()
        argv = _incoming_argv(args, kwargs)
        wants_help = "--help" in argv or "-h" in argv
        if wants_help and "--full" in argv:
            click.echo(build_full_help(self, _full_output_format(argv)))
            raise SystemExit(0)
        return super().main(*args, **kwargs)


@click.group(cls=_AiAgentGuardedGroup, context_settings=CONTEXT_SETTINGS)
@click.version_option(version=__version__, message="Tabsdata Client %(version)s")
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
    try:
        os.makedirs(DEFAULT_TABSDATA_DIRECTORY, exist_ok=True)
        ctx.obj = {
            "tabsdata_directory": DEFAULT_TABSDATA_DIRECTORY,
            "no_prompt": no_prompt,
        }
        # The server connection is set up lazily: the pinned project is applied
        # when the connection is first built (on first use), so commands that
        # never talk to the server don't construct it or import the server SDK.
        initialise_tabsdata_server_connection(ctx)
        # Every persisted setting -- hints, prompt, server, pins -- rides in
        # the one options store.
        load_cli_options(ctx)
        # 'tdk set --prompt false' is a standing '--no-prompt'; the flag turns
        # prompting off for a single invocation.
        ctx.obj["prompt_setting"] = cli_option_enabled(ctx, OPTION_PROMPT)
        ctx.obj["no_prompt"] = no_prompt or not ctx.obj["prompt_setting"]
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to initialize CLI: {e}")


# Attach every @cli_command-annotated group: the built-ins imported above, plus
# any enterprise-tier variant commands that register themselves when their
# package is installed. No manual add_command list to keep
# in sync -- a new command only needs the @cli_command annotation.
for _command in collect_cli_commands():
    cli.add_command(_command)


@cli.command()
@click.pass_context
def shell(ctx: click.Context):
    """Start an interactive shell session"""
    from tabsdatak.cli.shell import run_shell

    run_shell(
        ctx,
        cli,
        __version__,
        banner_title="Tabsdata Interactive Shell",
        tool_name="tdk",
    )


@cli.command()
def about():
    """Show build metadata and system information."""
    tdabout()


@cli.command()
@click.option("--third-party", is_flag=True, help="Show the third party dependencies.")
@click.option("--license", is_flag=True, help="Show the license.")
@click.option(
    "--release-notes",
    is_flag=True,
    help="Show the release notes of the current version of the CLI.",
)
def info(third_party: bool, license: bool, release_notes: bool):
    """
    Provide information about license and third party libraries
    """
    from tabsdatak.common._resources.reader import td_resource

    if license:
        license_path = td_resource("assets/manifest/LICENSE")
        with open(license_path, "r", encoding="utf-8") as f:
            console.print()
            console.print(f"{UNDERLINE}License{NO_UNDERLINE}")
            console.print()
            console.print(f"{CYAN}{textwrap.dedent(f.read()).strip()}{NO_CYAN}")
            console.print()
    if third_party:
        third_party_path = td_resource("assets/manifest/THIRD-PARTY")
        with open(third_party_path, "r", encoding="utf-8") as f:
            console.print()
            console.print(f"{UNDERLINE}Third Party Dependencies{NO_UNDERLINE}")
            console.print()
            console.print(f"{CYAN}{f.read()}{NO_CYAN}")
            console.print()
    if release_notes:
        release_notes_path = td_resource("assets/manifest/RELEASE-NOTES")
        with open(release_notes_path, "r", encoding="utf-8") as f:
            console.print()
            console.print(f"{UNDERLINE}Release Notes{NO_UNDERLINE}")
            console.print()
            console.print(f"{CYAN}{f.read()}{NO_CYAN}")
            console.print()


@cli.command()
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
@click.pass_context
def login(ctx: click.Context, server: str, user: str, password: str):
    """Login to the Tabsdata Server"""
    server = (
        server or configured_server(ctx) or logical_prompt(ctx, "Tabsdata Server URL")
    )
    user = user or logical_prompt(ctx, "Username for the Tabsdata Server")
    password = password or logical_prompt(
        ctx,
        "Password",
        hide_input=True,
    )
    utils_login(ctx, server, user, password)
    from tabsdatak.cli.td.config_utils import execute_actions_for_command

    execute_actions_for_command("login")


@cli.command()
@click.pass_context
def logout(ctx: click.Context):
    """Logout from the Tabsdata Server"""
    try:
        ctx.obj["tabsdataserver"].logout()
        click.echo("Logout successful.")
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to logout: {e}")
    finally:
        try:
            os.remove(get_credentials_file_path(ctx))
        except Exception:
            pass
        try:
            clear_pins(ctx)
            store_cli_options(ctx)
        except Exception:
            pass


@cli.command()
@click.pass_context
def status(ctx: click.Context):
    """Check the status of the server"""
    verify_login_or_prompt(ctx)
    click.echo("Obtaining server status")
    click.echo("-" * 10)
    try:
        current_status = ctx.obj["tabsdataserver"].status
        click.echo(str(current_status))
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to get status: {e}")


if __name__ == "__main__":
    from tabsdatak.common._compatibility import (
        PackageVersionError,
        check_sticky_version_packages,
    )

    try:
        check_sticky_version_packages()
    except PackageVersionError as error:
        print(error.messages, file=sys.stderr)
    cli()
