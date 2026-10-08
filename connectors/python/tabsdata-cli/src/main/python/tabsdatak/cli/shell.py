#
# Copyright 2026 Tabsdata Inc.
#

import logging
import os
import shlex
from urllib.parse import urlparse

import click
import click_repl._completer as _completer_mod
import click_repl.utils as _utils_mod
from click_repl import repl
from click_repl.exceptions import ExitReplException
from prompt_toolkit.application import Application
from prompt_toolkit.history import FileHistory
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.keys import Keys
from prompt_toolkit.styles import Style
from rich.panel import Panel
from rich.text import Text

from tabsdatak.cli.cli_utils import get_credentials_file_path, make_console

# noinspection PyProtectedMember
from tabsdatak.sdk import _credentials
from tabsdatak.sdk._apiserver import APIServer
from tabsdatak.sdk.tabsdata_server import TabsdataServer

# Reduce the escape key flush timeout from 0.5s to 0.05s so that
# pressing ESC feels instant (e.g. to dismiss the completion popup).
_orig_app_init = Application.__init__


def _fast_esc_app_init(self, *args, **kwargs):
    _orig_app_init(self, *args, **kwargs)
    self.ttimeoutlen = 0.05
    self.timeoutlen = 0.05


Application.__init__ = _fast_esc_app_init

# Suppress noisy asyncio debug logging (e.g. "Using selector: KqueueSelector")
logging.getLogger("asyncio").setLevel(logging.WARNING)

# ---------------------------------------------------------------------------
# Click 8.2+ compatibility patch for click-repl 0.3.0.
#
# Click 8.2 deprecated Context.protected_args (read-only property backed by
# _protected_args). click-repl 0.3.0 still writes to it, which crashes.
# The fix is merged upstream (click-contrib/click-repl#132) but unreleased.
# We apply the same approach: access _protected_args when available.
# Remove this patch once a fixed click-repl is released.
# ---------------------------------------------------------------------------


def get_protected_args(ctx):
    if hasattr(ctx, "_protected_args"):
        return ctx._protected_args
    return ctx.protected_args


def set_protected_args(ctx, value):
    if hasattr(ctx, "_protected_args"):
        ctx._protected_args = value
    else:
        ctx.protected_args = value


# Patch _resolve_context (used by completer and utils)
_original_resolve = _utils_mod._resolve_context


def patched_resolve(args, ctx=None):
    while args:
        command = ctx.command
        if isinstance(command, click.MultiCommand):
            try:
                if not command.chain:
                    name, cmd, args = command.resolve_command(ctx, args)
                    if cmd is None:
                        return ctx
                    ctx = cmd.make_context(
                        name, args, parent=ctx, resilient_parsing=True
                    )
                    args = get_protected_args(ctx) + ctx.args
                else:
                    while args:
                        name, cmd, args = command.resolve_command(ctx, args)
                        if cmd is None:
                            return ctx
                        sub_ctx = cmd.make_context(
                            name,
                            args,
                            parent=ctx,
                            allow_extra_args=True,
                            allow_interspersed_args=False,
                            resilient_parsing=True,
                        )
                        args = sub_ctx.args
                    ctx = sub_ctx
                    args = [*get_protected_args(sub_ctx), *sub_ctx.args]
            except click.UsageError:
                return ctx
        else:
            break
    return ctx


_utils_mod._resolve_context = patched_resolve
_completer_mod._resolve_context = patched_resolve

# Add the missing property setter so click-repl can write protected_args.
_protected_args = click.Context.__dict__.get("protected_args")
if isinstance(_protected_args, property) and _protected_args.fset is None:
    click.Context.protected_args = property(
        _protected_args.fget,
        lambda self, value: set_protected_args(self, value),
        _protected_args.fdel,
        _protected_args.__doc__,
    )


# -----------------------------------------------------------------------
# Shell builtin interception for !-prefixed commands.
#
# os.system() runs in a subshell, so builtins like cd and export have no
# lasting effect.  We intercept them and translate to Python equivalents,
# same approach as IPython's %cd / %env magics.
# -----------------------------------------------------------------------
def handle_shell_builtin(cmd_string):
    """Handle shell builtins that must run in-process. Returns True if handled."""
    try:
        tokens = shlex.split(cmd_string)
    except ValueError:
        tokens = cmd_string.split()
    if not tokens:
        return False

    builtin = tokens[0]

    if builtin == "cd":
        target = tokens[1] if len(tokens) > 1 else os.path.expanduser("~")
        try:
            os.chdir(os.path.expandvars(os.path.expanduser(target)))
        except OSError as e:
            click.echo(f"cd: {e}")
        return True

    if builtin in ("set", "export"):
        for arg in tokens[1:]:
            if "=" in arg:
                key, value = arg.split("=", 1)
                os.environ[key] = value
            else:
                click.echo(os.environ.get(arg, f"{arg}: not set"))
        return True

    if builtin == "unset":
        for arg in tokens[1:]:
            os.environ.pop(arg, None)
        return True

    return False


_original_dispatch = _utils_mod.dispatch_repl_commands


def patched_dispatch(command):
    if command.startswith("!"):
        cmd_string = command[1:].strip()
        if handle_shell_builtin(cmd_string):
            return True
        return _original_dispatch(command)
    return False


_utils_mod.dispatch_repl_commands = patched_dispatch

# Register :e as an alias for :exit
_utils_mod._register_internal_command(
    ["e"], _utils_mod._exit_internal, "exits the repl"
)

# :? will be overridden per-session in run_shell to show the banner.
# We remove it from click-repl's default help binding here so it doesn't
# conflict; it gets re-registered in run_shell with the banner callback.
_utils_mod._internal_commands.pop("?", None)


def show_banner(title, version):
    """Display the shell welcome banner."""
    console = make_console()

    body = Text()
    body.append("\n")
    body.append("help                  ", style="bold cyan")
    body.append("Show available commands\n")
    body.append("exit", style="bold cyan")
    body.append(" / ", style="dim")
    body.append("quit", style="bold cyan")
    body.append("           Leave the shell (also Ctrl+D)\n")
    body.append("!<command>            ", style="bold cyan")
    body.append("Run a system shell command\n")
    body.append("!cd <path>            ", style="bold cyan")
    body.append("Change working directory\n")
    body.append("!set VAR=val          ", style="bold cyan")
    body.append("Set an environment variable\n")
    body.append("!unset VAR            ", style="bold cyan")
    body.append("Remove an environment variable\n")
    body.append(":?", style="bold cyan")
    body.append("                    Show this quick reference\n")
    body.append(":help", style="bold cyan")
    body.append(" / ", style="dim")
    body.append(":exit", style="bold cyan")
    body.append(" / ", style="dim")
    body.append(":quit", style="bold cyan")
    body.append(" Built-in commands (also :h / :e / :q)\n")
    body.append("\n")
    body.append("Press ", style="dim")
    body.append("TAB", style="bold cyan")
    body.append(" for command autocompletion and suggestions\n", style="dim")
    body.append("Use ", style="dim")
    body.append("ESC", style="bold cyan")
    body.append(" or ", style="dim")
    body.append("Ctrl+C", style="bold cyan")
    body.append(" to abort input prompts", style="dim")

    console.print()
    console.print(
        Panel(
            body,
            title=f"[bold]{title}[/bold] v{version}",
            title_align="left",
            border_style="#82c369",
            padding=(0, 1),
            expand=False,
        )
    )
    console.print()


class SessionInfo:
    """Tracks login state by watching the connection file."""

    def __init__(self, credentials_path, tool_name):
        self._cred_path = credentials_path
        self._tool_name = tool_name
        self._mtime = None
        self.user = None
        self.server = None
        self.refresh()

    def refresh(self):
        try:
            mtime = os.path.getmtime(self._cred_path)
        except OSError:
            self._clear()
            return

        if mtime == self._mtime:
            return

        self._mtime = mtime
        try:
            creds = _credentials.read(self._cred_path) or {}
            parsed = urlparse(creds.get("url", ""))
            self.server = parsed.hostname or "unknown"
            if parsed.port:
                self.server += f":{parsed.port}"

            conn = APIServer(creds.get("url", ""))
            conn.bearer_token = creds.get("bearer_token")
            conn.refresh_token = creds.get("refresh_token")
            conn.token_type = creds.get("token_type")
            conn.expires_in = creds.get("expires_in")
            conn.expiration_time = creds.get("expiration_time")
            tds = TabsdataServer.__new__(TabsdataServer)
            tds.connection = conn
            info = tds.auth_info()
            self.user = info.get("name")
        except Exception:
            self._clear()

    def _clear(self):
        self._mtime = None
        self.user = None
        self.server = None

    @property
    def logged_in(self):
        return self.user is not None

    def build_prompt(self):
        self.refresh()
        if self.logged_in:
            return [
                ("#b4d296", self.server),
                ("dim", " - "),
                ("bold #b4d296", self.user),
                ("", " "),
                ("bold", self._tool_name),
                ("", " ❯ "),
            ]
        else:
            return [
                ("dim", "[not logged in]"),
                ("", " "),
                ("bold", self._tool_name),
                ("", " ❯ "),
            ]


def run_shell(ctx, cli_group, version, *, banner_title, tool_name):
    """Run an interactive shell (REPL)."""

    history_path = os.path.join(ctx.obj["tabsdata_directory"], f".{tool_name}.history")

    _utils_mod._register_internal_command(
        ["?"],
        lambda: show_banner(banner_title, version),
        "shows the shell quick reference",
    )

    show_banner(banner_title, version)

    # Register exit/quit/help so they work without the ':' prefix that
    # click-repl normally requires for internal commands.
    @click.command(hidden=True)
    def exit():
        """Exit the interactive shell."""
        raise ExitReplException()

    @click.command(hidden=True)
    @click.pass_context
    def help(ctx):
        """Show available commands."""
        click.echo(ctx.parent.get_help())

    cli_group.add_command(exit, "exit")
    cli_group.add_command(exit, "quit")
    cli_group.add_command(help, "help")

    session_info = SessionInfo(get_credentials_file_path(ctx), tool_name)

    # Key bindings: when the completion menu is visible, Enter accepts
    # the selected completion instead of executing the current input.
    td_bindings = KeyBindings()

    @td_bindings.add(Keys.Enter, filter=True)
    def _enter_handler(event):
        buf = event.current_buffer
        if buf.complete_state:
            buf.complete_state = None
        else:
            buf.validate_and_handle()

    @td_bindings.add(Keys.Escape, filter=True)
    def _escape_handler(event):
        buf = event.current_buffer
        if buf.complete_state:
            buf.cancel_completion()

    td_style = Style.from_dict({
        "completion-menu": "bg:default #82c369",
        "completion-menu.completion": "bg:default #b4d296",
        "completion-menu.completion.current": "bg:#2d3b28 #82c369 bold",
        "completion-menu.meta.completion": "bg:default #6a9e55",
        "completion-menu.meta.completion.current": "bg:#2d3b28 #82c369",
    })

    try:
        repl(
            click.get_current_context(),
            prompt_kwargs={
                "message": session_info.build_prompt,
                "history": FileHistory(history_path),
                "style": td_style,
                "key_bindings": td_bindings,
                "complete_while_typing": False,
            },
            allow_system_commands=True,
            allow_internal_commands=True,
        )
    finally:
        cli_group.commands.pop("exit", None)
        cli_group.commands.pop("quit", None)
        cli_group.commands.pop("help", None)
