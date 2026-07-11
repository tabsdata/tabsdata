#
# Copyright 2026 Tabs Data Inc.
#

import os
import sys
import textwrap
from importlib.metadata import PackageNotFoundError, version

import rich_click as click
from rich.console import Console

try:
    __version__ = version("tabsdata-cli")
except PackageNotFoundError:
    __version__ = "0.0.0"

from tabsdatak.cli._registry import collect_cli_commands
from tabsdatak.cli.cli_utils import (
    get_credentials_file_path,
    hint_common_solutions,
    initialise_tabsdata_server_connection,
    load_cli_options,
    load_pinned_objects,
    logical_prompt,
    set_current_cli_option,
    store_cli_options,
    store_pinned_objects,
    td_resource,
    tdabout,
    utils_login,
    verify_login_or_prompt,
)

# Built-in command groups, imported for their @cli_command side effects: each
# module registers its top-level group, attached below via collect_cli_commands.
# TODO: role, cron, and stage management are temporarily removed; reintroduce
# their modules here (they self-register via @cli_command) in a later version.
from tabsdatak.cli.td import (  # noqa: F401
    auth_group,
    collection_group,
    connection_group,
    examples_group,
    exe_group,
    fn_group,
    group_group,
    metrics_group,
    permission_group,
    pin_group,
    project_group,
    table_group,
    user_group,
)
from tabsdatak.cli.td.config_utils import execute_actions_for_command
from tabsdatak.common._compatibility import (
    PackageVersionError,
    check_sticky_version_packages,
)
from tabsdatak.sdk._apiserver import DEFAULT_TABSDATA_DIRECTORY

CYAN = "[cyan]"
NO_CYAN = "[/cyan]"

BOLD = "[bold]"
NO_BOLD = "[/bold]"

UNDERLINE = "[underline]"
NO_UNDERLINE = "[underline]"

console = Console()

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(version=__version__, message="Tabsdata Client %(version)s")
@click.option(
    "--no-prompt",
    is_flag=True,
    help=(
        "Disable all prompts. If a prompt is required for proper execution, "
        "the command will fail."
    ),
)
@click.pass_context
def cli(ctx: click.Context, no_prompt: bool):
    """Main CLI for the Tabsdata system"""
    try:
        os.makedirs(DEFAULT_TABSDATA_DIRECTORY, exist_ok=True)
        ctx.obj = {
            "tabsdata_directory": DEFAULT_TABSDATA_DIRECTORY,
            "no_prompt": no_prompt,
        }
        initialise_tabsdata_server_connection(ctx)
        load_pinned_objects(ctx)
        load_cli_options(ctx)
        # The active project rides on the connection: it defaults to the pinned
        # project, and a per-command `--proj` overrides it for that invocation.
        # Project-scoped commands never thread it -- the connection injects it
        # into every tenant path.
        server = ctx.obj.get("tabsdataserver")
        if server is not None:
            server.connection.project = ctx.obj["pinned_objects"].get("project")
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
        tool_name="td",
    )


@cli.command()
@click.argument("mode", type=click.Choice(["on", "off"]))
@click.pass_context
def hints(ctx: click.Context, mode: str):
    """Toggle hints in the CLI. Provide either 'on' or 'off' as an argument."""
    if mode == "on":
        set_current_cli_option(ctx, "hints", "enabled")
        store_cli_options(ctx)
        click.echo("Hints enabled. You will now see hints in the CLI.")
    else:
        set_current_cli_option(ctx, "hints", "disabled")
        store_cli_options(ctx)
        click.echo("Hints disabled. You will no longer see hints in the CLI.")


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
    help="URL of the Tabsdata Server. Will be prompted for it if not provided.",
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
    server = server or logical_prompt(ctx, "Tabsdata Server URL")
    user = user or logical_prompt(ctx, "Username for the Tabsdata Server")
    password = password or logical_prompt(
        ctx,
        "Password",
        hide_input=True,
    )
    utils_login(ctx, server, user, password)
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
            ctx.obj["pinned_objects"] = {}
            store_pinned_objects(ctx)
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
    try:
        check_sticky_version_packages()
    except PackageVersionError as error:
        print(error.messages, file=sys.stderr)
    cli()
