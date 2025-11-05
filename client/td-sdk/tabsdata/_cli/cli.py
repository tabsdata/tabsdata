#
# Copyright 2024 Tabs Data Inc.
#

import os
import shutil
import sys
import textwrap
from pathlib import Path

import rich_click as click
from rich.console import Console

from tabsdata import __version__
from tabsdata._cli import examples_guide
from tabsdata._cli.auth_group import auth
from tabsdata._cli.cli_utils import (
    get_credentials_file_path,
    hint_common_solutions,
    initialise_tabsdata_server_connection,
    load_cli_options,
    load_pinned_objects,
    logical_prompt,
    set_current_cli_option,
    show_hint,
    store_cli_options,
    store_pinned_objects,
    utils_login,
    verify_login_or_prompt,
)
from tabsdata._cli.collection_group import collection
from tabsdata._cli.config_utils import execute_actions_for_command
from tabsdata._cli.exe_group import exe
from tabsdata._cli.fn_group import fn
from tabsdata._cli.role_group import role
from tabsdata._cli.table_group import table
from tabsdata._cli.user_group import user
from tabsdata._utils.compatibility import (
    PackageVersionError,
    check_sticky_version_packages,
)
from tabsdata._utils.internal._about import tdabout
from tabsdata.api.apiserver import DEFAULT_TABSDATA_DIRECTORY

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
    except Exception as e:
        hint_common_solutions(ctx, e)
        raise click.ClickException(f"Failed to initialize CLI: {e}")


cli.add_command(auth)
cli.add_command(collection)
cli.add_command(exe)
cli.add_command(fn)
cli.add_command(role)
cli.add_command(table)
cli.add_command(user)


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
@click.option(
    "--dir",
    "-d",
    help=(
        "Directory to generate the examples in. The directory must not exist "
        "beforehand, as it will be created by the CLI."
    ),
)
@click.option(
    "--guide",
    "-g",
    is_flag=True,
    help="Open the examples guide in the browser.",
)
@click.pass_context
def examples(ctx: click.Context, dir: str, guide: bool):
    """Generate a folder with example use cases and/or open these example's
    guide."""

    def ignored_files(_folder, files):
        return [f for f in files if f == ".gitkeep"]

    if not dir and not guide:
        click.echo(ctx.get_help())
        ctx.exit()

    if dir:
        if os.path.exists(dir):
            raise click.ClickException(
                f"Failed to generate examples: {dir} already exists."
            )

        # noinspection PyProtectedMember
        import tabsdata.extensions._examples.cases as cases_module

        cases_folder = Path(cases_module.__path__[0])
        if os.path.exists(cases_folder):
            shutil.copytree(
                cases_folder,
                dir,
                dirs_exist_ok=True,
                ignore=ignored_files,
            )
            click.echo(f"Examples generated in '{dir}'")
            if not guide:
                show_hint(
                    ctx,
                    "If you want instructions on how to run the "
                    "examples, use 'td examples --guide' to open a "
                    "detailed walkthrough in your browser.",
                )
            show_hint(
                ctx,
                "Remember that in order to run the examples, tdserver must be "
                "running in the same host as the CLI.",
            )
        else:
            raise click.ClickException(
                "Failed to generate examples: internal error, could not find examples "
                "content folder in your local tabsdata package installation. As some "
                "files are missing, reinstalling the package is strongly recommended. "
                f"The missing folder is {cases_folder}."
            )

    if guide:
        examples_guide.run()


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
    assets_folder = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "assets", "manifest"
    )
    if license:
        with open(
            os.path.join(assets_folder, "LICENSE"),
            "r",
            encoding="utf-8",
        ) as f:
            console.print()
            console.print(f"{UNDERLINE}License{NO_UNDERLINE}")
            console.print()
            console.print(f"{CYAN}{textwrap.dedent(f.read()).strip()}{NO_CYAN}")
            console.print()
    if third_party:
        with open(
            os.path.join(assets_folder, "THIRD-PARTY"),
            "r",
            encoding="utf-8",
        ) as f:
            console.print()
            console.print(f"{UNDERLINE}Third Party Dependencies{NO_UNDERLINE}")
            console.print()
            console.print(f"{CYAN}{f.read()}{NO_CYAN}")
            console.print()
    if release_notes:
        with open(
            os.path.join(assets_folder, "RELEASE-NOTES"),
            "r",
            encoding="utf-8",
        ) as f:
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
    "--role",
    "-r",
    help="Role of the username in the Tabsdata Server.",
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
def login(ctx: click.Context, server: str, user: str, role: str, password: str):
    """Login to the Tabsdata Server"""
    server = server or logical_prompt(ctx, "Tabsdata Server URL")
    user = user or logical_prompt(ctx, "Username for the Tabsdata Server")
    password = password or logical_prompt(
        ctx,
        "Password",
        hide_input=True,
    )
    role = role or logical_prompt(ctx, "Role", default_value="user")
    utils_login(ctx, server, user, password, role)
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
