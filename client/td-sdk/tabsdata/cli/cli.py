#
# Copyright 2024 Tabs Data Inc.
#

import os
import shutil

import rich_click as click

from tabsdata.api.apiserver import DEFAULT_TABSDATA_DIRECTORY
from tabsdata.cli.auth_group import auth
from tabsdata.cli.cli_utils import (
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
from tabsdata.cli.collection_group import collection
from tabsdata.cli.exe_group import exe
from tabsdata.cli.fn_group import fn
from tabsdata.cli.table_group import table
from tabsdata.cli.user_group import user

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option()
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


cli.add_command(collection)
cli.add_command(exe)
cli.add_command(fn)
cli.add_command(auth)
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
    help=(
        "Directory to generate the example in. The directory must not exist "
        "beforehand, as it will be created by the CLI."
    ),
)
@click.pass_context
def example(ctx: click.Context, dir: str):
    """Create a folder with an example of a publisher, transformer and subscriber"""
    dir = dir or logical_prompt(
        ctx,
        "Directory to generate the example in. The directory must not exist "
        "beforehand, as it will be created by the CLI",
    )
    if not dir:
        raise click.ClickException(
            "Failed to generate examples: directory not provided."
        )
    elif os.path.exists(dir):
        raise click.ClickException(
            f"Failed to generate examples: {dir} already exists."
        )
    examples_folder = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "..",
            "examples",
        )
    )
    if os.path.exists(examples_folder):
        shutil.copytree(examples_folder, dir, dirs_exist_ok=True)
        output_folder = os.path.join(dir, "output")
        os.makedirs(output_folder, exist_ok=True)
        click.echo(f"Examples generated in '{dir}'.")
        show_hint(
            ctx,
            "Remember to set environment variable 'TDX' to the path of your "
            "'examples' directory. For more information, see section 'Set Up the "
            "Example Directory in an Environment Variable' in the 'Tabsdata Getting "
            "Started Example' documentation.",
        )
    else:
        raise click.ClickException(
            "Failed to generate examples: internal error, could not find examples "
            "content folder in your local tabsdata package installation. As some "
            "files are missing, reinstalling the package is strongly recommended. "
            f"The missing folder is {examples_folder}."
        )


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
            click.echo("License:")
            click.echo()
            click.echo("-" * 20)
            click.echo(f.read())
            click.echo("-" * 20)
            click.echo()
    if third_party:
        with open(
            os.path.join(assets_folder, "THIRD-PARTY"),
            "r",
            encoding="utf-8",
        ) as f:
            click.echo("3rd party dependencies:")
            click.echo()
            click.echo("-" * 20)
            click.echo(f.read())
            click.echo("-" * 20)
            click.echo()
    if release_notes:
        with open(
            os.path.join(assets_folder, "RELEASE-NOTES"),
            "r",
            encoding="utf-8",
        ) as f:
            click.echo("Release notes:")
            click.echo()
            click.echo("-" * 20)
            click.echo(f.read())
            click.echo("-" * 20)
            click.echo()


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
    cli()
