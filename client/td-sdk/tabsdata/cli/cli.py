#
# Copyright 2024 Tabs Data Inc.
#

import os
import shutil

import rich_click as click

from tabsdata.cli.cli_utils import (
    CONNECTION_FILE,
    DEFAULT_TABSDATA_DIRECTORY,
    initialise_tabsdata_server_connection,
    logical_prompt,
    utils_login,
    verify_login_or_prompt,
)
from tabsdata.cli.collection_group import collection
from tabsdata.cli.data_group import data
from tabsdata.cli.exec_group import exec
from tabsdata.cli.fn_group import fn
from tabsdata.cli.table_group import table
from tabsdata.cli.user_group import user


@click.group()
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
    os.makedirs(DEFAULT_TABSDATA_DIRECTORY, exist_ok=True)
    ctx.obj = {"tabsdata_directory": DEFAULT_TABSDATA_DIRECTORY, "no_prompt": no_prompt}
    initialise_tabsdata_server_connection(ctx)


cli.add_command(collection)
cli.add_command(data)
cli.add_command(exec)
cli.add_command(fn)
cli.add_command(table)
cli.add_command(user)


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
        click.echo(f"Examples generated in {dir}.")
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
@click.argument("server-url")
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
def login(ctx: click.Context, server_url: str, user: str, password: str):
    """Login to the Tabsdata Server"""
    user = user or logical_prompt(ctx, "Username for the Tabsdata Server")
    password = password or logical_prompt(
        ctx,
        "Password for the Tabsdata Server",
        hide_input=True,
    )
    utils_login(ctx, server_url, user, password)


@cli.command()
@click.pass_context
def logout(ctx: click.Context):
    """Logout from the Tabsdata Server"""
    try:
        os.remove(os.path.join(ctx.obj["tabsdata_directory"], CONNECTION_FILE))
    except FileNotFoundError:
        click.echo("No credentials found.")
    else:
        click.echo("Logout successful.")


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
        raise click.ClickException(f"Failed to get status: {e}")


if __name__ == "__main__":
    cli()
