#
# Copyright 2024 Tabs Data Inc.
#

import json
import os
import platform
import shutil
from datetime import datetime

import rich_click as click
from PIL import Image
from rich_click import Option, UsageError

from tabsdata.api.api_server import APIServer, obtain_connection
from tabsdata.api.tabsdata_server import TabsdataServer

CONNECTION_FILE = "connection.json"
DEFAULT_TABSDATA_DIRECTORY = os.path.join(os.path.expanduser("~"), ".tabsdata")

DOT_FOLDER = os.path.join(DEFAULT_TABSDATA_DIRECTORY, "dot")
DOT_FORMAT = "-Tjpg -Gdpi=300 -Nfontsize=10 -Nmargin=0.4 -Efontsize=10"


def beautify_list(list_to_show) -> str:
    if not list_to_show:
        return "<None>"
    if isinstance(list_to_show, list):
        return "\n".join(list_to_show)
    return str(list_to_show)


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
    return click.prompt(message, default=default_value, hide_input=hide_input)


def utils_login(ctx: click.Context, server_url: str, username: str, password: str):
    try:
        connection = obtain_connection(server_url, username, password)
    except Exception as e:
        raise click.ClickException(f"Failed to login: {e}")
    connection._store_in_file(
        os.path.join(ctx.obj["tabsdata_directory"], CONNECTION_FILE)
    )
    click.echo("Login successful.")


def request_login_information():
    server_url = click.prompt("Server URL")
    username = click.prompt("Username")
    password = click.prompt("Password", hide_input=True)
    return server_url, username, password


def initialise_tabsdata_server_connection(ctx: click.Context):
    try:
        credentials = json.load(
            open(os.path.join(DEFAULT_TABSDATA_DIRECTORY, CONNECTION_FILE))
        )
        connection = APIServer(credentials.get("url"))
        connection.refresh_token = credentials.get("refresh_token")
        connection.bearer_token = credentials.get("bearer_token")
        tabsdata_server = TabsdataServer.__new__(TabsdataServer)
        tabsdata_server.connection = connection
    except FileNotFoundError:
        tabsdata_server = None
    ctx.obj["tabsdataserver"] = tabsdata_server


def verify_login_or_prompt(ctx: click.Context):
    if not ctx.obj["tabsdataserver"]:
        click.echo("No credentials found. Please login first.")
        server_url, username, password = request_login_information()
        utils_login(ctx, server_url, username, password)
        initialise_tabsdata_server_connection(ctx)


def complete_datetime(incomplete_datetime: str | None) -> str | None:
    if not incomplete_datetime:
        return None
    # Define possible formats for incomplete datetime strings
    formats = [
        "%Y-%m-%d",
        "%Y-%m-%dT%HZ",
        "%Y-%m-%dT%H:%MZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%fZ",
    ]

    for fmt in formats:
        try:
            # Try to parse the incomplete datetime string
            dt = datetime.strptime(incomplete_datetime, fmt)
            # Format the datetime to the complete format
            complete_dt = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            return complete_dt
        except ValueError:
            continue

    raise ValueError(
        "Invalid datetime format string. It should be of one of the "
        f"following: {formats}."
    )


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


def cleanup_dot_files():
    click.echo("Cleaning up DOT files older than 30 minutes")
    try:
        for file in os.listdir(DOT_FOLDER):
            file_path = os.path.join(DOT_FOLDER, file)
            if os.path.isfile(file_path):
                if os.stat(file_path).st_mtime < datetime.now().timestamp() - 1800:
                    os.remove(file_path)
        click.echo("DOT files cleaned up successfully")
    except Exception as e:
        click.echo(f"Failed to clean up DOT files: {e}")
        click.echo("This will not affect the rest of the command execution.")


CURRENT_CLI_PLATFORM = CurrentPlatform()


def show_dot_file(full_path: str):
    if os.environ.get("TD_CLI_SHOW") in ["0", "False", "false", "no", "NO"]:
        click.echo("Skipping DOT file opening")
        return
    dot_binary = "dot"
    if CURRENT_CLI_PLATFORM.is_windows():
        dot_binary = "dot.exe"
    if not shutil.which(dot_binary):
        click.echo("Cannot open DOT file, dot binary not found")
        click.echo(
            "If you want to be able to open DOT files, please install "
            "the dot binary from Graphviz (https://graphviz.org/)"
        )
        return
    try:
        jpg_full_path = full_path[: -len(".dot")] + ".jpg"
        os.system(f"{dot_binary} {DOT_FORMAT} -o {jpg_full_path} {full_path}")
        click.echo(f"Generated DOT jpg file at {jpg_full_path}")
        click.echo("Opening DOT file")
        img = Image.open(jpg_full_path)
        img.show()
    except Exception as e:
        click.echo(f"Failed to open DOT file: {e}")
