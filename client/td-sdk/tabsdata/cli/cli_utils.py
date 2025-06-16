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

from tabsdata.api.apiserver import APIServer, obtain_connection
from tabsdata.api.tabsdata_server import STATUS_MAPPING, TabsdataServer

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


def utils_login(
    ctx: click.Context, server_url: str, username: str, password: str, role: str = None
):
    try:
        obtain_connection(
            server_url,
            username,
            password,
            role=role,
            credentials_file=get_credentials_file_path(ctx),
        )
    except Exception as e:
        raise click.ClickException(f"Failed to login: {e}")
    click.echo("Login successful.")


def request_login_information(ctx: click.Context):
    server_url = logical_prompt(ctx, "Server URL")
    username = logical_prompt(ctx, "Username")
    password = logical_prompt(ctx, "Password", hide_input=True)
    role = logical_prompt(ctx, "Role", default_value="user")
    return server_url, username, password, role


def initialise_tabsdata_server_connection(ctx: click.Context):
    try:
        credentials = json.load(
            open(os.path.join(DEFAULT_TABSDATA_DIRECTORY, CONNECTION_FILE))
        )
        connection = APIServer(
            credentials.get("url"),
            credentials_file=get_credentials_file_path(ctx),
        )
        connection.refresh_token = credentials.get("refresh_token")
        connection.bearer_token = credentials.get("bearer_token")
        connection.token_type = credentials.get("token_type")
        connection.expires_in = credentials.get("expires_in")
        connection.expiration_time = credentials.get("expiration_time")
        tabsdata_server = TabsdataServer.__new__(TabsdataServer)
        tabsdata_server.connection = connection
    except FileNotFoundError:
        tabsdata_server = None
    ctx.obj["tabsdataserver"] = tabsdata_server


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
        server_url, username, password, role = request_login_information(ctx)
        utils_login(ctx, server_url, username, password, role)
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


def cleanup_dot_files():
    try:
        for file in os.listdir(DOT_FOLDER):
            file_path = os.path.join(DOT_FOLDER, file)
            if os.path.isfile(file_path):
                if os.stat(file_path).st_mtime < datetime.now().timestamp() - 1800:
                    os.remove(file_path)
    except Exception:
        pass


CURRENT_CLI_PLATFORM = CurrentPlatform()


def generate_dot_image(full_path: str, open_image: bool = False):
    if os.environ.get("TD_CLI_SHOW") in ["0", "False", "false", "no", "NO"]:
        click.echo("Skipping DOT file opening")
        return
    dot_binary = "dot"
    if CURRENT_CLI_PLATFORM.is_windows():
        dot_binary = "dot.exe"
    if not shutil.which(dot_binary):
        click.echo("Cannot generate DOT image file, dot binary not found")
        click.echo(
            "If you want to be able to convert DOT files to images, please install "
            "the dot binary from Graphviz (https://graphviz.org/)"
        )
        return
    try:
        jpg_full_path = full_path[: -len(".dot")] + ".jpg"
        os.system(f"{dot_binary} {DOT_FORMAT} -o {jpg_full_path} {full_path}")
        click.echo(f"Plan DOT jpg at path: {jpg_full_path}")
        if open_image:
            click.echo("Opening DOT file")
            img = Image.open(jpg_full_path)
            img.show()
    except Exception as e:
        click.echo(f"Failed to open DOT file: {e}")


def get_credentials_file_path(ctx: click.Context) -> str:
    """
    Get the path to the credentials file.
    """
    return os.path.join(ctx.obj["tabsdata_directory"], CONNECTION_FILE)


def convert_user_provided_status_to_api_status(
    user_provided_status: str | None,
) -> str | None:
    """
    Convert a user-provided status string to the API status string.
    :param user_provided_status: The user-provided status string.
    :return: The API status string.
    """
    if not user_provided_status:
        return None

    user_provided_status = user_provided_status.lower()
    for key, value in STATUS_MAPPING.items():
        if user_provided_status == key.lower() or user_provided_status == value.lower():
            return key

    valid_statuses = ", ".join(STATUS_MAPPING.keys())
    valid_statuses += ", " + ", ".join(STATUS_MAPPING.values())
    raise ValueError(
        f"Invalid status: '{user_provided_status}'. "
        "Valid statuses are: "
        f"{valid_statuses}. Statuses are case-insensitive."
    )
