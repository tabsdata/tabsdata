#
# Copyright 2024 Tabs Data Inc.
#

import logging.config
import os
import platform
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname

import yaml

import tabsdata._utils.tableframe._constants as td_constants

TRACE = logging.DEBUG - 1


def trace(msg, *args, **kwargs):
    logging.log(TRACE, msg, *args, **kwargs)


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ABSOLUTE_LOCATION = os.path.dirname(os.path.abspath(__file__))


TABSDATA_IDENTIFIER_COLUMN = td_constants.StandardSystemColumns.TD_IDENTIFIER.value

FILE_URI_PREFIX = "file://"

HOME_FOLDER_SYMBOL = "~"
TABSDATA_FOLDER_NAME = ".tabsdata"
TABSDATA_ROOT_FOLDER_NAME = ".root"
TARGET_FOLDER_NAME = "target"
TDLOCAL_FOLDER_NAME = "tdlocal"


def setup_logging(
    default_path,
    default_level=logging.INFO,
    env_key="TD_LOG_CFG",
    logs_folder=None,
):
    """Setup logging configuration"""
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, "rt") as f:
            config = yaml.safe_load(f.read())
        if logs_folder:
            os.makedirs(logs_folder, exist_ok=True)
            handlers = config.get("handlers", {})
            for handler_name, handler_config in handlers.items():
                if handler_config.get("filename"):
                    handler_config["filename"] = os.path.join(
                        logs_folder, handler_config["filename"]
                    )
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


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


def convert_uri_to_path(uri: str) -> str:
    parsed = urlparse(uri)
    host = "{0}{0}{mnt}{0}".format(os.path.sep, mnt=parsed.netloc)
    path = os.path.normpath(os.path.join(host, url2pathname(parsed.path)))
    logger.debug(f"Converted URI '{uri}' to path '{path}'")
    return path


def convert_path_to_uri(path: str) -> str:
    uri = path if path.startswith(FILE_URI_PREFIX) else Path(path).as_uri()
    if path.endswith(os.path.sep) and not uri.endswith("/"):
        # This might seem redundant, but Path(path).as_uri() removes the trailing
        # slash, and we want to keep it if it was present in the original path.
        uri += "/"
    logger.debug(f"Converted path '{path}' to URI '{uri}'")
    return uri


def _get_root_folder() -> str:
    current_folder = os.path.dirname(__file__)
    trace(f"Current conftest folder is: {current_folder}")
    while True:
        git_folder = Path(os.path.join(current_folder, ".git"))
        root_file = Path(os.path.join(current_folder, ".root"))
        git_folder_exists = git_folder.exists() and os.path.isdir(git_folder)
        root_file_exists = root_file.exists() and root_file.is_file()
        if git_folder_exists or root_file_exists:
            trace(f"Root project folder for conftest is: {current_folder}")
            return current_folder
        else:
            parent_folder = os.path.abspath(os.path.join(current_folder, os.pardir))
            if current_folder == parent_folder:
                raise FileNotFoundError(
                    "Current folder not inside a Git repository or "
                    "owned by a .root file"
                )
            current_folder = parent_folder


try:
    ROOT_FOLDER = _get_root_folder()
except FileNotFoundError:
    home_dir = os.path.expanduser(HOME_FOLDER_SYMBOL)
    ROOT_FOLDER = os.path.join(
        home_dir,
        TABSDATA_FOLDER_NAME,
        TABSDATA_ROOT_FOLDER_NAME,
    )


def _get_target_folder() -> str:
    return os.path.join(ROOT_FOLDER, TARGET_FOLDER_NAME)


TARGET_FOLDER = _get_target_folder()


def _get_tdlocal_folder() -> str:
    return os.path.join(TARGET_FOLDER, TDLOCAL_FOLDER_NAME)


TDLOCAL_FOLDER = _get_tdlocal_folder()


def _get_locks_folder():
    return os.path.join(
        _get_tdlocal_folder(),
        "environment_locks",
    )


LOCKS_FOLDER = _get_locks_folder()

DEFAULT_DEVELOPMENT_LOCKS_LOCATION = LOCKS_FOLDER

CURRENT_PLATFORM = CurrentPlatform()
