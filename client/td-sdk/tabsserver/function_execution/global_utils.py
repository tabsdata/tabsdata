#
# Copyright 2024 Tabs Data Inc.
#

import logging.config
import os
import pathlib
import platform
from urllib.parse import urlparse
from urllib.request import url2pathname

import yaml

import tabsdata.utils.tableframe._constants as td_constants

logger = logging.getLogger(__name__)
ABSOLUTE_LOCATION = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DEVELOPMENT_LOCKS_LOCATION = os.path.join(
    os.path.dirname(ABSOLUTE_LOCATION),
    "local_dev",
    "environment_locks",
)

CSV_EXTENSION = "csv"
NDJSON_EXTENSION = "ndjson"
PARQUET_EXTENSION = "parquet"
TABSDATA_EXTENSION = "t"

TABSDATA_IDENTIFIER_COLUMN = td_constants.StandardSystemColumns.TD_IDENTIFIER.value

FILE_URI_PREFIX = "file://"


def setup_logging(
    default_path,
    default_level=logging.INFO,
    env_key="LOG_CFG",
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
        logger.info("Getting the current platform information.")
        self.platform = platform.system()
        logger.info(f"Platform: {self.platform}")

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
    logger.debug(f"Converted URI {uri} to path: {path}")
    return path


def convert_path_to_uri(path: str) -> str:
    return path if path.startswith(FILE_URI_PREFIX) else pathlib.Path(path).as_uri()


CURRENT_PLATFORM = CurrentPlatform()
