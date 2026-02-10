#
# Copyright 2024 Tabs Data Inc.
#

import logging.config
import os
import tarfile
import tempfile
from timeit import default_timer as timer
from typing import Literal

from tabsdata._tabsserver.function.global_utils import (
    CURRENT_PLATFORM,
    convert_uri_to_path,
)
from tabsdata._utils.envs import is_env_enabled
from tabsdata._utils.temps import tabsdata_temp_folder

logger = logging.getLogger(__name__)


ABSOLUTE_LOCATION = os.path.dirname(os.path.abspath(__file__))
UNCOMPRESSED_FUNCTION_BUNDLE_FOLDER = "uncompressed_function_bundle"

FilterType = Literal[
    "data",
    "tar",
    "fully_trusted",
]


class TimeBlock:

    def __enter__(self):
        self.start = timer()
        return self

    def __exit__(self, *args, **kwargs):
        self.end = timer()

    def time_taken(self):
        return self.end - self.start


def extract_tarfile_to_folder(tarfile_uri, destination_folder):
    logger.info(f"Extracting {tarfile_uri} to {destination_folder}")
    tarfile_path = convert_uri_to_path(tarfile_uri)
    logger.info(f"URI '{tarfile_uri}' converted to path '{tarfile_path}'")
    try:
        with tarfile.open(tarfile_path, "r:gz") as tar:
            filter_mode: FilterType = (
                "tar" if is_env_enabled("TD_SYMLINK_POLARS_LIBS_PYTEST") else "data"
            )
            tar.extractall(destination_folder, filter=filter_mode)
    except FileNotFoundError as e:
        logger.error(
            f"Error extracting tarfile {tarfile_uri}, file does not exist: {e}"
        )
        raise e


def extract_bundle_folder(bin_folder, compressed_context_folder):
    time_block = TimeBlock()
    if bin_folder:
        context_folder = os.path.join(bin_folder, UNCOMPRESSED_FUNCTION_BUNDLE_FOLDER)
    else:
        temporary_directory = tempfile.TemporaryDirectory(dir=tabsdata_temp_folder())
        if CURRENT_PLATFORM.is_windows():
            # noinspection PyUnresolvedReferences
            import win32api

            logger.debug(f"Short temp path: '{temporary_directory}")
            temporary_directory = win32api.GetLongPathName(temporary_directory.name)
            logger.debug(f"Long temp Path: {temporary_directory}")
        else:
            temporary_directory = temporary_directory.name
        context_folder = os.path.join(
            temporary_directory, UNCOMPRESSED_FUNCTION_BUNDLE_FOLDER
        )
    with time_block:
        extract_tarfile_to_folder(
            tarfile_uri=compressed_context_folder,
            destination_folder=context_folder,
        )
    logger.info(
        f"Extracted {compressed_context_folder} to {context_folder}. Time"
        f" taken: {time_block.time_taken():.2f}s"
    )
    return context_folder
