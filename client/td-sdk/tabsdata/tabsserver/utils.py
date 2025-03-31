#
# Copyright 2024 Tabs Data Inc.
#

import logging.config
import os
import tarfile
import tempfile
from timeit import default_timer as timer

from tabsdata.tabsserver.function.global_utils import (
    CURRENT_PLATFORM,
    convert_uri_to_path,
)

logger = logging.getLogger(__name__)


ABSOLUTE_LOCATION = os.path.dirname(os.path.abspath(__file__))
UNCOMPRESSED_FUNCTION_BUNDLE_FOLDER = "uncompressed_function_bundle"


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
            tar.extractall(destination_folder, filter="data")
    except FileNotFoundError as e:
        logger.error(
            f"Error extracting tarfile {tarfile_uri}, file does not exist: {e}"
        )
        raise e


def extract_context_folder(bin_folder, compressed_context_folder):
    time_block = TimeBlock()
    if bin_folder:
        context_folder = os.path.join(bin_folder, UNCOMPRESSED_FUNCTION_BUNDLE_FOLDER)
    else:
        temporary_directory = tempfile.TemporaryDirectory()
        if CURRENT_PLATFORM.is_windows():
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
