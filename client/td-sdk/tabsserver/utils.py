#
# Copyright 2024 Tabs Data Inc.
#

import logging.config
import os
import tarfile
from timeit import default_timer as timer

from tabsserver.function_execution.global_utils import convert_uri_to_path

logger = logging.getLogger(__name__)

ABSOLUTE_LOCATION = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DEVELOPMENT_LOCKS_LOCATION = os.path.join(
    os.path.dirname(ABSOLUTE_LOCATION),
    "local_dev",
    "environment_locks",
)


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
