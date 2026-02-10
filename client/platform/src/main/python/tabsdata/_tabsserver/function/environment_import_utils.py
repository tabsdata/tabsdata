#
# Copyright 2024 Tabs Data Inc.
#

import logging
import os
import sys

logger = logging.getLogger(__name__)


def update_syspath(folder_path: str):
    logger.debug(f"Old sys.path: {sys.path}")
    # Add root of code path to sys.path
    sys.path.insert(0, os.path.dirname(folder_path))
    add_folder_recursively_to_sys_path(folder_path)
    logger.debug(f"New sys.path: {sys.path}")


def add_folder_recursively_to_sys_path(folder_path: str):
    sys.path.insert(0, folder_path)  # Add folder path to sys.path
    for folder in os.listdir(folder_path):
        if os.path.isdir(os.path.join(folder_path, folder)):
            add_folder_recursively_to_sys_path(os.path.join(folder_path, folder))
