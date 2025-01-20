#
# Copyright 2024 Tabs Data Inc.
#

import logging
import os
from logging import BASIC_FORMAT, INFO


def env_project_root_folder():
    logging.basicConfig(level=INFO, format=BASIC_FORMAT)
    current_folder = os.getcwd()
    logging.info(f"Current project folder is: {current_folder}")
    while True:
        if os.path.isdir(os.path.join(current_folder, ".git")):
            logging.info(f"Root project folder is: {current_folder}")
            os.environ["ROOT_PROJECT_FOLDER"] = current_folder
            return
        else:
            parent_folder = os.path.abspath(os.path.join(current_folder, os.pardir))
            if current_folder == parent_folder:
                raise FileNotFoundError("Current folder not inside a Git repository")
            current_folder = parent_folder


env_project_root_folder()
