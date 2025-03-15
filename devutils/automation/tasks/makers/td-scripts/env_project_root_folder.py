#
# Copyright 2024 Tabs Data Inc.
#

import importlib
import importlib.util
import logging
import os
from logging import BASIC_FORMAT, INFO
from types import ModuleType


# noinspection DuplicatedCode
def load(module_name) -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        module_name,
        os.path.join(
            os.getenv("MAKE_LIBRARIES_PATH"),
            f"{module_name}.py",
        ),
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


logger = load("log").get_logger()


def env_project_root_folder():
    logging.basicConfig(level=INFO, format=BASIC_FORMAT)
    current_folder = os.getcwd()
    logging.info(f"Current project folder is: {current_folder}")
    while True:
        if os.path.isdir(os.path.join(current_folder, ".git")):
            logging.info(f"Root project folder is: {current_folder}")
            os.environ["ROOT_PROJECT_FOLDER"] = current_folder
            return
        elif os.path.isfile(os.path.join(current_folder, ".root")):
            logging.info(f"Root project folder is: {current_folder}")
            os.environ["ROOT_PROJECT_FOLDER"] = current_folder
            return
        else:
            parent_folder = os.path.abspath(os.path.join(current_folder, os.pardir))
            if current_folder == parent_folder:
                raise FileNotFoundError(
                    "Current folder not inside a Git repository or "
                    "owned by a .root file"
                )
            current_folder = parent_folder


env_project_root_folder()
