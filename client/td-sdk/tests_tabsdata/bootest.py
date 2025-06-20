#
# Copyright 2024 Tabs Data Inc.
#

"""Helper functions that run at early initialization. That is, before project packages &
modules are loaded.
"""
import logging
import os
import sys
from pathlib import Path

TRACE = logging.DEBUG - 1


def trace(msg, *args, **kwargs):
    logging.log(TRACE, msg, *args, **kwargs)


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

HOME_FOLDER_SYMBOL = "~"
TABSDATA_FOLDER_NAME = ".tabsdata"
TABSDATA_ROOT_FOLDER_NAME = ".root"
TARGET_FOLDER_NAME = "target"
TDLOCAL_FOLDER_NAME = "tdlocal"

TESTING_RESOURCES_PATH = os.path.join(
    os.path.dirname(__file__),
    "testing_resources",
)

TRUE_VALUES = {"1", "true", "yes", "y", "on"}


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


# Add different paths to sys.path to avoid issues with imports
def enrich_sys_path():
    root = ROOT_FOLDER

    sys.path.append(
        os.path.abspath(
            os.path.join(
                TESTING_RESOURCES_PATH,
                "test_folder_no_init_file",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                TESTING_RESOURCES_PATH,
                "test_input_plugin",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                TESTING_RESOURCES_PATH,
                "test_input_plugin_initial_values",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                TESTING_RESOURCES_PATH,
                "test_input_plugin_from_pypi",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                TESTING_RESOURCES_PATH,
                "test_output_plugin",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                TESTING_RESOURCES_PATH,
                "test_output_plugin_with_none",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                TESTING_RESOURCES_PATH,
                "test_output_plugin_multiple_outputs",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                TESTING_RESOURCES_PATH,
                "test_output_plugin_multiple_with_none",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                root,
                "client",
                "td-sdk",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                root,
                "client",
                "td-lib",
                "ta_features",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                root,
                "client",
                "td-lib",
                "ta_tableframe",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                root,
                "extensions",
                "python",
                "td-lib",
                "te_tableframe",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                root,
                "target",
                "python",
                "pytest",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                root,
                "connectors",
                "python",
                "tabsdata_databricks",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                root,
                "connectors",
                "python",
                "tabsdata_mongodb",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                root,
                "connectors",
                "python",
                "tabsdata_salesforce",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                root,
                "connectors",
                "python",
                "tabsdata_snowflake",
            )
        ),
    )

    visited_entries = set()
    shiny_entries = []
    for path in sys.path:
        if path not in visited_entries:
            visited_entries.add(path)
            shiny_entries.append(path)
    sys.path[:] = shiny_entries

    logger.debug("")
    logger.debug("Using sys.path entries for td-sdk tests...:")
    for path in sys.path:
        logger.debug(f"   - {path}")
    logger.debug("")


def check_assets():
    require_third_party = (
        os.getenv("REQUIRE_THIRD_PARTY", "False").lower() in TRUE_VALUES
    )
    variant_assets_folder = os.path.join("..", "..", "variant", "assets")
    client_assets_folder = os.path.join("tabsdata", "assets")
    if (
        not os.path.exists(
            os.path.join(variant_assets_folder, "manifest", "THIRD-PARTY")
        )
        and require_third_party
    ):
        raise FileNotFoundError(
            f"The THIRD-PARTY file is missing in {client_assets_folder}."
        )


# Meant to be used only to expose python packages to tdserver when running pytest tests.
if __name__ == "__main__":
    enrich_sys_path()
    check_assets()
    sys_path = os.pathsep.join(f'"{p}"' if " " in p else p for p in sys.path)
    sys.stdout.write(f"{sys_path}")
