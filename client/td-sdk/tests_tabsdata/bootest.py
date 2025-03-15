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

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

TESTING_RESOURCES_PATH = os.path.join(
    os.path.dirname(__file__),
    "testing_resources",
)


def root_folder() -> str:
    current_folder = os.path.dirname(__file__)
    logging.info(f"Current conftest folder is: {current_folder}")
    while True:
        git_folder = Path(os.path.join(current_folder, ".git"))
        root_file = Path(os.path.join(current_folder, ".root"))
        git_folder_exists = git_folder.exists() and os.path.isdir(git_folder)
        root_file_exists = root_file.exists() and root_file.is_file()
        if git_folder_exists or root_file_exists:
            logging.info(f"Root project folder for conftest is: {current_folder}")
            return current_folder
        else:
            parent_folder = os.path.abspath(os.path.join(current_folder, os.pardir))
            if current_folder == parent_folder:
                raise FileNotFoundError(
                    "Current folder not inside a Git repository or "
                    "owned by a .root file"
                )
            current_folder = parent_folder


# Add different paths to sys.path to avoid issues with imports
def enrich_sys_path():
    root = root_folder()

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
                "tabsdata",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                root,
                "client",
                "td-sdk",
                "tabsserver",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                root,
                "client",
                "td-lib",
                "td_features",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                root,
                "client",
                "td-lib",
                "ta_interceptor",
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
                "td_interceptor",
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
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                root,
                "client",
                "td-lib",
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
                "target",
                "python",
                "pytest",
            )
        ),
    )

    logger.info("")
    logger.info("Using sys.path entries for td-sdk tests...:")
    for path in sys.path:
        logger.info(f"   - {path}")
    logger.info("")


def check_assets():
    require_third_party = os.getenv("REQUIRE_THIRD_PARTY", "False").lower() in (
        "1",
        "true",
        "yes",
        "y",
        "on",
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
