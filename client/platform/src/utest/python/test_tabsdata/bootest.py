#
# Copyright 2024 Tabs Data Inc.
#

"""Helper functions that run at early initialization. That is, before project packages &
modules are loaded.
"""

import logging
import os
import shutil
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

# fmt: off
TESTING_RESOURCES_PATH = str(
    Path(__file__).absolute()
    .parent  # test_tabsdata/
    .parent  # python/
    .parent  # utest/
    / "resources"
    / "test_tabsdata"
)
# fmt: on

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


def _clean_tdlocal_folder():
    if os.path.isdir(TDLOCAL_FOLDER):
        for path in Path(TDLOCAL_FOLDER).iterdir():
            if path.is_dir() and path.name.startswith("test_"):
                shutil.rmtree(path)


_clean_tdlocal_folder()


# Add different paths to sys.path to avoid issues with imports
def enrich_sys_path():
    root = ROOT_FOLDER

    def gather_connectors() -> list[str]:
        connectors_folders = [
            os.path.join(root, "connectors", "python"),
            # os.path.join(root, "connectors.ee", "python"),
        ]

        tabsdata_connectors: list[str] = []

        for connectors_folder in connectors_folders:
            if not os.path.isdir(connectors_folder):
                continue

            for entry in os.scandir(connectors_folder):
                if entry.is_dir():
                    connector_name_parts = entry.name.split("-", 1)
                    if (
                        len(connector_name_parts) != 2
                        or connector_name_parts[0] != "tabsdata"
                    ):
                        logger.debug(f"⛔️ Invalid connector folder name: {entry.name}")
                    else:
                        tabsdata_connectors.append(entry.name)
                        logger.debug(f"📦️ Inserting connector {entry}")
        return sorted(tabsdata_connectors)

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
                "platform",
                "src",
                "utest",
                "python",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                root,
                "client",
                "platform",
                "src",
                "utest",
                "resources",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                root,
                "client",
                "libraries",
                "ta_features",
                "src",
                "utest",
                "python",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                root,
                "client",
                "libraries",
                "ta_features",
                "src",
                "utest",
                "resources",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                root,
                "client",
                "libraries",
                "ta_tableframe",
                "src",
                "utest",
                "python",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                root,
                "client",
                "libraries",
                "ta_tableframe",
                "src",
                "utest",
                "resources",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                root,
                "extensions",
                "python",
                "libraries",
                "te_examples",
                "src",
                "utest",
                "python",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                root,
                "extensions",
                "python",
                "libraries",
                "te_examples",
                "src",
                "utest",
                "resources",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                root,
                "extensions",
                "python",
                "libraries",
                "te_tableframe",
                "src",
                "utest",
                "python",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                root,
                "extensions",
                "python",
                "libraries",
                "te_tableframe",
                "src",
                "utest",
                "resources",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                root,
                "extensions",
                "python",
                "libraries",
                "te_variant",
                "src",
                "utest",
                "python",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                root,
                "extensions",
                "python",
                "libraries",
                "te_variant",
                "src",
                "utest",
                "resources",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                root,
                "expansions",
                "polars",
                "modules",
                "ty-tableframe",
                "python",
                "src",
                "utest",
                "python",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                root,
                "expansions",
                "polars",
                "modules",
                "ty-tableframe",
                "python",
                "src",
                "utest",
                "resources",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                root,
                "expansions",
                "polars",
                "extensions",
                "tz-tableframe",
                "python",
                "src",
                "utest",
                "python",
            )
        ),
    )
    sys.path.append(
        os.path.abspath(
            os.path.join(
                root,
                "expansions",
                "polars",
                "extensions",
                "tz-tableframe",
                "python",
                "src",
                "utest",
                "resources",
            )
        ),
    )
    connectors = gather_connectors()
    for connector in connectors:
        sys.path.append(
            os.path.abspath(
                os.path.join(
                    root,
                    "connectors",
                    "python",
                    connector,
                    "src",
                    "utest",
                    "python",
                )
            ),
        )
        sys.path.append(
            os.path.abspath(
                os.path.join(
                    root,
                    "connectors",
                    "python",
                    connector,
                    "src",
                    "utest",
                    "resources",
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
    logger.debug("Using sys.path entries for platform tests...:")
    for path in sys.path:
        logger.debug(f"   - {path}")
    logger.debug("")


def check_assets():
    require_third_party = (
        os.getenv("REQUIRE_THIRD_PARTY", "False").lower() in TRUE_VALUES
    )
    # fmt: off
    variant_assets_folder = str(
        Path(__file__).absolute()
        .parent  # test_tabsdata/
        .parent  # python/
        .parent  # utest/
        .parent  # src/
        .parent  # platform/
        .parent  # client/
        .parent  # tabsdata-*/
        / "variant"
        / "assets"
    )
    client_assets_folder_tabsdata = str(
        Path(__file__).absolute()
        .parent  # test_tabsdata/
        .parent  # python/
        .parent  # utest/
        .parent  # src/
        .parent  # platform/
        / "tabsdata"
        / "assets"
    )
    client_assets_folder_tabsdatak = str(
        Path(__file__).absolute()
        .parent  # test_tabsdata/
        .parent  # python/
        .parent  # utest/
        .parent  # src/
        .parent  # platform/
        / "tabsdatak"
        / "assets"
    )
    # fmt: on
    if (
        not os.path.exists(
            os.path.join(variant_assets_folder, "manifest", "THIRD-PARTY")
        )
        and require_third_party
    ):
        raise FileNotFoundError(
            "The THIRD-PARTY file is missing in"
            f" {client_assets_folder_tabsdata} and"
            f" {client_assets_folder_tabsdatak}."
        )


# This code is intentionally duplicated in module
# tabsdata._tabsserver.function.execute_function_bootstrap
# as it is trying to fix a wrong sys path order on Windows that interferes with
# correct import of namespace packages for editable installations.
# Just importing the other implementation wouldn't work as sys path is broken.
def order_sys_path():
    src_main_python_fragment = os.path.join(
        "src",
        "main",
        "python",
    )
    tabsdata_fragment = os.path.join(
        "client",
        "platform",
        "src",
        "main",
        "python",
    )
    tabsdatax_fragment = os.path.join(
        "devutils",
        "automation",
        "tasks",
        "python",
        "tabsdata-toolkit",
        "src",
        "main",
        "python",
    )

    tabsdata_sys_paths = []
    non_tabsdata_sys_paths = []

    for path in sys.path:
        if path.endswith(src_main_python_fragment):
            tabsdata_sys_paths.append(path)
        else:
            non_tabsdata_sys_paths.append(path)

    tabsdata_paths = []
    other_paths = []

    for path in tabsdata_sys_paths:
        if path.endswith(tabsdatax_fragment):
            tabsdata_paths.append(path)

    for path in tabsdata_sys_paths:
        if path.endswith(tabsdata_fragment):
            tabsdata_paths.append(path)

    for path in tabsdata_sys_paths:
        if path.endswith(tabsdata_fragment) or path.endswith(tabsdatax_fragment):
            pass
        else:
            other_paths.append(path)

    other_paths.sort()

    ordered_tabsdata_paths = list(tabsdata_paths)
    ordered_tabsdata_paths.extend(other_paths)
    sys.path[:] = non_tabsdata_sys_paths + ordered_tabsdata_paths


# Meant to be used only to expose python packages to tdserver when running pytest tests.
if __name__ == "__main__":
    order_sys_path()
    enrich_sys_path()
    check_assets()
    sys_path = os.pathsep.join(f'"{p}"' if " " in p else p for p in sys.path)
    sys.stdout.write(f"{sys_path}")
