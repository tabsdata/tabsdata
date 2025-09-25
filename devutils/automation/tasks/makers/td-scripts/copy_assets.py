#
# Copyright 2025 Tabs Data Inc.
#

import importlib
import importlib.util
import os
import shutil
from pathlib import Path
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


# noinspection DuplicatedCode
def copy_assets():  # noqa: C901
    require_third_party = os.getenv("REQUIRE_THIRD_PARTY", "False").lower() in (
        "1",
        "true",
        "yes",
        "y",
        "on",
    )
    require_pydoc_csv = os.getenv("REQUIRE_PYDOC_CSV", "False").lower() in (
        "1",
        "true",
        "yes",
        "y",
        "on",
    )
    logger.debug(f"✅ Current path in copy assets is {Path.cwd()}")
    # noinspection DuplicatedCode
    project_tabsdata_root_folder = os.getenv("PROJECT_TABSDATA_ROOT_FOLDER")
    if not project_tabsdata_root_folder:
        raise ValueError(
            "The environment variable PROJECT_TABSDATA_ROOT_FOLDER.csv is missing."
        )
    project_tabsdata_root_folder = Path(project_tabsdata_root_folder).resolve()
    project_tabsdata_agent_root_folder = os.path.join(
        project_tabsdata_root_folder,
        "..",
        "tabsdata-ag",
    )
    project_tabsdata_agent_root_folder = Path(
        project_tabsdata_agent_root_folder
    ).resolve()

    tabsdata_assets_folder = os.path.join(
        project_tabsdata_root_folder,
        "client",
        "td-sdk",
        "tabsdata",
        "assets",
    )
    tabsdata_agent_assets_folder = os.path.join(
        project_tabsdata_agent_root_folder,
        "tabsdata_agent",
        "assets",
    )

    tabsdata_variant_assets_folder = os.path.join(
        project_tabsdata_root_folder,
        "variant",
        "assets",
    )
    tabsdata_agent_variant_assets_folder = os.path.join(
        project_tabsdata_agent_root_folder,
        "variant",
        "assets",
    )

    logger.debug(
        f"✏️ 1 Copying contents of {tabsdata_variant_assets_folder} to"
        f" {tabsdata_assets_folder}"
    )
    if (
        not os.path.exists(
            os.path.join(
                tabsdata_variant_assets_folder,
                "manifest",
                "THIRD-PARTY",
            )
        )
        and require_third_party
    ):
        raise FileNotFoundError(
            f"The THIRD-PARTY file is missing in {tabsdata_assets_folder}."
        )
    shutil.copytree(
        tabsdata_variant_assets_folder,
        tabsdata_assets_folder,
        dirs_exist_ok=True,
        symlinks=False,
        ignore=shutil.ignore_patterns(".gitignore"),
    )

    if os.path.exists(tabsdata_agent_assets_folder):
        logger.debug(
            f"✏️ 2 Copying contents of {tabsdata_agent_variant_assets_folder} to"
            f" {tabsdata_agent_assets_folder}"
        )
        if (
            not os.path.exists(
                os.path.join(
                    tabsdata_agent_variant_assets_folder,
                    "manifest",
                    "THIRD-PARTY",
                )
            )
            and require_third_party
        ):
            raise FileNotFoundError(
                f"The THIRD-PARTY file is missing in {tabsdata_agent_assets_folder}."
            )
        shutil.copytree(
            tabsdata_agent_variant_assets_folder,
            tabsdata_agent_assets_folder,
            dirs_exist_ok=True,
            symlinks=False,
            ignore=shutil.ignore_patterns(".gitignore"),
        )

    tabsdata_pydoc_csv_source = Path(
        os.path.join(
            project_tabsdata_root_folder,
            "target",
            "pydoc",
            "PYDOC.csv",
        )
    )
    if not tabsdata_pydoc_csv_source.exists() and require_pydoc_csv:
        raise FileNotFoundError(
            f"The PYDOC.csv file is missing in {tabsdata_pydoc_csv_source}."
        )
    tabsdata_pydoc_csv_target = Path(
        os.path.join(tabsdata_variant_assets_folder, "manifest")
    )
    logger.debug(
        f"✏️ 3 Copying contents of {tabsdata_pydoc_csv_source} to"
        f" {tabsdata_pydoc_csv_target}"
    )
    if tabsdata_pydoc_csv_source.exists():
        tabsdata_pydoc_csv_target.mkdir(
            parents=True,
            exist_ok=True,
        )
        shutil.copy(
            tabsdata_pydoc_csv_source,
            tabsdata_pydoc_csv_target,
            follow_symlinks=True,
        )
    tabsdata_pydoc_csv_target = Path(
        os.path.join(
            tabsdata_assets_folder,
            "manifest",
        )
    )
    logger.debug(
        f"✏️ 4 Copying contents of {tabsdata_pydoc_csv_source} to"
        f" {tabsdata_pydoc_csv_target}"
    )
    if tabsdata_pydoc_csv_source.exists():
        tabsdata_pydoc_csv_target.mkdir(
            parents=True,
            exist_ok=True,
        )
        shutil.copy(
            tabsdata_pydoc_csv_source,
            tabsdata_pydoc_csv_target,
            follow_symlinks=True,
        )

    if os.path.exists(tabsdata_agent_assets_folder):
        tabsdata_agent_pydoc_csv_source = Path(
            os.path.join(
                project_tabsdata_agent_root_folder,
                "target",
                "pydoc",
                "PYDOC.csv",
            )
        )
        if not tabsdata_agent_pydoc_csv_source.exists() and require_pydoc_csv:
            raise FileNotFoundError(
                f"The PYDOC.csv file is missing in {tabsdata_agent_pydoc_csv_source}."
            )
        tabsdata_agent_pydoc_csv_target = Path(
            os.path.join(
                tabsdata_agent_variant_assets_folder,
                "manifest",
            )
        )
        logger.debug(
            f"✏️ 5 Copying contents of {tabsdata_agent_pydoc_csv_source} to"
            f" {tabsdata_agent_pydoc_csv_target}"
        )
        if tabsdata_agent_pydoc_csv_source.exists():
            tabsdata_agent_pydoc_csv_target.mkdir(
                parents=True,
                exist_ok=True,
            )
            shutil.copy(
                tabsdata_agent_pydoc_csv_source,
                tabsdata_agent_pydoc_csv_target,
                follow_symlinks=True,
            )
        tabsdata_agent_pydoc_csv_target = Path(
            os.path.join(
                tabsdata_agent_assets_folder,
                "manifest",
            )
        )
        logger.debug(
            f"✏️ 6 Copying contents of {tabsdata_agent_pydoc_csv_source} to"
            f" {tabsdata_agent_pydoc_csv_target}"
        )
        if tabsdata_agent_pydoc_csv_source.exists():
            tabsdata_agent_pydoc_csv_target.mkdir(
                parents=True,
                exist_ok=True,
            )
            shutil.copy(
                tabsdata_agent_pydoc_csv_source,
                tabsdata_agent_pydoc_csv_target,
                follow_symlinks=True,
            )


copy_assets()
