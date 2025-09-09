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


def copy_assets():
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
    client_assets_folder = os.path.join("client", "td-sdk", "tabsdata", "assets")

    variant_assets_folder = os.path.join("variant", "assets")
    logger.debug(
        f"✏️ Copying contents of {variant_assets_folder} to {client_assets_folder}"
    )
    if (
        not os.path.exists(
            os.path.join(variant_assets_folder, "manifest", "THIRD-PARTY")
        )
        and require_third_party
    ):
        raise FileNotFoundError(
            f"The THIRD-PARTY file is missing in {client_assets_folder}."
        )
    shutil.copytree(
        variant_assets_folder,
        client_assets_folder,
        dirs_exist_ok=True,
        symlinks=False,
    )

    pydoc_csv_source = Path(os.path.join("target", "pydoc", "PYDOC.csv"))
    if not pydoc_csv_source.exists() and require_pydoc_csv:
        raise FileNotFoundError(
            f"The PYDOC.csv file is missing in {client_assets_folder}."
        )

    pydoc_csv_target = Path(os.path.join(variant_assets_folder, "manifest"))
    logger.debug(f"✏️ Copying contents of {pydoc_csv_source} to {pydoc_csv_target}")
    if pydoc_csv_source.exists():
        pydoc_csv_target.mkdir(
            parents=True,
            exist_ok=True,
        )
        shutil.copy(
            pydoc_csv_source,
            pydoc_csv_target,
            follow_symlinks=True,
        )

    pydoc_csv_target = Path(os.path.join(client_assets_folder, "manifest"))
    logger.debug(f"✏️ Copying contents of {pydoc_csv_source} to {pydoc_csv_target}")
    if pydoc_csv_source.exists():
        pydoc_csv_target.mkdir(
            parents=True,
            exist_ok=True,
        )
        shutil.copy(
            pydoc_csv_source,
            pydoc_csv_target,
            follow_symlinks=True,
        )


copy_assets()
