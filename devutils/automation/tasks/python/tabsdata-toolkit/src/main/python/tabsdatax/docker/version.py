#
# Copyright 2026 Tabs Data Inc.
#

import tomllib
from pathlib import Path

from tabsdatax.docker.constants import (
    CARGO_FILE,
    GIT_FOLDER,
    PACKAGE_KEY,
    ROOT_FILE,
    VERSION_KEY,
    WORKSPACE_KEY,
)


def find_root_folder() -> Path:
    for folder in Path(__file__).parents:
        if (folder / GIT_FOLDER).is_dir() or (folder / ROOT_FILE).is_file():
            return folder
    raise FileNotFoundError("Current folder not inside a tabsdata project folder")


def project_version() -> str:
    cargo_toml = find_root_folder() / CARGO_FILE
    with cargo_toml.open("rb") as handle:
        data = tomllib.load(handle)
    return data[WORKSPACE_KEY][PACKAGE_KEY][VERSION_KEY]
