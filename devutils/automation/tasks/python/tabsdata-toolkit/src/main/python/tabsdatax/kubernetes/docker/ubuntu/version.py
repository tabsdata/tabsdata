#
# Copyright 2026 Tabsdata Inc.
#

from datetime import datetime, timezone
from pathlib import Path

from tabsdatax.kubernetes.docker.ubuntu.constants import (
    GIT_FOLDER,
    ROOT_FILE,
    VERSION_FILE,
    VERSION_TIMESTAMP_FORMAT,
)


def find_root_folder() -> Path:
    for folder in Path(__file__).resolve().parents:
        if (folder / GIT_FOLDER).is_dir() or (folder / ROOT_FILE).is_file():
            return folder
    raise FileNotFoundError("Current folder is not inside a tabsdata project folder.")


def project_version() -> str:
    return (find_root_folder() / VERSION_FILE).read_text().strip()


def timestamp_version() -> str:
    return datetime.now(timezone.utc).strftime(VERSION_TIMESTAMP_FORMAT)
