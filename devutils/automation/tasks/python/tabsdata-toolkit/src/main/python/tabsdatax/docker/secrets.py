#
# Copyright 2026 Tabsdata Inc.
#

import logging
import os
import sys

from tabsdatax.docker.constants import SECRETS_FILE_NAME
from tabsdatax.docker.resources.images import image_resources

log = logging.getLogger(__name__)


def secrets_keys(image: str) -> list[str]:
    secrets_file = image_resources(image).joinpath(SECRETS_FILE_NAME)
    if not secrets_file.is_file():
        return []
    names = []
    for line in secrets_file.read_text().splitlines():
        name = line.strip()
        if name and not name.startswith("#"):
            names.append(name)
    return names


def assert_secrets(image: str) -> None:
    missing = [name for name in secrets_keys(image) if name not in os.environ]
    if not missing:
        return
    listing = "\n  ".join(missing)
    log.error(
        f"Missing required environment variables for image '{image}':\n  {listing}"
    )
    sys.exit(1)


def secrets_map(image: str) -> dict[str, str]:
    return {name: os.environ[name] for name in secrets_keys(image)}


def secrets_map_present(image: str) -> dict[str, str]:
    return {
        name: os.environ[name] for name in secrets_keys(image) if name in os.environ
    }
