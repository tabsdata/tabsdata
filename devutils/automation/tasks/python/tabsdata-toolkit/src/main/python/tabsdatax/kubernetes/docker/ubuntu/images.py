#
# Copyright 2026 Tabs Data Inc.
#

import logging
from pathlib import Path

from tabsdatax.kubernetes.docker.ubuntu.constants import (
    DEFAULT_PLATFORM,
)

log = logging.getLogger(__name__)


def image_tag(image: str, version: str) -> str:
    raise NotImplementedError


def target_architecture(platform: str) -> str:
    raise NotImplementedError


def build_image(
    image: str,
    version: str,
    context_folder: Path,
    dockerfile: Path,
    platform: str = DEFAULT_PLATFORM,
    build_arguments: dict[str, str] | None = None,
    no_cache: bool = False,
    verbose: bool = False,
) -> str:
    raise NotImplementedError


def tag_latest(image: str, version: str) -> None:
    raise NotImplementedError


def image_exists(image: str, version: str) -> bool:
    raise NotImplementedError


def list_images(image: str | None = None) -> list[str]:
    raise NotImplementedError


def remove_image(image: str, version: str) -> None:
    raise NotImplementedError


def export_image(image: str, version: str, destination: Path) -> Path:
    raise NotImplementedError
