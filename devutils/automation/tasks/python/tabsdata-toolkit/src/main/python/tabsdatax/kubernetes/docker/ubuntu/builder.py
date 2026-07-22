#
# Copyright 2026 Tabsdata Inc.
#

import logging
from pathlib import Path

from tabsdatax.kubernetes.docker.ubuntu.constants import (
    DEFAULT_REPOSITORIES,
)

log = logging.getLogger(__name__)


def repositories_home() -> Path:
    raise NotImplementedError


def builder_container_name(version: str) -> str:
    raise NotImplementedError


def running_builder(version: str | None = None) -> str | None:
    raise NotImplementedError


def start_builder(version: str, platform: str) -> str:
    raise NotImplementedError


def copy_repositories(
    container: str,
    repositories: tuple[str, ...] = DEFAULT_REPOSITORIES,
) -> None:
    raise NotImplementedError


def package(
    container: str,
    repository: str,
    no_compile: bool = False,
    extra: tuple[str, ...] = (),
) -> None:
    raise NotImplementedError


def export_venvs(
    container: str,
    destination: Path,
) -> list[Path]:
    raise NotImplementedError


def stop_builder(version: str | None = None) -> None:
    raise NotImplementedError
