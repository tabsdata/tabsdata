#
# Copyright 2026 Tabs Data Inc.
#

import logging
from pathlib import Path

from tabsdatax.kubernetes.docker.ubuntu.constants import (
    APISERVER_PORT,
    DEFAULT_PLATFORM,
    VENV_MODE_COPY,
)

log = logging.getLogger(__name__)


def local_venvs_folder() -> Path:
    raise NotImplementedError


def build_runner(
    version: str,
    platform: str = DEFAULT_PLATFORM,
    no_cache: bool = False,
    verbose: bool = False,
) -> str:
    raise NotImplementedError


def start_runner(
    version: str,
    venv_mode: str = VENV_MODE_COPY,
    apiserver_port: int = APISERVER_PORT,
    command: tuple[str, ...] = (),
) -> None:
    raise NotImplementedError
