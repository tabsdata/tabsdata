#
# Copyright 2026 Tabsdata Inc.
#

"""Resolve a system/domain *name* to its descriptor file path.

tdx-system / tdx-domain address systems and domains by *name* (a plain
identifier), stored as a file in a *store directory*: systems under
``<backends-dir>/systems2`` and domains under ``<backends-dir>/domains2``, where
the backends dir is ``$TD_BACKENDS_DIR`` (else ``~/.tabsdatax``). The ``2``
suffix keeps this tool's descriptors separate from the v1 tools' ``systems`` /
``domains`` dirs so the two never collide while both exist.

An explicit ``--sys-dir`` / ``--domain-dir`` overrides the store directly. ``~``
is expanded; the directory is created by the writer's mkdir on first use.
"""

from __future__ import annotations

import os
from pathlib import Path

from tabsdatax.itest.tdksystems2 import constants


def backends_dir() -> Path:
    return Path(
        os.environ.get(constants.ENV_BACKENDS_DIR) or constants.DEFAULT_BACKENDS_DIR
    ).expanduser()


def system_dir(dir_opt: str | None = None) -> Path:
    return (
        Path(dir_opt).expanduser()
        if dir_opt
        else backends_dir() / constants.SYSTEMS_SUBDIR
    )


def domain_dir(dir_opt: str | None = None) -> Path:
    return (
        Path(dir_opt).expanduser()
        if dir_opt
        else backends_dir() / constants.DOMAINS_SUBDIR
    )


def system_path(name: str, dir_opt: str | None = None) -> Path:
    return system_dir(dir_opt) / name


def domain_path(name: str, dir_opt: str | None = None) -> Path:
    return domain_dir(dir_opt) / name
