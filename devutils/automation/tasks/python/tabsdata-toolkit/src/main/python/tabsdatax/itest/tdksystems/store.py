#
# Copyright 2026 Tabsdata Inc.
#

"""Where tdx descriptors live on disk — resolve a system/domain *name* to its
descriptor file path.

`tdx-system` / `tdx-domain` address systems and domains by *name* (a plain
identifier), not by path. Each name is a file in a *store directory*: systems
under ``<backends-dir>/systems`` and domains under ``<backends-dir>/domains``,
where the *backends dir* is ``$TD_BACKENDS_DIR`` (else ``~/.tabsdatax``). An
explicit ``--sys-dir`` / ``--domain-dir`` overrides the store directly:

    system store:  --sys-dir     | $TD_BACKENDS_DIR/systems | ~/.tabsdatax/systems
    domain store:  --domain-dir  | $TD_BACKENDS_DIR/domains | ~/.tabsdatax/domains

This module is the single place that precedence lives, so the CLI and the test
fixtures resolve a name to a path identically. ``~`` is expanded; the directory
is not created here (the writer's ``mkdir`` does that on first use)."""

from __future__ import annotations

import os
from pathlib import Path

BACKENDS_DIR_ENV = "TD_BACKENDS_DIR"
DEFAULT_BACKENDS_DIR = "~/.tabsdatax"


def backends_dir() -> Path:
    """The backends base dir: $TD_BACKENDS_DIR, else ~/.tabsdatax (~ expanded).
    The system/domain stores are its ``systems`` / ``domains`` subdirectories."""
    return Path(os.environ.get(BACKENDS_DIR_ENV) or DEFAULT_BACKENDS_DIR).expanduser()


def system_dir(dir_opt: str | None = None) -> Path:
    """The system store directory: --sys-dir, else <backends-dir>/systems."""
    return Path(dir_opt).expanduser() if dir_opt else backends_dir() / "systems"


def domain_dir(dir_opt: str | None = None) -> Path:
    """The domain store directory: --domain-dir, else <backends-dir>/domains."""
    return Path(dir_opt).expanduser() if dir_opt else backends_dir() / "domains"


def instances_dir(dir_opt: str | None = None) -> Path:
    """The tabsdata instances store directory: <backends-dir>/instances."""
    return Path(dir_opt).expanduser() if dir_opt else backends_dir() / "instances"


def system_path(name: str, dir_opt: str | None = None) -> Path:
    """Descriptor file path for the system named `name`."""
    return system_dir(dir_opt) / name


def domain_path(name: str, dir_opt: str | None = None) -> Path:
    """Descriptor file path for the domain named `name`."""
    return domain_dir(dir_opt) / name
