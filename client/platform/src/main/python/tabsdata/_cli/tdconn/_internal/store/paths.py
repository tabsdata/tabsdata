#
#  Copyright 2026. Tabs Data Inc.
#
"""Filesystem layout for a tabsdata instance.

Centralized so that every component (connection store, config loader,
future on-disk resources) resolves the same ``<instance_root>`` and
stays consistent when the layout changes.
"""

from __future__ import annotations

import os
from pathlib import Path

DEFAULT_INSTANCE = "tabsdata"

INSTANCE_PATH_ENV = "TD_PATH_INSTANCE"


def instance_root(instance: str | None = None) -> Path:
    """Return the on-disk root directory of a tabsdata instance.

    Precedence:
    1. Explicit *instance* name → ``~/.tabsdata/instances/<instance>/``
    2. ``TD_PATH_INSTANCE`` env var (set by the supervisor process
       that launches this agent)
    3. ``~/.tabsdata/instances/tabsdata/`` (default)
    """
    if instance is not None:
        return Path.home() / ".tabsdata" / "instances" / instance
    env = os.environ.get(INSTANCE_PATH_ENV, "").strip()
    if env:
        return Path(env)
    return Path.home() / ".tabsdata" / "instances" / DEFAULT_INSTANCE
