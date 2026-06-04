#
# Copyright 2026 Tabs Data Inc.
#

"""Env-var helpers."""

from __future__ import annotations

import os

from tabsdatak.common._constants import TRUE_VALUES


def is_env_enabled(env: str) -> bool:
    """``True`` when the ``env`` env var resolves to a truthy value
    (``1`` / ``true`` / ``yes`` / ``y`` / ``on``, case-insensitive)."""
    return os.getenv(env, "False").lower() in TRUE_VALUES
