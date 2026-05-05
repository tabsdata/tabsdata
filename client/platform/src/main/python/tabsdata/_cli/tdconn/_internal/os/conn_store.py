#
#  Copyright 2026. Tabs Data Inc.
#
"""Abstract persistence API for NamedConn JSON entries.

This layer is deliberately narrow — just a key/value-ish API over
connection names. The intent is to keep higher-level logic (validation,
refresh, orchestration) portable across backends (filesystem today,
potentially server-side tomorrow).
"""

from __future__ import annotations

import abc
from pathlib import Path
from typing import Any


class ConnStore(abc.ABC):
    """Abstract store of NamedConn JSON files, keyed by connection name."""

    @abc.abstractmethod
    def exists(self, name: str, debug: bool = False) -> bool:
        """Return True if a connection with this name is stored."""

    @abc.abstractmethod
    def put(self, name: str, source_path: Path, debug: bool = False) -> None:
        """Store the JSON at *source_path* under *name*, overwriting if present."""

    @abc.abstractmethod
    def get(
        self,
        name: str,
        resolved_envs: bool = False,
        debug: bool = False,
    ) -> dict[str, Any]:
        """Return the stored connection JSON as a dict. Fails if missing.

        When *resolved_envs* is ``True``, ``env:`` references inside
        ``conn_configs`` are replaced with the named environment
        variable's value before returning — same semantics as
        :class:`NamedConn._load` at function-run time. Keeping env
        resolution inside the store means callers (metadata
        extraction, verification, …) don't repeat the logic.
        """

    @abc.abstractmethod
    def delete(self, name: str, debug: bool = False) -> None:
        """Delete the connection named *name*. Fails if missing."""

    @abc.abstractmethod
    def names(self, debug: bool = False) -> list[str]:
        """Return the sorted list of stored connection names."""
