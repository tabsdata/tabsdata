#
# Copyright 2026 Tabsdata Inc.
#

"""Shared test helpers for connector unit tests.

Lives in sql-common's utest tree so every SQL connector test can
import the same minimal duck-typed ctx without redeclaring it.
"""

from __future__ import annotations

from pathlib import Path


class CtxStub:
    """Duck-typed SrcPluginCtx / DestContext for unit tests.

    Exposes ``work_dir`` plus a minimal in-memory scratch store so
    tests for the ctx-vs-Src-field fallback (see
    :func:`tabsdatak.conn.common.sql._engine.initial_values.resolve_initial_values`)
    can drive ``get_attr`` / ``set_attr`` without instantiating
    a real :class:`tabsdatak.spi.SrcPluginCtx`.
    """

    def __init__(self, work: Path, scratch: dict | None = None) -> None:
        self._work = work
        self._scratch: dict = dict(scratch) if scratch else {}

    @property
    def work_dir(self) -> Path:
        return self._work

    def get_attr(self, key):
        return self._scratch.get(key)

    def set_attr(self, key, val) -> None:
        self._scratch[key] = val
