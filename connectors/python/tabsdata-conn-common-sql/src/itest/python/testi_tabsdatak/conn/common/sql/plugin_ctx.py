#
# Copyright 2026 Tabsdata Inc.
#

"""Plugin contexts shared by the SQL connectors' integration tests, beyond the
inert stubs in :mod:`tabsdatax.itest.fixture.functions.files`.
"""

from __future__ import annotations

from pathlib import Path

from tabsdatax.itest.fixture.functions.files import PluginSrcCtx


class WatermarkSrcCtx(PluginSrcCtx):
    """A ``PluginSrcCtx`` that remembers what ``set_attr`` stored, standing in for
    the trx scratch the server carries between function runs -- what makes a
    second ``read_in`` resume from the advanced ``initial_values`` bind instead of
    the seed. The plain ``PluginSrcCtx`` is inert, so a second read there would be
    indistinguishable from a first one."""

    def __init__(self, work_dir: Path) -> None:
        super().__init__(work_dir)
        self._attrs: dict[str, object] = {}

    def get_attr(self, attr):
        return self._attrs.get(attr)

    def set_attr(self, attr, val):
        self._attrs[attr] = val
