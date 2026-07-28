#
# Copyright 2026 Tabsdata Inc.
#

"""Plugin contexts for the file-connector integration tests, beyond the inert stubs in
:mod:`tabsdatax.itest.fixture.functions.files`.
"""

from __future__ import annotations

from pathlib import Path

from tabsdatax.itest.fixture.functions.files import PluginSrcCtx


class LastModSrcCtx(PluginSrcCtx):
    """A ``PluginSrcCtx`` that remembers what ``set_attr`` stored, standing in for
    the trx scratch the server carries between function runs -- what makes a second
    ``read_in`` resume from the per-slot ``file-lastmod`` cursor instead of the
    ``initial_last_modified`` baseline. The plain ``PluginSrcCtx`` is inert, so a
    second read there would be indistinguishable from a first one.

    A run gets a fresh ``work_dir`` but inherits the previous run's ``attrs``, the
    way consecutive trxs of one function do."""

    def __init__(self, work_dir: Path, attrs: dict[str, object] | None = None) -> None:
        super().__init__(work_dir)
        self.attrs: dict[str, object] = dict(attrs or {})

    def get_attr(self, attr):
        return self.attrs.get(attr)

    def set_attr(self, attr, val):
        self.attrs[attr] = val
