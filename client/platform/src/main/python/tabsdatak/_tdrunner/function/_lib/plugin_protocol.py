#
# Copyright 2026 Tabs Data Inc.
#

"""Minimal protocols for source and destination plugins.

Plugin classes are tabsdata-defined and live in the user's bundle. From
tabsdatak's perspective we only need to know that a loaded plugin has a
callable `_run` attribute — enough for `run-source-plugin` and
`run-destination-plugin` to invoke it.

Both protocols are `runtime_checkable` so `isinstance(obj, SourcePlugin)`
works for the post-cloudpickle-load type check.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class SourcePlugin(Protocol):
    """Anything with a callable `_run` may serve as a source plugin."""

    def _run(self, *args: object, **kwargs: object) -> object: ...


@runtime_checkable
class DestinationPlugin(Protocol):
    """Anything with a callable `_run` may serve as a destination plugin."""

    def _run(self, *args: object, **kwargs: object) -> object: ...
