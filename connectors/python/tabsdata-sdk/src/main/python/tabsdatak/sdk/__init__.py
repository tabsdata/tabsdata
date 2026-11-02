#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

__all__ = ["APIServerError"]


def __getattr__(name: str):
    # Resolved on first access: the module imports requests, which tdk would
    # otherwise pay for on every invocation.
    if name == "APIServerError":
        from tabsdatak.sdk._apiserver import APIServerError

        return APIServerError
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
