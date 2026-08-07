#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
    TabsdataException,
)


class GrokException(TabsdataException):
    """Base class for grok errors.

    :categories: error
    """


class GrokInitException(GrokException):
    """Construction of a grok schema object failed validation.

    :categories: error
    """


class GrokErrorCode(ErrorCode):
    """Error codes for grok parsing.

    :categories: error

    Raise via `GrokErrorCode.GROK_N.exception`.
    """

    GROK_1 = ErrorDef(
        GrokInitException,
        "ColumnDefinition validation failed",
    )
