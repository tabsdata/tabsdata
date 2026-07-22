#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.common._error import CommonException
from tabsdatak.error import ErrorCode, ErrorDef


class ResolverException(CommonException): ...


class ResolverEnvException(ResolverException): ...


class ResolverErrorCode(ErrorCode):
    RESOLVER_1 = ErrorDef(
        ResolverEnvException,
        "Environment variable {variable!r} is not set",
    )
