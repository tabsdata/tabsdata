#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.common._error import CommonException
from tabsdatak.error import ErrorCode, ErrorDef


class MaterializerException(CommonException): ...


class MaterializerSecretException(MaterializerException): ...


class MaterializerErrorCode(ErrorCode):
    MATERIALIZER_1 = ErrorDef(
        MaterializerSecretException,
        "Secret key path {path!r} has an empty segment",
    )
    MATERIALIZER_2 = ErrorDef(
        MaterializerSecretException,
        "Environment variable {variable!r} for secret at path {path!r} is not set",
    )
