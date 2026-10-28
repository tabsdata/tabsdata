#
# Copyright 2026 Tabsdata Inc.
#

"""Error model for the prep-localize-state command (exit codes 10..)."""

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._lib.errors import (
    BadRequestError as _BadRequestError,
    ManifestWriteFailedError as _ManifestWriteFailedError,
)


class BadRequestError(_BadRequestError):
    exit_code = 10


class InputsManifestWriteFailedError(_ManifestWriteFailedError):
    """``--inputs-manifest`` could not be written."""

    exit_code = 13


class PrepLocalizeStateErrorCode(ErrorCode):
    PLS_1 = ErrorDef(
        BadRequestError,
        "unreadable request.yaml {path}: {detail}",
    )
    # ``Request.load``.

    PLS_4 = ErrorDef(
        InputsManifestWriteFailedError,
        "failed to write {path}: {detail}",
    )
    # ``InputManifest.write`` ``OSError``.
