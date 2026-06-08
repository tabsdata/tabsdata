#
# Copyright 2026 Tabs Data Inc.
#

"""Error model for the localize-fn-bundle command (exit codes 10..)."""

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._lib.errors import (
    CommandRuntimeException,
)


class BadRequestError(CommandRuntimeException):
    """``--request`` file missing, unreadable, or malformed."""

    exit_code = 10


class TransporterFailedError(CommandRuntimeException):
    """``td_transporter`` could not be launched or exited non-zero
    while fetching the bundle."""

    exit_code = 12


class ExtractFailedError(CommandRuntimeException):
    """Bundle archive could not be extracted (tar error, zstd error,
    missing archive)."""

    exit_code = 13


class LocalizeFnBundleErrorCode(ErrorCode):
    LFB_1 = ErrorDef(
        BadRequestError,
        "unreadable request.yaml {path}: {detail}",
    )
    # Raised by ``load_request_or_raise`` when the request file is
    # missing, has malformed YAML, or fails Pydantic validation.

    LFB_2 = ErrorDef(
        TransporterFailedError,
        "td_transporter not found: {detail}",
    )
    # Raised when the transporter subprocess can't be launched
    # (``TransporterNotFoundError``).

    LFB_3 = ErrorDef(
        TransporterFailedError,
        "td_transporter exited {rc} fetching bundle",
    )
    # Raised when ``td_transporter`` runs but exits non-zero. Same
    # exit code as LFB_2 because both are "the transporter step
    # failed".

    LFB_4 = ErrorDef(
        ExtractFailedError,
        "failed to extract bundle: {detail}",
    )
    # Raised when ``extract_bundle`` hits a ``tarfile.TarError``,
    # ``FileNotFoundError`` (archive absent), or ``zstandard.ZstdError``.
