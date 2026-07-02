#
# Copyright 2026 Tabs Data Inc.
#

"""Error model for the run-transporter command (exit codes 10..).

Same shape as :mod:`tabsdatak.server._tdruntime.python.error`: per-site
:class:`CommandRuntimeException` subclasses pin their own ``exit_code``,
:class:`RunTransporterErrorCode` binds each one to a templated message.
"""

from __future__ import annotations

from tabsdatak.error import ErrorCode, ErrorDef
from tabsdatak.server._tdrunner.function._lib.errors import (
    CommandRuntimeException,
)


class BinaryMissingError(CommandRuntimeException):
    """``td_transporter`` not on ``PATH`` or not executable."""

    exit_code = 10


class RunTransporterErrorCode(ErrorCode):
    RUN_TR_1 = ErrorDef(
        BinaryMissingError,
        "td_transporter not found: {detail}",
    )
    # Raised by ``main()`` when the transporter subprocess can't be
    # launched (``TransporterNotFoundError`` from ``_lib.transporter``).
