#
# Copyright 2026 Tabsdata Inc.
#

"""The document a failed ``jsonb`` invocation prints instead of its result,
for a person's scripts and a machine run alike."""

from __future__ import annotations

from collections.abc import Sequence

from tabsdatak.cli._errors import TdkError
from tabsdatak.cli.cli_utils import jsonb_document

SECRET_OPTIONS = ("--password", "--old-password", "--new-password", "--token")
SECRET_SHORT_OPTIONS = ("-p",)


def error_document(error: TdkError, command: Sequence[str]) -> dict:
    """``{"error": ...}``: the failure's type, code, message and hint, the
    ``command`` that failed, and the fields the failure adds, such as
    ``resume`` and ``state``."""
    envelope = {
        "type": error.type,
        "status": error.status,
        "code": error.code or f"tdk::{error.type}",
        "message": error.message,
        "hint": error.hint,
        "command": list(command),
        "retry_after": error.retry_after,
        "resume": None,
        "state": None,
    }
    envelope.update(error.extra)
    envelope["state"] = jsonb_document(envelope["state"])
    return {"error": envelope}


def redact(argv: Sequence[str]) -> list[str]:
    """``argv`` with the values of its secret options replaced by ``***``."""
    redacted = []
    hide_next = False
    for arg in argv:
        if hide_next:
            redacted.append("***")
            hide_next = False
            continue
        option, sep, _ = arg.partition("=")
        if option in SECRET_OPTIONS:
            if sep:
                redacted.append(f"{option}=***")
            else:
                redacted.append(arg)
                hide_next = True
            continue
        short = arg[:2]
        if short in SECRET_SHORT_OPTIONS:
            if len(arg) > 2:
                redacted.append(f"{short}***")
            else:
                redacted.append(arg)
                hide_next = True
            continue
        redacted.append(arg)
    return redacted
