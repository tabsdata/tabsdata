#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os

from tabsdatak.server._tdrunner.function._lib.errors import CommandErrorCode

ENVIRONMENT_PREFIX = "env:"
# Escapes a literal value that would otherwise read as an env: reference.
VALUE_PREFIX = "val:"


def resolve_value(argument: str) -> str:
    if argument.startswith(VALUE_PREFIX):
        return argument[len(VALUE_PREFIX) :]
    if argument.startswith(ENVIRONMENT_PREFIX):
        variable = argument[len(ENVIRONMENT_PREFIX) :]
        try:
            return os.environ[variable]
        except KeyError as error:
            raise CommandErrorCode.CMD_2.exception(cause=error, var=variable)
    return argument


def resolve_flag(argument: str) -> bool:
    value = resolve_value(argument)
    lower = value.lower()
    if lower == "true":
        return True
    if lower == "false":
        return False
    raise CommandErrorCode.CMD_1.exception(
        detail=(
            f"Flag value must be 'true' or 'false' (case insensitive), got {value!r}"
        ),
    )
