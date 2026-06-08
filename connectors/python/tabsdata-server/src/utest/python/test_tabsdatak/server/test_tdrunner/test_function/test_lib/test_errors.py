#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.error import TabsdataException
from tabsdatak.server._tdrunner.function._lib.errors import (
    COMMAND_SPECIFIC_MIN_EXIT_CODE,
    EXIT_INVALID_OPTIONS,
    EXIT_UNDEFINED_ENV,
    CommandError,
    CommandErrorCode,
    CommandException,
    CommandInitException,
    CommandRuntimeException,
    CommandSpecificError,
    InvalidOptionsError,
    UndefinedEnvError,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


# --- exit-code constants ---


def test_constants_match_spec():
    assert EXIT_INVALID_OPTIONS == 1
    assert EXIT_UNDEFINED_ENV == 2
    assert COMMAND_SPECIFIC_MIN_EXIT_CODE == 10


# --- CommandException hierarchy ---


def test_invalid_options_error_inheritance():
    assert issubclass(InvalidOptionsError, CommandInitException)
    assert issubclass(InvalidOptionsError, CommandException)
    assert issubclass(InvalidOptionsError, TabsdataException)


def test_invalid_options_error_carries_exit_code_on_class():
    assert InvalidOptionsError.exit_code == EXIT_INVALID_OPTIONS


def test_undefined_env_error_inheritance():
    assert issubclass(UndefinedEnvError, CommandRuntimeException)
    assert issubclass(UndefinedEnvError, CommandException)
    assert issubclass(UndefinedEnvError, TabsdataException)


def test_undefined_env_error_carries_exit_code_on_class():
    assert UndefinedEnvError.exit_code == EXIT_UNDEFINED_ENV


# --- CommandErrorCode raise pattern ---


def test_invalid_options_via_enum_raises_typed_exception():
    with pytest.raises(InvalidOptionsError) as exc_info:
        raise CommandErrorCode.CMD_1.exception(detail="missing --request")
    assert exc_info.value.exit_code == EXIT_INVALID_OPTIONS
    assert "missing --request" in str(exc_info.value)


def test_undefined_env_via_enum_raises_typed_exception():
    with pytest.raises(UndefinedEnvError) as exc_info:
        raise CommandErrorCode.CMD_2.exception(var="FOO")
    assert exc_info.value.exit_code == EXIT_UNDEFINED_ENV
    assert "FOO" in str(exc_info.value)


def test_invalid_options_via_enum_propagates_cause():
    inner = RuntimeError("inner")
    try:
        raise CommandErrorCode.CMD_1.exception(cause=inner, detail="bad args")
    except InvalidOptionsError as exc:
        assert exc.__cause__ is inner
        assert exc.cause is inner


def test_invalid_options_via_enum_stores_extra():
    try:
        raise CommandErrorCode.CMD_1.exception(detail="bad args", surplus="extra")
    except InvalidOptionsError as exc:
        assert exc.extra == {"detail": "bad args", "surplus": "extra"}


def test_command_subclass_caught_as_base():
    with pytest.raises(CommandException) as exc_info:
        raise CommandErrorCode.CMD_1.exception(detail="nope")
    assert exc_info.value.exit_code == EXIT_INVALID_OPTIONS
    assert isinstance(exc_info.value, InvalidOptionsError)


# --- legacy shim layer ---


def test_command_error_direct_instantiation_with_message():
    err = CommandError(7, "details")
    assert err.exit_code == 7
    assert str(err) == "[exit 7] details"


def test_command_error_str_without_message():
    err = CommandError(7)
    assert err.exit_code == 7
    assert str(err) == "[exit 7]"


def test_command_specific_error_at_minimum():
    err = CommandSpecificError(COMMAND_SPECIFIC_MIN_EXIT_CODE, "boom")
    assert isinstance(err, CommandError)
    assert err.exit_code == COMMAND_SPECIFIC_MIN_EXIT_CODE
    assert str(err) == "[exit 10] boom"


def test_command_specific_error_above_minimum_default_message():
    err = CommandSpecificError(42)
    assert err.exit_code == 42
    assert str(err) == "[exit 42]"


@pytest.mark.parametrize("exit_code", [-1, 0, 1, 2, 3, 9])
def test_command_specific_error_rejects_below_minimum(exit_code):
    with pytest.raises(ValueError) as exc_info:
        CommandSpecificError(exit_code)
    assert str(COMMAND_SPECIFIC_MIN_EXIT_CODE) in str(exc_info.value)
    assert str(exit_code) in str(exc_info.value)
