#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.server._tdrunner.function._lib.errors import (
    COMMAND_SPECIFIC_MIN_EXIT_CODE,
    EXIT_INVALID_OPTIONS,
    EXIT_UNDEFINED_ENV,
    CommandError,
    CommandSpecificError,
    InvalidOptionsError,
    UndefinedEnvError,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def test_constants_match_spec():
    assert EXIT_INVALID_OPTIONS == 1
    assert EXIT_UNDEFINED_ENV == 2
    assert COMMAND_SPECIFIC_MIN_EXIT_CODE == 10


def test_command_error_direct_instantiation_with_message():
    err = CommandError(7, "details")
    assert err.exit_code == 7
    assert str(err) == "[exit 7] details"


def test_command_error_str_without_message():
    err = CommandError(7)
    assert err.exit_code == 7
    assert str(err) == "[exit 7]"


def test_invalid_options_error_with_message():
    err = InvalidOptionsError("missing --request")
    assert isinstance(err, CommandError)
    assert err.exit_code == EXIT_INVALID_OPTIONS
    assert str(err) == "[exit 1] missing --request"


def test_invalid_options_error_default_message():
    err = InvalidOptionsError()
    assert err.exit_code == 1
    assert str(err) == "[exit 1]"


def test_undefined_env_error_with_message():
    err = UndefinedEnvError("FOO not set")
    assert isinstance(err, CommandError)
    assert err.exit_code == EXIT_UNDEFINED_ENV
    assert str(err) == "[exit 2] FOO not set"


def test_undefined_env_error_default_message():
    err = UndefinedEnvError()
    assert err.exit_code == 2
    assert str(err) == "[exit 2]"


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


def test_subclass_caught_as_base():
    with pytest.raises(CommandError) as exc_info:
        raise InvalidOptionsError("nope")
    assert exc_info.value.exit_code == 1
    assert isinstance(exc_info.value, InvalidOptionsError)
