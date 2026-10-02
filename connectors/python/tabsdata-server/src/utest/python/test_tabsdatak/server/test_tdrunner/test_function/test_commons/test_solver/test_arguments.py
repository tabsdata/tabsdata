#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.server._tdrunner.function._commons.solver.arguments import (
    resolve_flag,
    resolve_value,
)
from tabsdatak.server._tdrunner.function._lib.errors import (
    InvalidOptionsError,
    UndefinedEnvError,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


# --- resolve_value ---


def test_resolve_value_literal():
    assert resolve_value("hello") == "hello"


def test_resolve_value_empty_string():
    assert resolve_value("") == ""


def test_resolve_value_val_prefix_strips():
    assert resolve_value("val:hello") == "hello"


def test_resolve_value_val_prefix_strips_only_first():
    # `val:` forces the rest to be a literal even if it looks like `env:`
    assert resolve_value("val:env:foo") == "env:foo"


def test_resolve_value_val_prefix_empty_payload():
    assert resolve_value("val:") == ""


def test_resolve_value_env_resolves(monkeypatch):
    monkeypatch.setenv("TD_TEST_RESOLVE", "the-value")
    assert resolve_value("env:TD_TEST_RESOLVE") == "the-value"


def test_resolve_value_env_unset_raises(monkeypatch):
    monkeypatch.delenv("TD_TEST_RESOLVE_NOPE", raising=False)
    with pytest.raises(UndefinedEnvError) as exc_info:
        resolve_value("env:TD_TEST_RESOLVE_NOPE")
    assert "TD_TEST_RESOLVE_NOPE" in str(exc_info.value)
    assert exc_info.value.exit_code == 2


# --- resolve_flag ---


@pytest.mark.parametrize("arg", ["true", "TRUE", "True", "tRuE"])
def test_resolve_flag_true_case_insensitive(arg):
    assert resolve_flag(arg) is True


@pytest.mark.parametrize("arg", ["false", "FALSE", "False", "fAlSe"])
def test_resolve_flag_false_case_insensitive(arg):
    assert resolve_flag(arg) is False


def test_resolve_flag_via_val_prefix_true():
    assert resolve_flag("val:true") is True


def test_resolve_flag_via_val_prefix_false():
    assert resolve_flag("val:false") is False


def test_resolve_flag_via_env(monkeypatch):
    monkeypatch.setenv("TD_TEST_FLAG", "TRUE")
    assert resolve_flag("env:TD_TEST_FLAG") is True


def test_resolve_flag_via_env_unset(monkeypatch):
    monkeypatch.delenv("TD_TEST_NOFLAG", raising=False)
    with pytest.raises(UndefinedEnvError):
        resolve_flag("env:TD_TEST_NOFLAG")


@pytest.mark.parametrize("arg", ["yes", "no", "1", "0", "", "y", "n", "trueish"])
def test_resolve_flag_invalid_value(arg):
    with pytest.raises(InvalidOptionsError) as exc_info:
        resolve_flag(arg)
    assert exc_info.value.exit_code == 1


def test_resolve_flag_val_prefix_invalid_payload():
    # `val:env:true` -> literal "env:true" -> not a valid flag value
    with pytest.raises(InvalidOptionsError):
        resolve_flag("val:env:true")
