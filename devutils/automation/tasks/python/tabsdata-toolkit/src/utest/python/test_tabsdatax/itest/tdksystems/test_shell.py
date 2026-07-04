"""Tests for tabsdatax.itest.tdksystems.shell — pure-Python helpers (no docker)."""
from __future__ import annotations

import string

import pytest

from tabsdatax.itest.tdksystems.errors import DatagenError
from tabsdatax.itest.tdksystems.shell import (
    env_optional, env_required, find_free_port, gen_password, validate_name,
)


# ---- gen_password ----------------------------------------------------------

def test_gen_password_default_length_is_24():
    assert len(gen_password()) == 24


def test_gen_password_satisfies_complexity_classes():
    pw = gen_password()
    assert any(c in string.ascii_uppercase for c in pw)
    assert any(c in string.ascii_lowercase for c in pw)
    assert any(c in string.digits for c in pw)


def test_gen_password_only_alphanumeric():
    pw = gen_password()
    allowed = set(string.ascii_letters + string.digits)
    assert all(c in allowed for c in pw)


def test_gen_password_each_call_unique():
    assert gen_password() != gen_password()


# ---- find_free_port --------------------------------------------------------

def test_find_free_port_returns_a_valid_port():
    p = find_free_port()
    assert 1 <= p <= 65535


def test_find_free_port_yields_different_ports_under_load():
    ports = {find_free_port() for _ in range(5)}
    # We can't guarantee all unique (os may reuse), but the picker should
    # not be a constant.
    assert len(ports) >= 2


# ---- env_required / env_optional ------------------------------------------

def test_env_required_raises_when_missing(monkeypatch):
    monkeypatch.delenv("DATAGEN_TEST_X", raising=False)
    with pytest.raises(DatagenError, match="DATAGEN_TEST_X"):
        env_required("DATAGEN_TEST_X")


def test_env_required_returns_value(monkeypatch):
    monkeypatch.setenv("DATAGEN_TEST_X", "hello")
    assert env_required("DATAGEN_TEST_X") == "hello"


def test_env_required_treats_empty_as_missing(monkeypatch):
    monkeypatch.setenv("DATAGEN_TEST_X", "")
    with pytest.raises(DatagenError):
        env_required("DATAGEN_TEST_X")


def test_env_optional_default_when_missing(monkeypatch):
    monkeypatch.delenv("DATAGEN_TEST_Y", raising=False)
    assert env_optional("DATAGEN_TEST_Y", "fallback") == "fallback"


# ---- validate_name --------------------------------------------------------

@pytest.mark.parametrize("name", ["foo", "FOO_bar", "f1_2_3", "_lead", "X9"])
def test_validate_name_accepts_alphanumeric_underscore(name):
    validate_name(name)  # should not raise


@pytest.mark.parametrize("name", ["", "foo bar", "foo-bar", "foo/bar", "foo.bar"])
def test_validate_name_rejects_invalid(name):
    with pytest.raises(DatagenError):
        validate_name(name)


def test_validate_name_uses_label_in_error():
    with pytest.raises(DatagenError, match="schema"):
        validate_name("bad name", label="schema")
