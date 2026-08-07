#
# Copyright 2026 Tabsdata Inc.
#

"""What a command refuses to start without: the crypto key and the option values.

Both used to surface late or not at all — a store nobody could decrypt read as an
empty one, and a system whose connection comes from the environment reported its
missing variables one create at a time.
"""

from __future__ import annotations

import pytest

from tabsdatax.itest.tdksystems2 import crypto, tdx
from tabsdatax.itest.tdksystems2.errors import TdxError

_AZ_VARS = ("AZ__CONTAINER", "AZ__ACCOUNT_NAME", "AZ__ACCOUNT_KEY", "AZ__SYS_PATH")


def _sys(argv):
    return tdx._dispatch("tdx-system", argv)


@pytest.fixture
def no_key(monkeypatch):
    monkeypatch.delenv(crypto.ENV_VAR, raising=False)


@pytest.fixture
def no_az_env(monkeypatch):
    for var in _AZ_VARS:
        monkeypatch.delenv(var, raising=False)


@pytest.mark.parametrize("argv", [["list"], ["manifest"], ["info", "--sys", "s"]])
def test_a_store_reading_verb_demands_the_key(argv, stores, no_key):
    with pytest.raises(TdxError) as exc:
        _sys(argv)
    assert crypto.ENV_VAR in str(exc.value)


def test_an_empty_store_is_not_how_a_missing_key_looks(stores, no_key, capsys):
    # the regression: `list` walked the store, decrypted nothing and said so
    with pytest.raises(TdxError):
        _sys(["list"])
    assert "no systems" not in capsys.readouterr().out


def test_types_needs_no_key(stores, no_key):
    # it never reads a descriptor, so the key must not become a prerequisite
    assert _sys(["types"]) == 0


@pytest.mark.parametrize(
    "argv", [["list", "-h"], ["manifest", "-h"], ["clear-venvs", "-h"]]
)
def test_help_needs_no_key(argv, stores, no_key):
    with pytest.raises(SystemExit) as exc:
        _sys(argv)
    assert exc.value.code == 0


def test_manifest_refuses_a_descriptor_it_cannot_read(stores, monkeypatch, capsys):
    """A manifest feeds `create --manifest --sync`, which destroys what the
    manifest omits — so silently dropping an undecryptable system would arm a
    reconcile to delete it."""
    assert _sys(["create", "--type", "dev", "--sys", "d1"]) == 0
    capsys.readouterr()
    monkeypatch.setenv(crypto.ENV_VAR, "a-different-secret")
    with pytest.raises(TdxError) as exc:
        _sys(["manifest"])
    msg = str(exc.value)
    assert "d1" in msg and "could not be read" in msg
    assert "[]" not in capsys.readouterr().out  # never emits the empty manifest


def test_every_missing_option_is_reported_at_once(stores, no_az_env):
    with pytest.raises(TdxError) as exc:
        _sys(["create", "--type", "azureblob", "--sys", "ab"])
    msg = str(exc.value)
    for var in _AZ_VARS:
        assert f"${var}" in msg
    assert "--container VALUE" in msg  # and the flag that would supply it


def test_only_the_ones_actually_missing_are_reported(stores, no_az_env, monkeypatch):
    monkeypatch.setenv("AZ__CONTAINER", "c")
    monkeypatch.setenv("AZ__ACCOUNT_NAME", "n")
    with pytest.raises(TdxError) as exc:
        _sys(["create", "--type", "azureblob", "--sys", "ab"])
    msg = str(exc.value)
    assert "$AZ__ACCOUNT_KEY" in msg and "$AZ__SYS_PATH" in msg
    assert "$AZ__CONTAINER" not in msg and "$AZ__ACCOUNT_NAME" not in msg


def test_nothing_claims_to_be_creating_before_the_options_resolve(
    stores, no_az_env, capsys
):
    with pytest.raises(TdxError):
        _sys(["create", "--type", "azureblob", "--sys", "ab"])
    assert "creating system" not in capsys.readouterr().out
