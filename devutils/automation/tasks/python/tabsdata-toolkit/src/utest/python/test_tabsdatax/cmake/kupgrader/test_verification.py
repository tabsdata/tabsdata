#
# Copyright 2026 Tabsdata Inc.
#

import pytest

from tabsdatax.cmake.kupgrader.common.verification import (
    TD_KUPGRADER_BINARY_ROOT_VARIABLE,
    UPGRADER_BINARY,
    UPGRADER_PROFILE,
    VerificationError,
    locate_binary,
)


def binary_in(root):
    folder = root / "target" / UPGRADER_PROFILE
    folder.mkdir(parents=True)
    binary = folder / UPGRADER_BINARY
    binary.write_text("")
    return binary


def test_binary_is_taken_from_the_root(tmp_path, monkeypatch):
    monkeypatch.delenv(TD_KUPGRADER_BINARY_ROOT_VARIABLE, raising=False)
    binary = binary_in(tmp_path / "os")
    assert locate_binary(tmp_path / "os") == binary


def test_binary_root_is_preferred_when_it_holds_the_binary(tmp_path, monkeypatch):
    binary_in(tmp_path / "os")
    preferred = binary_in(tmp_path / "ee")
    monkeypatch.setenv(TD_KUPGRADER_BINARY_ROOT_VARIABLE, str(tmp_path / "ee"))
    assert locate_binary(tmp_path / "os") == preferred


def test_root_is_the_fallback_when_the_binary_root_lacks_it(tmp_path, monkeypatch):
    binary = binary_in(tmp_path / "os")
    (tmp_path / "ee").mkdir()
    monkeypatch.setenv(TD_KUPGRADER_BINARY_ROOT_VARIABLE, str(tmp_path / "ee"))
    assert locate_binary(tmp_path / "os") == binary


def test_missing_binary_names_every_place_looked_at(tmp_path, monkeypatch):
    monkeypatch.setenv(TD_KUPGRADER_BINARY_ROOT_VARIABLE, str(tmp_path / "ee"))
    with pytest.raises(VerificationError) as error:
        locate_binary(tmp_path / "os")
    assert str(tmp_path / "ee") in str(error.value)
    assert str(tmp_path / "os") in str(error.value)
