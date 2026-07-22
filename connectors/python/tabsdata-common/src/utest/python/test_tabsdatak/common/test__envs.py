#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

# noinspection PyProtectedMember
from tabsdatak.common._envs import is_env_enabled


@pytest.mark.parametrize("value", ["1", "true", "True", "TRUE", "yes", "y", "on", "ON"])
def test_is_env_enabled_truthy_values(monkeypatch, value):
    monkeypatch.setenv("X", value)
    assert is_env_enabled("X") is True


@pytest.mark.parametrize(
    "value",
    ["0", "false", "False", "FALSE", "no", "n", "off", "", "anything-else"],
)
def test_is_env_enabled_falsy_or_unknown_values(monkeypatch, value):
    monkeypatch.setenv("X", value)
    assert is_env_enabled("X") is False


def test_is_env_enabled_unset_var_is_false(monkeypatch):
    monkeypatch.delenv("X", raising=False)
    assert is_env_enabled("X") is False
